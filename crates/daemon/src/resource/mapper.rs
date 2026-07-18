//! The storage-facing resource facade.
//!
//! `ResourceMapper` maps between stored domain types and the public
//! `Resource` proto. It is the single entry point shared by the rpc handlers
//! (`crate::rpc::server`) and the resource anti-entropy task
//! (`crate::sync::resource`); the wire/domain converters live in
//! [`super::convert`].

use std::{
  collections::{BTreeMap, HashMap},
  sync::Arc,
};

use lycoris_core::ResourceScope;
use lycoris_proto::node::{Resource, ResourceKind, resource::Body};
use lycoris_storage::{
  AgentStorageError, MemoryEntry, PluginRecord, PluginStorageError, Storage, VersionedContentStore,
  VersionedStorage, WorkspaceRecord, WorkspaceStorageError,
};

use super::{
  convert::{
    VersionedKind, decode_kind, memory_to_resource, metadata_scope, node_to_resource,
    plugin_to_resource, resource_to_memory, resource_to_plugin, resource_to_rule,
    resource_to_skill, resource_to_workspace, session_to_resource, versioned_to_resource,
    workspace_to_resource,
  },
  error::MapperError,
};
use crate::{
  membership::{MembershipService, convert::register_to_proto},
  selector::matches_selector,
};

/// Maps between stored domain types and the public `Resource` proto.
#[derive(Debug, Clone)]
pub struct ResourceMapper {
  storage: Storage,
  service: Arc<MembershipService>,
}

/// Storage accessors and operation contexts that vary between the two
/// content-backed versioned kinds (skills and rules). Everything else about
/// the two kinds — listings, reads, conversion — is shared.
struct VersionedAccess<'a> {
  records: &'a dyn VersionedStorage,
  content: &'a dyn VersionedContentStore,
  get_context: &'static str,
  read_content_context: &'static str,
  list_context: &'static str,
  list_shared_context: &'static str,
}

fn versioned_access(storage: &Storage, kind: VersionedKind) -> VersionedAccess<'_> {
  let workspace = storage.workspace();
  match kind {
    VersionedKind::Skill => VersionedAccess {
      records: workspace.skills(),
      content: workspace.skill_content(),
      get_context: "failed to get skill",
      read_content_context: "failed to read skill content",
      list_context: "failed to list skills",
      list_shared_context: "failed to list shared skills",
    },
    VersionedKind::Rule => VersionedAccess {
      records: workspace.rules(),
      content: workspace.rule_content(),
      get_context: "failed to get rule",
      read_content_context: "failed to read rule content",
      list_context: "failed to list rules",
      list_shared_context: "failed to list shared rules",
    },
  }
}

/// Run the listing variant matching the optional scope filter: shared-only,
/// local-only, or unfiltered.
fn list_by_scope<T, E>(
  scope: Option<ResourceScope>, shared: impl FnOnce() -> Result<Vec<T>, E>,
  local: impl FnOnce() -> Result<Vec<T>, E>, all: impl FnOnce() -> Result<Vec<T>, E>,
) -> Result<Vec<T>, E> {
  match scope {
    Some(ResourceScope::ClusterShared) => shared(),
    Some(ResourceScope::NodeLocal) => local(),
    None => all(),
  }
}

/// Async variant of [`list_by_scope`] for stores whose listings are async.
async fn list_by_scope_async<T, E, Fut>(
  scope: Option<ResourceScope>, shared: impl FnOnce() -> Fut, local: impl FnOnce() -> Fut,
  all: impl FnOnce() -> Fut,
) -> Result<Vec<T>, E>
where
  Fut: std::future::Future<Output = Result<Vec<T>, E>>, {
  match scope {
    Some(ResourceScope::ClusterShared) => shared().await,
    Some(ResourceScope::NodeLocal) => local().await,
    None => all().await,
  }
}

/// Plugin records carry no user labels — plugin configuration lives in the
/// manifest (design section 4) — so label selectors match plugins only when
/// empty.
static EMPTY_LABELS: BTreeMap<String, String> = BTreeMap::new();

/// Filter stored records by the label selector and map them onto resources.
fn collect_matching<T, M>(
  records: Vec<T>, selector: &HashMap<String, String>, metadata: impl Fn(&T) -> &M,
  to_resource: impl Fn(T) -> Resource,
) -> Vec<Resource>
where
  for<'a> &'a M: IntoIterator<Item = (&'a String, &'a String)>, {
  records
    .into_iter()
    .filter(|record| matches_selector(metadata(record), selector))
    .map(to_resource)
    .collect()
}

impl ResourceMapper {
  pub fn new(storage: Storage, service: Arc<MembershipService>) -> Self {
    Self { storage, service }
  }

  pub async fn list(
    &self, kind: ResourceKind, selector: HashMap<String, String>, scope: Option<ResourceScope>,
  ) -> Result<Vec<Resource>, MapperError> {
    match kind {
      ResourceKind::Node => Ok(
        self
          .service
          .list_nodes(&selector)
          .await
          .into_iter()
          .map(|register| node_to_resource(register_to_proto(&register)))
          .collect(),
      ),
      ResourceKind::Session => {
        let sessions = self
          .storage
          .agent()
          .sessions()
          .list()
          .map_err(MapperError::agent("failed to list sessions"))?;
        Ok(collect_matching(
          sessions,
          &selector,
          |session| &session.metadata,
          session_to_resource,
        ))
      }
      ResourceKind::Memory => {
        let entries = self
          .list_memories(scope)
          .await
          .map_err(MapperError::agent("failed to list memories"))?;
        Ok(collect_matching(
          entries,
          &selector,
          |entry| &entry.metadata,
          |entry| memory_to_resource(entry, false),
        ))
      }
      ResourceKind::Skill => self.list_versioned(VersionedKind::Skill, &selector, scope),
      ResourceKind::Rule => self.list_versioned(VersionedKind::Rule, &selector, scope),
      ResourceKind::Plugin => {
        let plugins = self
          .list_plugins(scope)
          .map_err(MapperError::plugin("failed to list plugins"))?;
        Ok(collect_matching(
          plugins,
          &selector,
          |_| &EMPTY_LABELS,
          |record| plugin_to_resource(record, None),
        ))
      }
      ResourceKind::Workspace => {
        let workspaces = self
          .list_workspaces(scope)
          .map_err(MapperError::workspace("failed to list workspaces"))?;
        Ok(collect_matching(
          workspaces,
          &selector,
          |workspace| &workspace.metadata,
          workspace_to_resource,
        ))
      }
    }
  }

  pub async fn get(&self, kind: ResourceKind, id: &str) -> Result<Resource, MapperError> {
    match kind {
      ResourceKind::Node => self
        .service
        .list_nodes(&HashMap::new())
        .await
        .into_iter()
        .find(|register| register.node_id() == id)
        .map(|register| node_to_resource(register_to_proto(&register)))
        .ok_or_else(|| MapperError::NotFound(id.to_string())),
      ResourceKind::Session => self
        .storage
        .agent()
        .sessions()
        .get(id)
        .map_err(MapperError::agent("failed to get session"))?
        .map(session_to_resource)
        .ok_or_else(|| MapperError::NotFound(id.to_string())),
      ResourceKind::Memory => {
        let entry = self
          .storage
          .agent()
          .memory()
          .get(id)
          .await
          .map_err(MapperError::agent("failed to get memory"))?;
        entry
          .map(|entry| memory_to_resource(entry, true))
          .ok_or_else(|| MapperError::NotFound(id.to_string()))
      }
      ResourceKind::Skill => self.get_versioned(VersionedKind::Skill, id),
      ResourceKind::Rule => self.get_versioned(VersionedKind::Rule, id),
      ResourceKind::Plugin => {
        let record = self
          .storage
          .plugins()
          .get(id)
          .map_err(MapperError::plugin("failed to get plugin"))?
          .ok_or_else(|| MapperError::NotFound(id.to_string()))?;
        let artifact = self
          .storage
          .plugins()
          .blobs()
          .read(id)
          .map_err(MapperError::plugin("failed to read plugin artifact"))?;
        Ok(plugin_to_resource(record, artifact))
      }
      ResourceKind::Workspace => {
        let workspace = self
          .storage
          .workspace()
          .workspaces()
          .get(id)
          .map_err(MapperError::workspace("failed to get workspace"))?;
        workspace
          .map(workspace_to_resource)
          .ok_or_else(|| MapperError::NotFound(id.to_string()))
      }
    }
  }

  async fn list_memories(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    let memory = self.storage.agent().memory();
    list_by_scope_async(
      scope,
      || memory.list_shared(),
      || memory.list_local(),
      || memory.list(),
    )
    .await
  }

  fn list_workspaces(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError> {
    let workspaces = self.storage.workspace().workspaces();
    list_by_scope(
      scope,
      || workspaces.list_shared(),
      || workspaces.list_local(),
      || workspaces.list(),
    )
  }

  fn list_plugins(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<PluginRecord>, PluginStorageError> {
    let plugins = self.storage.plugins();
    list_by_scope(
      scope,
      || plugins.list_shared(),
      || plugins.list_local(),
      || plugins.list(),
    )
  }

  /// List skills or rules: both are content-backed versioned resources, so
  /// the scope-filtered listing, selector filtering, and conversion are
  /// shared; only the storage accessors and error contexts differ.
  fn list_versioned(
    &self, kind: VersionedKind, selector: &HashMap<String, String>, scope: Option<ResourceScope>,
  ) -> Result<Vec<Resource>, MapperError> {
    let access = versioned_access(&self.storage, kind);
    let records = list_by_scope(
      scope,
      || access.records.list_shared(),
      || access.records.list_local(),
      || access.records.list(),
    )
    .map_err(MapperError::workspace(access.list_context))?;
    Ok(collect_matching(
      records,
      selector,
      |record| &record.metadata,
      |record| versioned_to_resource(record, kind, None),
    ))
  }

  /// Get a skill or rule with its content body; see [`Self::list_versioned`]
  /// for why the two kinds share one path.
  fn get_versioned(&self, kind: VersionedKind, id: &str) -> Result<Resource, MapperError> {
    let access = versioned_access(&self.storage, kind);
    let Some(record) = access
      .records
      .get(id)
      .map_err(MapperError::workspace(access.get_context))?
    else {
      return Err(MapperError::NotFound(id.to_string()));
    };
    let content = access
      .content
      .read(id)
      .map_err(MapperError::workspace(access.read_content_context))?;
    Ok(versioned_to_resource(record, kind, content))
  }

  /// Merge an incoming shared resource into local storage.
  ///
  /// Only `ClusterShared` resources whose body matches their declared kind
  /// are accepted; anything else is rejected explicitly (D8) instead of being
  /// silently dropped. The content body is written to the filesystem-backed
  /// content store when present.
  pub async fn apply_resource(&self, resource: &Resource) -> Result<(), MapperError> {
    let metadata = resource
      .metadata
      .as_ref()
      .ok_or(MapperError::MissingMetadata)?;
    let kind = decode_kind(metadata.kind)?;
    let scope = metadata_scope(metadata)?;
    if scope != ResourceScope::ClusterShared {
      return Err(MapperError::NotShared {
        id: metadata.id.clone(),
        scope,
      });
    }

    match (kind, resource.body.as_ref()) {
      (ResourceKind::Memory, Some(Body::Memory(body))) => {
        let record = resource_to_memory(metadata, body)?;
        self
          .storage
          .agent()
          .apply_remote_memory(record)
          .await
          .map_err(MapperError::agent("failed to apply remote memory"))?;
      }
      (ResourceKind::Skill, Some(Body::Skill(body))) => {
        let record = resource_to_skill(metadata, body)?;
        self
          .storage
          .workspace()
          .apply_remote_skill(record, &body.content)
          .map_err(MapperError::workspace("failed to apply remote skill"))?;
      }
      (ResourceKind::Rule, Some(Body::Rule(body))) => {
        let record = resource_to_rule(metadata, body)?;
        self
          .storage
          .workspace()
          .apply_remote_rule(record, &body.content)
          .map_err(MapperError::workspace("failed to apply remote rule"))?;
      }
      (ResourceKind::Workspace, Some(Body::Workspace(body))) => {
        let record = resource_to_workspace(metadata, body)?;
        self
          .storage
          .workspace()
          .apply_remote_workspace(record)
          .map_err(MapperError::workspace("failed to apply remote workspace"))?;
      }
      (ResourceKind::Plugin, Some(Body::Plugin(body))) => {
        let record = resource_to_plugin(metadata, body)?;
        self
          .storage
          .plugins()
          .apply_remote_plugin(record, &body.artifact)
          .map_err(MapperError::plugin("failed to apply remote plugin"))?;
      }
      (ResourceKind::Node | ResourceKind::Session, _) => {
        return Err(MapperError::NotSynchronized { kind });
      }
      (_, None) => {
        return Err(MapperError::MissingBody {
          kind,
          id: metadata.id.clone(),
        });
      }
      _ => {
        return Err(MapperError::KindBodyMismatch { kind });
      }
    }

    Ok(())
  }

  /// Return all cluster-shared resources as `Resource` protos.
  pub async fn local_shared_resources(&self) -> Result<Vec<Resource>, MapperError> {
    let mut resources = Vec::new();

    let memories = self
      .storage
      .agent()
      .memory()
      .list_shared()
      .await
      .map_err(MapperError::agent("failed to list shared memories"))?;
    for entry in memories {
      resources.push(memory_to_resource(entry, true));
    }

    let workspaces = self
      .storage
      .workspace()
      .workspaces()
      .list_shared()
      .map_err(MapperError::workspace("failed to list shared workspaces"))?;
    for workspace in workspaces {
      resources.push(workspace_to_resource(workspace));
    }

    for kind in [VersionedKind::Skill, VersionedKind::Rule] {
      let access = versioned_access(&self.storage, kind);
      let records = access
        .records
        .list_shared()
        .map_err(MapperError::workspace(access.list_shared_context))?;
      for record in records {
        let content = access
          .content
          .read(&record.id)
          .map_err(MapperError::workspace(access.read_content_context))?;
        resources.push(versioned_to_resource(record, kind, content));
      }
    }

    let plugins = self
      .storage
      .plugins()
      .list_shared()
      .map_err(MapperError::plugin("failed to list shared plugins"))?;
    for record in plugins {
      let artifact = self
        .storage
        .plugins()
        .blobs()
        .read(&record.id)
        .map_err(MapperError::plugin("failed to read plugin artifact"))?;
      resources.push(plugin_to_resource(record, artifact));
    }

    Ok(resources)
  }
}

#[cfg(test)]
mod tests {
  use lycoris_membership::SwimConfig;
  use lycoris_proto::node::{
    MemoryBody, PluginBody, ResourceMetadata, ResourceScope as ProtoResourceScope, WorkspaceBody,
  };
  use lycoris_storage::DEFAULT_EMBEDDING_DIM;
  use tempfile::TempDir;

  use super::*;
  use crate::membership::MemberRegister;

  fn test_mapper() -> (TempDir, ResourceMapper) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let service = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    (dir, ResourceMapper::new(storage, service))
  }

  fn memory_resource(scope: ProtoResourceScope) -> Resource {
    let content = b"shared memory".to_vec();
    Resource {
      metadata: Some(ResourceMetadata {
        id: "mem-1".to_string(),
        name: "mem-1".to_string(),
        kind: ResourceKind::Memory as i32,
        scope: scope as i32,
        source_node_id: "peer".to_string(),
        updated_at_ms: 1,
        ..ResourceMetadata::default()
      }),
      body: Some(Body::Memory(MemoryBody {
        content_hash: MemoryEntry::compute_content_hash(b"shared memory"),
        content,
        embedding: vec![0.0; DEFAULT_EMBEDDING_DIM],
        version: 1,
        ..MemoryBody::default()
      })),
    }
  }

  #[tokio::test]
  async fn apply_resource_rejects_missing_metadata() {
    let (_dir, mapper) = test_mapper();
    let resource = Resource {
      metadata: None,
      body: None,
    };
    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::MissingMetadata));
  }

  #[tokio::test]
  async fn apply_resource_rejects_non_shared_scope() {
    let (_dir, mapper) = test_mapper();
    for scope in [
      ProtoResourceScope::NodeLocal,
      ProtoResourceScope::Unspecified,
    ] {
      let error = mapper
        .apply_resource(&memory_resource(scope))
        .await
        .unwrap_err();
      assert!(
        matches!(error, MapperError::NotShared { .. }),
        "scope: {scope:?}"
      );
    }
  }

  #[tokio::test]
  async fn apply_resource_rejects_kind_body_mismatch() {
    let (_dir, mapper) = test_mapper();
    let mut resource = memory_resource(ProtoResourceScope::ClusterShared);
    resource.body = Some(Body::Workspace(WorkspaceBody::default()));
    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::KindBodyMismatch { .. }));
  }

  #[tokio::test]
  async fn apply_resource_rejects_missing_body() {
    let (_dir, mapper) = test_mapper();
    let mut resource = memory_resource(ProtoResourceScope::ClusterShared);
    resource.body = None;
    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::MissingBody { .. }));
  }

  #[tokio::test]
  async fn apply_resource_stores_valid_shared_memory() {
    let (_dir, mapper) = test_mapper();
    mapper
      .apply_resource(&memory_resource(ProtoResourceScope::ClusterShared))
      .await
      .unwrap();
    let stored = mapper
      .get(ResourceKind::Memory, "mem-1")
      .await
      .expect("get memory");
    assert!(matches!(stored.body, Some(Body::Memory(_))));
  }

  fn plugin_resource(id: &str, scope: ProtoResourceScope, artifact: &[u8]) -> Resource {
    Resource {
      metadata: Some(ResourceMetadata {
        id: id.to_string(),
        name: format!("plugin-{id}"),
        kind: ResourceKind::Plugin as i32,
        scope: scope as i32,
        source_node_id: "peer".to_string(),
        created_at_ms: 1,
        updated_at_ms: 1,
        ..ResourceMetadata::default()
      }),
      body: Some(Body::Plugin(PluginBody {
        version: 1,
        content_hash: blake3::hash(artifact).to_hex().to_string(),
        engine: "lua".to_string(),
        entry: "invoke".to_string(),
        artifact: artifact.to_vec(),
        manifest: HashMap::from([("semver".to_string(), "0.1.0".to_string())]),
      })),
    }
  }

  fn plugin_body(resource: &Resource) -> &PluginBody {
    let Some(Body::Plugin(body)) = resource.body.as_ref() else {
      panic!("expected a plugin body");
    };
    body
  }

  #[tokio::test]
  async fn apply_resource_stores_valid_shared_plugin() {
    let (_dir, mapper) = test_mapper();
    let artifact = b"return { echo = true }";
    mapper
      .apply_resource(&plugin_resource(
        "plug-1",
        ProtoResourceScope::ClusterShared,
        artifact,
      ))
      .await
      .unwrap();

    let stored = mapper
      .get(ResourceKind::Plugin, "plug-1")
      .await
      .expect("get plugin");
    let metadata = stored.metadata.as_ref().expect("metadata");
    assert_eq!(metadata.name, "plugin-plug-1");
    assert_eq!(metadata.created_at_ms, 1);
    assert_eq!(metadata.source_node_id, "peer");
    let body = plugin_body(&stored);
    assert_eq!(body.artifact, artifact);
    assert_eq!(body.engine, "lua");
    assert_eq!(body.entry, "invoke");
    assert_eq!(body.manifest.get("semver"), Some(&"0.1.0".to_string()));
  }

  #[tokio::test]
  async fn apply_resource_rejects_non_shared_plugin() {
    let (_dir, mapper) = test_mapper();
    for scope in [
      ProtoResourceScope::NodeLocal,
      ProtoResourceScope::Unspecified,
    ] {
      let error = mapper
        .apply_resource(&plugin_resource("plug-local", scope, b"x"))
        .await
        .unwrap_err();
      assert!(
        matches!(error, MapperError::NotShared { .. }),
        "scope: {scope:?}"
      );
    }
  }

  #[tokio::test]
  async fn apply_resource_rejects_plugin_hash_mismatch() {
    let (_dir, mapper) = test_mapper();
    let mut resource = plugin_resource("plug-hash", ProtoResourceScope::ClusterShared, b"real");
    plugin_body_mut(&mut resource).content_hash = "wrong-hash".to_string();

    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::Plugin { .. }));
    assert!(matches!(
      mapper.get(ResourceKind::Plugin, "plug-hash").await,
      Err(MapperError::NotFound(_))
    ));
  }

  fn plugin_body_mut(resource: &mut Resource) -> &mut PluginBody {
    let Some(Body::Plugin(body)) = resource.body.as_mut() else {
      panic!("expected a plugin body");
    };
    body
  }

  #[tokio::test]
  async fn list_plugins_filters_by_scope_and_selector() {
    let (_dir, mapper) = test_mapper();
    for (id, artifact) in [("plug-a", b"a" as &[u8]), ("plug-b", b"b")] {
      mapper
        .apply_resource(&plugin_resource(
          id,
          ProtoResourceScope::ClusterShared,
          artifact,
        ))
        .await
        .unwrap();
    }

    let all = mapper
      .list(ResourceKind::Plugin, HashMap::new(), None)
      .await
      .unwrap();
    assert_eq!(all.len(), 2);
    // Listings stay artifact-free; `get` carries the full artifact.
    for resource in &all {
      assert!(plugin_body(resource).artifact.is_empty());
    }

    let shared = mapper
      .list(
        ResourceKind::Plugin,
        HashMap::new(),
        Some(ResourceScope::ClusterShared),
      )
      .await
      .unwrap();
    assert_eq!(shared.len(), 2);
    let local = mapper
      .list(
        ResourceKind::Plugin,
        HashMap::new(),
        Some(ResourceScope::NodeLocal),
      )
      .await
      .unwrap();
    assert!(local.is_empty());

    // Plugins carry no labels, so a non-empty selector matches nothing.
    let selected = mapper
      .list(
        ResourceKind::Plugin,
        HashMap::from([("a".to_string(), "b".to_string())]),
        None,
      )
      .await
      .unwrap();
    assert!(selected.is_empty());
  }

  #[tokio::test]
  async fn local_shared_resources_include_plugin_artifacts() {
    let (_dir, mapper) = test_mapper();
    mapper
      .apply_resource(&plugin_resource(
        "plug-sync",
        ProtoResourceScope::ClusterShared,
        b"sync-me",
      ))
      .await
      .unwrap();

    let resources = mapper.local_shared_resources().await.unwrap();
    let plugin = resources
      .iter()
      .find(|resource| matches!(resource.body, Some(Body::Plugin(_))))
      .expect("a plugin resource");
    assert_eq!(plugin_body(plugin).artifact, b"sync-me");
  }
}
