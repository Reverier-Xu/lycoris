//! Bidirectional converters between stored domain types and the public
//! `Resource` proto.
//!
//! Scope, source node, and timestamps have their single copy in
//! [`MetadataBuilder`]; body payloads do not repeat them. Unscoped resources
//! (nodes, sessions) simply never call [`MetadataBuilder::scope`], leaving
//! `RESOURCE_SCOPE_UNSPECIFIED`.

use std::collections::HashMap;

use lycoris_core::ResourceScope;
use lycoris_proto::{
  node::{
    MemoryBody, NodeBody, NodeInfo, PluginBody, Resource, ResourceKind, ResourceMetadata,
    ResourceScope as ProtoResourceScope, RuleBody, SessionBody, SkillBody, WorkspaceBody,
    resource::Body,
  },
  scope_from_proto, scope_to_proto,
};
use lycoris_storage::{
  MemoryEntry, PluginRecord, RuleRecord, Session, SkillRecord, VersionedResource, WorkspaceRecord,
};

use super::error::MapperError;

/// Decode a wire resource kind; the single raw-`i32` decoding point.
pub(crate) fn decode_kind(raw: i32) -> Result<ResourceKind, MapperError> {
  ResourceKind::try_from(raw).map_err(|_| MapperError::UnknownKind(raw))
}

/// Scaffolding for the metadata block of an outgoing `Resource`.
struct MetadataBuilder(ResourceMetadata);

impl MetadataBuilder {
  fn new(id: &str, name: &str, kind: ResourceKind) -> Self {
    Self(ResourceMetadata {
      id: id.to_string(),
      name: name.to_string(),
      kind: kind as i32,
      ..ResourceMetadata::default()
    })
  }

  fn labels(mut self, labels: HashMap<String, String>) -> Self {
    self.0.labels = labels;
    self
  }

  fn scope(mut self, scope: ResourceScope, source_node_id: Option<&str>) -> Self {
    self.0.scope = scope_to_proto(scope) as i32;
    self.0.source_node_id = source_node_id.unwrap_or_default().to_string();
    self
  }

  fn timestamps(mut self, created_at_ms: i64, updated_at_ms: i64) -> Self {
    self.0.created_at_ms = created_at_ms;
    self.0.updated_at_ms = updated_at_ms;
    self
  }

  fn build(self) -> ResourceMetadata {
    self.0
  }
}

pub(super) fn node_to_resource(node: NodeInfo) -> Resource {
  // The node's labels and annotations live on the `NodeInfo` payload only;
  // metadata carries the generic resource scaffold.
  let metadata = MetadataBuilder::new(&node.id, &node.id, ResourceKind::Node)
    .timestamps(0, node.last_heartbeat_unix_ms)
    .build();
  Resource {
    metadata: Some(metadata),
    body: Some(Body::Node(NodeBody { node: Some(node) })),
  }
}

pub(super) fn session_to_resource(session: Session) -> Resource {
  let title = session
    .metadata
    .get(Session::META_TITLE)
    .cloned()
    .unwrap_or_default();
  let host_node_id = session
    .metadata
    .get(Session::META_HOST_NODE_ID)
    .cloned()
    .unwrap_or_default();

  Resource {
    metadata: Some(MetadataBuilder::new(&session.id, &session.id, ResourceKind::Session).build()),
    body: Some(Body::Session(SessionBody {
      title,
      host_node_id,
      metadata: session.metadata,
    })),
  }
}

pub(super) fn memory_to_resource(entry: MemoryEntry, full: bool) -> Resource {
  let (content, embedding) = if full {
    (entry.content.clone(), entry.embedding.clone())
  } else {
    (Vec::new(), Vec::new())
  };

  Resource {
    metadata: Some(
      MetadataBuilder::new(&entry.id, &entry.id, ResourceKind::Memory)
        .labels(entry.metadata.clone())
        .scope(entry.scope, entry.source_node_id.as_deref())
        .timestamps(entry.created_at_ms, entry.updated_at_ms)
        .build(),
    ),
    body: Some(Body::Memory(MemoryBody {
      content,
      metadata: entry.metadata,
      content_hash: entry.content_hash,
      embedding,
      version: entry.version,
    })),
  }
}

/// The content-backed versioned kinds: skills and rules share the
/// `VersionedResource` record and differ only in wire vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VersionedKind {
  Skill,
  Rule,
}

impl VersionedKind {
  /// The wire kind carried in resource metadata.
  fn resource_kind(self) -> ResourceKind {
    match self {
      Self::Skill => ResourceKind::Skill,
      Self::Rule => ResourceKind::Rule,
    }
  }
}

/// Shared conversion for skills and rules: both are `VersionedResource`
/// records whose bodies carry identical fields; only the body variant and the
/// declared kind differ.
pub(super) fn versioned_to_resource(
  record: VersionedResource, kind: VersionedKind, content: Option<String>,
) -> Resource {
  let metadata = MetadataBuilder::new(&record.id, &record.name, kind.resource_kind())
    .labels(record.metadata.clone().into_iter().collect())
    .scope(record.scope, record.source_node_id.as_deref())
    .timestamps(record.created_at_ms, record.updated_at_ms)
    .build();
  let (version, content_hash, content, labels) = (
    record.version,
    record.content_hash,
    content.unwrap_or_default(),
    record.metadata.into_iter().collect(),
  );
  let body = match kind {
    VersionedKind::Skill => Body::Skill(SkillBody {
      version,
      content_hash,
      content,
      metadata: labels,
    }),
    VersionedKind::Rule => Body::Rule(RuleBody {
      version,
      content_hash,
      content,
      metadata: labels,
    }),
  };
  Resource {
    metadata: Some(metadata),
    body: Some(body),
  }
}

pub(super) fn workspace_to_resource(workspace: WorkspaceRecord) -> Resource {
  Resource {
    metadata: Some(
      MetadataBuilder::new(&workspace.id, &workspace.id, ResourceKind::Workspace)
        .labels(workspace.metadata.clone().into_iter().collect())
        .scope(workspace.scope, workspace.source_node_id.as_deref())
        .timestamps(workspace.created_at_ms, workspace.updated_at_ms)
        .build(),
    ),
    body: Some(Body::Workspace(WorkspaceBody {
      root: workspace.root.to_string_lossy().to_string(),
      session_ids: workspace.session_ids,
      metadata: workspace.metadata.into_iter().collect(),
      version: workspace.version,
      content_hash: workspace.content_hash,
    })),
  }
}

/// Convert a stored plugin record into the wire resource.
///
/// Plugin configuration lives in the manifest (design section 4), so plugin
/// metadata carries no labels. Listings pass `None` for the artifact to keep
/// list responses small; `get` and the anti-entropy snapshot carry the full
/// artifact bytes.
pub(super) fn plugin_to_resource(record: PluginRecord, artifact: Option<Vec<u8>>) -> Resource {
  Resource {
    metadata: Some(
      MetadataBuilder::new(&record.id, &record.name, ResourceKind::Plugin)
        .scope(record.scope, record.source_node_id.as_deref())
        .timestamps(record.created_at_ms, record.updated_at_ms)
        .build(),
    ),
    body: Some(Body::Plugin(PluginBody {
      version: record.version,
      content_hash: record.content_hash,
      engine: record.engine,
      entry: record.entry,
      artifact: artifact.unwrap_or_default(),
      manifest: record.manifest.into_iter().collect(),
    })),
  }
}

/// Decode the scope carried by resource metadata into the domain type.
pub(super) fn metadata_scope(metadata: &ResourceMetadata) -> Result<ResourceScope, MapperError> {
  let scope = ProtoResourceScope::try_from(metadata.scope)
    .map_err(|_| MapperError::UnknownScope(metadata.scope))?;
  Ok(scope_from_proto(scope))
}

/// Normalize the optional source node id carried by resource metadata.
fn metadata_source_node_id(metadata: &ResourceMetadata) -> Option<String> {
  Some(metadata.source_node_id.clone()).filter(|id| !id.is_empty())
}

pub(super) fn resource_to_memory(
  metadata: &ResourceMetadata, body: &MemoryBody,
) -> Result<MemoryEntry, MapperError> {
  Ok(MemoryEntry {
    id: metadata.id.clone(),
    content: body.content.clone(),
    embedding: body.embedding.clone(),
    metadata: body.metadata.clone(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    created_at_ms: metadata.created_at_ms,
    updated_at_ms: metadata.updated_at_ms,
    content_hash: body.content_hash.clone(),
    version: body.version,
  })
}

/// Shared conversion for skills and rules (both are `VersionedResource`
/// records).
fn resource_to_versioned(
  metadata: &ResourceMetadata, version: u64, content_hash: &str,
) -> Result<VersionedResource, MapperError> {
  Ok(VersionedResource {
    id: metadata.id.clone(),
    name: metadata.name.clone(),
    version,
    content_hash: content_hash.to_string(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    created_at_ms: metadata.created_at_ms,
    updated_at_ms: metadata.updated_at_ms,
    metadata: metadata.labels.clone().into_iter().collect(),
  })
}

pub(super) fn resource_to_skill(
  metadata: &ResourceMetadata, body: &SkillBody,
) -> Result<SkillRecord, MapperError> {
  resource_to_versioned(metadata, body.version, &body.content_hash)
}

pub(super) fn resource_to_rule(
  metadata: &ResourceMetadata, body: &RuleBody,
) -> Result<RuleRecord, MapperError> {
  resource_to_versioned(metadata, body.version, &body.content_hash)
}

pub(super) fn resource_to_workspace(
  metadata: &ResourceMetadata, body: &WorkspaceBody,
) -> Result<WorkspaceRecord, MapperError> {
  Ok(WorkspaceRecord {
    id: metadata.id.clone(),
    root: body.root.clone().into(),
    session_ids: body.session_ids.clone(),
    metadata: body.metadata.clone().into_iter().collect(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    version: body.version,
    content_hash: body.content_hash.clone(),
    created_at_ms: metadata.created_at_ms,
    updated_at_ms: metadata.updated_at_ms,
  })
}

pub(super) fn resource_to_plugin(
  metadata: &ResourceMetadata, body: &PluginBody,
) -> Result<PluginRecord, MapperError> {
  Ok(PluginRecord {
    id: metadata.id.clone(),
    name: metadata.name.clone(),
    version: body.version,
    engine: body.engine.clone(),
    entry: body.entry.clone(),
    content_hash: body.content_hash.clone(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    created_at_ms: metadata.created_at_ms,
    updated_at_ms: metadata.updated_at_ms,
    manifest: body.manifest.clone().into_iter().collect(),
  })
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::*;

  fn plugin_record(manifest: BTreeMap<String, String>) -> PluginRecord {
    PluginRecord {
      id: "plug-1".to_string(),
      name: "echo".to_string(),
      version: 7,
      engine: "lua".to_string(),
      entry: "invoke".to_string(),
      content_hash: "abc123".to_string(),
      scope: ResourceScope::ClusterShared,
      source_node_id: Some("node-a".to_string()),
      created_at_ms: 11,
      updated_at_ms: 22,
      manifest,
    }
  }

  fn small_manifest() -> BTreeMap<String, String> {
    BTreeMap::from([
      ("semver".to_string(), "1.2.3".to_string()),
      ("selector".to_string(), r#"{"gpu":"true"}"#.to_string()),
    ])
  }

  /// Extract back the (metadata, plugin body) pair of a wire resource.
  fn split(resource: &Resource) -> (&ResourceMetadata, &PluginBody) {
    let metadata = resource.metadata.as_ref().expect("metadata");
    let Some(Body::Plugin(body)) = resource.body.as_ref() else {
      panic!("expected a plugin body");
    };
    (metadata, body)
  }

  #[test]
  fn plugin_conversion_round_trip() {
    let record = plugin_record(small_manifest());
    let artifact = b"return {}".to_vec();

    let resource = plugin_to_resource(record.clone(), Some(artifact.clone()));
    let (metadata, body) = split(&resource);

    assert_eq!(metadata.kind, ResourceKind::Plugin as i32);
    assert!(metadata.labels.is_empty());
    assert_eq!(body.artifact, artifact);
    let restored = resource_to_plugin(metadata, body).expect("convert back");
    assert_eq!(restored, record);
  }

  #[test]
  fn plugin_conversion_round_trip_without_artifact() {
    let record = plugin_record(BTreeMap::new());

    let resource = plugin_to_resource(record.clone(), None);
    let (metadata, body) = split(&resource);

    assert!(body.artifact.is_empty());
    let restored = resource_to_plugin(metadata, body).expect("convert back");
    assert_eq!(restored, record);
  }

  #[test]
  fn plugin_conversion_round_trip_with_large_manifest() {
    let manifest: BTreeMap<String, String> = (0..2048)
      .map(|index| (format!("key-{index}"), format!("value-{index}")))
      .collect();
    let record = plugin_record(manifest);

    let resource = plugin_to_resource(record.clone(), Some(vec![0xAB; 4096]));
    let (metadata, body) = split(&resource);

    assert_eq!(body.manifest.len(), 2048);
    let restored = resource_to_plugin(metadata, body).expect("convert back");
    assert_eq!(restored, record);
  }
}
