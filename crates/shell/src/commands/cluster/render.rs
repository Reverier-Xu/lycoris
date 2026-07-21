use std::collections::HashMap;

use lycoris_proto::node::{
  NodeBody, NodeState, Resource as ProtoResource, ResourceKind, resource::Body,
};

use super::parse::{resource_name, scope_from_proto};

pub(crate) fn render_list(
  kind: ResourceKind, resources: &[ProtoResource], local_id: &str,
  local_labels: &HashMap<String, String>,
) {
  match kind {
    ResourceKind::Node => render_node_list(resources, local_id),
    ResourceKind::Extension => render_extension_list(resources, local_labels),
    _ => render_generic_list(resources),
  }
}

fn render_node_list(resources: &[ProtoResource], local_id: &str) {
  tracing::info!("{}\t{}", "NODE ID", "STATE");

  let mut skipped = 0usize;
  for resource in resources {
    let Some(Body::Node(NodeBody { node: Some(node) })) = resource.body.as_ref() else {
      skipped += 1;
      continue;
    };
    let marker = if node.id == local_id { "->" } else { "" };
    let current = if node.id == local_id {
      " (current)"
    } else {
      ""
    };
    tracing::info!(
      "{marker} {}{current}\t{}\n  address: {}",
      node.id,
      state_display(node.state),
      node.address
    );
  }
  if skipped > 0 {
    tracing::warn!("note: {skipped} resource(s) had no node body and were not displayed");
  }
}

fn render_generic_list(resources: &[ProtoResource]) {
  tracing::info!("{}\t{}\t{}\t{}", "NAME", "KIND", "SCOPE", "SOURCE");

  for resource in resources {
    let metadata = match resource.metadata.as_ref() {
      Some(m) => m,
      None => continue,
    };
    tracing::info!(
      "{}\t{}\t{}\t{}",
      metadata.name,
      kind_display(metadata.kind),
      scope_display(metadata.scope),
      metadata.source_node_id.as_str(),
    );
  }
}

/// The extension listing: one row per package with the activation-relevant
/// facts — engine, human-facing semver, and whether the manifest selector
/// matches this node's configured labels.
fn render_extension_list(resources: &[ProtoResource], local_labels: &HashMap<String, String>) {
  tracing::info!(
    "{}\t{}\t{}\t{}\t{}",
    "ID",
    "NAME",
    "ENGINE",
    "SEMVER",
    "LOCAL"
  );

  let mut skipped = 0usize;
  for resource in resources {
    let (Some(metadata), Some(Body::Extension(body))) =
      (resource.metadata.as_ref(), resource.body.as_ref())
    else {
      skipped += 1;
      continue;
    };
    let selector: HashMap<String, String> = body
      .manifest
      .get("selector")
      .and_then(|raw| serde_json::from_str(raw).ok())
      .unwrap_or_default();
    let local = if selector_matches(local_labels, &selector) {
      "yes"
    } else {
      "no"
    };
    let semver = body.manifest.get("semver").map_or("-", String::as_str);
    tracing::info!(
      "{}\t{}\t{}\t{}\t{}",
      metadata.id,
      metadata.name,
      body.engine,
      semver,
      local,
    );
  }
  if skipped > 0 {
    tracing::warn!("note: {skipped} resource(s) had no extension body and were not displayed");
  }
}

/// Display-only mirror of the daemon's selector semantics (subset match: an
/// empty selector matches every node). Activation decisions are made by the
/// daemon; this only decides how a row renders.
fn selector_matches(labels: &HashMap<String, String>, selector: &HashMap<String, String>) -> bool {
  selector
    .iter()
    .all(|(key, value)| labels.get(key) == Some(value))
}

pub(crate) fn render_resource(resource: &ProtoResource, kind: ResourceKind, local_id: &str) {
  match kind {
    ResourceKind::Node => render_node(resource, local_id),
    _ => render_generic(resource),
  }
}

fn render_node(resource: &ProtoResource, local_id: &str) {
  let Some(Body::Node(NodeBody { node: Some(node) })) = resource.body.as_ref() else {
    tracing::warn!("note: the returned resource has no node body; nothing to display");
    return;
  };

  let current = if node.id == local_id {
    " (current)"
  } else {
    ""
  };
  tracing::info!(
    "{}{current}\n\
     \x20 address:        {}\n\
     \x20 state:          {}\n\
     \x20 incarnation:    {}\n\
     \x20 heartbeat:      {}\n\
     \x20 last heartbeat: {}\n\
     \x20 labels:         {:?}\n\
     \x20 annotations:    {:?}",
    node.id,
    node.address,
    state_display(node.state),
    node.incarnation,
    node.heartbeat,
    node.last_heartbeat_unix_ms,
    node.labels,
    node.annotations
  );
}

fn render_generic(resource: &ProtoResource) {
  let metadata = match resource.metadata.as_ref() {
    Some(m) => m,
    None => {
      tracing::warn!("(missing metadata)");
      return;
    }
  };

  tracing::info!(
    "{name}\n\
     \x20 kind:           {kind}\n\
     \x20 id:             {id}\n\
     \x20 scope:          {scope}\n\
     \x20 source node:    {source}\n\
     \x20 created at:     {created}\n\
     \x20 updated at:     {updated}\n\
     \x20 labels:         {labels:?}\n\
     \x20 annotations:    {annotations:?}",
    name = metadata.name,
    kind = kind_display(metadata.kind),
    id = metadata.id,
    scope = scope_display(metadata.scope),
    source = metadata.source_node_id,
    created = metadata.created_at_ms,
    updated = metadata.updated_at_ms,
    labels = metadata.labels,
    annotations = metadata.annotations
  );

  match resource.body.as_ref() {
    Some(Body::Session(body)) => {
      tracing::info!(
        "  title:          {}\n  host node:      {}\n  metadata:       {:?}",
        body.title,
        body.host_node_id,
        body.metadata
      );
    }
    Some(Body::Memory(body)) => {
      tracing::info!(
        "  content length: {}\n  embedding dim:  {}\n  content hash:   {}\n  metadata:       {:?}",
        body.content.len(),
        body.embedding.len(),
        body.content_hash,
        body.metadata
      );
    }
    Some(Body::Skill(body)) => {
      tracing::info!(
        "  version:        {}\n  content hash:   {}\n  content length: {}\n  metadata:       {:?}",
        body.version,
        body.content_hash,
        body.content.len(),
        body.metadata
      );
    }
    Some(Body::Rule(body)) => {
      tracing::info!(
        "  version:        {}\n  content hash:   {}\n  content length: {}\n  metadata:       {:?}",
        body.version,
        body.content_hash,
        body.content.len(),
        body.metadata
      );
    }
    Some(Body::Workspace(body)) => {
      tracing::info!(
        "  root:           {}\n  version:        {}\n  content hash:   {}\n  sessions:       {:?}\n  metadata:       {:?}",
        body.root,
        body.version,
        body.content_hash,
        body.session_ids,
        body.metadata
      );
    }
    Some(Body::Extension(body)) => {
      tracing::info!(
        "  version:        {}\n  engine:         {}\n  entry:          {}\n  content hash:   {}\n  artifact size:  {} bytes\n  manifest:       {:?}",
        body.version,
        body.engine,
        body.entry,
        body.content_hash,
        body.artifact.len(),
        body.manifest
      );
    }
    Some(Body::Node(_)) | None => {}
  }
}

/// Display form of a raw wire kind value; values outside the known kinds
/// render as "unknown" instead of being guessed as nodes.
fn kind_display(raw: i32) -> &'static str {
  ResourceKind::try_from(raw).map_or("unknown", resource_name)
}

/// Display form of a node state; unset or unknown wire values render as
/// "unknown" instead of being guessed.
fn state_display(raw: i32) -> &'static str {
  match NodeState::try_from(raw) {
    Ok(NodeState::Active) => "active",
    Ok(NodeState::Suspected) => "suspected",
    Ok(NodeState::Offline) => "offline",
    Ok(NodeState::Leaving) => "leaving",
    _ => "unknown",
  }
}

/// Display form of a metadata scope: the `"shared"` / `"local"` spellings come
/// from `lycoris_core`; unscoped resources render as empty.
fn scope_display(raw: i32) -> &'static str {
  scope_from_proto(raw).map_or("", |scope| scope.as_str())
}
