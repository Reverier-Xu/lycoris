use std::collections::HashMap;

use lycoris_proto::node::{
  NodeBody, NodeState, Resource as ProtoResource, ResourceKind, resource::Body,
};
use owo_colors::OwoColorize;

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
  println!(
    "{}  {}",
    "NODE ID".bold().underline(),
    "STATE".bold().underline(),
  );

  let mut skipped = 0usize;
  for resource in resources {
    let Some(Body::Node(NodeBody { node: Some(node) })) = resource.body.as_ref() else {
      skipped += 1;
      continue;
    };
    let marker = if node.id == local_id {
      "-> ".cyan().to_string()
    } else {
      "   ".to_string()
    };
    let id = if node.id == local_id {
      node.id.cyan().to_string()
    } else {
      node.id.clone()
    };
    println!("{}{}  {}", marker, id, state_display(node.state),);
    println!("  address: {}", node.address);
  }
  if skipped > 0 {
    eprintln!("note: {skipped} resource(s) had no node body and were not displayed");
  }
}

fn render_generic_list(resources: &[ProtoResource]) {
  println!(
    "{}  {}  {}  {}",
    "NAME".bold().underline(),
    "KIND".bold().underline(),
    "SCOPE".bold().underline(),
    "SOURCE".bold().underline()
  );

  for resource in resources {
    let metadata = match resource.metadata.as_ref() {
      Some(m) => m,
      None => continue,
    };
    println!(
      "{}  {}  {}  {}",
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
  println!(
    "{}  {}  {}  {}  {}",
    "ID".bold().underline(),
    "NAME".bold().underline(),
    "ENGINE".bold().underline(),
    "SEMVER".bold().underline(),
    "LOCAL".bold().underline(),
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
    println!(
      "{}  {}  {}  {}  {}",
      metadata.id, metadata.name, body.engine, semver, local,
    );
  }
  if skipped > 0 {
    eprintln!("note: {skipped} resource(s) had no extension body and were not displayed");
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
    eprintln!("note: the returned resource has no node body; nothing to display");
    return;
  };

  let marker = if node.id == local_id {
    " (current)".cyan().to_string()
  } else {
    String::new()
  };

  println!("{}{}", node.id.bold().cyan(), marker);
  println!("  address:        {}", node.address);
  println!("  state:          {}", state_display(node.state));
  println!("  incarnation:    {}", node.incarnation);
  println!("  heartbeat:      {}", node.heartbeat);
  println!("  last heartbeat: {}", node.last_heartbeat_unix_ms);
  println!("  labels:         {:?}", node.labels);
  println!("  annotations:    {:?}", node.annotations);
}

fn render_generic(resource: &ProtoResource) {
  let metadata = match resource.metadata.as_ref() {
    Some(m) => m,
    None => {
      println!("(missing metadata)");
      return;
    }
  };

  println!("{}", metadata.name.bold());
  println!("  kind:           {}", kind_display(metadata.kind));
  println!("  id:             {}", metadata.id);
  println!("  scope:          {}", scope_display(metadata.scope));
  println!("  source node:    {}", metadata.source_node_id);
  println!("  created at:     {}", metadata.created_at_ms);
  println!("  updated at:     {}", metadata.updated_at_ms);
  println!("  labels:         {:?}", metadata.labels);
  println!("  annotations:    {:?}", metadata.annotations);

  match resource.body.as_ref() {
    Some(Body::Session(body)) => {
      println!("  title:          {}", body.title);
      println!("  host node:      {}", body.host_node_id);
      println!("  metadata:       {:?}", body.metadata);
    }
    Some(Body::Memory(body)) => {
      println!("  content length: {}", body.content.len());
      println!("  embedding dim:  {}", body.embedding.len());
      println!("  content hash:   {}", body.content_hash);
      println!("  metadata:       {:?}", body.metadata);
    }
    Some(Body::Skill(body)) => {
      println!("  version:        {}", body.version);
      println!("  content hash:   {}", body.content_hash);
      println!("  content length: {}", body.content.len());
      println!("  metadata:       {:?}", body.metadata);
    }
    Some(Body::Rule(body)) => {
      println!("  version:        {}", body.version);
      println!("  content hash:   {}", body.content_hash);
      println!("  content length: {}", body.content.len());
      println!("  metadata:       {:?}", body.metadata);
    }
    Some(Body::Workspace(body)) => {
      println!("  root:           {}", body.root);
      println!("  version:        {}", body.version);
      println!("  content hash:   {}", body.content_hash);
      println!("  sessions:       {:?}", body.session_ids);
      println!("  metadata:       {:?}", body.metadata);
    }
    Some(Body::Extension(body)) => {
      println!("  version:        {}", body.version);
      println!("  engine:         {}", body.engine);
      println!("  entry:          {}", body.entry);
      println!("  content hash:   {}", body.content_hash);
      // The artifact itself is never dumped; its size is enough to confirm
      // what the node holds.
      println!("  artifact size:  {} bytes", body.artifact.len());
      println!("  manifest:       {:?}", body.manifest);
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
