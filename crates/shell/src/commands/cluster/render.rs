use lycoris_proto::node::{
  NodeBody, NodeState, Resource as ProtoResource, ResourceKind, resource::Body,
};
use owo_colors::OwoColorize;

use super::parse::{resource_name, scope_from_proto};

pub(crate) fn render_list(kind: ResourceKind, resources: &[ProtoResource], local_id: &str) {
  match kind {
    ResourceKind::Node => render_node_list(resources, local_id),
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
    Some(Body::Node(_)) | Some(Body::Extension(_)) | None => {}
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
