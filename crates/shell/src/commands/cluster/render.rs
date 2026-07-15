use lycoris_proto::node::{NodeBody, Resource as ProtoResource, ResourceKind, resource::Body};
use owo_colors::OwoColorize;

use super::parse::resource_name;

pub fn render_list(kind: ResourceKind, resources: &[ProtoResource], local_id: &str) {
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

  for resource in resources {
    let Some(Body::Node(NodeBody { node: Some(node) })) = resource.body.as_ref() else {
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
    println!("{}{}  {}", marker, id, node.state,);
    println!("  address: {}", node.address);
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
      resource_name(ResourceKind::try_from(metadata.kind).unwrap_or(ResourceKind::Node)),
      metadata.scope.as_str(),
      metadata.source_node_id.as_str(),
    );
  }
}

pub fn render_resource(
  resource: &ProtoResource, kind: ResourceKind, local_id: &str, compact: bool,
) {
  match kind {
    ResourceKind::Node => render_node(resource, local_id, compact),
    _ => render_generic(resource, compact),
  }
}

fn render_node(resource: &ProtoResource, local_id: &str, compact: bool) {
  let Some(Body::Node(NodeBody { node: Some(node) })) = resource.body.as_ref() else {
    return;
  };

  let marker = if node.id == local_id {
    " (current)".cyan().to_string()
  } else {
    String::new()
  };

  if compact {
    println!("{}{}", node.id.bold().cyan(), marker);
    println!("  address:        {}", node.address);
    println!("  state:          {}", node.state);
  } else {
    println!("{}{}", node.id.bold().cyan(), marker);
    println!("  address:        {}", node.address);
    println!("  state:          {}", node.state);
    println!("  incarnation:    {}", node.incarnation);
    println!("  heartbeat:      {}", node.heartbeat);
    println!("  last heartbeat: {}", node.last_heartbeat_unix_ms);
    println!("  labels:         {:?}", node.labels);
    println!("  annotations:    {:?}", node.annotations);
  }
}

fn render_generic(resource: &ProtoResource, compact: bool) {
  let metadata = match resource.metadata.as_ref() {
    Some(m) => m,
    None => {
      println!("(missing metadata)");
      return;
    }
  };

  println!("{}", metadata.name.bold());
  println!(
    "  kind:           {}",
    resource_name(ResourceKind::try_from(metadata.kind).unwrap_or(ResourceKind::Node))
  );
  println!("  id:             {}", metadata.id);
  println!("  scope:          {}", metadata.scope);
  println!("  source node:    {}", metadata.source_node_id);
  println!("  created at:     {}", metadata.created_at_ms);
  println!("  updated at:     {}", metadata.updated_at_ms);
  println!("  labels:         {:?}", metadata.labels);
  println!("  annotations:    {:?}", metadata.annotations);

  if !compact {
    match resource.body.as_ref() {
      Some(Body::Session(body)) => {
        println!("  title:          {}", body.title);
        println!("  host node:      {}", body.host_node_id);
        println!("  metadata:       {:?}", body.metadata);
      }
      Some(Body::Memory(body)) => {
        println!("  content length: {}", body.content.len());
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
        println!("  sessions:       {:?}", body.session_ids);
        println!("  metadata:       {:?}", body.metadata);
      }
      Some(Body::Node(_)) | None => {}
    }
  }
}
