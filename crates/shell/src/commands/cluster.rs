use std::collections::HashMap;

use lycoris_api::{
  ClusterRpcClient,
  proto::{NodeBody, Resource as ProtoResource, ResourceKind, resource::Body},
  tls::load_client_tls,
};
use lycoris_config::{
  ClientConfig, ClusterKey, DaemonConfig, NodeInfo, default_cluster_key_path,
  paths::default_daemon_config_path,
};
use owo_colors::OwoColorize;

use crate::error::ShellError;

pub async fn get_resources(
  client_config: &ClientConfig, resource: &str, name: Option<String>, selectors: &[String],
  scope: Option<String>,
) -> Result<(), ShellError> {
  let client = connect_cluster(client_config).await?;
  let kind = parse_resource_kind(resource)?;
  let kind_name = resource_name(kind);

  match name {
    Some(id) => {
      let resource = client
        .get_resource(kind, &id)
        .await
        .map_err(|source| ShellError::GetResource {
          kind: kind_name.clone(),
          id: id.clone(),
          source,
        })?
        .ok_or_else(|| ShellError::ResourceNotFound {
          kind: kind_name.clone(),
          id: id.clone(),
        })?;
      render_resource(&resource, kind, &local_node_id().unwrap_or_default(), true);
    }
    None => {
      let selector = parse_selectors(selectors)?;
      let resources = client
        .list_resources(kind, selector, scope.unwrap_or_default())
        .await
        .map_err(|source| ShellError::ListResources {
          kind: kind_name.clone(),
          source,
        })?;
      render_list(kind, &resources, &local_node_id().unwrap_or_default());
      println!("total: {}", resources.len());
    }
  }

  Ok(())
}

pub async fn describe_resource(
  client_config: &ClientConfig, resource: &str, name: &str,
) -> Result<(), ShellError> {
  let client = connect_cluster(client_config).await?;
  let kind = parse_resource_kind(resource)?;
  let kind_name = resource_name(kind);

  let resource = client
    .describe_resource(kind, name)
    .await
    .map_err(|source| ShellError::DescribeResource {
      kind: kind_name.clone(),
      id: name.to_string(),
      source,
    })?
    .ok_or_else(|| ShellError::ResourceNotFound {
      kind: kind_name.clone(),
      id: name.to_string(),
    })?;

  render_resource(&resource, kind, &local_node_id().unwrap_or_default(), false);
  Ok(())
}

pub async fn register(
  client_config: &ClientConfig, id: String, address: String,
) -> Result<(), ShellError> {
  let client = connect_cluster(client_config).await?;
  let node = SimpleNode {
    id: id.clone(),
    address,
    labels: HashMap::new(),
    annotations: HashMap::new(),
  };
  client.register(&node).await.map_err(ShellError::Register)?;
  println!("registered node {}", id.cyan());
  Ok(())
}

pub fn init_cluster(key: Option<String>) -> Result<(), ShellError> {
  let cluster_key = match key {
    Some(hex) => ClusterKey::from_hex(&hex).map_err(ShellError::ClusterKey)?,
    None => ClusterKey::generate().map_err(ShellError::ClusterKey)?,
  };

  let path = default_cluster_key_path();
  cluster_key.save(&path).map_err(ShellError::ClusterKey)?;
  println!(
    "initialized cluster with key {}",
    cluster_key.to_hex().cyan()
  );
  println!("key stored at: {}", path.display());
  Ok(())
}

pub async fn join_cluster(
  client_config: &ClientConfig, peer: String, key: String,
) -> Result<(), ShellError> {
  let daemon_config = load_daemon_config()?;
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .map_err(ShellError::TlsLoad)?;
  let client = ClusterRpcClient::connect(&peer, tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: peer.clone(),
      source,
    })?;

  let node = SimpleNode {
    id: daemon_config.node.id.clone(),
    address: daemon_config.node.address.clone(),
    labels: HashMap::new(),
    annotations: HashMap::new(),
  };

  client.join(&node, &key).await.map_err(ShellError::Join)?;

  let local_client = connect_cluster(client_config).await?;
  local_client
    .set_primary_endpoint(&peer)
    .await
    .map_err(ShellError::SetPrimary)?;

  println!(
    "node {} joined cluster through {}",
    daemon_config.node.id.cyan(),
    peer.cyan()
  );
  Ok(())
}

pub async fn leave_cluster(client_config: &ClientConfig) -> Result<(), ShellError> {
  let daemon_config = load_daemon_config()?;
  let client = connect_cluster(client_config).await?;
  client
    .leave(&daemon_config.node.id)
    .await
    .map_err(ShellError::Leave)?;
  println!(
    "node {} is leaving the cluster",
    daemon_config.node.id.cyan()
  );
  Ok(())
}

pub fn show_key() -> Result<(), ShellError> {
  let path = default_cluster_key_path();
  if !path.is_file() {
    return Err(ShellError::ClusterKeyNotFound);
  }

  let key = ClusterKey::load(&path).map_err(ShellError::ClusterKey)?;
  println!("{}", key.to_hex());
  Ok(())
}

async fn connect_cluster(client_config: &ClientConfig) -> Result<ClusterRpcClient, ShellError> {
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .map_err(ShellError::TlsLoad)?;
  ClusterRpcClient::connect(&client_config.api_address, tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: client_config.api_address.clone(),
      source,
    })
}

fn load_daemon_config() -> Result<DaemonConfig, ShellError> {
  let path = default_daemon_config_path().ok_or(ShellError::ConfigNotFound)?;
  DaemonConfig::from_file(&path).map_err(Into::into)
}

fn local_node_id() -> Option<String> {
  load_daemon_config().map(|config| config.node.id).ok()
}

fn parse_resource_kind(raw: &str) -> Result<ResourceKind, ShellError> {
  match raw.to_ascii_lowercase().as_str() {
    "node" | "nodes" | "no" => Ok(ResourceKind::Node),
    "session" | "sessions" | "sess" => Ok(ResourceKind::Session),
    "memory" | "memories" | "mem" => Ok(ResourceKind::Memory),
    "skill" | "skills" | "sk" => Ok(ResourceKind::Skill),
    "rule" | "rules" | "ru" => Ok(ResourceKind::Rule),
    "workspace" | "workspaces" | "ws" => Ok(ResourceKind::Workspace),
    _ => Err(ShellError::UnknownResourceKind(raw.to_string())),
  }
}

fn resource_name(kind: ResourceKind) -> String {
  match kind {
    ResourceKind::Node => "node",
    ResourceKind::Session => "session",
    ResourceKind::Memory => "memory",
    ResourceKind::Skill => "skill",
    ResourceKind::Rule => "rule",
    ResourceKind::Workspace => "workspace",
  }
  .to_string()
}

fn parse_selectors(raw: &[String]) -> Result<HashMap<String, String>, ShellError> {
  let mut selector = HashMap::new();
  for item in raw {
    let (key, value) = item
      .split_once('=')
      .ok_or_else(|| ShellError::InvalidSelector(item.clone()))?;
    selector.insert(key.to_string(), value.to_string());
  }
  Ok(selector)
}

fn format_degree(degree: &[String]) -> String {
  if degree.is_empty() {
    "-".to_string()
  } else {
    degree.join(", ")
  }
}

fn render_list(kind: ResourceKind, resources: &[ProtoResource], local_id: &str) {
  match kind {
    ResourceKind::Node => render_node_list(resources, local_id),
    _ => render_generic_list(resources),
  }
}

fn render_node_list(resources: &[ProtoResource], local_id: &str) {
  println!(
    "{}  {}  {}  {}",
    "NODE ID".bold().underline(),
    "STATE".bold().underline(),
    "IN".bold().underline(),
    "OUT".bold().underline()
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
    println!(
      "{}{}  {}  {}  {}",
      marker,
      id,
      node.state,
      format_degree(&node.in_degree),
      format_degree(&node.out_degree),
    );
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

fn render_resource(resource: &ProtoResource, kind: ResourceKind, local_id: &str, compact: bool) {
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
    println!("  in-degree:      {}", format_degree(&node.in_degree));
    println!("  out-degree:     {}", format_degree(&node.out_degree));
  } else {
    println!("{}{}", node.id.bold().cyan(), marker);
    println!("  address:        {}", node.address);
    println!("  state:          {}", node.state);
    println!("  incarnation:    {}", node.incarnation);
    println!("  heartbeat:      {}", node.heartbeat);
    println!("  last heartbeat: {}", node.last_heartbeat_unix_ms);
    println!("  in-degree:      {}", format_degree(&node.in_degree));
    println!("  out-degree:     {}", format_degree(&node.out_degree));
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

#[derive(Debug, Clone)]
struct SimpleNode {
  id: String,
  address: String,
  labels: HashMap<String, String>,
  annotations: HashMap<String, String>,
}

impl NodeInfo for SimpleNode {
  fn id(&self) -> &str {
    &self.id
  }

  fn address(&self) -> &str {
    &self.address
  }

  fn labels(&self) -> HashMap<String, String> {
    self.labels.clone()
  }

  fn annotations(&self) -> HashMap<String, String> {
    self.annotations.clone()
  }
}
