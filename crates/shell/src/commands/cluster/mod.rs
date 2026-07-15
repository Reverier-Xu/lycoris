use lycoris_client::ClusterClient;
use lycoris_config::{ClientConfig, DaemonConfig};
use lycoris_core::{
  ClusterKey, SimpleNode, default_cluster_key_path, paths::default_daemon_config_path,
};
use lycoris_tls::load_client_tls;
use owo_colors::OwoColorize;

use crate::error::ShellError;

mod parse;
mod render;

pub async fn get_resources(
  client_config: &ClientConfig, resource: &str, name: Option<String>, selectors: &[String],
  scope: Option<String>,
) -> Result<(), ShellError> {
  let mut client = connect_cluster(client_config).await?;
  let kind = parse::parse_resource_kind(resource)?;
  let kind_name = parse::resource_name(kind);

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
      render::render_resource(&resource, kind, &local_node_id().unwrap_or_default(), true);
    }
    None => {
      let selector = parse::parse_selectors(selectors)?;
      let resources = client
        .list_resources(kind, selector, scope.unwrap_or_default())
        .await
        .map_err(|source| ShellError::ListResources {
          kind: kind_name.clone(),
          source,
        })?;
      render::render_list(kind, &resources, &local_node_id().unwrap_or_default());
      println!("total: {}", resources.len());
    }
  }

  Ok(())
}

pub async fn describe_resource(
  client_config: &ClientConfig, resource: &str, name: &str,
) -> Result<(), ShellError> {
  let mut client = connect_cluster(client_config).await?;
  let kind = parse::parse_resource_kind(resource)?;
  let kind_name = parse::resource_name(kind);

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

  render::render_resource(&resource, kind, &local_node_id().unwrap_or_default(), false);
  Ok(())
}

pub async fn register(
  client_config: &ClientConfig, id: String, address: String, key: String,
) -> Result<(), ShellError> {
  let key = key.trim().to_string();
  let mut client = connect_cluster(client_config).await?;
  let node = SimpleNode::new(
    id.clone(),
    address,
    std::collections::HashMap::new(),
    std::collections::HashMap::new(),
  );
  client
    .register(&node, &key)
    .await
    .map_err(ShellError::Register)?;
  println!("registered node {}", id.cyan());
  Ok(())
}

pub fn init_cluster(key: Option<String>) -> Result<(), ShellError> {
  let cluster_key = match key {
    Some(hex) => ClusterKey::from_hex(hex.trim()).map_err(ShellError::ClusterKey)?,
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
  let key = key.trim().to_string();
  let daemon_config = load_daemon_config()?;
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .map_err(ShellError::TlsLoad)?;
  let mut client = ClusterClient::connect_with_tls(&peer, tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: peer.clone(),
      source,
    })?;

  let node = SimpleNode::new(
    daemon_config.node.id.clone(),
    daemon_config.node.address.clone(),
    std::collections::HashMap::new(),
    std::collections::HashMap::new(),
  );

  client.join(&node, &key).await.map_err(ShellError::Join)?;

  let mut local_client = connect_cluster(client_config).await?;
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
  let mut client = connect_cluster(client_config).await?;
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

async fn connect_cluster(client_config: &ClientConfig) -> Result<ClusterClient, ShellError> {
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .map_err(ShellError::TlsLoad)?;
  ClusterClient::connect_with_tls(&client_config.api_address, tls)
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
