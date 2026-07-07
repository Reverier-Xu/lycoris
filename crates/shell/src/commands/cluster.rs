use std::collections::HashMap;

use lycoris_api::{ClusterRpcClient, tls::load_client_tls};
use lycoris_config::{ClientConfig, NodeInfo};
use owo_colors::OwoColorize;

use crate::error::ShellError;

pub async fn list_nodes(
  client_config: &ClientConfig, selectors: &[String],
) -> Result<(), ShellError> {
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .map_err(ShellError::TlsLoad)?;
  let client = ClusterRpcClient::connect(&client_config.api_address, tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: client_config.api_address.clone(),
      source,
    })?;

  let selector = parse_selectors(selectors)?;
  let response = client
    .list_nodes(selector)
    .await
    .map_err(ShellError::ListNodes)?;

  println!("{}", "NODE ID".bold().underline());
  for node in &response.nodes {
    println!(
      "{}\n  address: {}\n  labels: {:?}\n  annotations: {:?}\n  last heartbeat: {}",
      node.id.cyan(),
      node.address,
      node.labels,
      node.annotations,
      node.last_heartbeat_unix_ms
    );
  }
  println!("total: {}", response.nodes.len());
  Ok(())
}

pub async fn register(
  client_config: &ClientConfig, id: String, address: String,
) -> Result<(), ShellError> {
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .map_err(ShellError::TlsLoad)?;
  let client = ClusterRpcClient::connect(&client_config.api_address, tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: client_config.api_address.clone(),
      source,
    })?;

  let node = SimpleNode {
    id: id.clone(),
    address,
  };
  client.register(&node).await.map_err(ShellError::Register)?;
  println!("registered node {}", id.cyan());
  Ok(())
}

#[derive(Debug, Clone)]
struct SimpleNode {
  id: String,
  address: String,
}

impl NodeInfo for SimpleNode {
  fn id(&self) -> &str {
    &self.id
  }

  fn address(&self) -> &str {
    &self.address
  }

  fn labels(&self) -> HashMap<String, String> {
    HashMap::new()
  }

  fn annotations(&self) -> HashMap<String, String> {
    HashMap::new()
  }
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
