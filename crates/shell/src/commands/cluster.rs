use std::collections::HashMap;

use anyhow::Context;
use lycoris_api::{ClusterRpcClient, tls::load_client_tls};
use lycoris_config::{ClientConfig, NodeInfo};
use owo_colors::OwoColorize;

pub async fn list_nodes(client_config: &ClientConfig, selectors: &[String]) -> anyhow::Result<()> {
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .with_context(|| "failed to load client TLS material")?;
  let client = ClusterRpcClient::connect(&client_config.api_address, tls)
    .await
    .with_context(|| format!("failed to connect to {}", client_config.api_address))?;

  let selector = parse_selectors(selectors)?;
  let response = client
    .list_nodes(selector)
    .await
    .context("failed to list cluster nodes")?;

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
) -> anyhow::Result<()> {
  let tls = load_client_tls(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )
  .with_context(|| "failed to load client TLS material")?;
  let client = ClusterRpcClient::connect(&client_config.api_address, tls)
    .await
    .with_context(|| format!("failed to connect to {}", client_config.api_address))?;

  let node = SimpleNode {
    id: id.clone(),
    address,
  };
  client
    .register(&node)
    .await
    .context("failed to register node")?;
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

fn parse_selectors(raw: &[String]) -> anyhow::Result<HashMap<String, String>> {
  let mut selector = HashMap::new();
  for item in raw {
    let (key, value) = item
      .split_once('=')
      .with_context(|| format!("invalid selector '{item}', expected key=value"))?;
    selector.insert(key.to_string(), value.to_string());
  }
  Ok(selector)
}
