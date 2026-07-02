use std::{collections::HashMap, path::Path};

use anyhow::Context;
use lycoris_api::ClusterRpcClient;
use lycoris_config::ClientConfig;
use owo_colors::OwoColorize;
use tonic::transport::ClientTlsConfig;

pub async fn list_nodes(client_config: &ClientConfig, selectors: &[String]) -> anyhow::Result<()> {
  let tls = load_client_tls(client_config)?;
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

fn load_client_tls(client_config: &ClientConfig) -> anyhow::Result<ClientTlsConfig> {
  let cert = read_pem(&client_config.cert).with_context(|| {
    format!(
      "failed to read client certificate from {}",
      client_config.cert
    )
  })?;
  let key = read_pem(&client_config.key)
    .with_context(|| format!("failed to read client key from {}", client_config.key))?;
  let ca = read_pem(&client_config.ca_cert).with_context(|| {
    format!(
      "failed to read CA certificate from {}",
      client_config.ca_cert
    )
  })?;

  Ok(
    ClientTlsConfig::new()
      .identity(tonic::transport::Identity::from_pem(cert, key))
      .ca_certificate(tonic::transport::Certificate::from_pem(ca)),
  )
}

fn read_pem<P: AsRef<Path>>(path: P) -> std::io::Result<String> {
  std::fs::read_to_string(path.as_ref())
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
