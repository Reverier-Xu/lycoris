use std::{collections::HashMap, time::Duration};

use lycoris_client::{ClientError, ClusterClient};
use lycoris_proto::node::NodeInfo;
use lycoris_tls::{install_crypto_provider, load_tls_bundle};
use thiserror::Error;

#[derive(Debug, Error)]
enum ExampleError {
  #[error("failed to install rustls crypto provider: {0:?}")]
  CryptoProvider(std::sync::Arc<rustls::crypto::CryptoProvider>),
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("tls error: {0}")]
  Tls(#[from] lycoris_tls::TlsError),
  #[error("cluster client error: {0}")]
  Cluster(#[from] ClientError),
  #[error("{0} not visible on {1}")]
  NotVisible(String, String),
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
  install_crypto_provider().map_err(ExampleError::CryptoProvider)?;

  let args: Vec<String> = std::env::args().collect();
  if args.len() != 8 {
    eprintln!(
      "usage: {} <register-addr> <query-addr> <ca> <cert> <key> <expected-id> <cluster-key>",
      args[0]
    );
    std::process::exit(1);
  }

  let register_addr = &args[1];
  let query_addr = &args[2];
  let ca_path = &args[3];
  let cert_path = &args[4];
  let key_path = &args[5];
  let expected_id = &args[6];
  let cluster_key = &args[7];

  let tls = load_tls_bundle(cert_path, key_path, ca_path)?;

  let mut client = ClusterClient::connect(register_addr, &tls)
    .await?
    .with_cluster_key(cluster_key.to_string());
  let node = NodeInfo::new(
    expected_id.clone(),
    "127.0.0.1:59999".to_string(),
    HashMap::new(),
    HashMap::new(),
  );
  client.register(node).await?;
  println!("registered {expected_id} via {register_addr}");

  tokio::time::sleep(Duration::from_secs(2)).await;

  let mut client = ClusterClient::connect(query_addr, &tls)
    .await?
    .with_cluster_key(cluster_key.to_string());
  let resources = client
    .list_resources(
      lycoris_proto::node::ResourceKind::Node,
      HashMap::new(),
      lycoris_proto::node::ResourceScope::Unspecified,
    )
    .await?;
  let ids: Vec<String> = resources
    .into_iter()
    .filter_map(|resource| match resource.body {
      Some(lycoris_proto::node::resource::Body::Node(lycoris_proto::node::NodeBody {
        node: Some(node),
      })) => Some(node.id),
      _ => None,
    })
    .collect();
  println!("nodes on {query_addr}: {ids:?}");

  if ids.contains(expected_id) {
    println!("ok: {expected_id} visible on {query_addr}");
    Ok(())
  } else {
    Err(ExampleError::NotVisible(
      expected_id.clone(),
      query_addr.clone(),
    ))
  }
}
