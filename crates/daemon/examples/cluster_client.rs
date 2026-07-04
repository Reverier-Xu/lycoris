use std::{collections::HashMap, time::Duration};

use lycoris_api::{ClusterRpcClient, tls::load_client_tls};
use lycoris_config::NodeConfig;
use lycoris_daemon::node::info::LocalNode;
use lycoris_storage::ClusterStorage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  lycoris_daemon::install_crypto_provider()
    .map_err(|e| anyhow::anyhow!("failed to install crypto provider: {e:?}"))?;

  let args: Vec<String> = std::env::args().collect();
  if args.len() != 7 {
    eprintln!(
      "usage: {} <register-addr> <query-addr> <ca> <cert> <key> <expected-id>",
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

  let tls = load_client_tls(cert_path, key_path, ca_path)?;

  let client = ClusterRpcClient::connect(register_addr, tls.clone()).await?;
  let storage_dir = std::env::temp_dir().join(format!("lycoris-client-{expected_id}"));
  std::fs::create_dir_all(&storage_dir)?;
  let storage = ClusterStorage::open(storage_dir.join("client.db"))?;
  let node = LocalNode::from_config(
    &NodeConfig {
      id: expected_id.clone(),
      address: "127.0.0.1:59999".to_string(),
    },
    storage,
  );
  client.register(&node).await?;
  println!("registered {expected_id} via {register_addr}");

  tokio::time::sleep(Duration::from_secs(2)).await;

  let client = ClusterRpcClient::connect(query_addr, tls).await?;
  let nodes = client.list_nodes(HashMap::new()).await?.nodes;
  let ids: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
  println!("nodes on {query_addr}: {ids:?}");

  if ids.contains(expected_id) {
    println!("ok: {expected_id} visible on {query_addr}");
    Ok(())
  } else {
    anyhow::bail!("{expected_id} not visible on {query_addr}");
  }
}
