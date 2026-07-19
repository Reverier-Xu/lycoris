use std::{collections::HashMap, error::Error, sync::Arc};

use lycoris_client::{ClientError, PeerClient};
use lycoris_tls::TlsBundle;
use tokio::sync::Mutex;

/// A cache of peer gRPC channels.
///
/// `PeerPool` owns the TLS client configuration and a map of already-connected
/// channels so that anti-entropy, SWIM probes, and push forwarding can reuse
/// open connections. Peer endpoint bookkeeping (primary/fallback selection,
/// seen/attempt marks) is owned by the callers in `crate::sync`, which hold
/// the storage node domain directly.
///
/// The pool also carries the daemon's own cluster key: clients it hands out
/// present that key on the cluster-key-guarded services (`Cluster`,
/// `Extension`), which is what lets extension forwarding pass the receiving
/// node's admission check with the *forwarding* node's identity (extension
/// system design, sections 7 and 10).
#[derive(Debug, Clone)]
pub struct PeerPool {
  tls: TlsBundle,
  cluster_key: Option<String>,
  clients: Arc<Mutex<HashMap<String, PeerClient>>>,
}

impl PeerPool {
  pub fn new(tls_bundle: &TlsBundle, cluster_key: Option<String>) -> Self {
    Self {
      tls: tls_bundle.clone(),
      cluster_key,
      clients: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  /// Return a connected `PeerClient`, reusing a cached channel when possible.
  pub async fn connect(&self, address: &str) -> Result<PeerClient, ClientError> {
    {
      let clients = self.clients.lock().await;
      if let Some(client) = clients.get(address) {
        return Ok(client.clone());
      }
    }

    // No extra timeout wrapper here: `PeerClient::connect` already bounds the
    // handshake with the endpoint's 3s `connect_timeout`, so an outer timeout
    // of the same length could never fire first.
    let client = match PeerClient::connect(address, &self.tls).await {
      Ok(client) => client,
      Err(error) => {
        tracing::debug!(%address, %error, "failed to connect to peer");
        if let Some(source) = error.source() {
          tracing::debug!(%address, %source, "peer connection error source");
        }
        return Err(error);
      }
    };
    let client = match &self.cluster_key {
      Some(key) => client.with_cluster_key(key.clone()),
      None => client,
    };

    let mut clients = self.clients.lock().await;
    clients.insert(address.to_string(), client.clone());
    Ok(client)
  }

  /// Remove a cached channel (e.g., after an error).
  pub async fn remove(&self, address: &str) {
    let mut clients = self.clients.lock().await;
    clients.remove(address);
  }
}
