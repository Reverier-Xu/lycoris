use std::{
  collections::{HashMap, HashSet},
  error::Error,
  sync::Arc,
  time::Duration,
};

use lycoris_client::{ClientError, PeerClient};
use lycoris_storage::NodeDomain;
use lycoris_tls::TlsBundle;
use tokio::{sync::Mutex, time::timeout};

/// A cache of peer gRPC channels with simple target selection.
///
/// `PeerPool` owns the TLS client configuration and a map of already-connected
/// channels so that anti-entropy, SWIM probes, and push forwarding can reuse
/// open connections.
#[derive(Debug, Clone)]
pub struct PeerPool {
  node: NodeDomain,
  tls: TlsBundle,
  clients: Arc<Mutex<HashMap<String, PeerClient>>>,
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

impl PeerPool {
  pub fn new(node: NodeDomain, tls_bundle: &TlsBundle) -> Self {
    Self {
      node,
      tls: tls_bundle.clone(),
      clients: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  /// Access the underlying node storage domain.
  pub(crate) fn node(&self) -> &NodeDomain {
    &self.node
  }

  /// Return a connected `PeerClient`, reusing a cached channel when possible.
  pub async fn connect(&self, address: &str) -> Result<PeerClient, ClientError> {
    {
      let clients = self.clients.lock().await;
      if let Some(client) = clients.get(address) {
        return Ok(client.clone());
      }
    }

    let connect = PeerClient::connect(address, &self.tls);
    let client = match timeout(CONNECT_TIMEOUT, connect).await {
      Ok(Ok(client)) => client,
      Ok(Err(error)) => {
        tracing::debug!(%address, %error, "failed to connect to peer");
        if let Some(source) = error.source() {
          tracing::debug!(%address, %source, "peer connection error source");
        }
        return Err(error);
      }
      Err(_) => {
        return Err(ClientError::Io(std::io::Error::new(
          std::io::ErrorKind::TimedOut,
          "peer connection timed out",
        )));
      }
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

  /// Return candidate peer addresses for sync/gossip, excluding the local
  /// address.
  pub fn targets(&self, local_address: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();

    if let Ok(Some(primary)) = self.node.peers().get_primary()
      && primary != local_address
    {
      seen.insert(primary.clone());
      targets.push(primary);
    }

    if let Ok(fallbacks) = self.node.peers().fallback_addresses() {
      for peer in fallbacks {
        if peer != local_address && seen.insert(peer.clone()) {
          targets.push(peer);
        }
      }
    }

    targets
  }
}
