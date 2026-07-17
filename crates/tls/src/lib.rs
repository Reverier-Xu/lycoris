#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod generate;

use std::path::Path;

pub use generate::ensure_tls_bundle;
use thiserror::Error;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// Install the rustls ring crypto provider as the process default.
///
/// This must be called before any TLS connection is established. Subsequent
/// calls return the already-installed provider and can be ignored by callers
/// that only need to ensure the provider is present.
pub fn install_crypto_provider() -> Result<(), std::sync::Arc<rustls::crypto::CryptoProvider>> {
  rustls::crypto::ring::default_provider().install_default()
}

#[derive(Debug, Clone)]
pub struct TlsBundle {
  pub identity: Identity,
  pub ca: Certificate,
}

impl TlsBundle {
  /// Return a client TLS config using this bundle's identity and CA.
  pub fn client_config(&self) -> ClientTlsConfig {
    ClientTlsConfig::new()
      .identity(self.identity.clone())
      .ca_certificate(self.ca.clone())
  }

  /// Return a server TLS config using this bundle's identity and client CA.
  pub fn server_config(&self) -> ServerTlsConfig {
    ServerTlsConfig::new()
      .identity(self.identity.clone())
      .client_ca_root(self.ca.clone())
  }
}

/// Load a node identity (cert + key) and the cluster CA certificate from disk.
pub fn load_tls_bundle<P>(cert_path: P, key_path: P, ca_path: P) -> Result<TlsBundle, TlsError>
where
  P: AsRef<Path>, {
  let cert = std::fs::read_to_string(cert_path.as_ref())?;
  let key = std::fs::read_to_string(key_path.as_ref())?;
  let ca = std::fs::read_to_string(ca_path.as_ref())?;

  Ok(TlsBundle {
    identity: Identity::from_pem(cert, key),
    ca: Certificate::from_pem(ca),
  })
}

#[derive(Debug, Error)]
pub enum TlsError {
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("certificate generation error: {0}")]
  Generation(#[from] rcgen::Error),
  #[error("invalid advertise address '{0}': expected https://<host>:<port>")]
  InvalidAddress(String),
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;
  use crate::generate::ensure_tls_bundle;

  #[test]
  fn load_tls_bundle_round_trip() {
    let dir = TempDir::new().unwrap();
    let ca_cert = dir.path().join("ca.crt");
    let ca_key = dir.path().join("ca.key");
    let cert = dir.path().join("node.crt");
    let key = dir.path().join("node.key");

    let bundle = ensure_tls_bundle(
      &ca_cert,
      &ca_key,
      &cert,
      &key,
      "test-node",
      "https://127.0.0.1:5001",
    )
    .unwrap();
    let _client = load_tls_bundle(&cert, &key, &ca_cert)
      .unwrap()
      .client_config();
    let _server = bundle.server_config();
  }
}
