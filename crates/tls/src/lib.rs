#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod generate;

use std::path::Path;

pub use generate::ensure_tls_bundle;
use thiserror::Error;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// Install the rustls ring crypto provider as the process default.
///
/// This must be called before any TLS connection is established. The call is
/// idempotent: a provider that is already installed — by an earlier call or
/// by the embedding process — is kept and reported as success, so callers
/// can (and should) treat any returned error as fatal.
pub fn install_crypto_provider() -> Result<(), std::sync::Arc<rustls::crypto::CryptoProvider>> {
  if rustls::crypto::CryptoProvider::get_default().is_some() {
    return Ok(());
  }
  match rustls::crypto::ring::default_provider().install_default() {
    Ok(()) => Ok(()),
    // A concurrent installation won the race: a provider is present, which is
    // all this function guarantees.
    Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => Ok(()),
    Err(provider) => Err(provider),
  }
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
  /// A certificate stored on disk could not be parsed; the message carries
  /// the underlying PEM/DER/ASN.1 detail.
  #[error("failed to parse a stored certificate: {0}")]
  Parse(String),
  #[error("invalid advertise address '{0}': expected https://<host>:<port>")]
  InvalidAddress(String),
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;
  use crate::generate::ensure_tls_bundle;

  #[test]
  fn install_crypto_provider_is_idempotent() {
    install_crypto_provider().unwrap();
    install_crypto_provider().unwrap();
  }

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
