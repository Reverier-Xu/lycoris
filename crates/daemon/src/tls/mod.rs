pub mod generate;

use std::path::Path;

pub use generate::ensure_tls_bundle;
use thiserror::Error;
use tonic::transport::{Certificate, Identity};

#[derive(Debug, Clone)]
pub struct TlsBundle {
  pub identity: Identity,
  pub ca: Certificate,
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
  #[error("failed to generate key pair")]
  KeyGeneration,
}
