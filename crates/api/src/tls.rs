//! TLS helpers for gRPC clients.

use std::path::Path;

use tonic::transport::{Certificate, ClientTlsConfig, Identity};

/// Load a client TLS config from PEM files.
///
/// Returns an error if any of the files cannot be read.
pub fn load_client_tls<P>(
  cert_path: P, key_path: P, ca_path: P,
) -> Result<ClientTlsConfig, std::io::Error>
where
  P: AsRef<Path>, {
  let cert = std::fs::read_to_string(cert_path.as_ref())?;
  let key = std::fs::read_to_string(key_path.as_ref())?;
  let ca = std::fs::read_to_string(ca_path.as_ref())?;

  Ok(
    ClientTlsConfig::new()
      .identity(Identity::from_pem(cert, key))
      .ca_certificate(Certificate::from_pem(ca)),
  )
}

/// Load a client TLS config from PEM file contents.
pub fn client_tls_from_pems(cert_pem: String, key_pem: String, ca_pem: String) -> ClientTlsConfig {
  ClientTlsConfig::new()
    .identity(Identity::from_pem(cert_pem, key_pem))
    .ca_certificate(Certificate::from_pem(ca_pem))
}
