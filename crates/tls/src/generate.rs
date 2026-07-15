use std::path::Path;

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tonic::transport::{Certificate, Identity};

use crate::{TlsBundle, TlsError};

const CA_SUBJECT: &str = "lycoris-cluster-ca";

/// Ensure a usable TLS bundle exists at the configured paths.
///
/// - If the CA certificate does not exist, a new CA keypair and certificate are
///   generated and written to disk, along with a node certificate signed by
///   that CA.
/// - If the CA certificate exists but the node certificate does not, a new node
///   certificate is generated and signed by the existing CA.
/// - Otherwise the existing files are loaded.
pub fn ensure_tls_bundle<P>(
  ca_cert_path: P, ca_key_path: P, cert_path: P, key_path: P, node_id: &str,
) -> Result<TlsBundle, TlsError>
where
  P: AsRef<Path>, {
  let ca_cert_path = ca_cert_path.as_ref();
  let ca_key_path = ca_key_path.as_ref();
  let cert_path = cert_path.as_ref();
  let key_path = key_path.as_ref();

  if !ca_cert_path.exists() {
    let (ca_cert_pem, ca_key_pem, cert_pem, key_pem) = generate_fresh_ca_and_node(node_id)?;

    write_file(ca_cert_path, ca_cert_pem.clone())?;
    write_file(ca_key_path, ca_key_pem.clone())?;
    write_file(cert_path, cert_pem.clone())?;
    write_file(key_path, key_pem.clone())?;

    return bundle_from_strings(cert_pem, key_pem, ca_cert_pem);
  }

  let ca_cert_pem = std::fs::read_to_string(ca_cert_path)?;
  let ca_key_pem = std::fs::read_to_string(ca_key_path)?;
  let ca_key = KeyPair::from_pem(&ca_key_pem)?;
  let ca_cert = reconstruct_ca(&ca_key)?;

  if !cert_path.exists() || !key_path.exists() {
    let key = KeyPair::generate()?;
    let params = node_cert_params(node_id)?;
    let cert = params.signed_by(&key, &ca_cert, &ca_key)?;
    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();
    write_file(cert_path, cert_pem.clone())?;
    write_file(key_path, key_pem.clone())?;
  }

  let cert_pem = std::fs::read_to_string(cert_path)?;
  let key_pem = std::fs::read_to_string(key_path)?;

  bundle_from_strings(cert_pem, key_pem, ca_cert_pem)
}

fn generate_fresh_ca_and_node(node_id: &str) -> Result<(String, String, String, String), TlsError> {
  let ca_key = KeyPair::generate()?;
  let mut ca_params = CertificateParams::new(vec![CA_SUBJECT.to_string()])?;
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  let ca_cert = ca_params.self_signed(&ca_key)?;

  let node_key = KeyPair::generate()?;
  let node_params = node_cert_params(node_id)?;
  let node_cert = node_params.signed_by(&node_key, &ca_cert, &ca_key)?;

  Ok((
    ca_cert.pem(),
    ca_key.serialize_pem(),
    node_cert.pem(),
    node_key.serialize_pem(),
  ))
}

fn node_cert_params(node_id: &str) -> Result<CertificateParams, TlsError> {
  let mut names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
  if !names.contains(&node_id.to_string()) {
    names.push(node_id.to_string());
  }
  Ok(CertificateParams::new(names)?)
}

fn reconstruct_ca(ca_key: &KeyPair) -> Result<rcgen::Certificate, TlsError> {
  let mut params = CertificateParams::new(vec![CA_SUBJECT.to_string()])?;
  params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  Ok(params.self_signed(ca_key)?)
}

fn write_file<P: AsRef<Path>>(path: P, content: String) -> Result<(), TlsError> {
  if let Some(parent) = path.as_ref().parent() {
    std::fs::create_dir_all(parent)?;
  }
  std::fs::write(path.as_ref(), content)?;
  Ok(())
}

fn bundle_from_strings(
  cert_pem: String, key_pem: String, ca_pem: String,
) -> Result<TlsBundle, TlsError> {
  Ok(TlsBundle {
    identity: Identity::from_pem(cert_pem, key_pem),
    ca: Certificate::from_pem(ca_pem),
  })
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  #[test]
  fn generates_ca_and_node_cert_when_missing() {
    let dir = TempDir::new().unwrap();
    let ca_cert = dir.path().join("ca.crt");
    let ca_key = dir.path().join("ca.key");
    let cert = dir.path().join("node.crt");
    let key = dir.path().join("node.key");

    let bundle = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node").unwrap();
    assert!(ca_cert.exists());
    assert!(cert.exists());
    let _ = bundle;
  }

  #[test]
  fn generates_node_cert_when_ca_exists() {
    let dir = TempDir::new().unwrap();
    let ca_cert = dir.path().join("ca.crt");
    let ca_key = dir.path().join("ca.key");
    let cert = dir.path().join("node.crt");
    let key = dir.path().join("node.key");

    let _first = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node").unwrap();

    std::fs::remove_file(&cert).unwrap();
    std::fs::remove_file(&key).unwrap();

    let _second = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node").unwrap();
    assert!(cert.exists());
  }
}
