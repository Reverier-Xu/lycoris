//! Cluster certificate fixtures: a test CA and node identities signed by
//! it, materialized on disk the way the daemon's TLS configuration expects.

use std::path::{Path, PathBuf};

use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use tempfile::TempDir;

/// One node identity: certificate and private key paths.
pub struct NodeIdentity {
  pub cert: PathBuf,
  pub key: PathBuf,
}

/// Certificate fixtures: the test CA plus every node identity signed by it.
pub struct TestCerts {
  pub ca_cert: PathBuf,
  pub ca_key: PathBuf,
  pub nodes: Vec<NodeIdentity>,
}

/// Write a test CA (`ca.crt`/`ca.key`) and `node_count` node identities
/// (`node{i}.crt`/`node{i}.key`, each carrying a `127.0.0.1` SAN) into
/// `dir`.
pub fn write_test_certs(dir: &Path, node_count: usize) -> TestCerts {
  let ca_key = KeyPair::generate().unwrap_or_else(|err| panic!("generate the CA key: {err}"));
  let mut ca_params = CertificateParams::new(vec!["lycoris-test-ca".to_string()])
    .unwrap_or_else(|err| panic!("build the CA params: {err}"));
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  let issuer = Issuer::from_params(&ca_params, &ca_key);
  let ca_cert = ca_params
    .self_signed(&ca_key)
    .unwrap_or_else(|err| panic!("self-sign the CA: {err}"));

  let ca_cert_path = dir.join("ca.crt");
  let ca_key_path = dir.join("ca.key");
  std::fs::write(&ca_cert_path, ca_cert.pem())
    .unwrap_or_else(|err| panic!("write the CA certificate: {err}"));
  std::fs::write(&ca_key_path, ca_key.serialize_pem())
    .unwrap_or_else(|err| panic!("write the CA key: {err}"));

  let mut nodes = Vec::with_capacity(node_count);
  for i in 0..node_count {
    let key = KeyPair::generate().unwrap_or_else(|err| panic!("generate a node key: {err}"));
    let params = CertificateParams::new(vec!["127.0.0.1".to_string()])
      .unwrap_or_else(|err| panic!("build the node params: {err}"));
    let cert = params
      .signed_by(&key, &issuer)
      .unwrap_or_else(|err| panic!("sign the node certificate: {err}"));

    let cert_path = dir.join(format!("node{i}.crt"));
    let key_path = dir.join(format!("node{i}.key"));
    std::fs::write(&cert_path, cert.pem())
      .unwrap_or_else(|err| panic!("write the node certificate: {err}"));
    std::fs::write(&key_path, key.serialize_pem())
      .unwrap_or_else(|err| panic!("write the node key: {err}"));

    nodes.push(NodeIdentity {
      cert: cert_path,
      key: key_path,
    });
  }

  TestCerts {
    ca_cert: ca_cert_path,
    ca_key: ca_key_path,
    nodes,
  }
}

/// Generate the fixtures into a fresh temporary directory, returning the
/// directory alongside them (dropping it deletes the fixtures).
pub fn temp_test_certs(node_count: usize) -> (TempDir, TestCerts) {
  let dir = TempDir::new().unwrap_or_else(|err| panic!("create the fixture temp dir: {err}"));
  let certs = write_test_certs(dir.path(), node_count);
  (dir, certs)
}
