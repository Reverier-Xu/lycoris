use std::{path::Path, time::Duration};

use rcgen::{
  BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, SanType,
};
use time::OffsetDateTime;
use tonic::transport::{Certificate, Identity};

use crate::{TlsBundle, TlsError};

const CA_SUBJECT: &str = "lycoris-cluster-ca";
/// Node certificates are issued for one year and renewed automatically (see
/// `needs_renewal`), so clusters keep working without manual rotation.
const NODE_CERT_VALIDITY: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// The cluster CA is issued for ten years. Renewal re-signs the CA with the
/// same key and subject, so previously issued node certificates stay
/// verifiable and peer trust anchors keep working.
const CA_CERT_VALIDITY: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
/// A certificate is renewed once less than a third of its total validity
/// remains. A relative threshold scales with the certificate's own lifetime:
/// short-lived externally provisioned certificates are only renewed when they
/// are genuinely close to expiry.
const RENEWAL_THRESHOLD_DIVISOR: i64 = 3;
/// Newly issued certificates are backdated to tolerate clock skew between
/// cluster nodes.
const NOT_BEFORE_SKEW: Duration = Duration::from_secs(60 * 60);

/// Ensure a usable TLS bundle exists at the configured paths.
///
/// - If the CA certificate does not exist, a new CA keypair and certificate are
///   generated and written to disk, along with a node certificate signed by
///   that CA.
/// - Otherwise the stored CA certificate is parsed and used as the issuer for
///   any newly signed node certificate. A CA or node certificate that is
///   expired (or has less than a third of its validity left) is renewed in
///   place; the CA keeps its key and subject, so existing trust anchors and
///   issued node certificates stay valid.
/// - The node certificate is re-issued when it is missing, unparsable, due for
///   renewal, or when its SANs do not cover the host of `advertise_addr` — the
///   name cluster peers verify this node against.
pub fn ensure_tls_bundle<P>(
  ca_cert_path: P, ca_key_path: P, cert_path: P, key_path: P, node_id: &str, advertise_addr: &str,
) -> Result<TlsBundle, TlsError>
where
  P: AsRef<Path>, {
  let ca_cert_path = ca_cert_path.as_ref();
  let ca_key_path = ca_key_path.as_ref();
  let cert_path = cert_path.as_ref();
  let key_path = key_path.as_ref();
  let host = advertise_host(advertise_addr)?;

  if !ca_cert_path.exists() {
    let (ca_cert_pem, ca_key_pem, cert_pem, key_pem) = generate_fresh_ca_and_node(node_id, &host)?;

    write_file(ca_cert_path, ca_cert_pem.clone())?;
    write_private_file(ca_key_path, &ca_key_pem)?;
    write_file(cert_path, cert_pem.clone())?;
    write_private_file(key_path, &key_pem)?;

    return bundle_from_strings(cert_pem, key_pem, ca_cert_pem);
  }

  let ca_key_pem = std::fs::read_to_string(ca_key_path)?;
  let ca_key = KeyPair::from_pem(&ca_key_pem)?;
  // Parse the actual stored CA certificate instead of reconstructing it from
  // constants: the issuer DN embedded in freshly signed node certificates must
  // match the CA certificate every peer trusts, exactly as stored.
  let mut ca_params = CertificateParams::from_ca_cert_pem(&std::fs::read_to_string(ca_cert_path)?)?;

  let now = OffsetDateTime::now_utc();
  if needs_renewal(&ca_params, now) {
    tracing::info!(
      path = %ca_cert_path.display(),
      "cluster CA certificate is expired or close to expiry; renewing it with the same key"
    );
    set_validity(&mut ca_params, now, CA_CERT_VALIDITY);
    let renewed = ca_params.clone().self_signed(&ca_key)?;
    write_file(ca_cert_path, renewed.pem())?;
  }

  // `signed_by` only reads the issuer's subject DN, key identifier and key
  // usages, so materializing the parsed CA params with the CA key yields a
  // faithful issuer handle; the on-disk CA file is untouched unless it was
  // renewed above.
  let issuer = ca_params.self_signed(&ca_key)?;

  let mut reissue = !cert_path.exists() || !key_path.exists();
  if !reissue {
    // `from_ca_cert_pem` performs no CA-specific validation; here it is used
    // purely as an X.509 parser to read the existing certificate's validity
    // and SANs.
    match CertificateParams::from_ca_cert_pem(&std::fs::read_to_string(cert_path)?) {
      Ok(existing) if needs_renewal(&existing, now) => {
        tracing::info!(
          path = %cert_path.display(),
          "node certificate is expired or close to expiry; re-issuing"
        );
        reissue = true;
      }
      Ok(existing) if !san_covers(&existing.subject_alt_names, &host) => {
        tracing::info!(
          path = %cert_path.display(),
          host,
          "node certificate does not cover the advertise address; re-issuing"
        );
        reissue = true;
      }
      Ok(_) => {}
      Err(error) => {
        tracing::warn!(
          path = %cert_path.display(),
          %error,
          "node certificate cannot be parsed; re-issuing"
        );
        reissue = true;
      }
    }
  }

  if reissue {
    let key = KeyPair::generate()?;
    let params = node_cert_params(node_id, &host, now)?;
    let cert = params.signed_by(&key, &issuer, &ca_key)?;
    write_file(cert_path, cert.pem())?;
    write_private_file(key_path, &key.serialize_pem())?;
  }

  let cert_pem = std::fs::read_to_string(cert_path)?;
  let key_pem = std::fs::read_to_string(key_path)?;
  let ca_cert_pem = std::fs::read_to_string(ca_cert_path)?;

  bundle_from_strings(cert_pem, key_pem, ca_cert_pem)
}

/// Extract the host part of an `https://host:port` advertise address. Cluster
/// peers verify the node certificate against exactly this name: tonic derives
/// the expected TLS server name from the URI host when no explicit domain is
/// configured, so the name must appear in the certificate SANs.
fn advertise_host(address: &str) -> Result<String, TlsError> {
  let uri: tonic::codegen::http::Uri = address
    .parse()
    .map_err(|_| TlsError::InvalidAddress(address.to_string()))?;
  // Cluster addresses are required to be `https://` URLs (see config
  // validation); without the scheme the URI parser would silently accept
  // authority-form strings like `node-0:5001`.
  if uri.scheme_str() != Some("https") {
    return Err(TlsError::InvalidAddress(address.to_string()));
  }
  let host = uri
    .host()
    .ok_or_else(|| TlsError::InvalidAddress(address.to_string()))?;
  // `http::Uri::host()` keeps the brackets of IPv6 literals; rcgen expects
  // the bare address to build an IP SAN.
  Ok(
    host
      .trim_start_matches('[')
      .trim_end_matches(']')
      .to_string(),
  )
}

fn generate_fresh_ca_and_node(
  node_id: &str, advertise_host: &str,
) -> Result<(String, String, String, String), TlsError> {
  let now = OffsetDateTime::now_utc();

  let ca_key = KeyPair::generate()?;
  let mut ca_params = CertificateParams::new(vec![CA_SUBJECT.to_string()])?;
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  ca_params.distinguished_name = common_name(CA_SUBJECT);
  set_validity(&mut ca_params, now, CA_CERT_VALIDITY);
  let ca_cert = ca_params.self_signed(&ca_key)?;

  let node_key = KeyPair::generate()?;
  let node_params = node_cert_params(node_id, advertise_host, now)?;
  let node_cert = node_params.signed_by(&node_key, &ca_cert, &ca_key)?;

  Ok((
    ca_cert.pem(),
    ca_key.serialize_pem(),
    node_cert.pem(),
    node_key.serialize_pem(),
  ))
}

fn node_cert_params(
  node_id: &str, advertise_host: &str, now: OffsetDateTime,
) -> Result<CertificateParams, TlsError> {
  // The certificate binds both identities of the node: the network name peers
  // dial (`advertise_host`), which is what rustls actually verifies, and the
  // cluster-level `node_id`, which is informational today.
  let mut names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
  for name in [node_id, advertise_host] {
    if !names.iter().any(|existing| existing == name) {
      names.push(name.to_string());
    }
  }
  let mut params = CertificateParams::new(names)?;
  params.distinguished_name = common_name(node_id);
  set_validity(&mut params, now, NODE_CERT_VALIDITY);
  Ok(params)
}

fn common_name(cn: &str) -> DistinguishedName {
  let mut name = DistinguishedName::new();
  name.push(DnType::CommonName, cn);
  name
}

fn set_validity(params: &mut CertificateParams, now: OffsetDateTime, validity: Duration) {
  params.not_before = now - NOT_BEFORE_SKEW;
  params.not_after = now + validity;
}

/// True when the certificate is expired, has a broken validity window, or has
/// less than a third of its total validity left.
fn needs_renewal(params: &CertificateParams, now: OffsetDateTime) -> bool {
  let total = params.not_after.unix_timestamp() - params.not_before.unix_timestamp();
  let remaining = params.not_after.unix_timestamp() - now.unix_timestamp();
  total <= 0 || remaining <= 0 || remaining < total / RENEWAL_THRESHOLD_DIVISOR
}

fn san_covers(sans: &[SanType], name: &str) -> bool {
  sans.iter().any(|san| match san {
    SanType::DnsName(dns) => dns.as_str().eq_ignore_ascii_case(name),
    SanType::IpAddress(ip) => name
      .parse::<std::net::IpAddr>()
      .is_ok_and(|parsed| parsed == *ip),
    _ => false,
  })
}

fn write_file<P: AsRef<Path>>(path: P, content: String) -> Result<(), TlsError> {
  if let Some(parent) = path.as_ref().parent() {
    std::fs::create_dir_all(parent)?;
  }
  std::fs::write(path.as_ref(), content)?;
  Ok(())
}

/// Write a private key file with owner-only permissions (`0o600` on unix).
fn write_private_file<P: AsRef<Path>>(path: P, content: &str) -> Result<(), TlsError> {
  lycoris_core::write_private_file(path, content.as_bytes())?;
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

  const TEST_ADDR: &str = "https://127.0.0.1:5001";

  fn cert_paths(
    dir: &TempDir,
  ) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
  ) {
    (
      dir.path().join("ca.crt"),
      dir.path().join("ca.key"),
      dir.path().join("node.crt"),
      dir.path().join("node.key"),
    )
  }

  fn parse_cert(pem_path: &Path) -> CertificateParams {
    CertificateParams::from_ca_cert_pem(&std::fs::read_to_string(pem_path).unwrap()).unwrap()
  }

  #[test]
  fn generates_ca_and_node_cert_when_missing() {
    let dir = TempDir::new().unwrap();
    let (ca_cert, ca_key, cert, key) = cert_paths(&dir);

    let bundle = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();
    assert!(ca_cert.exists());
    assert!(cert.exists());
    let _ = bundle;

    let node = parse_cert(&cert);
    assert!(san_covers(&node.subject_alt_names, "127.0.0.1"));
    assert!(san_covers(&node.subject_alt_names, "test-node"));
    assert!(!needs_renewal(&node, OffsetDateTime::now_utc()));
    let ca = parse_cert(&ca_cert);
    assert!(!needs_renewal(&ca, OffsetDateTime::now_utc()));
  }

  #[test]
  fn generates_node_cert_when_ca_exists() {
    let dir = TempDir::new().unwrap();
    let (ca_cert, ca_key, cert, key) = cert_paths(&dir);

    let _first = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();

    std::fs::remove_file(&cert).unwrap();
    std::fs::remove_file(&key).unwrap();

    let _second =
      ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();
    assert!(cert.exists());
  }

  #[test]
  fn reissues_node_cert_when_advertise_host_changes() {
    let dir = TempDir::new().unwrap();
    let (ca_cert, ca_key, cert, key) = cert_paths(&dir);

    let _ = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();
    let before = std::fs::read_to_string(&cert).unwrap();

    let _ = ensure_tls_bundle(
      &ca_cert,
      &ca_key,
      &cert,
      &key,
      "test-node",
      "https://node.example:5001",
    )
    .unwrap();
    let after = std::fs::read_to_string(&cert).unwrap();

    assert_ne!(before, after);
    let node = parse_cert(&cert);
    assert!(san_covers(&node.subject_alt_names, "node.example"));
    assert!(san_covers(&node.subject_alt_names, "127.0.0.1"));
  }

  #[test]
  fn renews_expired_node_certificate() {
    let dir = TempDir::new().unwrap();
    let (ca_cert, ca_key, cert, key) = cert_paths(&dir);

    let _ = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();

    // Replace the node certificate with an already-expired one signed by the
    // same CA.
    let ca_key_pair = KeyPair::from_pem(&std::fs::read_to_string(&ca_key).unwrap()).unwrap();
    let issuer = parse_cert(&ca_cert).self_signed(&ca_key_pair).unwrap();
    let expired_key = KeyPair::generate().unwrap();
    let mut expired_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    let now = OffsetDateTime::now_utc();
    expired_params.not_before = now - Duration::from_secs(3600);
    expired_params.not_after = now - Duration::from_secs(60);
    let expired = expired_params
      .signed_by(&expired_key, &issuer, &ca_key_pair)
      .unwrap();
    std::fs::write(&cert, expired.pem()).unwrap();
    std::fs::write(&key, expired_key.serialize_pem()).unwrap();

    let _ = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();

    let node = parse_cert(&cert);
    assert!(!needs_renewal(&node, OffsetDateTime::now_utc()));
  }

  #[test]
  fn renews_expiring_ca_with_same_key() {
    let dir = TempDir::new().unwrap();
    let (ca_cert, ca_key, cert, key) = cert_paths(&dir);

    let _ = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();
    let node_before = std::fs::read_to_string(&cert).unwrap();

    // Replace the CA certificate with an expired self-signed one using the
    // same key and subject.
    let ca_key_pair = KeyPair::from_pem(&std::fs::read_to_string(&ca_key).unwrap()).unwrap();
    let mut expired_ca = parse_cert(&ca_cert);
    let now = OffsetDateTime::now_utc();
    expired_ca.not_before = now - Duration::from_secs(3600);
    expired_ca.not_after = now - Duration::from_secs(60);
    std::fs::write(
      &ca_cert,
      expired_ca.self_signed(&ca_key_pair).unwrap().pem(),
    )
    .unwrap();

    let _ = ensure_tls_bundle(&ca_cert, &ca_key, &cert, &key, "test-node", TEST_ADDR).unwrap();

    let ca = parse_cert(&ca_cert);
    assert!(!needs_renewal(&ca, OffsetDateTime::now_utc()));
    // The CA renewal keeps key and subject, so the node certificate issued
    // before the renewal remains untouched and verifiable.
    assert_eq!(node_before, std::fs::read_to_string(&cert).unwrap());
  }

  #[test]
  fn extracts_advertise_host() {
    assert_eq!(advertise_host("https://node-0:5001").unwrap(), "node-0");
    assert_eq!(advertise_host("https://10.0.0.5:5001").unwrap(), "10.0.0.5");
    assert_eq!(advertise_host("https://[::1]:5001").unwrap(), "::1");
    assert!(advertise_host("node-0:5001").is_err());
  }
}
