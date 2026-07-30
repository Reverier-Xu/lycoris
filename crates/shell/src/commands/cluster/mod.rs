use lycoris_client::{ClusterClient, ExtensionClient};
use lycoris_config::{ClientConfig, DaemonConfig, default_daemon_config_path};
use lycoris_core::{ClusterKey, cluster_key_path_in};
use lycoris_proto::node::{NodeInfo, ResourceKind};

use crate::error::ShellError;

pub(crate) mod ext;
mod parse;
mod render;

#[tracing::instrument(name = "get_resources", skip_all, fields(resource = %resource))]
pub(crate) async fn get_resources(
  client_config: &ClientConfig, resource: &str, name: Option<String>, selectors: &[String],
  scope: Option<String>,
) -> Result<(), ShellError> {
  // Validate every argument before touching the network: a malformed kind,
  // selector, or scope must fail fast without opening a connection.
  let kind = parse::parse_resource_kind(resource)?;
  let kind_name = parse::resource_name(kind);
  let selector = parse::parse_selectors(selectors)?;
  let scope = parse::parse_scope(scope)?;
  // The local node marker is only rendered for node listings; the local
  // selector-match marker only for extension listings. Other kinds skip the
  // daemon-config read (and its warning) entirely.
  let local_id = if kind == ResourceKind::Node {
    local_node_id()
  } else {
    String::new()
  };
  let local_labels = if kind == ResourceKind::Extension {
    local_node_labels()
  } else {
    std::collections::HashMap::new()
  };

  let mut client = connect_cluster(client_config).await?;
  match name {
    Some(id) => {
      let resource = client
        .get_resource(kind, &id)
        .await
        .map_err(|source| ShellError::GetResource {
          kind: kind_name.to_string(),
          id: id.clone(),
          source,
        })?
        .ok_or_else(|| ShellError::ResourceNotFound {
          kind: kind_name.to_string(),
          id: id.clone(),
        })?;
      render::render_resource(&resource, kind, &local_id);
    }
    None => {
      let resources = client
        .list_resources(kind, selector, scope)
        .await
        .map_err(|source| ShellError::ListResources {
          kind: kind_name.to_string(),
          source,
        })?;
      render::render_list(kind, &resources, &local_id, &local_labels);
      tracing::info!(total = resources.len(), "resources listed");
    }
  }

  Ok(())
}

#[tracing::instrument(name = "register", skip_all, fields(id = %id, address = %address))]
pub(crate) async fn register(
  client_config: &ClientConfig, id: String, address: String, key: Option<String>,
) -> Result<(), ShellError> {
  let key = resolve_key(client_config, key)?;
  let mut client = connect_cluster(client_config).await?.with_cluster_key(key);
  let node = NodeInfo::new(
    id.clone(),
    address,
    std::collections::HashMap::new(),
    std::collections::HashMap::new(),
  );
  client.register(node).await.map_err(ShellError::Register)?;
  tracing::info!(id = %id, "node registered");
  Ok(())
}

pub(crate) fn init_cluster(key: Option<String>) -> Result<(), ShellError> {
  let (config_path, mut config) = daemon_config_with_path()?;
  let cluster_key = match key {
    Some(hex) => ClusterKey::from_hex(hex.trim())?,
    None => ClusterKey::generate()?,
  };

  let path = cluster_key_path_in(std::path::Path::new(&config.data_dir));
  cluster_key.save(&path)?;
  config.cluster.join = None;
  config.write_to_file(config_path)?;
  tracing::info!(
    key = %cluster_key.to_hex(),
    path = %path.display(),
    "cluster initialized"
  );
  Ok(())
}

#[tracing::instrument(name = "join_cluster", skip_all, fields(%peer))]
pub(crate) fn join_cluster(peer: String, key: Option<String>) -> Result<(), ShellError> {
  let (config_path, _) = daemon_config_with_path()?;
  configure_join(&config_path, peer, key)
}

fn configure_join(
  config_path: &std::path::Path, peer: String, key: Option<String>,
) -> Result<(), ShellError> {
  let address: lycoris_overlay::Multiaddr = peer
    .parse()
    .map_err(|_| ShellError::setup("join peer must be a valid libp2p multiaddr"))?;
  let canonical = address.to_string();
  let mut suffix = canonical.rsplit('/');
  let has_peer_suffix =
    suffix.next().is_some_and(|peer| !peer.is_empty()) && suffix.next() == Some("p2p");
  if !has_peer_suffix {
    return Err(ShellError::setup("join peer must end in /p2p/<peer-id>"));
  }

  let mut config = DaemonConfig::from_file(config_path)?;
  let key_path = cluster_key_path_in(std::path::Path::new(&config.data_dir));
  let cluster_key = match key {
    Some(hex) => ClusterKey::from_hex(hex.trim())?,
    None => ClusterKey::load(&key_path)?,
  };
  // Persist the secret first. A crash before the config write leaves the node
  // standalone; the reverse ordering could start a join without its key.
  cluster_key.save(&key_path)?;
  config.cluster.join = Some(canonical.clone());
  config.write_to_file(config_path)?;
  tracing::info!(peer = %canonical, "overlay join configured; restart the daemon to enroll");
  Ok(())
}

#[tracing::instrument(name = "leave_cluster", skip_all)]
pub(crate) async fn leave_cluster(client_config: &ClientConfig) -> Result<(), ShellError> {
  let daemon_config = DaemonConfig::load(None)?;
  let node_id = configured_node_id(&daemon_config)?;
  let mut client = connect_cluster(client_config).await?;
  client.leave(&node_id).await.map_err(ShellError::Leave)?;
  tracing::info!(%node_id, "node leaving cluster");
  Ok(())
}

pub(crate) fn show_key() -> Result<(), ShellError> {
  let (_, config) = daemon_config_with_path()?;
  let path = cluster_key_path_in(std::path::Path::new(&config.data_dir));
  if !path.is_file() {
    return Err(ShellError::ClusterKeyNotFound);
  }

  let key = ClusterKey::load(&path)?;
  tracing::info!(key = %key.to_hex(), "cluster key");
  Ok(())
}

fn daemon_config_with_path() -> Result<(std::path::PathBuf, DaemonConfig), ShellError> {
  let path = default_daemon_config_path().ok_or(lycoris_config::ConfigError::NotFound)?;
  let config = DaemonConfig::from_file(&path)?;
  Ok((path, config))
}

pub(crate) fn show_identity(json: bool) -> Result<(), ShellError> {
  let (_, config) = daemon_config_with_path()?;
  let identity = lycoris_overlay::NodeIdentity::load(
    std::path::Path::new(&config.data_dir).join("node.identity"),
  )?;
  let peer_id = identity.peer_id().to_string();
  let addresses: Vec<String> = config
    .cluster
    .overlay_listen
    .iter()
    .map(|address| format!("{address}/p2p/{peer_id}"))
    .collect();
  if json {
    println!(
      "{}",
      serde_json::json!({
        "node_id": identity.node_id().to_string(),
        "peer_id": peer_id,
        "join_addresses": addresses,
      })
    );
  } else {
    println!("node id: {}", identity.node_id());
    println!("peer id: {peer_id}");
    for address in addresses {
      println!("join address: {address}");
    }
  }
  Ok(())
}

async fn connect_cluster(client_config: &ClientConfig) -> Result<ClusterClient, ShellError> {
  // A missing key is not fatal here: the server rejects unauthenticated
  // calls anyway. A key that exists but fails to load (e.g. corrupted) is
  // surfaced instead of silently degrading to "no key".
  let key = resolve_optional_cluster_key(client_config);
  let tls = lycoris_tls::load_tls_bundle(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )?;
  let client = ClusterClient::connect(&client_config.api_address, &tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: client_config.api_address.clone(),
      source,
    })?;
  Ok(match key {
    Some(key) => client.with_cluster_key(key),
    None => client,
  })
}

/// Connect to the cluster-key-guarded `Extension` service; same key and TLS
/// handling as [`connect_cluster`].
async fn connect_extension(client_config: &ClientConfig) -> Result<ExtensionClient, ShellError> {
  let key = resolve_optional_cluster_key(client_config);
  let tls = lycoris_tls::load_tls_bundle(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )?;
  let client = ExtensionClient::connect(&client_config.api_address, &tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: client_config.api_address.clone(),
      source,
    })?;
  Ok(match key {
    Some(key) => client.with_cluster_key(key),
    None => client,
  })
}

/// Resolve the cluster key for commands that authenticate with one: an
/// explicit `--key` wins, otherwise the local cluster key file is used so the
/// secret stays out of shell history and process listings.
fn resolve_key(client_config: &ClientConfig, key: Option<String>) -> Result<String, ShellError> {
  match key {
    Some(key) => Ok(key.trim().to_string()),
    None => load_cluster_key(client_config),
  }
}

fn load_cluster_key(client_config: &ClientConfig) -> Result<String, ShellError> {
  let path = client_config
    .resolve_cluster_key_path()
    .ok_or(ShellError::ClusterKeyNotFound)?;
  Ok(ClusterKey::load(&path)?.to_hex())
}

fn configured_node_id(config: &DaemonConfig) -> Result<String, ShellError> {
  Ok(
    lycoris_overlay::NodeIdentity::load(
      std::path::Path::new(&config.data_dir).join("node.identity"),
    )?
    .node_id()
    .to_string(),
  )
}

fn resolve_optional_cluster_key(client_config: &ClientConfig) -> Option<String> {
  match load_cluster_key(client_config) {
    Ok(key) => Some(key),
    Err(ShellError::ClusterKeyNotFound) => None,
    Err(error) => {
      tracing::warn!("failed to load cluster key, continuing without one: {error}");
      None
    }
  }
}

/// Best-effort local node id used to mark the current node in listings.
///
/// The daemon configuration is read once per command; a failure is surfaced
/// as a warning and degrades to no marker instead of failing the query or
/// being swallowed silently.
fn local_node_id() -> String {
  match DaemonConfig::load(None) {
    Ok(config) => match configured_node_id(&config) {
      Ok(node_id) => node_id,
      Err(error) => {
        tracing::warn!(%error, "failed to load node identity, local node will not be marked");
        String::new()
      }
    },
    Err(error) => {
      tracing::warn!(%error, "failed to load daemon config, local node will not be marked");
      String::new()
    }
  }
}

/// Best-effort local node labels used to mark selector-matching extensions in
/// listings; same degradation policy as [`local_node_id`].
fn local_node_labels() -> std::collections::HashMap<String, String> {
  match DaemonConfig::load(None) {
    Ok(config) => config.node.labels,
    Err(error) => {
      tracing::warn!(%error, "failed to load daemon config, local selector matches will not be marked");
      std::collections::HashMap::new()
    }
  }
}

#[cfg(test)]
mod tests {
  use lycoris_config::{ClusterConfig, ExtensionsConfig, NodeConfig, TlsConfig};
  use tempfile::TempDir;

  use super::*;

  fn test_config(data_dir: &std::path::Path) -> DaemonConfig {
    DaemonConfig {
      node: NodeConfig {
        id: "node".to_string(),
        address: "https://127.0.0.1:7796".to_string(),
        labels: std::collections::HashMap::new(),
      },
      cluster: ClusterConfig {
        listen_address: "127.0.0.1:7796".to_string(),
        bootstrap_peers: Vec::new(),
        overlay_listen: vec!["/ip4/127.0.0.1/tcp/7797".to_string()],
        join: None,
      },
      tls: TlsConfig {
        ca_cert: "ca.crt".to_string(),
        ca_key: "ca.key".to_string(),
        cert: "node.crt".to_string(),
        key: "node.key".to_string(),
      },
      data_dir: data_dir.to_string_lossy().to_string(),
      extensions: ExtensionsConfig::default(),
    }
  }

  #[test]
  fn configure_join_persists_the_target_key_and_multiaddr() -> Result<(), Box<dyn std::error::Error>>
  {
    let dir = TempDir::new()?;
    let config_path = dir.path().join("lycoris.conf");
    let data_dir = dir.path().join("data");
    test_config(&data_dir).write_to_file(&config_path)?;
    let sponsor = lycoris_overlay::NodeIdentity::generate();
    let peer = format!("/ip4/127.0.0.1/tcp/9000/p2p/{}", sponsor.peer_id());
    let key = ClusterKey::generate()?;

    configure_join(&config_path, peer.clone(), Some(key.to_hex()))?;

    let loaded = DaemonConfig::from_file(&config_path)?;
    assert_eq!(loaded.cluster.join.as_deref(), Some(peer.as_str()));
    assert_eq!(ClusterKey::load(cluster_key_path_in(&data_dir))?, key);
    Ok(())
  }

  #[test]
  fn configure_join_rejects_control_plane_urls() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let error = configure_join(
      &dir.path().join("missing.conf"),
      "https://127.0.0.1:7796".to_string(),
      None,
    );
    assert!(error.is_err());
    Ok(())
  }
}
