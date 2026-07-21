use lycoris_client::{ClusterClient, ExtensionClient};
use lycoris_config::{ClientConfig, DaemonConfig};
use lycoris_core::{ClusterKey, default_cluster_key_path};
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
  let cluster_key = match key {
    Some(hex) => ClusterKey::from_hex(hex.trim())?,
    None => ClusterKey::generate()?,
  };

  let path = default_cluster_key_path();
  cluster_key.save(&path)?;
  tracing::info!(
    key = %cluster_key.to_hex(),
    path = %path.display(),
    "cluster initialized"
  );
  Ok(())
}

#[tracing::instrument(name = "join_cluster", skip_all, fields(%peer))]
pub(crate) async fn join_cluster(
  client_config: &ClientConfig, peer: String, key: Option<String>,
) -> Result<(), ShellError> {
  let key = resolve_key(client_config, key)?;
  let daemon_config = DaemonConfig::load(None)?;
  let tls = lycoris_tls::load_tls_bundle(
    &client_config.cert,
    &client_config.key,
    &client_config.ca_cert,
  )?;
  let mut client = ClusterClient::connect(&peer, &tls)
    .await
    .map_err(|source| ShellError::Connect {
      address: peer.clone(),
      source,
    })?
    .with_cluster_key(key);

  let node = NodeInfo::new(
    daemon_config.node.id.clone(),
    daemon_config.node.address.clone(),
    std::collections::HashMap::new(),
    std::collections::HashMap::new(),
  );

  client.join(node).await.map_err(ShellError::Join)?;

  let mut local_client = connect_cluster(client_config).await?;
  local_client
    .set_primary_endpoint(&peer)
    .await
    .map_err(ShellError::SetPrimary)?;

  tracing::info!(
    node_id = %daemon_config.node.id,
    %peer,
    "node joined cluster"
  );
  Ok(())
}

#[tracing::instrument(name = "leave_cluster", skip_all)]
pub(crate) async fn leave_cluster(client_config: &ClientConfig) -> Result<(), ShellError> {
  let daemon_config = DaemonConfig::load(None)?;
  let mut client = connect_cluster(client_config).await?;
  client
    .leave(&daemon_config.node.id)
    .await
    .map_err(ShellError::Leave)?;
  tracing::info!(node_id = %daemon_config.node.id, "node leaving cluster");
  Ok(())
}

pub(crate) fn show_key() -> Result<(), ShellError> {
  let path = default_cluster_key_path();
  if !path.is_file() {
    return Err(ShellError::ClusterKeyNotFound);
  }

  let key = ClusterKey::load(&path)?;
  tracing::info!(key = %key.to_hex(), "cluster key");
  Ok(())
}

async fn connect_cluster(client_config: &ClientConfig) -> Result<ClusterClient, ShellError> {
  // A missing key is not fatal here: the server rejects unauthenticated
  // calls anyway. A key that exists but fails to load (e.g. corrupted) is
  // suspicious though, so surface it instead of silently degrading to "no
  // key".
  let key = match load_cluster_key(client_config) {
    Ok(key) => Some(key),
    Err(ShellError::ClusterKeyNotFound) => None,
    Err(error) => {
      tracing::warn!("failed to load cluster key, continuing without one: {error}");
      None
    }
  };
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
  let key = match load_cluster_key(client_config) {
    Ok(key) => Some(key),
    Err(ShellError::ClusterKeyNotFound) => None,
    Err(error) => {
      tracing::warn!("failed to load cluster key, continuing without one: {error}");
      None
    }
  };
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

/// Best-effort local node id used to mark the current node in listings.
///
/// The daemon configuration is read once per command; a failure is surfaced
/// as a warning and degrades to no marker instead of failing the query or
/// being swallowed silently.
fn local_node_id() -> String {
  match DaemonConfig::load(None) {
    Ok(config) => config.node.id,
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
