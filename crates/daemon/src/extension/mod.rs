//! Selector-driven extension activation and capability announcement
//! (extension system design, sections 6 and 7).
//!
//! The [`ExtensionManager`] reconciles the desired set — all synced extension
//! records in storage — with the running set of engine instances: a record
//! whose manifest selector matches the node's own labels is loaded and served
//! locally, everything else is unloaded. After each pass the manager
//! republishes the `{ext.<id> = <semver>}` capability set onto the local
//! member register's annotations and gossips the change through the existing
//! Alive path. Reconcile is triggered by the resource-apply path (a `Notify`
//! wired into the `ResourceMapper`) with a 30 s safety-net tick.
//!
//! Loads are lazy-safe: a failed load (bad manifest, quarantined artifact,
//! engine error) is logged and retried on the next trigger; it never blocks
//! reconcile and never takes down a still-serviceable previous instance.

use std::{
  collections::{HashMap, HashSet},
  sync::Arc,
  time::Duration,
};

use lycoris_config::ExtensionsConfig;
use lycoris_extension::{
  DEFAULT_ENTRY, EngineKind, EngineLimits, ExtensionEngine, ExtensionError, ExtensionInstance,
  ExtensionManifest, ExtensionPackage, LuaEngine, WasmEngine,
};
use lycoris_storage::{ExtensionRecord, ExtensionStorageError, Storage};
use thiserror::Error;
use tokio::{
  sync::{Mutex, Notify},
  time::{self, MissedTickBehavior},
};

use crate::{
  membership::{EXTENSION_ANNOTATION_PREFIX, MembershipService},
  selector::matches_selector,
  sync::ClusterSync,
};

/// Reconcile safety-net cadence (design section 6); the apply-path notify is
/// the primary trigger.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// Errors surfaced by the extension manager.
#[derive(Debug, Error)]
pub enum ExtensionManagerError {
  /// The extension has no running instance on this node: it is unknown, its
  /// selector does not match, or it is quarantined. Routing (a later change)
  /// turns this into a forwarding decision.
  // Constructed by `invoke_local`; the rpc surface lands with routing.
  #[allow(dead_code)]
  #[error("extension {0:?} is not running on this node")]
  NotRunning(String),
  /// The record's artifact is missing from the blob store.
  #[error("extension {0:?} has no artifact in the blob store")]
  MissingArtifact(String),
  /// The engine boundary reported a failure.
  #[error(transparent)]
  Extension(#[from] ExtensionError),
  /// The extension storage domain reported a failure.
  #[error(transparent)]
  Storage(#[from] ExtensionStorageError),
}

/// Map the node-local `[extensions]` config section onto engine limits.
///
/// The mapping lives in the daemon (not in `lycoris-config`) so the config
/// crate stays a pure data crate with no dependency on the execution engines.
fn engine_limits(config: &ExtensionsConfig) -> EngineLimits {
  EngineLimits {
    wasm_fuel_per_call: config.wasm_fuel_per_call,
    wasm_max_memory_bytes: usize::try_from(config.wasm_max_memory_bytes).unwrap_or(usize::MAX),
    lua_instructions_per_call: config.lua_instructions_per_call,
    lua_max_memory_bytes: usize::try_from(config.lua_max_memory_bytes).unwrap_or(usize::MAX),
    invoke_timeout: Duration::from_millis(config.invoke_timeout_ms),
  }
}

/// A loaded extension instance plus the bookkeeping needed for reload and
/// capability announcement decisions.
struct LoadedExtension {
  /// Monotonic record version the instance was loaded from.
  version: u64,
  /// Human-facing semver advertised in the capability annotation.
  semver: String,
  /// `Arc` so `invoke_local` runs without holding the instances lock: a guest
  /// call may occupy its engine deadline (seconds), and serializing unrelated
  /// invocations — or the next reconcile — behind it would stall the
  /// subsystem.
  // Read by `invoke_local`; the rpc surface lands with routing.
  #[allow(dead_code)]
  instance: Arc<dyn ExtensionInstance>,
}

/// Reconciles synced extension records with locally running instances.
pub struct ExtensionManager {
  wasm: WasmEngine,
  lua: LuaEngine,
  instances: Arc<Mutex<HashMap<String, LoadedExtension>>>,
  storage: Storage,
  membership: Arc<MembershipService>,
  /// Gossip handle for capability announcements. `Option` (same pattern as
  /// `ClusterService::cluster_sync`): the manager is constructed before
  /// `ClusterSync` exists — the mapper needs its notify handle first — and
  /// completed via [`Self::with_cluster_sync`]; unit tests leave it unset and
  /// assert the membership effect directly.
  cluster_sync: Option<ClusterSync>,
  notify: Arc<Notify>,
}

impl ExtensionManager {
  /// Build the manager and its engines from the node-local `[extensions]`
  /// config section.
  pub fn new(
    config: &ExtensionsConfig, storage: Storage, membership: Arc<MembershipService>,
  ) -> Result<Self, ExtensionError> {
    let limits = engine_limits(config);
    Ok(Self {
      wasm: WasmEngine::new(limits)?,
      lua: LuaEngine::new(limits),
      instances: Arc::new(Mutex::new(HashMap::new())),
      storage,
      membership,
      cluster_sync: None,
      notify: Arc::new(Notify::new()),
    })
  }

  /// Inject the gossip handle used to broadcast capability announcements.
  pub fn with_cluster_sync(mut self, cluster_sync: ClusterSync) -> Self {
    self.cluster_sync = Some(cluster_sync);
    self
  }

  /// The reconcile trigger wired into the resource-apply path: fired whenever
  /// an EXTENSION resource was applied (design section 6).
  pub fn notify(&self) -> Arc<Notify> {
    self.notify.clone()
  }

  /// Invoke a locally running extension. `payload` is JSON; the return value
  /// is JSON. Extensions without a local instance surface as
  /// [`ExtensionManagerError::NotRunning`] — cluster-wide routing is a later
  /// change.
  // The rpc surface (ExtensionService routing change) is the caller.
  #[allow(dead_code)]
  pub async fn invoke_local(
    &self, id: &str, method: &str, payload: &[u8],
  ) -> Result<Vec<u8>, ExtensionManagerError> {
    let instance = {
      let instances = self.instances.lock().await;
      instances.get(id).map(|loaded| loaded.instance.clone())
    };
    let Some(instance) = instance else {
      return Err(ExtensionManagerError::NotRunning(id.to_string()));
    };
    Ok(instance.invoke(method, payload).await?)
  }

  /// Reconcile the desired set (all synced extension records) with the
  /// running set, then republish capability annotations (design section 7).
  /// A storage read failure keeps the current running set; the next trigger
  /// retries.
  pub async fn reconcile(&self) {
    let records = match self.storage.extensions().list() {
      Ok(records) => records,
      Err(error) => {
        tracing::warn!(%error, "failed to list extension records; keeping the running set");
        return;
      }
    };
    // The node's own labels — the same labels the node registers into
    // membership — decide per-node activation (design section 6).
    let labels = match self.storage.node().local().labels() {
      Ok(labels) => labels,
      Err(error) => {
        tracing::warn!(%error, "failed to read node labels; keeping the running set");
        return;
      }
    };

    let mut instances = self.instances.lock().await;
    for record in &records {
      let manifest = match ExtensionManifest::from_map(&record.manifest) {
        Ok(manifest) => manifest,
        Err(error) => {
          tracing::warn!(extension = %record.id, %error, "invalid extension manifest; not loading");
          continue;
        }
      };
      // `matches_selector` speaks `HashMap` on the selector side; the
      // manifest's `BTreeMap` converts once per record.
      let selector: HashMap<String, String> = manifest.selector.clone().into_iter().collect();
      if !matches_selector(&labels, &selector) {
        if instances.remove(&record.id).is_some() {
          tracing::info!(extension = %record.id, "selector no longer matches; extension unloaded");
        }
        continue;
      }
      let up_to_date = instances
        .get(&record.id)
        .is_some_and(|loaded| loaded.version == record.version);
      if up_to_date {
        continue;
      }
      match self.load(record, manifest).await {
        Ok(loaded) => {
          tracing::info!(extension = %record.id, version = record.version, semver = %loaded.semver, "extension loaded");
          instances.insert(record.id.clone(), loaded);
        }
        Err(error) => {
          // Quarantine: not loaded, not advertised; a previous instance keeps
          // serving and the next trigger retries (design sections 6 and 10).
          tracing::warn!(extension = %record.id, version = record.version, %error, "extension load failed; quarantined");
        }
      }
    }
    // Records that disappeared from storage must not keep running.
    let record_ids: HashSet<&str> = records.iter().map(|record| record.id.as_str()).collect();
    instances.retain(|id, _| record_ids.contains(id.as_str()));

    // Announce the running set as capability annotations (design section 7);
    // an unchanged set produces no actions and therefore no gossip.
    let capabilities: HashMap<String, String> = instances
      .iter()
      .map(|(id, loaded)| {
        (
          format!("{EXTENSION_ANNOTATION_PREFIX}{id}"),
          loaded.semver.clone(),
        )
      })
      .collect();
    drop(instances);
    let actions = self.membership.update_local_annotations(capabilities).await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
    }
  }

  /// Drive the reconcile loop: wake on the apply-path notify, with a periodic
  /// safety-net pass (design section 6). Runs until the surrounding task is
  /// cancelled (the runtime wraps it in the shutdown watcher).
  pub async fn run(&self) {
    let mut ticker = time::interval(RECONCILE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first interval tick fires immediately; the runtime reconciles once
    // at startup, so the loop starts with the notify/timer wait instead of a
    // duplicate pass.
    ticker.tick().await;
    loop {
      tokio::select! {
        _ = ticker.tick() => self.reconcile().await,
        _ = self.notify.notified() => self.reconcile().await,
      }
    }
  }

  /// Load one record into a runnable instance: assemble the package, let the
  /// engine verify the artifact against its declared content hash, and
  /// enforce all engine-specific shape. Failures quarantine the extension:
  /// the caller logs and keeps it unloaded (design section 10).
  async fn load(
    &self, record: &ExtensionRecord, manifest: ExtensionManifest,
  ) -> Result<LoadedExtension, ExtensionManagerError> {
    let engine_kind: EngineKind = record.engine.parse()?;
    let artifact = self
      .storage
      .extensions()
      .blobs()
      .read(&record.id)?
      .ok_or_else(|| ExtensionManagerError::MissingArtifact(record.id.clone()))?;
    // An empty entry rides the record as-is; the package contract defaults it.
    let entry = if record.entry.is_empty() {
      DEFAULT_ENTRY.to_string()
    } else {
      record.entry.clone()
    };
    let package = ExtensionPackage {
      id: record.id.clone(),
      name: record.name.clone(),
      version: record.version,
      engine: engine_kind,
      entry,
      manifest,
      artifact,
      content_hash: record.content_hash.clone(),
    };
    let engine: &dyn ExtensionEngine = match engine_kind {
      EngineKind::Wasm => &self.wasm,
      EngineKind::Lua => &self.lua,
    };
    let semver = package.manifest.semver.to_string();
    let instance = engine.load(&package).await?;
    Ok(LoadedExtension {
      version: record.version,
      semver,
      instance: Arc::from(instance),
    })
  }
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use lycoris_membership::SwimConfig;
  use lycoris_storage::ResourceScope;
  use tempfile::TempDir;

  use super::*;
  use crate::membership::MemberRegister;

  const ECHO_SOURCE: &[u8] = b"function invoke(method, payload) return payload end";

  fn test_manager(dir: &TempDir) -> (Storage, Arc<MembershipService>, ExtensionManager) {
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let membership = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    let manager = ExtensionManager::new(
      &ExtensionsConfig::default(),
      storage.clone(),
      membership.clone(),
    )
    .unwrap();
    (storage, membership, manager)
  }

  /// Apply a lua extension record through the storage pipeline. `manifest`
  /// overrides the default semver-only manifest.
  fn apply_extension_with_manifest(
    storage: &Storage, id: &str, version: u64, artifact: &[u8], manifest: BTreeMap<String, String>,
  ) {
    let record = ExtensionRecord {
      id: id.to_string(),
      name: format!("extension-{id}"),
      version,
      engine: "lua".to_string(),
      entry: "invoke".to_string(),
      content_hash: blake3::hash(artifact).to_hex().to_string(),
      scope: ResourceScope::ClusterShared,
      source_node_id: None,
      created_at_ms: 0,
      updated_at_ms: 0,
      manifest,
      labels: BTreeMap::new(),
    };
    storage
      .extensions()
      .apply_remote_extension(record, artifact)
      .unwrap();
  }

  fn apply_extension(storage: &Storage, id: &str, version: u64, selector_json: Option<&str>) {
    let mut manifest = BTreeMap::from([("semver".to_string(), "1.0.0".to_string())]);
    if let Some(selector) = selector_json {
      manifest.insert("selector".to_string(), selector.to_string());
    }
    apply_extension_with_manifest(storage, id, version, ECHO_SOURCE, manifest);
  }

  async fn running(manager: &ExtensionManager) -> Vec<String> {
    let mut ids: Vec<String> = manager.instances.lock().await.keys().cloned().collect();
    ids.sort_unstable();
    ids
  }

  async fn local_annotations(membership: &MembershipService) -> HashMap<String, String> {
    let mut registers = membership.fetch_registers(&["local"]).await;
    registers
      .pop()
      .map(|register| register.annotations().clone())
      .unwrap_or_default()
  }

  #[test]
  fn config_defaults_match_engine_defaults() {
    let limits = engine_limits(&ExtensionsConfig::default());
    let engine = EngineLimits::default();
    assert_eq!(limits.wasm_fuel_per_call, engine.wasm_fuel_per_call);
    assert_eq!(limits.wasm_max_memory_bytes, engine.wasm_max_memory_bytes);
    assert_eq!(
      limits.lua_instructions_per_call,
      engine.lua_instructions_per_call
    );
    assert_eq!(limits.lua_max_memory_bytes, engine.lua_max_memory_bytes);
    assert_eq!(limits.invoke_timeout, engine.invoke_timeout);
  }

  #[test]
  fn config_overrides_map_field_by_field() {
    let config = ExtensionsConfig {
      wasm_fuel_per_call: 1,
      wasm_max_memory_bytes: 2,
      lua_instructions_per_call: 3,
      lua_max_memory_bytes: 4,
      invoke_timeout_ms: 5,
    };
    let limits = engine_limits(&config);
    assert_eq!(limits.wasm_fuel_per_call, 1);
    assert_eq!(limits.wasm_max_memory_bytes, 2);
    assert_eq!(limits.lua_instructions_per_call, 3);
    assert_eq!(limits.lua_max_memory_bytes, 4);
    assert_eq!(limits.invoke_timeout, Duration::from_millis(5));
  }

  #[tokio::test]
  async fn reconcile_loads_selector_matches_and_skips_mismatches() {
    let dir = TempDir::new().unwrap();
    let (storage, membership, manager) = test_manager(&dir);
    storage.node().local().set_label("role", "worker").unwrap();
    apply_extension(&storage, "ext-match", 1, Some(r#"{"role":"worker"}"#));
    apply_extension(&storage, "ext-skip", 1, Some(r#"{"role":"controller"}"#));

    manager.reconcile().await;

    assert_eq!(running(&manager).await, vec!["ext-match".to_string()]);
    // Capability announcement: only the running extension is advertised.
    assert_eq!(
      local_annotations(&membership).await,
      HashMap::from([("ext.ext-match".to_string(), "1.0.0".to_string())])
    );
  }

  #[tokio::test]
  async fn reconcile_unloads_when_the_selector_stops_matching() {
    let dir = TempDir::new().unwrap();
    let (storage, membership, manager) = test_manager(&dir);
    storage.node().local().set_label("role", "worker").unwrap();
    apply_extension(&storage, "ext-match", 1, Some(r#"{"role":"worker"}"#));
    manager.reconcile().await;
    assert_eq!(running(&manager).await, vec!["ext-match".to_string()]);

    storage
      .node()
      .local()
      .set_label("role", "controller")
      .unwrap();
    manager.reconcile().await;

    assert!(running(&manager).await.is_empty());
    assert_eq!(local_annotations(&membership).await, HashMap::new());
  }

  #[tokio::test]
  async fn reconcile_reloads_when_the_record_version_changes() {
    let dir = TempDir::new().unwrap();
    let (storage, _membership, manager) = test_manager(&dir);
    apply_extension(&storage, "ext-versioned", 1, None);
    manager.reconcile().await;
    assert_eq!(manager.instances.lock().await["ext-versioned"].version, 1);

    apply_extension(&storage, "ext-versioned", 2, None);
    manager.reconcile().await;
    assert_eq!(manager.instances.lock().await["ext-versioned"].version, 2);
  }

  #[tokio::test]
  async fn reconcile_quarantines_a_tampered_artifact() {
    let dir = TempDir::new().unwrap();
    let (storage, membership, manager) = test_manager(&dir);
    apply_extension(&storage, "ext-tampered", 1, None);
    // Disk corruption between ingest and load: the blob no longer matches the
    // declared content hash.
    storage
      .extensions()
      .blobs()
      .write("ext-tampered", b"tampered")
      .unwrap();

    manager.reconcile().await;

    assert!(running(&manager).await.is_empty());
    assert_eq!(local_annotations(&membership).await, HashMap::new());
  }

  #[tokio::test]
  async fn reconcile_does_not_load_a_bad_manifest() {
    let dir = TempDir::new().unwrap();
    let (storage, membership, manager) = test_manager(&dir);
    // The apply pipeline validates integrity, not manifest semantics: a
    // manifest without `semver` reaches the manager and must be skipped.
    apply_extension_with_manifest(&storage, "ext-bad", 1, ECHO_SOURCE, BTreeMap::new());

    manager.reconcile().await;

    assert!(running(&manager).await.is_empty());
    assert_eq!(local_annotations(&membership).await, HashMap::new());
  }

  #[tokio::test]
  async fn invoke_local_round_trips_json_and_reports_missing_instances() {
    let dir = TempDir::new().unwrap();
    let (storage, _membership, manager) = test_manager(&dir);
    apply_extension(&storage, "ext-echo", 1, None);
    manager.reconcile().await;

    let output = manager
      .invoke_local("ext-echo", "echo", br#"{"a":1}"#)
      .await
      .unwrap();
    assert_eq!(output, br#"{"a":1}"#.to_vec());

    let error = manager.invoke_local("ghost", "m", b"{}").await.unwrap_err();
    assert!(
      matches!(&error, ExtensionManagerError::NotRunning(id) if id.as_str() == "ghost"),
      "expected NotRunning, got {error}"
    );
  }

  #[tokio::test]
  async fn reconcile_unloads_and_unadvertises_deleted_records() {
    let dir = TempDir::new().unwrap();
    let (storage, membership, manager) = test_manager(&dir);
    apply_extension(&storage, "ext-echo", 1, None);
    manager.reconcile().await;
    assert_eq!(
      local_annotations(&membership).await,
      HashMap::from([("ext.ext-echo".to_string(), "1.0.0".to_string())])
    );

    storage.extensions().delete("ext-echo").unwrap();
    manager.reconcile().await;

    assert!(running(&manager).await.is_empty());
    assert_eq!(local_annotations(&membership).await, HashMap::new());
  }
}
