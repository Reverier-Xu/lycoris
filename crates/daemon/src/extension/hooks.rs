//! Hook dispatch (extension system design, section 9).
//!
//! Workflow code emits `(point, context_json)`; the [`HookDispatcher`]
//! resolves the subscribers of `point` from the synced extension registry and
//! invokes them through [`ExtensionManager::invoke`], so a hook runs wherever
//! its extension runs — locally or on a capable peer, the manager decides.
//! Each subscription's manifest `on_error` policy decides what a failure
//! means: `abort` stops the dispatch after recording the failure, `ignore`
//! records it and continues with the next subscriber.
//!
//! Dispatch is deterministic: subscribers are invoked in record-id order, so
//! every node resolves the same ordering for the same registry state.

use std::sync::Arc;

use lycoris_extension::{ExtensionManifest, HookErrorPolicy};
use lycoris_storage::Storage;

use super::{ExtensionManager, ExtensionManagerError};

/// The outcome of one hook invocation: the extension id and either the JSON
/// it returned or the error it failed with. Outcomes are collected in
/// invocation order, so a dispatch result replays exactly what happened.
#[derive(Debug)]
pub struct HookOutcome {
  pub id: String,
  pub result: Result<Vec<u8>, ExtensionManagerError>,
}

/// Resolves hook subscribers from the synced extension registry and invokes
/// them in manifest-declared policy through the manager's routing path.
pub struct HookDispatcher {
  storage: Storage,
  manager: Arc<ExtensionManager>,
}

/// A resolved subscription: the extension to call and the failure policy its
/// manifest declared for this hook point.
struct Subscriber {
  id: String,
  on_error: HookErrorPolicy,
}

impl HookDispatcher {
  pub fn new(storage: Storage, manager: Arc<ExtensionManager>) -> Self {
    Self { storage, manager }
  }

  /// Dispatch one hook emission: invoke every subscriber of `point` with
  /// `context` as the JSON payload, in record-id order. The hook point is
  /// passed to the extension as the invoke method, so a guest switches on
  /// the point name. Every invocation — success or failure — is recorded as
  /// a [`HookOutcome`]; a failed subscriber with the `abort` policy ends the
  /// dispatch after its outcome is recorded.
  pub async fn dispatch(&self, point: &str, context: serde_json::Value) -> Vec<HookOutcome> {
    let payload = match serde_json::to_vec(&context) {
      Ok(payload) => payload,
      Err(error) => {
        // A `serde_json::Value` always serializes; guard instead of unwrap.
        tracing::warn!(%error, %point, "failed to encode the hook context; dispatch skipped");
        return Vec::new();
      }
    };

    let mut outcomes = Vec::new();
    for subscriber in self.subscribers(point) {
      let result = self
        .manager
        .invoke(&subscriber.id, point, &payload, None)
        .await
        .map(|outcome| outcome.payload);
      let abort = result.is_err() && subscriber.on_error == HookErrorPolicy::Abort;
      if let Err(error) = &result {
        tracing::warn!(extension = %subscriber.id, %point, %error, "hook invocation failed");
      }
      outcomes.push(HookOutcome {
        id: subscriber.id,
        result,
      });
      if abort {
        break;
      }
    }
    outcomes
  }

  /// The subscribers of `point`: every synced record whose manifest declares
  /// the point, in record-id order so the invocation order is deterministic.
  /// Records with unparseable manifests are skipped with a warning — the same
  /// quarantine posture the manager's reconcile takes. A storage read failure
  /// degrades to "no subscribers" instead of failing the workflow.
  fn subscribers(&self, point: &str) -> Vec<Subscriber> {
    let records = match self.storage.extensions().list() {
      Ok(records) => records,
      Err(error) => {
        tracing::warn!(%error, %point, "failed to list extension records; no hook subscribers");
        return Vec::new();
      }
    };
    let mut subscribers: Vec<Subscriber> = records
      .into_iter()
      .filter_map(|record| {
        let manifest = match ExtensionManifest::from_map(&record.manifest) {
          Ok(manifest) => manifest,
          Err(error) => {
            tracing::warn!(extension = %record.id, %error, "invalid extension manifest; skipping hook subscription");
            return None;
          }
        };
        manifest
          .hooks
          .iter()
          .find(|hook| hook.point == point)
          .map(|hook| Subscriber {
            id: record.id,
            on_error: hook.on_error,
          })
      })
      .collect();
    subscribers.sort_by(|a, b| a.id.cmp(&b.id));
    subscribers
  }
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use lycoris_config::ExtensionsConfig;
  use lycoris_membership::SwimConfig;
  use lycoris_storage::{ExtensionRecord, ResourceScope};
  use tempfile::TempDir;

  use super::*;
  use crate::membership::{MemberRegister, MembershipService};

  const ECHO_SOURCE: &[u8] = b"function invoke(method, payload) return payload end";
  const FAILING_SOURCE: &[u8] = b"function invoke(method, payload) error(\"boom\") end";

  fn test_dispatcher(dir: &TempDir) -> (Storage, Arc<ExtensionManager>, HookDispatcher) {
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let membership = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    let manager = Arc::new(
      ExtensionManager::new(&ExtensionsConfig::default(), storage.clone(), membership).unwrap(),
    );
    let dispatcher = HookDispatcher::new(storage.clone(), manager.clone());
    (storage, manager, dispatcher)
  }

  /// Apply a lua extension record declaring `hooks` as its hook subscription
  /// list (a JSON fragment), with an optional label selector.
  fn apply_hook_extension(
    storage: &Storage, id: &str, source: &[u8], hooks: &str, selector: Option<&str>,
  ) {
    let mut manifest = BTreeMap::from([
      ("semver".to_string(), "1.0.0".to_string()),
      ("hooks".to_string(), hooks.to_string()),
    ]);
    if let Some(selector) = selector {
      manifest.insert("selector".to_string(), selector.to_string());
    }
    let record = ExtensionRecord {
      id: id.to_string(),
      name: format!("extension-{id}"),
      version: 1,
      engine: "lua".to_string(),
      entry: "invoke".to_string(),
      content_hash: blake3::hash(source).to_hex().to_string(),
      scope: ResourceScope::ClusterShared,
      source_node_id: None,
      created_at_ms: 0,
      updated_at_ms: 0,
      manifest,
      labels: BTreeMap::new(),
    };
    storage
      .extensions()
      .apply_remote_extension(record, source)
      .unwrap();
  }

  #[tokio::test]
  async fn dispatch_without_subscribers_returns_empty() {
    let dir = TempDir::new().unwrap();
    let (storage, manager, dispatcher) = test_dispatcher(&dir);
    apply_hook_extension(
      &storage,
      "ext-echo",
      ECHO_SOURCE,
      r#"[{"point":"other.point"}]"#,
      None,
    );
    manager.reconcile().await;

    let outcomes = dispatcher
      .dispatch("skill.invoke.pre", serde_json::json!({}))
      .await;
    assert!(outcomes.is_empty());
  }

  #[tokio::test]
  async fn dispatch_invokes_subscribers_in_record_id_order() {
    let dir = TempDir::new().unwrap();
    let (storage, manager, dispatcher) = test_dispatcher(&dir);
    // Applied in reverse id order on purpose: the dispatch order must follow
    // the record ids, not the apply order.
    for id in ["zeta-hook", "alpha-hook"] {
      apply_hook_extension(
        &storage,
        id,
        ECHO_SOURCE,
        r#"[{"point":"skill.invoke.pre","on_error":"ignore"}]"#,
        None,
      );
    }
    manager.reconcile().await;

    let context = serde_json::json!({"skill": "demo"});
    let outcomes = dispatcher
      .dispatch("skill.invoke.pre", context.clone())
      .await;

    let ids: Vec<&str> = outcomes.iter().map(|outcome| outcome.id.as_str()).collect();
    assert_eq!(ids, vec!["alpha-hook", "zeta-hook"]);
    for outcome in &outcomes {
      let payload = outcome.result.as_ref().unwrap();
      let echoed: serde_json::Value = serde_json::from_slice(payload).unwrap();
      assert_eq!(echoed, context);
    }
  }

  #[tokio::test]
  async fn dispatch_abort_stops_after_the_first_failure() {
    let dir = TempDir::new().unwrap();
    let (storage, manager, dispatcher) = test_dispatcher(&dir);
    // `on_error` defaults to abort when omitted.
    apply_hook_extension(
      &storage,
      "alpha-hook",
      FAILING_SOURCE,
      r#"[{"point":"p"}]"#,
      None,
    );
    apply_hook_extension(
      &storage,
      "beta-hook",
      ECHO_SOURCE,
      r#"[{"point":"p"}]"#,
      None,
    );
    manager.reconcile().await;

    let outcomes = dispatcher.dispatch("p", serde_json::json!({})).await;

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].id, "alpha-hook");
    let error = outcomes[0].result.as_ref().unwrap_err();
    assert!(
      matches!(
        error,
        ExtensionManagerError::Extension(lycoris_extension::ExtensionError::Script(_))
      ),
      "expected a script error, got {error}"
    );
  }

  #[tokio::test]
  async fn dispatch_ignore_continues_after_a_failure() {
    let dir = TempDir::new().unwrap();
    let (storage, manager, dispatcher) = test_dispatcher(&dir);
    apply_hook_extension(
      &storage,
      "alpha-hook",
      FAILING_SOURCE,
      r#"[{"point":"p","on_error":"ignore"}]"#,
      None,
    );
    apply_hook_extension(
      &storage,
      "beta-hook",
      ECHO_SOURCE,
      r#"[{"point":"p"}]"#,
      None,
    );
    manager.reconcile().await;

    let outcomes = dispatcher.dispatch("p", serde_json::json!({})).await;

    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].id, "alpha-hook");
    assert!(outcomes[0].result.is_err());
    assert_eq!(outcomes[1].id, "beta-hook");
    assert!(outcomes[1].result.is_ok());
  }

  #[tokio::test]
  async fn dispatch_routes_remote_subscribers_through_the_manager() {
    let dir = TempDir::new().unwrap();
    let (storage, manager, dispatcher) = test_dispatcher(&dir);
    // "remote-hook"'s selector does not match the (label-less) local node, so
    // no local instance loads; with no membership candidate advertising it,
    // the manager's routing surfaces NotFound — proving the dispatch went
    // through the routing path rather than failing a local lookup.
    apply_hook_extension(
      &storage,
      "remote-hook",
      ECHO_SOURCE,
      r#"[{"point":"p","on_error":"ignore"}]"#,
      Some(r#"{"role":"runner"}"#),
    );
    apply_hook_extension(
      &storage,
      "local-hook",
      ECHO_SOURCE,
      r#"[{"point":"p"}]"#,
      None,
    );
    manager.reconcile().await;

    let outcomes = dispatcher.dispatch("p", serde_json::json!({})).await;

    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].id, "local-hook");
    assert!(outcomes[0].result.is_ok());
    assert_eq!(outcomes[1].id, "remote-hook");
    let error = outcomes[1].result.as_ref().unwrap_err();
    assert!(
      matches!(error, ExtensionManagerError::NotFound(id) if id == "remote-hook"),
      "expected NotFound from the routing path, got {error}"
    );
  }
}
