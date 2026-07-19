//! The daemon-side typed LLM facade (llm-provider design, section 2):
//! [`ExtensionLlmProvider`] implements the typed `LlmProvider` trait on top
//! of the extension subsystem, and [`LlmRouter`] resolves which extension
//! serves LLM calls.
//!
//! The facade serializes each typed request into the section 3 wire
//! convention and calls [`ExtensionManager::invoke`], so locality, routing,
//! and the hop limit are inherited unchanged — a call issued here may
//! execute locally or one hop away without the caller knowing. The
//! invoke outcome's executed-by bookkeeping is deliberately dropped at this
//! boundary: the typed contract hides placement.
//!
//! [`LlmRouter`] keeps v1 selection static and explicit: name an extension,
//! or ask for the cluster's single registered provider — zero or several
//! providers surface as [`LlmError::Unavailable`], since v1 has no failover
//! policy to pick between them (llm-provider design, section 8).

use std::sync::Arc;

use async_trait::async_trait;
use lycoris_extension::{
  CHAT_METHOD, ChatRequest, ChatResponse, EMBED_METHOD, EmbedRequest, EmbedResponse,
  ExtensionError, ExtensionManifest, LlmError, LlmProvider, MODELS_METHOD, PROVIDES_LLM, from_wire,
  to_wire,
};
use lycoris_storage::Storage;
use serde::Deserialize;

use crate::extension::{ExtensionManager, ExtensionManagerError};

/// Map a manager-layer failure onto the typed LLM error surface.
///
/// - `NotFound`, `Unavailable`, and `NotRunning` all mean no reachable
///   provider: the extension is served nowhere, every routed candidate failed,
///   or the local instance vanished mid-call. They surface as
///   [`LlmError::Unavailable`].
/// - Engine-boundary failures already arrive as the engine's own error type
///   (guest trap, script error, invocation timeout, budget, payload shape) and
///   pass through into [`LlmError::Extension`] unchanged.
/// - Everything left is host-side infrastructure above the engine: the
///   forwarding transport (`Forwarded`, including its client-side timeout), the
///   hop-limit guard (`AlreadyForwarded` — unreachable from this facade, which
///   never sets an origin), a missing artifact, or a storage failure. None of
///   it is guest misbehaviour and none of it matches `Provider`, `Unavailable`,
///   or `InvalidResponse`, so it surfaces engine-class.
fn map_manager_error(error: ExtensionManagerError) -> LlmError {
  use ExtensionManagerError as Error;
  match error {
    Error::NotFound(_) | Error::Unavailable { .. } | Error::NotRunning(_) => {
      LlmError::Unavailable(error.to_string())
    }
    Error::Extension(error) => LlmError::Extension(error),
    error => LlmError::Extension(ExtensionError::Engine(error.to_string())),
  }
}

/// The typed LLM facade over one extension: serializes requests into the
/// section 3 wire convention, invokes through the extension manager, and
/// decodes the response — or maps a section 3 error document to
/// [`LlmError::Provider`] first.
pub struct ExtensionLlmProvider {
  manager: Arc<ExtensionManager>,
  extension_id: String,
}

impl ExtensionLlmProvider {
  pub(crate) fn new(manager: Arc<ExtensionManager>, extension_id: String) -> Self {
    Self {
      manager,
      extension_id,
    }
  }

  /// The extension this facade invokes.
  pub fn extension_id(&self) -> &str {
    &self.extension_id
  }

  /// Invoke one wire method and peel off the outcomes the typed contract
  /// models separately: manager failures become [`LlmError`], and an error
  /// document becomes [`LlmError::Provider`] before the payload is treated
  /// as a response (llm-provider design, section 3).
  async fn invoke_wire(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, LlmError> {
    let outcome = self
      .manager
      // A direct call: no origin, so the manager may route one hop out.
      .invoke(&self.extension_id, method, payload, None)
      .await
      .map_err(map_manager_error)?;
    if let Some(error) = LlmError::from_wire_error(&outcome.payload) {
      return Err(error);
    }
    Ok(outcome.payload)
  }
}

#[async_trait]
impl LlmProvider for ExtensionLlmProvider {
  async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
    let output = self.invoke_wire(CHAT_METHOD, &to_wire(&request)?).await?;
    from_wire(&output)
  }

  async fn embed(&self, request: EmbedRequest) -> Result<EmbedResponse, LlmError> {
    let output = self.invoke_wire(EMBED_METHOD, &to_wire(&request)?).await?;
    from_wire(&output)
  }

  async fn models(&self) -> Result<Vec<String>, LlmError> {
    let output = self.invoke_wire(MODELS_METHOD, b"{}").await?;
    from_wire::<ModelsDocument>(&output)
      .map(|document| document.data.into_iter().map(|entry| entry.id).collect())
  }
}

/// The section 3 `models` wire document (`{data: [{id}, ...]}`); the facade
/// flattens it to bare ids.
#[derive(Deserialize)]
struct ModelsDocument {
  data: Vec<ModelEntry>,
}

/// One entry of the `models` wire document; unknown fields are ignored.
#[derive(Deserialize)]
struct ModelEntry {
  id: String,
}

/// Resolves which extension serves LLM calls (llm-provider design, section
/// 2). Cheap to construct; resolution reads the synced records on every
/// call, so registrations take effect without any cache to invalidate.
pub struct LlmRouter {
  manager: Arc<ExtensionManager>,
  storage: Storage,
}

impl LlmRouter {
  pub(crate) fn new(manager: Arc<ExtensionManager>, storage: Storage) -> Self {
    Self { manager, storage }
  }

  /// The provider served by one explicitly named extension. Resolution is
  /// lazy: reachability is decided by routing at invoke time, not here.
  pub fn provider(&self, id: &str) -> ExtensionLlmProvider {
    ExtensionLlmProvider::new(self.manager.clone(), id.to_string())
  }

  /// The cluster's single registered LLM provider: every synced extension
  /// record whose manifest declares `provides = ["llm"]` (llm-provider
  /// design, section 3). Exactly one resolves; zero providers and ambiguous
  /// ones both surface as [`LlmError::Unavailable`].
  pub fn default_provider(&self) -> Result<ExtensionLlmProvider, LlmError> {
    let records = self.storage.extensions().list().map_err(|error| {
      // A discovery-read failure is host-side infrastructure, not "no
      // provider": reporting Unavailable would send callers down a
      // re-registration path that cannot help.
      LlmError::Extension(ExtensionError::Engine(format!(
        "failed to list extension records: {error}"
      )))
    })?;
    let mut providers: Vec<&str> = records
      .iter()
      .filter_map(|record| match ExtensionManifest::from_map(&record.manifest) {
        Ok(manifest) => manifest
          .provides
          .iter()
          .any(|provides| provides == PROVIDES_LLM)
          .then_some(record.id.as_str()),
        // An unparseable manifest never loads (reconcile quarantines it), so
        // it must not count as a provider either.
        Err(error) => {
          tracing::warn!(extension = %record.id, %error, "skipping an extension record with an invalid manifest during llm provider discovery");
          None
        }
      })
      .collect();
    providers.sort_unstable();
    match providers.as_slice() {
      [] => Err(LlmError::Unavailable(
        "no llm provider registered".to_string(),
      )),
      [id] => Ok(self.provider(id)),
      _ => Err(LlmError::Unavailable(format!(
        "ambiguous llm providers: {}; select one explicitly with LlmRouter::provider",
        providers.join(", ")
      ))),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use lycoris_config::ExtensionsConfig;
  use lycoris_extension::{ChatMessage, Role, Usage};
  use lycoris_membership::SwimConfig;
  use lycoris_storage::{ExtensionRecord, ResourceScope};
  use tempfile::TempDir;

  use super::*;
  use crate::membership::{MemberRegister, MembershipService};

  /// A Lua fixture implementing the section 3 wire convention — the same
  /// contract the WASM guest implements, which is exactly the point: the
  /// facade speaks the wire format, not an engine.
  const LLM_FIXTURE: &[u8] = br#"
    function invoke(method, payload)
      if method == "chat" then
        if payload.model == "rate-me" then
          return { error = { message = "slow down", type = "rate_limit_error", code = "rate_limit_exceeded", status = 429 } }
        end
        if payload.model == "garbage" then
          return 42
        end
        if payload.model == "explode" then
          error("boom")
        end
        return {
          model = payload.model,
          choices = {
            { index = 0, message = { role = "assistant", content = "canned hello" }, finish_reason = "stop" }
          },
          usage = { prompt_tokens = 5, completion_tokens = 2, total_tokens = 7 }
        }
      end
      if method == "embed" then
        return { data = { { index = 0, embedding = { 0.5, -1.5 } } } }
      end
      if method == "models" then
        return { data = { { id = "gpt-mock" }, { id = "gpt-mini" } } }
      end
      error("unknown method: " .. method)
    end
  "#;

  fn test_manager(dir: &TempDir) -> (Storage, ExtensionManager) {
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let membership = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    let manager =
      ExtensionManager::new(&ExtensionsConfig::default(), storage.clone(), membership).unwrap();
    (storage, manager)
  }

  fn test_router(dir: &TempDir) -> (Storage, Arc<ExtensionManager>, LlmRouter) {
    let (storage, manager) = test_manager(dir);
    let manager = Arc::new(manager);
    let router = LlmRouter::new(manager.clone(), storage.clone());
    (storage, manager, router)
  }

  /// Apply the lua fixture as an extension record through the storage
  /// pipeline. `provides` rides in the manifest as the section 3 discovery
  /// JSON; `None` leaves the key out entirely.
  fn apply_llm_fixture(storage: &Storage, id: &str, provides: Option<&str>) {
    let mut manifest = BTreeMap::from([("semver".to_string(), "1.0.0".to_string())]);
    if let Some(provides) = provides {
      manifest.insert("provides".to_string(), provides.to_string());
    }
    let record = ExtensionRecord {
      id: id.to_string(),
      name: format!("extension-{id}"),
      version: 1,
      engine: "lua".to_string(),
      entry: "invoke".to_string(),
      content_hash: blake3::hash(LLM_FIXTURE).to_hex().to_string(),
      scope: ResourceScope::ClusterShared,
      source_node_id: None,
      created_at_ms: 0,
      updated_at_ms: 0,
      manifest,
      labels: BTreeMap::new(),
    };
    storage
      .extensions()
      .apply_remote_extension(record, LLM_FIXTURE)
      .unwrap();
  }

  fn chat_request(model: &str) -> ChatRequest {
    ChatRequest {
      model: model.to_string(),
      messages: vec![ChatMessage {
        role: Role::User,
        content: "hi".to_string(),
      }],
      temperature: None,
      max_tokens: None,
    }
  }

  async fn loaded_provider(dir: &TempDir) -> ExtensionLlmProvider {
    let (storage, manager) = test_manager(dir);
    apply_llm_fixture(&storage, "ext-llm", Some(r#"["llm"]"#));
    manager.reconcile().await;
    ExtensionLlmProvider::new(Arc::new(manager), "ext-llm".to_string())
  }

  #[tokio::test]
  async fn chat_round_trips_through_a_lua_guest() {
    let dir = TempDir::new().unwrap();
    let provider = loaded_provider(&dir).await;

    let response = provider.chat(chat_request("gpt-mock")).await.unwrap();
    assert_eq!(response.model, "gpt-mock");
    assert_eq!(response.choices.len(), 1);
    assert_eq!(response.choices[0].message.role, Role::Assistant);
    assert_eq!(response.choices[0].message.content, "canned hello");
    assert_eq!(response.choices[0].finish_reason, "stop");
    assert_eq!(
      response.usage,
      Some(Usage {
        prompt_tokens: 5,
        completion_tokens: 2,
        total_tokens: 7,
      })
    );
  }

  #[tokio::test]
  async fn embed_round_trips_through_a_lua_guest() {
    let dir = TempDir::new().unwrap();
    let provider = loaded_provider(&dir).await;

    let response = provider
      .embed(EmbedRequest {
        model: "embed-x".to_string(),
        input: vec!["a".to_string()],
      })
      .await
      .unwrap();
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].index, 0);
    assert_eq!(response.data[0].embedding, vec![0.5, -1.5]);
  }

  #[tokio::test]
  async fn models_flattens_the_wire_document() {
    let dir = TempDir::new().unwrap();
    let provider = loaded_provider(&dir).await;

    let models = provider.models().await.unwrap();
    assert_eq!(models, vec!["gpt-mock".to_string(), "gpt-mini".to_string()]);
  }

  #[tokio::test]
  async fn an_error_document_maps_to_a_provider_error() {
    let dir = TempDir::new().unwrap();
    let provider = loaded_provider(&dir).await;

    let error = provider.chat(chat_request("rate-me")).await.unwrap_err();
    match error {
      LlmError::Provider { status, message } => {
        assert_eq!(status, 429);
        assert_eq!(message, "slow down");
      }
      other => panic!("expected a provider error, got {other:?}"),
    }
  }

  #[tokio::test]
  async fn an_off_contract_payload_is_an_invalid_response() {
    let dir = TempDir::new().unwrap();
    let provider = loaded_provider(&dir).await;

    let error = provider.chat(chat_request("garbage")).await.unwrap_err();
    assert!(
      matches!(error, LlmError::InvalidResponse(_)),
      "expected an invalid response error, got {error:?}"
    );
  }

  #[tokio::test]
  async fn an_engine_failure_passes_through_as_an_extension_error() {
    let dir = TempDir::new().unwrap();
    let provider = loaded_provider(&dir).await;

    let error = provider.chat(chat_request("explode")).await.unwrap_err();
    assert!(
      matches!(error, LlmError::Extension(ExtensionError::Script(_))),
      "expected a script error passthrough, got {error:?}"
    );
  }

  #[tokio::test]
  async fn an_unserved_extension_is_unavailable() {
    let dir = TempDir::new().unwrap();
    let (_storage, manager) = test_manager(&dir);
    let provider = ExtensionLlmProvider::new(Arc::new(manager), "ghost".to_string());

    let error = provider.chat(chat_request("gpt-mock")).await.unwrap_err();
    match error {
      LlmError::Unavailable(message) => {
        assert!(message.contains("ghost"), "unexpected message: {message}")
      }
      other => panic!("expected unavailable, got {other:?}"),
    }
  }

  #[test]
  fn the_router_reports_zero_providers() {
    let dir = TempDir::new().unwrap();
    let (storage, _manager, router) = test_router(&dir);
    // A record without the llm contract does not count, and neither does a
    // record declaring an unrelated one.
    apply_llm_fixture(&storage, "ext-plain", None);
    apply_llm_fixture(&storage, "ext-other", Some(r#"["other"]"#));

    let error = match router.default_provider() {
      Err(error) => error,
      Ok(provider) => panic!(
        "expected resolution to fail, got {}",
        provider.extension_id()
      ),
    };
    match error {
      LlmError::Unavailable(message) => {
        assert_eq!(message, "no llm provider registered");
      }
      other => panic!("expected unavailable, got {other:?}"),
    }
  }

  #[tokio::test]
  async fn the_router_resolves_the_single_provider() {
    let dir = TempDir::new().unwrap();
    let (storage, manager, router) = test_router(&dir);
    apply_llm_fixture(&storage, "ext-llm", Some(r#"["llm"]"#));
    // A non-llm record must not confuse resolution.
    apply_llm_fixture(&storage, "ext-plain", None);
    manager.reconcile().await;

    let provider = router.default_provider().unwrap();
    assert_eq!(provider.extension_id(), "ext-llm");
    // The resolved provider is the working facade end to end.
    let response = provider.chat(chat_request("gpt-mock")).await.unwrap();
    assert_eq!(response.choices[0].message.content, "canned hello");

    // Explicit selection stays available alongside the default.
    assert_eq!(router.provider("ext-plain").extension_id(), "ext-plain");
  }

  #[test]
  fn the_router_rejects_ambiguous_providers() {
    let dir = TempDir::new().unwrap();
    let (storage, _manager, router) = test_router(&dir);
    apply_llm_fixture(&storage, "ext-llm-b", Some(r#"["llm"]"#));
    apply_llm_fixture(&storage, "ext-llm-a", Some(r#"["llm", "other"]"#));

    let error = match router.default_provider() {
      Err(error) => error,
      Ok(provider) => panic!(
        "expected resolution to fail, got {}",
        provider.extension_id()
      ),
    };
    match error {
      LlmError::Unavailable(message) => {
        assert!(
          message.contains("ambiguous"),
          "unexpected message: {message}"
        );
        assert!(
          message.contains("ext-llm-a"),
          "expected ext-llm-a listed: {message}"
        );
        assert!(
          message.contains("ext-llm-b"),
          "expected ext-llm-b listed: {message}"
        );
      }
      other => panic!("expected unavailable, got {other:?}"),
    }
  }
}
