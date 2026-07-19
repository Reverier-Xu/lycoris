#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! OpenAI-compatible LLM provider extension (llm-provider design, section
//! 6): a WASM guest implementing the section 3 wire convention (`configure`,
//! `chat`, `embed`, `models`) on top of the `lycoris.http` host import.
//!
//! The crate is split so everything but the ABI glue is host-testable:
//!
//! - [`provider`]: the pure core — settings parsing, request construction,
//!   upstream response mapping;
//! - [`invoke`]: the wire dispatch with per-instance settings state;
//! - the `wasm32`-only glue exports `lycoris_alloc` / `lycoris_invoke` through
//!   [`lycoris_extension_guest::export_extension!`].
//!
//! Error semantics: provider-side failures (upstream non-2xx, transport,
//! off-contract bodies, missing configuration) are answered as the section 3
//! error payload — they are data for the caller. Only a `configure`
//! rejection (which must fail the load) and unknown methods return `Err`,
//! which the generated shim turns into a trap.

use std::cell::RefCell;

use serde_json::Value;

mod provider;

pub use provider::{
  DEFAULT_BASE_URL, HttpRequestSpec, Settings, chat_to_openai, embed_to_openai, error_document,
  models_to_openai, not_configured_document, openai_to_chat, openai_to_embed, openai_to_models,
};

/// Wire method names of the section 3 convention, plus `configure` (section
/// 5); the same strings `lycoris-extension::llm` pins on the host side.
const CONFIGURE_METHOD: &str = "configure";
const CHAT_METHOD: &str = "chat";
const EMBED_METHOD: &str = "embed";
const MODELS_METHOD: &str = "models";

thread_local! {
  /// The configured settings of this instance; `None` until `configure`
  /// runs. wasm32 guests are single-threaded and each engine instance is a
  /// fresh store, so a thread-local cell is the whole story.
  static SETTINGS: RefCell<Option<Settings>> = const { RefCell::new(None) };
}

/// The guest entry point behind the generated `lycoris_invoke` export:
/// dispatch one wire method. Public so host-side tests can drive the full
/// dispatch (with `host::http` stubbed off `wasm32`).
pub fn invoke(method: &str, payload: Value) -> Result<Value, String> {
  match method {
    CONFIGURE_METHOD => configure(payload),
    CHAT_METHOD => Ok(chat(payload)),
    EMBED_METHOD => Ok(embed(payload)),
    MODELS_METHOD => Ok(models(payload)),
    other => Err(format!("unknown method: {other}")),
  }
}

/// Accept and store the settings. A rejection propagates as `Err`, so the
/// shim traps and the engine fails the load (llm-provider design, section
/// 5). Idempotent: a repeated `configure` replaces the settings.
fn configure(payload: Value) -> Result<Value, String> {
  let settings = Settings::from_json(payload)?;
  SETTINGS.with(|cell| *cell.borrow_mut() = Some(settings));
  Ok(serde_json::json!({}))
}

/// `chat`: translate, egress, map back. Provider-side failures are section 3
/// error documents, never `Err`.
fn chat(payload: Value) -> Value {
  with_settings(|settings| match chat_to_openai(payload, settings) {
    Err(message) => error_document("invalid_request", &message, None, None),
    Ok(spec) => execute(spec, openai_to_chat),
  })
}

/// `embed`: same shape as [`chat`].
fn embed(payload: Value) -> Value {
  with_settings(|settings| match embed_to_openai(payload, settings) {
    Err(message) => error_document("invalid_request", &message, None, None),
    Ok(spec) => execute(spec, openai_to_embed),
  })
}

/// `models`: same shape as [`chat`].
fn models(_payload: Value) -> Value {
  with_settings(|settings| execute(models_to_openai(settings), openai_to_models))
}

/// Run `f` with the configured settings, or answer the section 5
/// "not configured" document.
fn with_settings(f: impl FnOnce(&Settings) -> Value) -> Value {
  let settings = SETTINGS.with(|cell| cell.borrow().clone());
  match settings {
    Some(settings) => f(&settings),
    None => not_configured_document(),
  }
}

/// Execute one outbound request through the `lycoris.http` host import and
/// map the answer with `map`. The host's own structured error document
/// (transport failures, disallowed hosts, ...) passes through unchanged.
fn execute(spec: HttpRequestSpec, map: impl FnOnce(u16, &str) -> Result<Value, String>) -> Value {
  let response = match lycoris_extension_guest::host::http(&spec.to_json()) {
    Ok(response) => response,
    Err(message) => return error_document("transport", &message, None, None),
  };
  if response.get("error").is_some() {
    return response;
  }
  let Some(status) = response
    .get("status")
    .and_then(Value::as_u64)
    .and_then(|status| u16::try_from(status).ok())
  else {
    return error_document(
      "invalid_response",
      "the host http response carries no status",
      None,
      None,
    );
  };
  let body = response
    .get("body")
    .and_then(Value::as_str)
    .unwrap_or_default();
  match map(status, body) {
    Ok(document) => document,
    Err(message) => error_document("invalid_response", &message, None, Some(status)),
  }
}

/// The `wasm32` glue: the `lycoris-abi-v1` exports dispatching to
/// [`invoke`].
#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)] // The generated shims do raw linear-memory IO; see lycoris-extension-guest.
mod glue {
  lycoris_extension_guest::export_extension!(crate::invoke);
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn configure_stores_settings_and_is_idempotent() {
    assert_eq!(
      invoke("configure", serde_json::json!({"api_key": "sk-a"})).unwrap(),
      serde_json::json!({})
    );
    assert_eq!(
      invoke(
        "configure",
        serde_json::json!({"api_key": "sk-b", "base_url": "http://mock/v1"})
      )
      .unwrap(),
      serde_json::json!({})
    );
    let stored = SETTINGS.with(|cell| cell.borrow().clone()).unwrap();
    assert_eq!(stored.api_key, "sk-b");
    assert_eq!(stored.base_url, "http://mock/v1");
  }

  #[test]
  fn configure_rejection_is_an_error_so_the_load_fails() {
    let result = invoke("configure", serde_json::json!({}));
    assert!(result.unwrap_err().contains("api_key"));
  }

  #[test]
  fn business_methods_before_configure_answer_not_configured() {
    // `#[test]` fns run on separate threads, so this thread-local is empty.
    for method in ["chat", "embed", "models"] {
      let document = invoke(method, serde_json::json!({})).unwrap();
      assert_eq!(
        document["error"],
        serde_json::json!({"message": "not configured", "type": "not_configured", "status": 0}),
        "expected the not_configured document for {method}"
      );
    }
  }

  #[test]
  fn unknown_methods_are_errors() {
    assert_eq!(
      invoke("summarize", serde_json::json!({})).unwrap_err(),
      "unknown method: summarize"
    );
  }

  #[cfg(not(target_arch = "wasm32"))]
  #[test]
  fn business_methods_off_wasm_answer_a_transport_error() {
    invoke("configure", serde_json::json!({"api_key": "sk-a"})).unwrap();
    // The host::http stub errors off wasm32; the guest surfaces that as a
    // transport-class error document, not a trap.
    let document = invoke("chat", serde_json::json!({"model": "m", "messages": []})).unwrap();
    assert_eq!(document["error"]["type"], "transport");
  }

  #[test]
  fn malformed_business_payloads_are_error_documents_not_traps() {
    invoke("configure", serde_json::json!({"api_key": "sk-a"})).unwrap();
    let document = invoke("chat", serde_json::json!(42)).unwrap();
    assert_eq!(document["error"]["type"], "invalid_request");
    assert_eq!(
      document["error"]["message"],
      "chat request must be a JSON object"
    );
  }
}
