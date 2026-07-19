//! The pure, host-testable core of the OpenAI-compatible provider
//! (llm-provider design, sections 3 and 6): settings, outbound request
//! construction, and upstream response mapping. No host imports, no wasm —
//! the `wasm32` glue in `lib.rs` binds these to the exported `invoke`.
//!
//! The guest speaks the section 3 wire convention: requests arrive as the
//! section 2 JSON documents and answers are the section 2 response
//! documents; when the upstream provider fails, the answer is the section 3
//! error payload (`{"error": {message, type, code?, status?}}`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The default OpenAI API base.
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// Per-node settings delivered through `configure` (llm-provider design,
/// section 5): the API key and friends are node-local secrets and must never
/// ride the synced manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
  /// Provider API key (required).
  pub api_key: String,
  /// API base URL; defaults to [`DEFAULT_BASE_URL`]. Trailing slashes are
  /// trimmed so endpoint paths join with exactly one slash.
  #[serde(default = "default_base_url")]
  pub base_url: String,
  /// Optional OpenAI organization id, sent as the `openai-organization`
  /// header.
  #[serde(default)]
  pub organization: Option<String>,
}

fn default_base_url() -> String {
  DEFAULT_BASE_URL.to_string()
}

impl Settings {
  /// Parse settings from the `configure` payload. Unknown keys (e.g.
  /// `http_allow_hosts`, consumed host-side by the engine) are ignored.
  pub fn from_json(value: Value) -> Result<Self, String> {
    let mut settings: Self =
      serde_json::from_value(value).map_err(|err| format!("invalid settings: {err}"))?;
    if settings.api_key.is_empty() {
      return Err("settings.api_key must not be empty".to_string());
    }
    settings.base_url = settings.base_url.trim_end_matches('/').to_string();
    if settings.base_url.is_empty() {
      return Err("settings.base_url must not be empty".to_string());
    }
    Ok(settings)
  }
}

/// One outbound HTTP request in the shape of the section 4 request document
/// (`{method, url, headers, body?}`) the `lycoris.http` host import
/// executes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestSpec {
  /// HTTP method (`GET`/`POST`).
  pub method: &'static str,
  /// Full URL.
  pub url: String,
  /// Request headers.
  pub headers: BTreeMap<String, String>,
  /// Text body (provider APIs are JSON), absent for `GET`.
  pub body: Option<String>,
}

impl HttpRequestSpec {
  /// Serialize into the section 4 request document.
  pub fn to_json(&self) -> Value {
    let mut document = serde_json::json!({
      "method": self.method,
      "url": self.url,
      "headers": self.headers,
    });
    if let Some(body) = &self.body {
      document["body"] = Value::String(body.clone());
    }
    document
  }
}

/// The section 3 error payload (`{"error": {...}}`); the same shape
/// `lycoris-extension`'s `WireError` models on the host side, rebuilt here
/// because the guest cannot depend on the host crate.
pub fn error_document(kind: &str, message: &str, code: Option<&str>, status: Option<u16>) -> Value {
  let mut error = serde_json::json!({ "message": message, "type": kind });
  if let Some(code) = code {
    error["code"] = Value::String(code.to_string());
  }
  if let Some(status) = status {
    error["status"] = Value::from(status);
  }
  serde_json::json!({ "error": error })
}

/// Headers every provider call carries: bearer auth, JSON content type, and
/// the optional organization.
fn auth_headers(settings: &Settings) -> BTreeMap<String, String> {
  let mut headers = BTreeMap::from([
    (
      "authorization".to_string(),
      format!("Bearer {}", settings.api_key),
    ),
    ("content-type".to_string(), "application/json".to_string()),
  ]);
  if let Some(organization) = &settings.organization {
    headers.insert("openai-organization".to_string(), organization.clone());
  }
  headers
}

/// The wire error document answered when a business method runs before
/// `configure` delivered settings (llm-provider design, section 5: the guest
/// answers `Provider { status: 0, message: "not configured" }`).
pub fn not_configured_document() -> Value {
  error_document("not_configured", "not configured", None, Some(0))
}

/// Translate a section 2 `ChatRequest` payload into the outbound chat
/// completion call: `POST {base}/chat/completions` with `stream: false`
/// pinned (streaming is out of scope for invoke semantics). Unknown request
/// fields pass through, so OpenAI-compatible extensions of the request shape
/// keep working.
pub fn chat_to_openai(request: Value, settings: &Settings) -> Result<HttpRequestSpec, String> {
  let mut request = request;
  let object = request
    .as_object_mut()
    .ok_or_else(|| "chat request must be a JSON object".to_string())?;
  object.insert("stream".to_string(), Value::Bool(false));
  Ok(HttpRequestSpec {
    method: "POST",
    url: format!("{}/chat/completions", settings.base_url),
    headers: auth_headers(settings),
    body: Some(request.to_string()),
  })
}

/// Translate a section 2 `EmbedRequest` payload into the outbound embeddings
/// call: `POST {base}/embeddings`.
pub fn embed_to_openai(request: Value, settings: &Settings) -> Result<HttpRequestSpec, String> {
  if !request.is_object() {
    return Err("embed request must be a JSON object".to_string());
  }
  Ok(HttpRequestSpec {
    method: "POST",
    url: format!("{}/embeddings", settings.base_url),
    headers: auth_headers(settings),
    body: Some(request.to_string()),
  })
}

/// The outbound model listing call: `GET {base}/models`.
pub fn models_to_openai(settings: &Settings) -> HttpRequestSpec {
  HttpRequestSpec {
    method: "GET",
    url: format!("{}/models", settings.base_url),
    headers: auth_headers(settings),
    body: None,
  }
}

/// An upstream error body: OpenAI answers failures as
/// `{"error": {message, type, param, code}}`.
#[derive(Deserialize)]
struct UpstreamError {
  error: UpstreamErrorBody,
}

#[derive(Deserialize)]
struct UpstreamErrorBody {
  message: String,
  #[serde(default)]
  r#type: Option<String>,
  #[serde(default)]
  code: Option<String>,
}

/// Map a non-2xx upstream answer to the section 3 error payload, extracting
/// OpenAI's error body when it has one.
fn provider_error_document(status: u16, body: &str) -> Value {
  match serde_json::from_str::<UpstreamError>(body) {
    Ok(parsed) => error_document(
      parsed.error.r#type.as_deref().unwrap_or("provider_error"),
      &parsed.error.message,
      parsed.error.code.as_deref(),
      Some(status),
    ),
    Err(_) => error_document(
      "provider_error",
      &format!("upstream returned status {status}: {body}"),
      None,
      Some(status),
    ),
  }
}

/// Map an upstream chat completion answer to the section 2 `ChatResponse`
/// wire JSON. Non-2xx answers map to the section 3 error payload (a
/// successful translation); `Err` means the upstream claimed success but
/// returned something off-contract.
pub fn openai_to_chat(status: u16, body: &str) -> Result<Value, String> {
  if !(200..300).contains(&status) {
    return Ok(provider_error_document(status, body));
  }

  /// The upstream completion; fields the wire does not model are ignored.
  #[derive(Deserialize)]
  struct Completion {
    model: String,
    choices: Vec<CompletionChoice>,
    #[serde(default)]
    usage: Option<CompletionUsage>,
  }

  #[derive(Deserialize)]
  struct CompletionChoice {
    index: u32,
    message: CompletionMessage,
    #[serde(default)]
    finish_reason: Option<String>,
  }

  #[derive(Deserialize)]
  struct CompletionMessage {
    role: String,
    // Null for tool-call-only messages; the wire models text content only.
    #[serde(default)]
    content: Option<String>,
  }

  #[derive(Deserialize)]
  struct CompletionUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
  }

  let completion: Completion = serde_json::from_str(body)
    .map_err(|err| format!("upstream chat response is not a chat completion: {err}"))?;

  let mut response = serde_json::json!({
    "model": completion.model,
    "choices": completion
      .choices
      .into_iter()
      .map(|choice| {
        serde_json::json!({
          "index": choice.index,
          "message": {
            "role": choice.message.role,
            "content": choice.message.content.unwrap_or_default(),
          },
          "finish_reason": choice.finish_reason.unwrap_or_default(),
        })
      })
      .collect::<Vec<_>>(),
  });
  if let Some(usage) = completion.usage {
    response["usage"] = serde_json::json!({
      "prompt_tokens": usage.prompt_tokens,
      "completion_tokens": usage.completion_tokens,
      "total_tokens": usage.total_tokens,
    });
  }
  Ok(response)
}

/// Map an upstream embeddings answer to the section 2 `EmbedResponse` wire
/// JSON; same failure split as [`openai_to_chat`].
pub fn openai_to_embed(status: u16, body: &str) -> Result<Value, String> {
  if !(200..300).contains(&status) {
    return Ok(provider_error_document(status, body));
  }

  #[derive(Deserialize)]
  struct Embeddings {
    data: Vec<EmbeddingData>,
  }

  #[derive(Deserialize)]
  struct EmbeddingData {
    index: u32,
    // Passed through untouched: the guest translates documents, it does not
    // round-trip floats through f32 and lose precision.
    embedding: Value,
  }

  let parsed: Embeddings = serde_json::from_str(body)
    .map_err(|err| format!("upstream embeddings response is off-contract: {err}"))?;
  Ok(serde_json::json!({
    "data": parsed
      .data
      .into_iter()
      .map(|entry| serde_json::json!({"index": entry.index, "embedding": entry.embedding}))
      .collect::<Vec<_>>(),
  }))
}

/// Map an upstream model listing answer to the section 3 `models` shape
/// (`{data: [{id}]}`; the facade flattens it). Same failure split as
/// [`openai_to_chat`].
pub fn openai_to_models(status: u16, body: &str) -> Result<Value, String> {
  if !(200..300).contains(&status) {
    return Ok(provider_error_document(status, body));
  }

  #[derive(Deserialize)]
  struct Models {
    data: Vec<ModelData>,
  }

  #[derive(Deserialize)]
  struct ModelData {
    id: String,
  }

  let parsed: Models = serde_json::from_str(body)
    .map_err(|err| format!("upstream models response is off-contract: {err}"))?;
  Ok(serde_json::json!({
    "data": parsed
      .data
      .into_iter()
      .map(|model| serde_json::json!({"id": model.id}))
      .collect::<Vec<_>>(),
  }))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn settings() -> Settings {
    Settings {
      api_key: "sk-test".to_string(),
      base_url: DEFAULT_BASE_URL.to_string(),
      organization: None,
    }
  }

  #[test]
  fn settings_parse_with_defaults() {
    let parsed = Settings::from_json(serde_json::json!({"api_key": "sk-x"})).unwrap();
    assert_eq!(parsed.base_url, DEFAULT_BASE_URL);
    assert_eq!(parsed.organization, None);
  }

  #[test]
  fn settings_require_an_api_key() {
    let missing = Settings::from_json(serde_json::json!({}));
    assert!(missing.unwrap_err().contains("api_key"));
    let empty = Settings::from_json(serde_json::json!({"api_key": ""}));
    assert_eq!(empty.unwrap_err(), "settings.api_key must not be empty");
  }

  #[test]
  fn settings_trim_trailing_slashes_and_ignore_unknown_keys() {
    let parsed = Settings::from_json(serde_json::json!({
      "api_key": "sk-x",
      "base_url": "http://localhost:8080/v1/",
      "organization": "org-1",
      "http_allow_hosts": "[\"localhost\"]",
    }))
    .unwrap();
    assert_eq!(parsed.base_url, "http://localhost:8080/v1");
    assert_eq!(parsed.organization, Some("org-1".to_string()));
  }

  #[test]
  fn settings_reject_an_empty_base_url() {
    let result = Settings::from_json(serde_json::json!({"api_key": "sk-x", "base_url": "//"}));
    assert_eq!(result.unwrap_err(), "settings.base_url must not be empty");
  }

  #[test]
  fn chat_request_builds_the_openai_call() {
    let spec = chat_to_openai(
      serde_json::json!({
        "model": "gpt-x",
        "messages": [{"role": "user", "content": "hi"}],
        "temperature": 0.5,
        "response_format": {"type": "json_object"},
      }),
      &settings(),
    )
    .unwrap();
    assert_eq!(spec.method, "POST");
    assert_eq!(spec.url, "https://api.openai.com/v1/chat/completions");
    assert_eq!(spec.headers["authorization"], "Bearer sk-test");
    assert_eq!(spec.headers["content-type"], "application/json");
    assert!(!spec.headers.contains_key("openai-organization"));
    let body: Value = serde_json::from_str(&spec.body.unwrap()).unwrap();
    assert_eq!(body["stream"], false);
    assert_eq!(body["model"], "gpt-x");
    assert_eq!(
      body["messages"],
      serde_json::json!([{"role": "user", "content": "hi"}])
    );
    // Unknown fields pass through for OpenAI-compatible extensions.
    assert_eq!(
      body["response_format"],
      serde_json::json!({"type": "json_object"})
    );
  }

  #[test]
  fn chat_request_pins_stream_off_even_when_set() {
    let spec = chat_to_openai(
      serde_json::json!({"model": "gpt-x", "messages": [], "stream": true}),
      &settings(),
    )
    .unwrap();
    let body: Value = serde_json::from_str(&spec.body.unwrap()).unwrap();
    assert_eq!(body["stream"], false);
  }

  #[test]
  fn chat_request_rejects_non_object_payloads() {
    let result = chat_to_openai(serde_json::json!(["not", "an", "object"]), &settings());
    assert_eq!(result.unwrap_err(), "chat request must be a JSON object");
  }

  #[test]
  fn chat_request_joins_a_trailing_slash_base_url() {
    let mut settings = settings();
    settings.base_url = "http://mock/v1/".to_string();
    // The constructor normalizes; direct struct edits are re-trimmed by hand.
    settings.base_url = settings.base_url.trim_end_matches('/').to_string();
    let spec =
      chat_to_openai(serde_json::json!({"model": "m", "messages": []}), &settings).unwrap();
    assert_eq!(spec.url, "http://mock/v1/chat/completions");
  }

  #[test]
  fn organization_header_rides_when_set() {
    let mut settings = settings();
    settings.organization = Some("org-9".to_string());
    let spec = models_to_openai(&settings);
    assert_eq!(spec.headers["openai-organization"], "org-9");
  }

  #[test]
  fn chat_response_maps_to_the_wire_shape() {
    let body = r#"{
      "id": "chatcmpl-1",
      "object": "chat.completion",
      "created": 1700000000,
      "model": "gpt-x",
      "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "hello there", "refusal": null},
        "finish_reason": "stop",
        "logprobs": null
      }],
      "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}
    }"#;
    let response = openai_to_chat(200, body).unwrap();
    assert_eq!(
      response,
      serde_json::json!({
        "model": "gpt-x",
        "choices": [{
          "index": 0,
          "message": {"role": "assistant", "content": "hello there"},
          "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10},
      })
    );
  }

  #[test]
  fn chat_response_tolerates_missing_usage_and_null_fields() {
    let body = r#"{
      "model": "gpt-x",
      "choices": [{"index": 0, "message": {"role": "assistant", "content": null}, "finish_reason": null}]
    }"#;
    let response = openai_to_chat(200, body).unwrap();
    assert_eq!(
      response,
      serde_json::json!({
        "model": "gpt-x",
        "choices": [{
          "index": 0,
          "message": {"role": "assistant", "content": ""},
          "finish_reason": "",
        }],
      })
    );
    assert!(response.get("usage").is_none());
  }

  #[test]
  fn chat_response_maps_upstream_errors_to_the_error_contract() {
    let body = r#"{"error": {"message": "rate limited", "type": "rate_limit_error", "code": "rate_limit_exceeded"}}"#;
    let document = openai_to_chat(429, body).unwrap();
    assert_eq!(
      document,
      serde_json::json!({
        "error": {
          "message": "rate limited",
          "type": "rate_limit_error",
          "code": "rate_limit_exceeded",
          "status": 429,
        }
      })
    );
  }

  #[test]
  fn chat_response_maps_unparseable_error_bodies() {
    let document = openai_to_chat(502, "bad gateway").unwrap();
    assert_eq!(
      document,
      serde_json::json!({
        "error": {
          "message": "upstream returned status 502: bad gateway",
          "type": "provider_error",
          "status": 502,
        }
      })
    );
  }

  #[test]
  fn chat_response_rejects_off_contract_success_bodies() {
    let result = openai_to_chat(200, r#"{"unexpected": true}"#);
    assert!(result.unwrap_err().contains("not a chat completion"));
  }

  #[test]
  fn embed_request_builds_the_openai_call() {
    let spec = embed_to_openai(
      serde_json::json!({"model": "embed-x", "input": ["a", "b"]}),
      &settings(),
    )
    .unwrap();
    assert_eq!(spec.method, "POST");
    assert_eq!(spec.url, "https://api.openai.com/v1/embeddings");
    let body: Value = serde_json::from_str(&spec.body.unwrap()).unwrap();
    assert_eq!(
      body,
      serde_json::json!({"model": "embed-x", "input": ["a", "b"]})
    );
    assert!(embed_to_openai(serde_json::json!(42), &settings()).is_err());
  }

  #[test]
  fn embed_response_maps_to_the_wire_shape() {
    let body = r#"{
      "object": "list",
      "data": [
        {"object": "embedding", "index": 0, "embedding": [0.1, 0.2]},
        {"object": "embedding", "index": 1, "embedding": [0.3]}
      ],
      "usage": {"prompt_tokens": 2, "total_tokens": 2}
    }"#;
    let response = openai_to_embed(200, body).unwrap();
    assert_eq!(
      response,
      serde_json::json!({
        "data": [
          {"index": 0, "embedding": [0.1, 0.2]},
          {"index": 1, "embedding": [0.3]},
        ],
      })
    );
    let error = openai_to_embed(
      500,
      r#"{"error": {"message": "boom", "type": "server_error"}}"#,
    )
    .unwrap();
    assert_eq!(error["error"]["status"], 500);
    assert!(openai_to_embed(200, "junk").is_err());
  }

  #[test]
  fn models_request_builds_the_openai_call() {
    let spec = models_to_openai(&settings());
    assert_eq!(spec.method, "GET");
    assert_eq!(spec.url, "https://api.openai.com/v1/models");
    assert_eq!(spec.body, None);
    let document = spec.to_json();
    assert_eq!(document["method"], "GET");
    assert!(document.get("body").is_none());
  }

  #[test]
  fn models_response_maps_to_the_wire_shape() {
    let body = r#"{
      "object": "list",
      "data": [
        {"id": "gpt-x", "object": "model", "created": 1, "owned_by": "openai"},
        {"id": "gpt-y", "object": "model", "created": 2, "owned_by": "openai"}
      ]
    }"#;
    let response = openai_to_models(200, body).unwrap();
    assert_eq!(
      response,
      serde_json::json!({"data": [{"id": "gpt-x"}, {"id": "gpt-y"}]})
    );
    let error = openai_to_models(401, r#"{"error": {"message": "bad key", "type": "authentication_error", "code": "invalid_api_key"}}"#).unwrap();
    assert_eq!(error["error"]["code"], "invalid_api_key");
    assert_eq!(error["error"]["status"], 401);
    assert!(openai_to_models(200, "junk").is_err());
  }

  #[test]
  fn not_configured_document_matches_the_design() {
    assert_eq!(
      not_configured_document(),
      serde_json::json!({
        "error": {"message": "not configured", "type": "not_configured", "status": 0},
      })
    );
  }
}
