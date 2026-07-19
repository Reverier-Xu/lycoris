//! LLM provider contract: the typed host-side trait and the wire convention
//! an extension implements to serve LLM calls (llm-provider design, sections
//! 2 and 3).
//!
//! Three independently testable layers meet here:
//!
//! 1. The typed [`LlmProvider`] trait is what Rust callers see.
//! 2. The method/JSON convention ([`CHAT_METHOD`], [`EMBED_METHOD`],
//!    [`MODELS_METHOD`]) is what any extension — on any engine — implements to
//!    be a provider; the JSON of the types in this module *is* the wire format,
//!    exchanged through `ExtensionManager::invoke` unchanged.
//! 3. Provider mapping (e.g. OpenAI request/response translation) lives in the
//!    guest extension; only the wire shapes cross this crate.
//!
//! Error convention (section 3): when the upstream provider failed, the
//! payload is a [`WireError`] document (`{"error": {message, type, code?,
//! status?}}`) and the facade maps it to [`LlmError::Provider`] via
//! [`LlmError::from_wire_error`]. Transport and engine failures are engine
//! errors, never synthesized error payloads.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::ExtensionError;

/// Wire method for a chat completion (llm-provider design, section 3).
pub const CHAT_METHOD: &str = "chat";
/// Wire method for embeddings (llm-provider design, section 3).
pub const EMBED_METHOD: &str = "embed";
/// Wire method listing the provider's models (llm-provider design, section 3).
pub const MODELS_METHOD: &str = "models";

/// The `provides` contract name an extension manifest declares to be
/// discoverable as an LLM provider (llm-provider design, section 3).
pub const PROVIDES_LLM: &str = "llm";

/// The role of a chat message author (OpenAI-flavored, engine-neutral).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
  /// System prompt steering the conversation.
  System,
  /// End-user input.
  User,
  /// Model output.
  Assistant,
  /// Tool result fed back into the conversation.
  Tool,
}

/// One message in a chat conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
  /// Author role.
  pub role: Role,
  /// Text content.
  pub content: String,
}

/// A chat completion request. Streaming is out of scope for invoke
/// semantics; callers never set a stream flag on this wire shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatRequest {
  /// Provider-side model identifier.
  pub model: String,
  /// Conversation so far, oldest first.
  pub messages: Vec<ChatMessage>,
  /// Sampling temperature; omitted when unset so provider defaults apply.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub temperature: Option<f32>,
  /// Maximum completion tokens; omitted when unset.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub max_tokens: Option<u32>,
}

/// A chat completion response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatResponse {
  /// Model that produced the completion (as reported by the provider).
  pub model: String,
  /// Completion candidates; v1 providers return exactly one.
  pub choices: Vec<Choice>,
  /// Token accounting, when the provider reports it.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub usage: Option<Usage>,
}

/// One completion candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Choice {
  /// Candidate position.
  pub index: u32,
  /// The produced message.
  pub message: ChatMessage,
  /// Why generation stopped (`stop`, `length`, ...); provider vocabulary.
  pub finish_reason: String,
}

/// Token accounting for one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
  /// Tokens in the prompt.
  pub prompt_tokens: u32,
  /// Tokens in the completion.
  pub completion_tokens: u32,
  /// Total tokens billed.
  pub total_tokens: u32,
}

/// An embeddings request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbedRequest {
  /// Provider-side embedding model identifier.
  pub model: String,
  /// Inputs to embed, one vector per entry.
  pub input: Vec<String>,
}

/// An embeddings response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbedResponse {
  /// Vectors in request order.
  pub data: Vec<Embedding>,
}

/// One embedding vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Embedding {
  /// Index of the input this vector belongs to.
  pub index: u32,
  /// The embedding vector.
  pub embedding: Vec<f32>,
}

/// The section 3 error payload: what an extension returns when the upstream
/// provider failed (`{"error": {...}}` on the wire).
///
/// `status` carries the upstream HTTP status when there is one; guests use 0
/// (or omit it) for failures without an upstream response, e.g. answering
/// "not configured" before `configure` ran (llm-provider design, section 5).
/// The facade maps the document to [`LlmError::Provider`]; `type` and `code`
/// stay on the wire for diagnostics but are not modelled in [`LlmError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
  /// Human-readable failure description.
  pub message: String,
  /// Failure class (`provider_error`, `not_configured`, ...); free-form.
  pub r#type: String,
  /// Provider-specific error code, when it reports one.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub code: Option<String>,
  /// Upstream HTTP status; 0/absent for failures without one.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub status: Option<u16>,
}

impl WireError {
  /// A provider failure without a provider-specific code or status.
  pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
    Self {
      message: message.into(),
      r#type: kind.into(),
      code: None,
      status: None,
    }
  }

  /// Serialize into the wire envelope (`{"error": {...}}`). Encoding a plain
  /// data struct is infallible in practice; callers that cannot propagate
  /// fall back to an empty-object document.
  pub fn to_payload(&self) -> Vec<u8> {
    #[derive(Serialize)]
    struct Envelope<'a> {
      error: &'a WireError,
    }
    serde_json::to_vec(&Envelope { error: self }).unwrap_or_else(|_| b"{}".to_vec())
  }
}

/// Errors surfaced by the typed LLM layer.
#[derive(Debug, Error)]
pub enum LlmError {
  /// The upstream provider said no; `status` is its HTTP status, or 0 for
  /// failures without an upstream response (llm-provider design, section 2).
  #[error("provider error (status {status}): {message}")]
  Provider {
    /// Upstream HTTP status; 0 when no upstream response exists.
    status: u16,
    /// Provider's failure message.
    message: String,
  },
  /// No provider extension is reachable (none registered, or routing found
  /// no node serving one).
  #[error("no llm provider available: {0}")]
  Unavailable(String),
  /// Passthrough of the extension engine/manager error underneath the call.
  /// This layer sees the engine's [`ExtensionError`]; the daemon facade maps
  /// its manager errors onto this variant.
  #[error("extension error: {0}")]
  Extension(#[from] ExtensionError),
  /// The extension returned a payload that does not match the wire
  /// convention (malformed JSON, missing fields, wrong types).
  #[error("invalid llm response: {0}")]
  InvalidResponse(String),
}

impl LlmError {
  /// Map a section 3 error payload to [`LlmError::Provider`]; `None` when
  /// `payload` is not an error document (i.e. it should be decoded as a
  /// normal response instead). A missing `status` maps to 0.
  pub fn from_wire_error(payload: &[u8]) -> Option<LlmError> {
    #[derive(Deserialize)]
    struct Envelope {
      error: WireError,
    }
    let envelope: Envelope = serde_json::from_slice(payload).ok()?;
    Some(LlmError::Provider {
      status: envelope.error.status.unwrap_or(0),
      message: envelope.error.message,
    })
  }
}

/// The typed host-side LLM contract (llm-provider design, section 2).
#[async_trait]
pub trait LlmProvider: Send + Sync {
  /// Run a chat completion.
  async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, LlmError>;
  /// Embed a batch of inputs.
  async fn embed(&self, request: EmbedRequest) -> Result<EmbedResponse, LlmError>;
  /// List the provider's model identifiers (the facade flattens the wire's
  /// `{data: [{id}]}` shape).
  async fn models(&self) -> Result<Vec<String>, LlmError>;
}

/// Serialize a typed value into its wire JSON.
pub fn to_wire<T: Serialize>(value: &T) -> Result<Vec<u8>, LlmError> {
  serde_json::to_vec(value)
    .map_err(|err| LlmError::InvalidResponse(format!("failed to encode the wire payload: {err}")))
}

/// Decode a wire payload into its typed form; malformed payloads are
/// [`LlmError::InvalidResponse`].
pub fn from_wire<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, LlmError> {
  serde_json::from_slice(bytes)
    .map_err(|err| LlmError::InvalidResponse(format!("failed to decode the wire payload: {err}")))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn chat_request() -> ChatRequest {
    ChatRequest {
      model: "gpt-x".to_string(),
      messages: vec![
        ChatMessage {
          role: Role::System,
          content: "be brief".to_string(),
        },
        ChatMessage {
          role: Role::User,
          content: "hello".to_string(),
        },
      ],
      temperature: Some(0.5),
      max_tokens: Some(64),
    }
  }

  fn chat_response() -> ChatResponse {
    ChatResponse {
      model: "gpt-x".to_string(),
      choices: vec![Choice {
        index: 0,
        message: ChatMessage {
          role: Role::Assistant,
          content: "hi".to_string(),
        },
        finish_reason: "stop".to_string(),
      }],
      usage: Some(Usage {
        prompt_tokens: 10,
        completion_tokens: 2,
        total_tokens: 12,
      }),
    }
  }

  #[test]
  fn role_wire_names_are_lowercase() {
    for (role, name) in [
      (Role::System, "system"),
      (Role::User, "user"),
      (Role::Assistant, "assistant"),
      (Role::Tool, "tool"),
    ] {
      assert_eq!(serde_json::to_value(role).unwrap(), serde_json::json!(name));
      assert_eq!(
        serde_json::from_value::<Role>(serde_json::json!(name)).unwrap(),
        role
      );
    }
  }

  #[test]
  fn chat_request_round_trips() {
    let request = chat_request();
    let wire = to_wire(&request).unwrap();
    assert_eq!(from_wire::<ChatRequest>(&wire).unwrap(), request);
  }

  #[test]
  fn chat_request_omits_unset_optionals() {
    let mut request = chat_request();
    request.temperature = None;
    request.max_tokens = None;
    let wire: serde_json::Value = serde_json::from_slice(&to_wire(&request).unwrap()).unwrap();
    assert_eq!(
      wire,
      serde_json::json!({
        "model": "gpt-x",
        "messages": [
          {"role": "system", "content": "be brief"},
          {"role": "user", "content": "hello"},
        ],
      })
    );
  }

  #[test]
  fn chat_response_round_trips_with_usage() {
    let response = chat_response();
    let wire = to_wire(&response).unwrap();
    assert_eq!(from_wire::<ChatResponse>(&wire).unwrap(), response);
  }

  #[test]
  fn chat_response_defaults_missing_usage() {
    let wire = br#"{"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}"#;
    let response = from_wire::<ChatResponse>(wire).unwrap();
    assert_eq!(response.usage, None);
    // ... and an absent usage stays absent on the wire.
    assert!(
      !to_wire(&response)
        .unwrap()
        .windows(5)
        .any(|w| w == b"usage")
    );
  }

  #[test]
  fn unknown_fields_are_ignored_on_decode() {
    // Upstream responses carry fields the contract does not model (id,
    // created, object, logprobs, ...); they must not break decoding.
    let wire = br#"{
      "id": "chatcmpl-1",
      "object": "chat.completion",
      "created": 1,
      "model": "gpt-x",
      "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "hi", "refusal": null},
        "finish_reason": "stop",
        "logprobs": null
      }],
      "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2, "details": {}}
    }"#;
    let response = from_wire::<ChatResponse>(wire).unwrap();
    assert_eq!(response.choices.len(), 1);
    assert_eq!(
      response.usage,
      Some(Usage {
        prompt_tokens: 1,
        completion_tokens: 1,
        total_tokens: 2,
      })
    );
  }

  #[test]
  fn embed_types_round_trip() {
    let request = EmbedRequest {
      model: "embed-x".to_string(),
      input: vec!["a".to_string(), "b".to_string()],
    };
    let wire = to_wire(&request).unwrap();
    assert_eq!(from_wire::<EmbedRequest>(&wire).unwrap(), request);

    let response = EmbedResponse {
      data: vec![Embedding {
        index: 0,
        embedding: vec![0.25, -1.5],
      }],
    };
    let wire = to_wire(&response).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(
      value,
      serde_json::json!({"data": [{"index": 0, "embedding": [0.25, -1.5]}]})
    );
    assert_eq!(from_wire::<EmbedResponse>(&wire).unwrap(), response);
  }

  #[test]
  fn required_fields_stay_required_on_decode() {
    assert!(from_wire::<ChatRequest>(br#"{"messages":[]}"#).is_err());
    assert!(from_wire::<ChatResponse>(br#"{"model":"gpt-x"}"#).is_err());
    assert!(from_wire::<ChatMessage>(br#"{"content":"hi"}"#).is_err());
  }

  #[test]
  fn malformed_wire_payloads_are_invalid_responses() {
    let Err(LlmError::InvalidResponse(message)) = from_wire::<ChatResponse>(b"not json") else {
      panic!("expected an invalid response error");
    };
    assert!(message.contains("decode"), "unexpected message: {message}");
  }

  /// Assert the mapped provider error: [`LlmError`], like [`ExtensionError`],
  /// deliberately carries no `PartialEq`.
  fn assert_provider(error: Option<LlmError>, status: u16, message: &str) {
    match error {
      Some(LlmError::Provider {
        status: got,
        message: got_message,
      }) => {
        assert_eq!(got, status);
        assert_eq!(got_message, message);
      }
      other => panic!("expected a provider error, got {other:?}"),
    }
  }

  #[test]
  fn wire_error_documents_map_to_provider_errors() {
    let payload = WireError {
      message: "rate limited".to_string(),
      r#type: "rate_limit_error".to_string(),
      code: Some("rate_limit_exceeded".to_string()),
      status: Some(429),
    }
    .to_payload();
    assert_provider(LlmError::from_wire_error(&payload), 429, "rate limited");
  }

  #[test]
  fn wire_error_status_defaults_to_zero() {
    // "not configured" and friends carry no upstream status (design section 5).
    let payload = WireError::new("not_configured", "not configured").to_payload();
    assert_provider(LlmError::from_wire_error(&payload), 0, "not configured");
  }

  #[test]
  fn non_error_payloads_are_not_wire_errors() {
    assert!(LlmError::from_wire_error(&to_wire(&chat_response()).unwrap()).is_none());
    assert!(LlmError::from_wire_error(br#"{"error":"nope"}"#).is_none());
    assert!(LlmError::from_wire_error(b"not json").is_none());
  }

  #[test]
  fn wire_error_payload_shape_matches_the_convention() {
    let payload = WireError {
      message: "bad key".to_string(),
      r#type: "authentication_error".to_string(),
      code: Some("invalid_api_key".to_string()),
      status: Some(401),
    }
    .to_payload();
    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(
      value,
      serde_json::json!({
        "error": {
          "message": "bad key",
          "type": "authentication_error",
          "code": "invalid_api_key",
          "status": 401,
        }
      })
    );
    // Optional fields disappear instead of serializing as null.
    let value: serde_json::Value =
      serde_json::from_slice(&WireError::new("not_configured", "not configured").to_payload())
        .unwrap();
    assert_eq!(
      value,
      serde_json::json!({"error": {"message": "not configured", "type": "not_configured"}})
    );
  }

  #[test]
  fn llm_error_display_is_informative() {
    assert_eq!(
      LlmError::Provider {
        status: 401,
        message: "bad key".to_string(),
      }
      .to_string(),
      "provider error (status 401): bad key"
    );
    assert_eq!(
      LlmError::Unavailable("nothing registered".to_string()).to_string(),
      "no llm provider available: nothing registered"
    );
    assert!(
      LlmError::from(ExtensionError::Timeout(std::time::Duration::from_secs(1)))
        .to_string()
        .starts_with("extension error:")
    );
  }

  #[test]
  fn method_names_match_the_wire_convention() {
    assert_eq!(CHAT_METHOD, "chat");
    assert_eq!(EMBED_METHOD, "embed");
    assert_eq!(MODELS_METHOD, "models");
    assert_eq!(PROVIDES_LLM, "llm");
  }

  /// A typed-caller smoke double: proves the trait is implementable and
  /// object-safe for the facade and for test doubles.
  struct MockProvider;

  #[async_trait]
  impl LlmProvider for MockProvider {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
      let mut response = chat_response();
      response.model = request.model;
      Ok(response)
    }

    async fn embed(&self, request: EmbedRequest) -> Result<EmbedResponse, LlmError> {
      Ok(EmbedResponse {
        data: request
          .input
          .iter()
          .enumerate()
          .map(|(index, _)| Embedding {
            index: u32::try_from(index).unwrap_or(u32::MAX),
            embedding: vec![1.0],
          })
          .collect(),
      })
    }

    async fn models(&self) -> Result<Vec<String>, LlmError> {
      Ok(vec!["gpt-x".to_string()])
    }
  }

  #[tokio::test]
  async fn the_trait_is_implementable_and_object_safe() {
    let provider: Box<dyn LlmProvider> = Box::new(MockProvider);
    let response = provider.chat(chat_request()).await.unwrap();
    assert_eq!(response.model, "gpt-x");
    let embeddings = provider
      .embed(EmbedRequest {
        model: "m".to_string(),
        input: vec!["a".to_string(), "b".to_string()],
      })
      .await
      .unwrap();
    assert_eq!(embeddings.data.len(), 2);
    assert_eq!(provider.models().await.unwrap(), vec!["gpt-x".to_string()]);
  }
}
