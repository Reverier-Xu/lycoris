//! End-to-end smoke test for the real WASM artifact: build
//! `lycoris_ext_openai.wasm`, load it through the actual `WasmEngine`
//! (capability gating, `configure` delivery at load), and drive a `chat`
//! call through the real `lycoris.http` host import against a mock OpenAI
//! server. The guest's answers are validated against the typed wire
//! contract in `lycoris-extension::llm`.
//!
//! Ignored by default: the test requires the `wasm32-unknown-unknown`
//! target, so a plain `cargo test` skips it; CI installs the target and runs
//! it explicitly (the `wasm-provider-tests` job). Run locally with:
//!
//! ```sh
//! cargo test -p lycoris-ext-openai --test wasm_e2e -- --ignored
//! ```

use std::collections::BTreeMap;

use lycoris_extension::{
  ChatResponse, EngineKind, EngineLimits, ExtensionEngine, ExtensionManifest, ExtensionPackage,
  LlmError, WasmEngine, from_wire,
};
use lycoris_testkit::http::{MockHttpServer, MockResponse, RecordedRequest};

/// Canned OpenAI-compatible responses for the guest e2e, including the
/// upstream rate-limit case the provider must translate into wire errors.
fn mock_openai_response(request: &RecordedRequest) -> MockResponse {
  if request.path() != "/v1/chat/completions" {
    return MockResponse::new(
      404,
      "Not Found",
      r#"{"error":{"message":"no such route","type":"invalid_request_error"}}"#,
    )
    .header("content-type", "application/json");
  }
  if request.body.contains("rate-me") {
    return MockResponse::new(
      429,
      "Too Many Requests",
      r#"{"error":{"message":"slow down","type":"rate_limit_error","code":"rate_limit_exceeded"}}"#,
    )
    .header("content-type", "application/json");
  }
  MockResponse::new(
    200,
    "OK",
    r#"{
      "id": "chatcmpl-mock",
      "object": "chat.completion",
      "created": 1700000000,
      "model": "gpt-mock",
      "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "canned hello"},
        "finish_reason": "stop"
      }],
      "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
    }"#,
  )
  .header("content-type", "application/json")
}

#[tokio::test]
#[ignore = "requires the wasm32-unknown-unknown target; run with --ignored"]
async fn wasm_guest_end_to_end_configure_and_chat() {
  let artifact = lycoris_testkit::wasm::build_wasm_artifact("lycoris-ext-openai");
  let server = MockHttpServer::start(mock_openai_response).await;

  let manifest = ExtensionManifest::from_map(&BTreeMap::from([
    ("semver".to_string(), "0.1.0".to_string()),
    ("capabilities".to_string(), r#"["http"]"#.to_string()),
    ("provides".to_string(), r#"["llm"]"#.to_string()),
  ]))
  .unwrap();
  let package = ExtensionPackage::new(
    "openai".to_string(),
    "openai".to_string(),
    1,
    EngineKind::Wasm,
    String::new(),
    manifest,
    std::fs::read(&artifact).unwrap(),
  );

  // Loading delivers `configure` with the settings. The default fuel budget
  // is sized for the real guest's JSON work, so no override is needed here.
  let engine = WasmEngine::new(EngineLimits::default()).unwrap();
  let instance = engine
    .load(
      &package,
      serde_json::json!({"api_key": "sk-test", "base_url": format!("{}/v1", server.base_url())}),
    )
    .await
    .unwrap();

  // Happy path: the typed wire contract decodes the real guest's answer.
  let output = instance
    .invoke(
      "chat",
      &serde_json::to_vec(&serde_json::json!({
        "model": "gpt-mock",
        "messages": [{"role": "user", "content": "hi"}],
      }))
      .unwrap(),
    )
    .await
    .unwrap();
  let response: ChatResponse = from_wire(&output).unwrap();
  assert_eq!(response.model, "gpt-mock");
  assert_eq!(response.choices.len(), 1);
  assert_eq!(
    response.choices[0].message.role,
    lycoris_extension::Role::Assistant
  );
  assert_eq!(response.choices[0].message.content, "canned hello");
  assert_eq!(response.choices[0].finish_reason, "stop");
  let usage = response.usage.unwrap();
  assert_eq!(usage.total_tokens, 7);

  // The request that left the guest carried auth and a pinned stream flag.
  {
    let recorded = server.recorded();
    assert_eq!(recorded.len(), 1);
    assert!(
      recorded[0].head.starts_with("POST /v1/chat/completions"),
      "unexpected request: {}",
      recorded[0].head
    );
    assert!(
      recorded[0]
        .head
        .to_ascii_lowercase()
        .contains("authorization: bearer sk-test"),
      "missing the bearer header: {}",
      recorded[0].head
    );
    let upstream_body: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
    assert_eq!(upstream_body["stream"], false);
    assert_eq!(upstream_body["model"], "gpt-mock");
  }

  // Provider failure: upstream 429 rides back as the section 3 error
  // payload, mapping to LlmError::Provider on the host side.
  let output = instance
    .invoke(
      "chat",
      &serde_json::to_vec(&serde_json::json!({
        "model": "rate-me",
        "messages": [{"role": "user", "content": "hi"}],
      }))
      .unwrap(),
    )
    .await
    .unwrap();
  match LlmError::from_wire_error(&output) {
    Some(LlmError::Provider { status, message }) => {
      assert_eq!(status, 429);
      assert_eq!(message, "slow down");
    }
    other => panic!("expected a provider error document, got {other:?}"),
  }
}
