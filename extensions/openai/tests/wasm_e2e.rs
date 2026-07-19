//! End-to-end smoke test for the real WASM artifact: build
//! `lycoris_ext_openai.wasm`, load it through the actual `WasmEngine`
//! (capability gating, `configure` delivery at load), and drive a `chat`
//! call through the real `lycoris.http` host import against a mock OpenAI
//! server. The guest's answers are validated against the typed wire
//! contract in `lycoris-extension::llm`.
//!
//! Ignored by default: the test requires the `wasm32-unknown-unknown`
//! target, which CI does not install yet (a later batch adds the target to
//! CI and enables this). Run locally with:
//!
//! ```sh
//! cargo test -p lycoris-ext-openai --test wasm_e2e -- --ignored
//! ```

use std::{
  collections::BTreeMap,
  path::PathBuf,
  process::Command,
  sync::{Arc, Mutex},
};

use lycoris_extension::{
  ChatResponse, EngineKind, EngineLimits, ExtensionEngine, ExtensionManifest, ExtensionPackage,
  LlmError, WasmEngine, from_wire,
};

/// Fail loudly, with the remediation, when the wasm target is missing.
fn ensure_wasm32_target() {
  let Ok(output) = Command::new("rustup")
    .args(["target", "list", "--installed"])
    .output()
  else {
    return; // No rustup: the build below reports its own failure.
  };
  let installed = String::from_utf8_lossy(&output.stdout);
  assert!(
    installed
      .lines()
      .any(|line| line.trim() == "wasm32-unknown-unknown"),
    "the wasm32-unknown-unknown target is not installed; run `rustup target add wasm32-unknown-unknown` first"
  );
}

/// Build the release wasm artifact with the workspace's own cargo and
/// return its path. Uses `--locked` so the committed lockfile is what gets
/// built.
fn build_wasm_artifact() -> PathBuf {
  let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("../..")
    .canonicalize()
    .unwrap();
  ensure_wasm32_target();
  let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
    .args([
      "build",
      "--release",
      "--locked",
      "--target",
      "wasm32-unknown-unknown",
      "--package",
      "lycoris-ext-openai",
    ])
    .current_dir(&root)
    .status()
    .unwrap();
  assert!(
    status.success(),
    "the wasm32 build of lycoris-ext-openai failed"
  );
  let target_dir = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
    || root.join("target"),
    |dir| {
      let dir = PathBuf::from(dir);
      if dir.is_absolute() {
        dir
      } else {
        root.join(dir)
      }
    },
  );
  let artifact = target_dir.join("wasm32-unknown-unknown/release/lycoris_ext_openai.wasm");
  assert!(
    artifact.is_file(),
    "expected the wasm artifact at {}",
    artifact.display()
  );
  artifact
}

/// One recorded request the mock server saw.
struct Recorded {
  head: String,
  body: String,
}

/// A minimal mock OpenAI server: one request per connection, canned
/// per-path answers, every request recorded. Aborted on drop.
struct MockOpenAi {
  base_url: String,
  recorded: Arc<Mutex<Vec<Recorded>>>,
  task: tokio::task::JoinHandle<()>,
}

impl MockOpenAi {
  async fn start() -> Self {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&recorded);
    let task = tokio::spawn(async move {
      loop {
        let Ok((mut stream, _)) = listener.accept().await else {
          break;
        };
        let recorder = Arc::clone(&recorder);
        tokio::spawn(async move {
          // Byte-wise header read avoids over-reading into the body.
          let mut head = Vec::new();
          let mut byte = [0u8; 1];
          while !head.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await.unwrap_or(0) == 0 {
              return;
            }
            head.push(byte[0]);
            if head.len() > 64 * 1024 {
              return;
            }
          }
          let head = String::from_utf8_lossy(&head).into_owned();
          let content_length: usize = head
            .lines()
            .find_map(|line| {
              line
                .to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse().ok())
            })
            .unwrap_or(0);
          let mut body = vec![0u8; content_length];
          if stream.read_exact(&mut body).await.is_err() {
            return;
          }
          let body = String::from_utf8_lossy(&body).into_owned();
          let path = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();
          recorder.lock().unwrap().push(Recorded {
            head: head.clone(),
            body: body.clone(),
          });

          let (status, reason, response_body): (u16, &str, String) = if path
            == "/v1/chat/completions"
          {
            if body.contains("rate-me") {
              (
                429,
                "Too Many Requests",
                r#"{"error":{"message":"slow down","type":"rate_limit_error","code":"rate_limit_exceeded"}}"#
                  .to_string(),
              )
            } else {
              (
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
                }"#
                  .to_string(),
              )
            }
          } else {
            (
              404,
              "Not Found",
              r#"{"error":{"message":"no such route","type":"invalid_request_error"}}"#.to_string(),
            )
          };
          let response = format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
            response_body.len()
          );
          let _ = stream.write_all(response.as_bytes()).await;
        });
      }
    });
    Self {
      base_url,
      recorded,
      task,
    }
  }
}

impl Drop for MockOpenAi {
  fn drop(&mut self) {
    self.task.abort();
  }
}

#[tokio::test]
#[ignore = "requires the wasm32-unknown-unknown target (not installed on CI yet); run with --ignored"]
async fn wasm_guest_end_to_end_configure_and_chat() {
  let artifact = build_wasm_artifact();
  let server = MockOpenAi::start().await;

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

  // Loading delivers `configure` with the settings; the wasm guest does real
  // JSON work, so give it comfortable fuel.
  let limits = EngineLimits {
    wasm_fuel_per_call: 100_000_000,
    ..EngineLimits::default()
  };
  let engine = WasmEngine::new(limits).unwrap();
  let instance = engine
    .load(
      &package,
      serde_json::json!({"api_key": "sk-test", "base_url": format!("{}/v1", server.base_url)}),
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
    let recorded = server.recorded.lock().unwrap();
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
