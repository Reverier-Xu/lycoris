//! Cluster-level integration test for the real WASM OpenAI provider
//! (llm-provider design, section 7): build `lycoris_ext_openai.wasm` inside
//! the test, register it on node-0 with a selector only node-1's label
//! matches, then drive a typed `chat` from node-0's `LlmRouter`. The call
//! finds no local instance on node-0 and forwards one hop to node-1, whose
//! guest egresses to a mock OpenAI server using the node-local
//! `[extensions.local.openai]` settings — asserting sync, capability
//! announcement, routing, HTTP egress, and response mapping in one pass.
//!
//! Ignored by default: the test requires the `wasm32-unknown-unknown`
//! target, so a plain `cargo test` skips it; CI installs the target and runs
//! it explicitly (the `wasm-provider-tests` job). Run locally with:
//!
//! ```sh
//! cargo test -p lycoris-daemon --test extension_wasm -- --ignored
//! ```

use std::{
  collections::HashMap,
  path::PathBuf,
  process::Command,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU16, Ordering},
  },
  time::Duration,
};

use lycoris_client::{ClusterClient, ExtensionClient};
use lycoris_config::{ClusterConfig, DaemonConfig, ExtensionsConfig, NodeConfig, TlsConfig};
use lycoris_core::ClusterKey;
use lycoris_extension::{ChatMessage, ChatRequest, LlmProvider, Role, Usage};
use lycoris_proto::node::{
  RegisterExtensionRequest, ResourceKind, ResourceScope as ProtoResourceScope,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use tempfile::TempDir;
use tokio::time;

// Distinct from the integration.rs range (56000) so the two targets can run
// side by side without port collisions.
static NEXT_BASE_PORT: AtomicU16 = AtomicU16::new(58000);

fn alloc_base_port() -> u16 {
  // Each test reserves a 100-port block so internal and external addresses
  // cannot collide.
  NEXT_BASE_PORT.fetch_add(100, Ordering::SeqCst)
}

fn generate_test_certs(
  node_count: usize,
) -> (TempDir, PathBuf, PathBuf, Vec<PathBuf>, Vec<PathBuf>) {
  let dir = TempDir::new().unwrap();

  let ca_key = KeyPair::generate().unwrap();
  let mut ca_params = CertificateParams::new(vec!["lycoris-test-ca".to_string()]).unwrap();
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  let ca_issuer_for_signing = Issuer::from_params(&ca_params, &ca_key);
  let ca_cert = ca_params.self_signed(&ca_key).unwrap();

  let ca_cert_path = dir.path().join("ca.crt");
  let ca_key_path = dir.path().join("ca.key");
  std::fs::write(&ca_cert_path, ca_cert.pem()).unwrap();
  std::fs::write(&ca_key_path, ca_key.serialize_pem()).unwrap();

  let mut cert_paths = Vec::with_capacity(node_count);
  let mut key_paths = Vec::with_capacity(node_count);

  for i in 0..node_count {
    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    let cert = params.signed_by(&key, &ca_issuer_for_signing).unwrap();

    let cert_path = dir.path().join(format!("node{i}.crt"));
    let key_path = dir.path().join(format!("node{i}.key"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();

    cert_paths.push(cert_path);
    key_paths.push(key_path);
  }

  (dir, ca_cert_path, ca_key_path, cert_paths, key_paths)
}

fn client_tls_bundle(
  cert_path: &std::path::Path, key_path: &std::path::Path, ca_path: &std::path::Path,
) -> lycoris_tls::TlsBundle {
  lycoris_tls::load_tls_bundle(cert_path, key_path, ca_path).unwrap()
}

/// Connect to a freshly spawned node, retrying until its listener is up
/// (same rationale as integration.rs: a fixed startup sleep flakes under
/// parallel test load).
async fn connect_client(url: &str, tls: &lycoris_tls::TlsBundle, key_hex: &str) -> ClusterClient {
  let start = std::time::Instant::now();
  loop {
    match ClusterClient::connect(url, tls).await {
      Ok(client) => return client.with_cluster_key(key_hex.to_string()),
      Err(error) => {
        if start.elapsed() >= Duration::from_secs(10) {
          panic!("failed to connect to {url}: {error}");
        }
        time::sleep(Duration::from_millis(100)).await;
      }
    }
  }
}

/// Connect an extension client to a freshly spawned node, retrying until its
/// listener is up (same rationale as `connect_client`).
async fn connect_extension_client(
  url: &str, tls: &lycoris_tls::TlsBundle, key_hex: &str,
) -> ExtensionClient {
  let start = std::time::Instant::now();
  loop {
    match ExtensionClient::connect(url, tls).await {
      Ok(client) => return client.with_cluster_key(key_hex.to_string()),
      Err(error) => {
        if start.elapsed() >= Duration::from_secs(10) {
          panic!("failed to connect to {url}: {error}");
        }
        time::sleep(Duration::from_millis(100)).await;
      }
    }
  }
}

#[allow(clippy::too_many_arguments)]
fn build_config(
  id: &str, listen_port: u16, bootstrap_peers: Vec<String>, data_dir: PathBuf,
  ca_cert_path: &std::path::Path, ca_key_path: &std::path::Path, cert_path: &std::path::Path,
  key_path: &std::path::Path,
) -> DaemonConfig {
  DaemonConfig {
    node: NodeConfig {
      id: id.to_string(),
      address: format!("https://127.0.0.1:{listen_port}"),
      labels: HashMap::new(),
    },
    cluster: ClusterConfig {
      listen_address: format!("127.0.0.1:{listen_port}"),
      bootstrap_peers,
    },
    tls: TlsConfig {
      ca_cert: ca_cert_path.to_string_lossy().to_string(),
      ca_key: ca_key_path.to_string_lossy().to_string(),
      cert: cert_path.to_string_lossy().to_string(),
      key: key_path.to_string_lossy().to_string(),
    },
    data_dir: data_dir.to_string_lossy().to_string(),
    extensions: ExtensionsConfig::default(),
  }
}

fn spawn_runtime(config: DaemonConfig, key: ClusterKey) {
  tokio::spawn(async move {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    if let Err(e) =
      lycoris_daemon::runtime::run_with_shutdown(config, shutdown_tx, shutdown_rx, Some(key)).await
    {
      eprintln!("runtime error: {e:?}");
    }
  });
}

/// Spawn a node that hands its in-process typed facades (llm-provider
/// design, section 2) back through the returned receiver.
fn spawn_runtime_with_handles(
  config: DaemonConfig, key: ClusterKey,
) -> tokio::sync::oneshot::Receiver<lycoris_daemon::runtime::NodeHandles> {
  let (handles_tx, handles_rx) = tokio::sync::oneshot::channel();
  tokio::spawn(async move {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    if let Err(e) = lycoris_daemon::runtime::run_with_shutdown_and_handles(
      config,
      shutdown_tx,
      shutdown_rx,
      Some(key),
      handles_tx,
    )
    .await
    {
      eprintln!("runtime error: {e:?}");
    }
  });
  handles_rx
}

async fn wait_for_resource(
  client: &mut ClusterClient, kind: ResourceKind, id: &str, timeout: Duration,
) {
  let start = std::time::Instant::now();
  loop {
    let resources = client
      .list_resources(kind, HashMap::new(), ProtoResourceScope::Unspecified)
      .await
      .expect("list resources failed");
    if resources.iter().any(|resource| {
      resource
        .metadata
        .as_ref()
        .map(|metadata| metadata.id == id)
        .unwrap_or(false)
    }) {
      return;
    }
    if start.elapsed() >= timeout {
      panic!("timed out waiting for {id} to appear in {kind:?}");
    }
    time::sleep(Duration::from_millis(200)).await;
  }
}

/// Poll until `node_id`'s register (as seen through `client`) carries the
/// annotation `key`, returning its value.
async fn wait_for_annotation(
  client: &mut ClusterClient, node_id: &str, key: &str, timeout: Duration,
) -> String {
  let start = std::time::Instant::now();
  loop {
    let resources = client
      .list_resources(
        ResourceKind::Node,
        HashMap::new(),
        ProtoResourceScope::Unspecified,
      )
      .await
      .expect("list resources failed");
    for resource in resources {
      let Some(lycoris_proto::node::resource::Body::Node(lycoris_proto::node::NodeBody {
        node: Some(node),
      })) = resource.body
      else {
        continue;
      };
      if node.id == node_id
        && let Some(value) = node.annotations.get(key)
      {
        return value.clone();
      }
    }
    if start.elapsed() >= timeout {
      panic!("timed out waiting for annotation {key} on {node_id}");
    }
    time::sleep(Duration::from_millis(200)).await;
  }
}

/// Whether the `wasm32-unknown-unknown` target is installed. Without rustup
/// the build below is left to report its own failure.
fn wasm32_target_installed() -> bool {
  let Ok(output) = Command::new("rustup")
    .args(["target", "list", "--installed"])
    .output()
  else {
    return true;
  };
  String::from_utf8_lossy(&output.stdout)
    .lines()
    .any(|line| line.trim() == "wasm32-unknown-unknown")
}

/// Build the release wasm artifact with the workspace's own cargo and
/// return its path, or `None` — after printing the remediation — when the
/// `wasm32-unknown-unknown` target is not installed, so the test skips
/// itself instead of failing (the skip policy of the crate-local wasm e2e).
/// Uses `--locked` so the committed lockfile is what gets built.
fn build_wasm_artifact() -> Option<PathBuf> {
  if !wasm32_target_installed() {
    eprintln!(
      "skipping: the wasm32-unknown-unknown target is not installed; run `rustup target add wasm32-unknown-unknown` first"
    );
    return None;
  }
  let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("../..")
    .canonicalize()
    .unwrap();
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
  Some(artifact)
}

/// One recorded request the mock server saw.
struct Recorded {
  head: String,
  body: String,
}

/// A minimal mock OpenAI server: one request per connection, a canned chat
/// completion, every request recorded. Aborted on drop.
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
#[ignore = "requires the wasm32-unknown-unknown target; run with --ignored"]
async fn wasm_openai_provider_serves_cluster_chat_from_the_capable_node() {
  let Some(artifact) = build_wasm_artifact() else {
    return;
  };
  let _ = lycoris_tls::install_crypto_provider();
  let server = MockOpenAi::start().await;

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  let mut configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  // node-1 serves the provider: the label the manifest selector matches,
  // plus the node-local `[extensions.local.openai]` settings the design
  // reserves for secrets (llm-provider design, section 5) — the api key and
  // base url never leave the node.
  configs[1]
    .node
    .labels
    .insert("role".to_string(), "runner".to_string());
  configs[1].extensions.local.insert(
    "openai".to_string(),
    HashMap::from([
      ("api_key".to_string(), "sk-test".to_string()),
      ("base_url".to_string(), format!("{}/v1", server.base_url)),
    ]),
  );

  spawn_runtime(configs[1].clone(), cluster_key.clone());
  let handles_rx = spawn_runtime_with_handles(configs[0].clone(), cluster_key.clone());
  let handles = time::timeout(Duration::from_secs(10), handles_rx)
    .await
    .expect("node-0 did not hand out its handles in time")
    .expect("node-0 handles channel closed before startup finished");

  // Register the real artifact on node-0 through the operator RPC: a
  // cluster-shared record whose selector matches only node-1's label.
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut extension_client = connect_extension_client(&node0_url, &client_tls, &key_hex).await;
  extension_client
    .register(RegisterExtensionRequest {
      id: "openai".to_string(),
      name: "openai".to_string(),
      version: 1,
      engine: "wasm".to_string(),
      entry: String::new(),
      artifact: std::fs::read(&artifact).expect("read the wasm artifact"),
      manifest: HashMap::from([
        ("semver".to_string(), "0.1.0".to_string()),
        ("capabilities".to_string(), r#"["http"]"#.to_string()),
        ("provides".to_string(), r#"["llm"]"#.to_string()),
        ("selector".to_string(), r#"{"role":"runner"}"#.to_string()),
      ]),
      labels: HashMap::new(),
    })
    .await
    .expect("register the openai extension");

  // The record converges to node-1 through resource anti-entropy.
  let client_tls = client_tls_bundle(&cert_paths[1], &key_paths[1], &ca_cert_path);
  let mut node1_client = connect_client(&node1_url, &client_tls, &key_hex).await;
  wait_for_resource(
    &mut node1_client,
    ResourceKind::Extension,
    "openai",
    Duration::from_secs(30),
  )
  .await;

  // node-1 loads the guest (selector match) and announces the capability;
  // the register gossip delivers the annotation to node-0's membership view.
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut node0_client = connect_client(&node0_url, &client_tls, &key_hex).await;
  let announced = wait_for_annotation(
    &mut node0_client,
    "node-1",
    "ext.openai",
    Duration::from_secs(30),
  )
  .await;
  assert_eq!(announced, "0.1.0");

  // The typed facade on node-0 resolves the cluster's single llm provider
  // and invokes it: node-0's labels match no selector, so the call forwards
  // one hop to node-1, whose guest egresses to the mock.
  let provider = handles
    .llm
    .default_provider()
    .expect("resolve the default llm provider");
  assert_eq!(provider.extension_id(), "openai");
  let response = provider
    .chat(ChatRequest {
      model: "gpt-mock".to_string(),
      messages: vec![ChatMessage {
        role: Role::User,
        content: "hi".to_string(),
      }],
      temperature: None,
      max_tokens: None,
    })
    .await
    .expect("chat through the routed provider");
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

  // The request that left node-1's guest carried the node-local secret —
  // proof the merged `[extensions.local.openai]` settings reached the guest —
  // and the pinned stream flag of the section 3 wire convention.
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
}
