//! A minimal mock HTTP/1.1 server: one request per connection, every
//! request recorded, responses routed by a caller-provided handler. The
//! accept task is aborted on drop.

use std::sync::{Arc, Mutex};

use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::TcpStream,
  task::JoinHandle,
};

/// One recorded request the mock server saw.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
  pub head: String,
  pub body: String,
}

impl RecordedRequest {
  /// The request path (the second token of the request line).
  pub fn path(&self) -> &str {
    self
      .head
      .lines()
      .next()
      .and_then(|line| line.split_whitespace().nth(1))
      .unwrap_or("/")
  }
}

/// One canned response.
#[derive(Debug, Clone)]
pub struct MockResponse {
  pub status: u16,
  pub reason: String,
  pub headers: Vec<(String, String)>,
  pub body: Vec<u8>,
}

impl MockResponse {
  /// A response with a plain body and no extra headers.
  pub fn new(status: u16, reason: &str, body: impl Into<Vec<u8>>) -> Self {
    Self {
      status,
      reason: reason.to_string(),
      headers: Vec::new(),
      body: body.into(),
    }
  }

  /// A `content-type: application/json` response serializing `body`.
  pub fn json(status: u16, reason: &str, body: &serde_json::Value) -> Self {
    Self::new(status, reason, body.to_string()).header("content-type", "application/json")
  }

  /// Add one response header.
  pub fn header(mut self, name: &str, value: &str) -> Self {
    self.headers.push((name.to_string(), value.to_string()));
    self
  }
}

/// The running mock server.
pub struct MockHttpServer {
  base_url: String,
  recorded: Arc<Mutex<Vec<RecordedRequest>>>,
  task: JoinHandle<()>,
}

impl MockHttpServer {
  /// Bind an ephemeral loopback port and serve `handler` until dropped.
  pub async fn start(
    handler: impl Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static,
  ) -> Self {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
      .await
      .unwrap_or_else(|err| panic!("bind the mock http listener: {err}"));
    let base_url = format!(
      "http://{}",
      listener
        .local_addr()
        .unwrap_or_else(|err| panic!("read the listener address: {err}"))
    );
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let handler = Arc::new(handler);

    let task = {
      let recorded = Arc::clone(&recorded);
      tokio::spawn(async move {
        loop {
          let Ok((mut stream, _)) = listener.accept().await else {
            break;
          };
          let recorded = Arc::clone(&recorded);
          let handler = Arc::clone(&handler);
          tokio::spawn(async move {
            let Some(request) = read_request(&mut stream).await else {
              return;
            };
            recorded
              .lock()
              .unwrap_or_else(|err| panic!("the recorded lock is poisoned: {err}"))
              .push(request.clone());
            let response = handler(&request);
            let mut head = format!("HTTP/1.1 {} {}\r\n", response.status, response.reason);
            for (name, value) in &response.headers {
              head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str(&format!(
              "content-length: {}\r\nconnection: close\r\n\r\n",
              response.body.len()
            ));
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&response.body).await;
          });
        }
      })
    };

    Self {
      base_url,
      recorded,
      task,
    }
  }

  /// The `http://127.0.0.1:<port>` base URL.
  pub fn base_url(&self) -> &str {
    &self.base_url
  }

  /// A snapshot of every request the server has seen so far.
  pub fn recorded(&self) -> Vec<RecordedRequest> {
    self
      .recorded
      .lock()
      .unwrap_or_else(|err| panic!("the recorded lock is poisoned: {err}"))
      .clone()
  }
}

impl Drop for MockHttpServer {
  fn drop(&mut self) {
    self.task.abort();
  }
}

/// Read one request: headers byte-wise (avoiding over-reads into the body),
/// then exactly the content-length body bytes. `None` on a broken
/// connection.
async fn read_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
  let mut head = Vec::new();
  let mut byte = [0u8; 1];
  while !head.ends_with(b"\r\n\r\n") {
    if stream.read(&mut byte).await.ok()? == 0 {
      return None;
    }
    head.push(byte[0]);
    if head.len() > 64 * 1024 {
      return None;
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
  stream.read_exact(&mut body).await.ok()?;
  Some(RecordedRequest {
    head,
    body: String::from_utf8_lossy(&body).into_owned(),
  })
}
