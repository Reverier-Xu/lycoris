//! Outbound HTTP host capability for guests (`lycoris-abi-v1` upgrade,
//! llm-provider design section 4).
//!
//! A guest hands the host a JSON request `{method, url, headers, body?}`
//! through linear memory and gets back a JSON response
//! `{status, headers, body}`. Bodies are text (provider APIs are JSON).
//! Failures that belong to the protocol — malformed request documents,
//! disallowed schemes or hosts, transport errors, over-limit responses —
//! come back to the guest as a structured error document
//! `{error: {type, message}}` instead of trapping it; only failures of the
//! ABI machinery itself (e.g. the guest allocator refusing the response
//! buffer) trap.
//!
//! Enforcement: only `http:`/`https:` URLs pass, the response body is
//! capped at [`MAX_RESPONSE_BODY_BYTES`], a single request may take at most
//! [`REQUEST_TIMEOUT`] (the engine invoke deadline is the backstop), and a
//! non-empty host allowlist rejects every other host. Non-2xx statuses are
//! *not* errors: the status passes through to the guest, which owns the
//! retry/failover policy.

use std::time::Duration;

/// Response body cap in bytes (llm-provider design section 4).
pub const MAX_RESPONSE_BODY_BYTES: u64 = 8 * 1024 * 1024;

/// Per-request wall-clock timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Build the shared HTTP agent: rustls/ring with webpki roots (the crate
/// features select exactly that stack). `http_status_as_error(false)` keeps
/// 4xx/5xx statuses as pass-through responses for the guest.
pub fn agent() -> ureq::Agent {
  let config = ureq::Agent::config_builder()
    .http_status_as_error(false)
    .timeout_global(Some(REQUEST_TIMEOUT))
    .build();
  ureq::Agent::new_with_config(config)
}

/// Execute one request document against `agent`, honouring the per-instance
/// host allowlist (`None` allows every host), and return the response (or a
/// structured error document) as JSON bytes. Blocking: callers run this on
/// `tokio::task::spawn_blocking`.
pub fn execute(agent: &ureq::Agent, request: &[u8], allow_hosts: &Option<Vec<String>>) -> Vec<u8> {
  match execute_inner(agent, request, allow_hosts) {
    Ok(response) => response,
    Err(error) => error.document(),
  }
}

/// A protocol-level failure reported to the guest as an error document.
struct HttpFailure {
  kind: &'static str,
  message: String,
}

impl HttpFailure {
  fn new(kind: &'static str, message: impl Into<String>) -> Self {
    Self {
      kind,
      message: message.into(),
    }
  }

  /// The structured error document handed back to the guest.
  fn document(&self) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
      "error": { "type": self.kind, "message": self.message }
    }))
    .unwrap_or_else(|_| {
      b"{\"error\":{\"type\":\"internal\",\"message\":\"encoding failed\"}}".to_vec()
    })
  }
}

/// The request document as decoded from the guest payload.
#[derive(serde::Deserialize)]
struct Request {
  method: String,
  url: String,
  #[serde(default)]
  headers: std::collections::BTreeMap<String, String>,
  body: Option<String>,
}

fn execute_inner(
  agent: &ureq::Agent, request: &[u8], allow_hosts: &Option<Vec<String>>,
) -> std::result::Result<Vec<u8>, HttpFailure> {
  let request: Request = serde_json::from_slice(request).map_err(|err| {
    HttpFailure::new(
      "invalid_request",
      format!("request is not a valid http document: {err}"),
    )
  })?;

  // The scheme check runs on the raw string: `http::Uri` rejects some
  // non-http schemes at parse time, which would blur the error taxonomy.
  let lower = request.url.to_ascii_lowercase();
  if !(lower.starts_with("http://") || lower.starts_with("https://")) {
    return Err(HttpFailure::new(
      "unsupported_scheme",
      format!(
        "only http: and https: URLs are allowed, got {:?}",
        request.url
      ),
    ));
  }
  let uri: ureq::http::Uri = request
    .url
    .parse()
    .map_err(|err| HttpFailure::new("invalid_request", format!("invalid url: {err}")))?;
  let host = uri
    .host()
    .ok_or_else(|| HttpFailure::new("invalid_request", "url has no host"))?;
  if let Some(allow_hosts) = allow_hosts {
    let host = host.to_ascii_lowercase();
    if !allow_hosts
      .iter()
      .any(|allowed| allowed.to_ascii_lowercase() == host)
    {
      return Err(HttpFailure::new(
        "host_not_allowed",
        format!("host {host:?} is not in http_allow_hosts"),
      ));
    }
  }

  let mut builder = ureq::http::Request::builder()
    .method(request.method.as_str())
    .uri(request.url.as_str());
  for (name, value) in &request.headers {
    builder = builder.header(name, value);
  }
  let request = match request.body {
    Some(body) => builder.body(body),
    None => builder.body(String::new()),
  }
  .map_err(|err| {
    HttpFailure::new(
      "invalid_request",
      format!("failed to build the request: {err}"),
    )
  })?;
  let response = agent
    .run(request)
    .map_err(|err| HttpFailure::new("transport", format!("request failed: {err}")))?;

  let status = response.status().as_u16();
  // Header names repeat across values; the JSON object keeps the last value
  // per name (v1: multi-value headers such as set-cookie are not modelled).
  let headers: serde_json::Map<String, serde_json::Value> = response
    .headers()
    .iter()
    .map(|(name, value)| {
      (
        name.as_str().to_string(),
        serde_json::Value::String(String::from_utf8_lossy(value.as_bytes()).into_owned()),
      )
    })
    .collect();
  let body = match response
    .into_body()
    .with_config()
    .limit(MAX_RESPONSE_BODY_BYTES)
    .read_to_string()
  {
    Ok(body) => body,
    Err(ureq::Error::BodyExceedsLimit(_)) => {
      return Err(HttpFailure::new(
        "response_too_large",
        format!(
          "response body exceeds the {} byte cap",
          MAX_RESPONSE_BODY_BYTES
        ),
      ));
    }
    Err(err) => {
      return Err(HttpFailure::new(
        "transport",
        format!("failed to read the response body: {err}"),
      ));
    }
  };

  serde_json::to_vec(&serde_json::json!({
    "status": status,
    "headers": headers,
    "body": body,
  }))
  .map_err(|err| HttpFailure::new("internal", format!("failed to encode the response: {err}")))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn request_document(url: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
      "method": "GET",
      "url": url,
      "headers": {},
    }))
    .unwrap()
  }

  fn error_type(document: &[u8]) -> String {
    let value: serde_json::Value = serde_json::from_slice(document).unwrap();
    value["error"]["type"].as_str().unwrap().to_string()
  }

  #[test]
  fn malformed_request_documents_are_structured_errors() {
    let agent = agent();
    assert_eq!(
      error_type(&execute(&agent, b"not json", &None)),
      "invalid_request"
    );
  }

  #[test]
  fn non_http_schemes_are_rejected() {
    let agent = agent();
    for url in ["file:///etc/passwd", "gopher://example.com", "ftp://x"] {
      let document = request_document(url);
      assert_eq!(
        error_type(&execute(&agent, &document, &None)),
        "unsupported_scheme",
        "expected scheme rejection for {url}"
      );
    }
  }

  #[test]
  fn the_allowlist_rejects_unlisted_hosts_before_any_io() {
    let agent = agent();
    let document = request_document("http://example.com/");
    let allow = Some(vec!["api.openai.com".to_string()]);
    assert_eq!(
      error_type(&execute(&agent, &document, &allow)),
      "host_not_allowed"
    );
  }

  #[test]
  fn the_allowlist_matches_hosts_case_insensitively() {
    let agent = agent();
    // Port 1 refuses the connection, so any outcome past the allowlist check
    // is a transport error; a case-sensitive (buggy) match would surface as
    // host_not_allowed instead.
    let document = request_document("http://LOCALHOST:1/");
    let allow = Some(vec!["localhost".to_string()]);
    assert_eq!(error_type(&execute(&agent, &document, &allow)), "transport");
  }
}
