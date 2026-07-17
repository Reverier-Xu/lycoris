#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod runtime;

pub(crate) mod membership;
pub(crate) mod resource;
pub(crate) mod rpc;
pub(crate) mod selector;
pub(crate) mod sync;
pub(crate) mod transport;

/// Read a persisted monotonic `u64` counter from the node meta table.
///
/// A missing value returns `None` so the caller can supply its own default;
/// unreadable or corrupt values are logged and also degrade to `None` — a
/// counter that cannot be trusted must not block startup.
pub(crate) fn persisted_counter(meta: &lycoris_storage::MetaStorage, key: &str) -> Option<u64> {
  match meta.get(key) {
    Ok(Some(raw)) => match raw.parse::<u64>() {
      Ok(value) => Some(value),
      Err(_) => {
        tracing::warn!(key, %raw, "ignoring corrupt persisted counter");
        None
      }
    },
    Ok(None) => None,
    Err(error) => {
      tracing::warn!(key, %error, "failed to read persisted counter");
      None
    }
  }
}

/// Build the canonical "peer RPC timed out" client error, keeping the mapping
/// in a single place inside the daemon.
pub(crate) fn peer_timeout(context: &'static str) -> lycoris_client::ClientError {
  lycoris_client::ClientError::Timeout(context)
}
