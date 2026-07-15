//! Small time utilities shared across the workspace.

use std::time::{SystemTime, UNIX_EPOCH};

/// Return the current Unix timestamp in milliseconds.
///
/// On systems where the clock predates the Unix epoch, returns `0`.
pub fn now_ms() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
    .unwrap_or(0)
}
