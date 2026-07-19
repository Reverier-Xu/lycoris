//! Validation for resource ids used as content-store file names.
//!
//! Ids arrive from remote peers during anti-entropy; without this check a
//! malicious id such as `../escape` would let a peer write outside the content
//! directory. Only non-empty ASCII alphanumeric ids with `-`/`_`/`.` are
//! accepted; any `..` sequence or leading dot is rejected. Every domain that
//! stores content on the filesystem (skills, rules, extension artifacts) shares
//! this single whitelist.

/// A resource id that is not safe to use as a content-store file name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid resource id: {0:?}")]
pub struct InvalidResourceId(pub String);

/// Validate a resource id before it is used to build a content file path.
///
/// Public so admission-side write paths (extension registration) can reject a
/// bad id with the same whitelist before anything reaches the stores.
pub fn validate(id: &str) -> Result<(), InvalidResourceId> {
  let valid = !id.is_empty()
    && !id.starts_with('.')
    && !id.contains("..")
    && id
      .chars()
      .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'));
  if valid {
    Ok(())
  } else {
    Err(InvalidResourceId(id.to_string()))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rejects_ids_that_escape_the_content_directory() {
    for id in ["../escape", "a/b", "", ".hidden", "a..b", "a\\b", "a b"] {
      assert!(validate(id).is_err(), "id: {id:?}");
    }
  }

  #[test]
  fn accepts_ids_within_the_whitelist() {
    for id in ["skill-1", "a_b.C", "UPPER.lower-9", "x"] {
      assert!(validate(id).is_ok(), "id: {id:?}");
    }
  }
}
