use std::collections::HashMap;

/// Return true if `metadata` matches all key/value pairs in `selector`.
///
/// An empty selector matches everything.
pub fn matches_selector(
  metadata: &HashMap<String, String>, selector: &HashMap<String, String>,
) -> bool {
  if selector.is_empty() {
    return true;
  }
  selector
    .iter()
    .all(|(key, value)| metadata.get(key) == Some(value))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn empty_selector_matches_everything() {
    let metadata = HashMap::from([("k".to_string(), "v".to_string())]);
    assert!(matches_selector(&metadata, &HashMap::new()));
  }

  #[test]
  fn matches_when_all_pairs_present() {
    let metadata = HashMap::from([
      ("a".to_string(), "1".to_string()),
      ("b".to_string(), "2".to_string()),
    ]);
    let selector = HashMap::from([("a".to_string(), "1".to_string())]);
    assert!(matches_selector(&metadata, &selector));
  }

  #[test]
  fn mismatches_when_any_pair_missing_or_different() {
    let metadata = HashMap::from([("a".to_string(), "1".to_string())]);
    let selector = HashMap::from([("a".to_string(), "2".to_string())]);
    assert!(!matches_selector(&metadata, &selector));

    let selector = HashMap::from([("b".to_string(), "1".to_string())]);
    assert!(!matches_selector(&metadata, &selector));
  }
}
