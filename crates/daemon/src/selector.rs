use std::collections::HashMap;

/// Return true if `metadata` matches all key/value pairs in `selector`.
///
/// `metadata` accepts any string map used by the domain records (`HashMap` or
/// `BTreeMap`). An empty selector matches everything.
pub fn matches_selector<M>(metadata: &M, selector: &HashMap<String, String>) -> bool
where
  for<'a> &'a M: IntoIterator<Item = (&'a String, &'a String)>, {
  selector.iter().all(|(key, value)| {
    metadata
      .into_iter()
      .any(|(meta_key, meta_value)| meta_key == key && meta_value == value)
  })
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
