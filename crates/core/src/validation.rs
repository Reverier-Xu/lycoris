//! Shared serde deserialization helpers.

use serde::Deserialize;

/// Deserialize a `String` and reject empty values at parse time.
pub fn non_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
  D: serde::Deserializer<'de>, {
  use serde::de::Error;

  let value = String::deserialize(deserializer)?;
  if value.is_empty() {
    return Err(D::Error::custom("value must not be empty"));
  }
  Ok(value)
}

#[cfg(test)]
mod tests {
  use serde::Deserialize;

  use super::*;

  #[derive(Debug, Deserialize)]
  struct Wrapper {
    #[serde(deserialize_with = "non_empty_string")]
    name: String,
  }

  #[test]
  fn accepts_non_empty_string() {
    let parsed: Wrapper = toml::from_str(r#"name = "alice""#).unwrap();
    assert_eq!(parsed.name, "alice");
  }

  #[test]
  fn rejects_empty_string() {
    let result: Result<Wrapper, _> = toml::from_str(r#"name = """#);
    assert!(result.is_err());
  }
}
