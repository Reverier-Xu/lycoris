use std::collections::HashMap;

/// Abstraction of node metadata. Every daemon instance implements this trait
/// so that the cluster can query labels, annotations and addressing info.
pub trait NodeInfo {
  fn id(&self) -> &str;
  fn address(&self) -> &str;
  fn labels(&self) -> &HashMap<String, String>;
  fn annotations(&self) -> &HashMap<String, String>;
}
