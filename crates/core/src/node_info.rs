use std::collections::HashMap;

/// Abstraction of node metadata. Every daemon instance implements this trait
/// so that the cluster can query labels, annotations and addressing info.
pub trait NodeInfo {
  fn id(&self) -> &str;
  fn address(&self) -> &str;
  fn labels(&self) -> &HashMap<String, String>;
  fn annotations(&self) -> &HashMap<String, String>;
}

/// A plain, in-memory implementation of [`NodeInfo`].
///
/// This is useful for tests, examples, and external callers that already know
/// a node's id and address and do not need to read dynamic labels from
/// persistent storage.
#[derive(Debug, Clone, Default)]
pub struct SimpleNode {
  id: String,
  address: String,
  labels: HashMap<String, String>,
  annotations: HashMap<String, String>,
}

impl SimpleNode {
  /// Build a node from its identifying metadata.
  pub fn new(
    id: impl Into<String>, address: impl Into<String>, labels: HashMap<String, String>,
    annotations: HashMap<String, String>,
  ) -> Self {
    Self {
      id: id.into(),
      address: address.into(),
      labels,
      annotations,
    }
  }

  /// Replace the address, useful when the listening endpoint differs from the
  /// initially configured value.
  pub fn with_address(mut self, address: impl Into<String>) -> Self {
    self.address = address.into();
    self
  }
}

impl NodeInfo for SimpleNode {
  fn id(&self) -> &str {
    &self.id
  }

  fn address(&self) -> &str {
    &self.address
  }

  fn labels(&self) -> &HashMap<String, String> {
    &self.labels
  }

  fn annotations(&self) -> &HashMap<String, String> {
    &self.annotations
  }
}
