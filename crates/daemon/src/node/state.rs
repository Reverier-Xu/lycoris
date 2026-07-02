use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
  Alive,
  Offline,
  Unknown,
}

impl fmt::Display for NodeState {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      NodeState::Alive => write!(f, "Alive"),
      NodeState::Offline => write!(f, "Offline"),
      NodeState::Unknown => write!(f, "Unknown"),
    }
  }
}
