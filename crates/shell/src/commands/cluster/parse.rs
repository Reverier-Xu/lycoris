use std::collections::HashMap;

use lycoris_proto::node::ResourceKind;

use crate::error::ShellError;

pub fn parse_resource_kind(raw: &str) -> Result<ResourceKind, ShellError> {
  match raw.to_ascii_lowercase().as_str() {
    "node" | "nodes" | "no" => Ok(ResourceKind::Node),
    "session" | "sessions" | "sess" => Ok(ResourceKind::Session),
    "memory" | "memories" | "mem" => Ok(ResourceKind::Memory),
    "skill" | "skills" | "sk" => Ok(ResourceKind::Skill),
    "rule" | "rules" | "ru" => Ok(ResourceKind::Rule),
    "workspace" | "workspaces" | "ws" => Ok(ResourceKind::Workspace),
    _ => Err(ShellError::UnknownResourceKind(raw.to_string())),
  }
}

pub fn resource_name(kind: ResourceKind) -> String {
  match kind {
    ResourceKind::Node => "node",
    ResourceKind::Session => "session",
    ResourceKind::Memory => "memory",
    ResourceKind::Skill => "skill",
    ResourceKind::Rule => "rule",
    ResourceKind::Workspace => "workspace",
  }
  .to_string()
}

pub fn parse_selectors(raw: &[String]) -> Result<HashMap<String, String>, ShellError> {
  let mut selector = HashMap::new();
  for item in raw {
    let (key, value) = item
      .split_once('=')
      .ok_or_else(|| ShellError::InvalidSelector(item.clone()))?;
    selector.insert(key.to_string(), value.to_string());
  }
  Ok(selector)
}
