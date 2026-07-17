use std::collections::HashMap;

use lycoris_core::ResourceScope;
use lycoris_proto::node::{ResourceKind, ResourceScope as ProtoResourceScope};

use crate::error::ShellError;

/// Resource kinds accepted on the CLI, each with its accepted spellings. The
/// first spelling is the canonical name used in output — parsing and display
/// share this single table.
const RESOURCE_KINDS: &[(ResourceKind, &[&str])] = &[
  (ResourceKind::Node, &["node", "nodes", "no"]),
  (ResourceKind::Session, &["session", "sessions", "sess"]),
  (ResourceKind::Memory, &["memory", "memories", "mem"]),
  (ResourceKind::Skill, &["skill", "skills", "sk"]),
  (ResourceKind::Rule, &["rule", "rules", "ru"]),
  (ResourceKind::Workspace, &["workspace", "workspaces", "ws"]),
];

pub(crate) fn parse_resource_kind(raw: &str) -> Result<ResourceKind, ShellError> {
  let normalized = raw.to_ascii_lowercase();
  RESOURCE_KINDS
    .iter()
    .find(|(_, names)| names.contains(&normalized.as_str()))
    .map(|(kind, _)| *kind)
    .ok_or_else(|| ShellError::UnknownResourceKind(raw.to_string()))
}

pub(crate) fn resource_name(kind: ResourceKind) -> &'static str {
  RESOURCE_KINDS
    .iter()
    .find(|(candidate, _)| *candidate == kind)
    .map_or("unknown", |(_, names)| names[0])
}

/// Parse the CLI `--scope` value into the wire enum; absent means no filter.
///
/// The `"shared"` / `"local"` spellings come from `lycoris_core` (the single
/// codec source); only the enum mapping is local to this proto boundary.
pub(crate) fn parse_scope(raw: Option<String>) -> Result<ProtoResourceScope, ShellError> {
  let Some(raw) = raw else {
    return Ok(ProtoResourceScope::Unspecified);
  };
  let scope = raw
    .parse::<ResourceScope>()
    .map_err(|_| ShellError::UnknownScope(raw.clone()))?;
  Ok(scope_to_proto(scope))
}

/// Map the wire scope to the domain scope for display purposes; unscoped or
/// unknown values yield `None`.
pub(crate) fn scope_from_proto(raw: i32) -> Option<ResourceScope> {
  match ProtoResourceScope::try_from(raw) {
    Ok(ProtoResourceScope::ClusterShared) => Some(ResourceScope::ClusterShared),
    Ok(ProtoResourceScope::NodeLocal) => Some(ResourceScope::NodeLocal),
    _ => None,
  }
}

fn scope_to_proto(scope: ResourceScope) -> ProtoResourceScope {
  match scope {
    ResourceScope::ClusterShared => ProtoResourceScope::ClusterShared,
    ResourceScope::NodeLocal => ProtoResourceScope::NodeLocal,
  }
}

pub(crate) fn parse_selectors(raw: &[String]) -> Result<HashMap<String, String>, ShellError> {
  let mut selector = HashMap::new();
  for item in raw {
    let (key, value) = item
      .split_once('=')
      .ok_or_else(|| ShellError::InvalidSelector(item.clone()))?;
    selector.insert(key.to_string(), value.to_string());
  }
  Ok(selector)
}
