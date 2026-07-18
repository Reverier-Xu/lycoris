//! Wire conversions between proto node messages and membership domain types.
//!
//! These live outside [`service`](crate::membership::service) so that
//! `MembershipService` speaks only domain types (D8); the rpc boundary and
//! the `crate::sync` proto round-trips convert through this module.

use lycoris_membership::{MemberRegister, MemberState};
use lycoris_proto::node::{NodeInfo as ProtoNodeInfo, NodeState};

/// Convert a wire `NodeInfo` into a domain register.
///
/// `updated_at_ms` is left at the register default on purpose: the merge
/// paths in `MembershipService` overwrite it with the local clock — the only
/// clock this node trusts — before the register enters local state.
pub fn proto_to_register(info: &ProtoNodeInfo) -> MemberRegister {
  MemberRegister::new(
    info.id.clone(),
    info.address.clone(),
    info.incarnation.max(1),
    info.heartbeat,
  )
  .with_state(state_from_proto(info.state))
  .with_labels(info.labels.clone())
  .with_annotations(info.annotations.clone())
}

pub fn register_to_proto(register: &MemberRegister) -> ProtoNodeInfo {
  ProtoNodeInfo {
    id: register.node_id().to_string(),
    address: register.address().to_string(),
    labels: register.labels().clone(),
    annotations: register.annotations().clone(),
    last_heartbeat_unix_ms: register.updated_at_ms(),
    state: state_to_proto(register.state()) as i32,
    incarnation: register.incarnation(),
    heartbeat: register.heartbeat(),
  }
}

/// The single domain-to-wire state mapping.
fn state_to_proto(state: MemberState) -> NodeState {
  match state {
    MemberState::Active => NodeState::Active,
    MemberState::Suspected => NodeState::Suspected,
    MemberState::Leaving => NodeState::Leaving,
    MemberState::Offline => NodeState::Offline,
  }
}

/// Decode the wire state into the domain state.
///
/// `UNSPECIFIED` and unrecognized values degrade to `Active` instead of being
/// rejected. The mapping is deterministic, so convergence (I1) is unaffected;
/// and keeping the register present with the neutral state is strictly better
/// for membership liveness (I2) than dropping a version-skewed peer's register
/// — or worse, inflating an unparseable rumor into `Offline`.
fn state_from_proto(raw: i32) -> MemberState {
  match NodeState::try_from(raw) {
    Ok(NodeState::Suspected) => MemberState::Suspected,
    Ok(NodeState::Leaving) => MemberState::Leaving,
    Ok(NodeState::Offline) => MemberState::Offline,
    Ok(NodeState::Active | NodeState::Unspecified) | Err(_) => MemberState::Active,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn register(id: &str) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", 1, 0).with_updated_at_ms(0)
  }

  #[test]
  fn register_to_proto_preserves_all_states() {
    for (state, expected) in [
      (MemberState::Active, NodeState::Active),
      (MemberState::Suspected, NodeState::Suspected),
      (MemberState::Leaving, NodeState::Leaving),
      (MemberState::Offline, NodeState::Offline),
    ] {
      let mut register = register("node");
      register.set_state(state);
      let proto = register_to_proto(&register);
      assert_eq!(proto.state, expected as i32);
      assert_eq!(proto.id, "node");
    }
  }

  #[test]
  fn proto_to_register_round_trips_all_states() {
    for state in [
      NodeState::Active,
      NodeState::Suspected,
      NodeState::Leaving,
      NodeState::Offline,
    ] {
      let mut proto = register_to_proto(&register("node"));
      proto.state = state as i32;
      let register = proto_to_register(&proto);
      assert_eq!(register_to_proto(&register).state, state as i32);
    }
  }

  #[test]
  fn proto_to_register_degrades_unknown_states_to_active() {
    let mut proto = register_to_proto(&register("node"));
    for raw in [NodeState::Unspecified as i32, -1, 42] {
      proto.state = raw;
      assert_eq!(
        proto_to_register(&proto).state(),
        MemberState::Active,
        "raw state {raw} must degrade to Active"
      );
    }
  }
}
