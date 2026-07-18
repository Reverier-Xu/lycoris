//! SWIM failure-detector plumbing: mapping membership actions onto the wire,
//! sending probes, and answering inbound probe/state-message RPCs.

use lycoris_proto::node::{
  LeaveMessage as ProtoLeave, StateMessage, SuspectMessage as ProtoSuspect,
};
use tokio::time::timeout;

use super::{ClusterSync, RPC_TIMEOUT};
use crate::membership::{MemberRegister, SwimAction, SwimMessage, convert::register_to_proto};

/// An outbound action that [`ClusterSync`] can put on the wire.
///
/// `SwimAction::SendAck` is answered inline by the probe handler (acks travel
/// inside the `ProbeResponse`), and the SWIM state machine never broadcasts
/// Ping/Ack messages, so only these four combinations are dispatchable.
enum OutboundAction {
  Ping { target: String, seq: u64 },
  Alive { register: MemberRegister },
  Suspect { node_id: String, incarnation: u64 },
  Leave { node_id: String, incarnation: u64 },
}

impl OutboundAction {
  fn from_swim(action: SwimAction) -> Option<Self> {
    match action {
      SwimAction::SendPing { target, seq } => Some(Self::Ping { target, seq }),
      SwimAction::Broadcast(SwimMessage::Alive { register }) => Some(Self::Alive { register }),
      SwimAction::Broadcast(SwimMessage::Suspect {
        node_id,
        incarnation,
      }) => Some(Self::Suspect {
        node_id,
        incarnation,
      }),
      SwimAction::Broadcast(SwimMessage::Leave {
        node_id,
        incarnation,
      }) => Some(Self::Leave {
        node_id,
        incarnation,
      }),
      SwimAction::SendAck { .. }
      | SwimAction::Broadcast(SwimMessage::Ping { .. } | SwimMessage::Ack { .. }) => None,
    }
  }
}

impl ClusterSync {
  /// Dispatch a batch of SWIM actions produced by the membership service.
  pub async fn dispatch(&self, actions: Vec<SwimAction>) {
    for action in actions.into_iter().filter_map(OutboundAction::from_swim) {
      match action {
        OutboundAction::Ping { target, seq } => {
          let _ = self.send_probe_to(&target, seq).await;
        }
        OutboundAction::Alive { register } => {
          let sequence = self.sequence.next();
          let origin = self.local_node_id.clone();
          self
            .seen_pushes
            .lock()
            .await
            .insert((origin.clone(), sequence));
          self
            .broadcast_push(register_to_proto(&register), origin, sequence)
            .await;
        }
        OutboundAction::Suspect {
          node_id,
          incarnation,
        } => {
          self
            .seen_states
            .lock()
            .await
            .insert((node_id.clone(), incarnation, 1));
          self
            .broadcast_state_message(StateMessage {
              payload: Some(lycoris_proto::node::state_message::Payload::Suspect(
                ProtoSuspect {
                  node_id,
                  incarnation,
                },
              )),
            })
            .await;
        }
        OutboundAction::Leave {
          node_id,
          incarnation,
        } => {
          self
            .seen_states
            .lock()
            .await
            .insert((node_id.clone(), incarnation, 2));
          self
            .broadcast_state_message(StateMessage {
              payload: Some(lycoris_proto::node::state_message::Payload::Leave(
                ProtoLeave {
                  node_id,
                  incarnation,
                },
              )),
            })
            .await;
        }
      }
    }
  }

  /// Dispatch SWIM actions in the background.
  ///
  /// Used by anti-entropy merge paths (register fetches, full syncs, incoming
  /// pushes) where merging a rumor about the local node can produce a
  /// refutation broadcast (D4) that must not block the in-flight RPC.
  pub(crate) async fn spawn_dispatch(&self, actions: Vec<SwimAction>) {
    if actions.is_empty() {
      return;
    }
    let sync = self.clone();
    self
      .spawn_task(async move {
        sync.dispatch(actions).await;
      })
      .await;
  }

  /// Send one SWIM probe to a member. Failures share the reachability
  /// bookkeeping of every other peer RPC in this module tree (failed attempt
  /// mark + channel eviction): probe targets and sync endpoints are the same
  /// addresses, and the single selection policy (D9) should back off from a
  /// member that probes cannot reach either.
  async fn send_probe_to(&self, target_id: &str, seq: u64) -> bool {
    let address = match self.resolve_address(target_id).await {
      Some(addr) => addr,
      None => return false,
    };

    let mut client = match self.pool.connect(&address).await {
      Ok(client) => client,
      Err(_) => {
        self.record_peer_failure(&address).await;
        return false;
      }
    };

    match timeout(RPC_TIMEOUT, client.membership.probe(seq, "")).await {
      Ok(Ok(response)) => {
        if response.ack {
          self
            .service
            .on_probe(target_id, SwimMessage::Ack { seq })
            .await;
        }
        response.ack
      }
      Ok(Err(error)) => {
        tracing::warn!(%target_id, %error, "probe failed");
        self.record_peer_failure(&address).await;
        false
      }
      Err(_) => {
        tracing::warn!(%target_id, "probe timed out");
        self.record_peer_failure(&address).await;
        false
      }
    }
  }

  async fn resolve_address(&self, node_id: &str) -> Option<String> {
    self.service.member_address(node_id).await
  }

  /// Handle an incoming SWIM probe, returning whether to ack.
  pub async fn serve_probe(&self, seq: u64) -> bool {
    let from = self.local_node_id.clone();

    // Indirect probing is not implemented: probes always carry an empty
    // `target`, so every probe is treated as a direct ping to this node.
    let actions = self
      .service
      .on_probe(&from, SwimMessage::Ping { seq })
      .await;
    let ack = actions
      .iter()
      .any(|action| matches!(action, SwimAction::SendAck { .. }));
    self.spawn_dispatch(actions).await;
    ack
  }

  /// Handle an incoming state message (Suspect/Leave rumor).
  pub async fn serve_state_message(&self, message: StateMessage) {
    let from = self.local_node_id.clone();

    // Deduplicate gossiped Suspect/Leave state messages to prevent them from
    // cycling around the graph forever.
    let state_key = match &message.payload {
      Some(lycoris_proto::node::state_message::Payload::Suspect(suspect)) => {
        Some((suspect.node_id.clone(), suspect.incarnation, 1u8))
      }
      Some(lycoris_proto::node::state_message::Payload::Leave(leave)) => {
        Some((leave.node_id.clone(), leave.incarnation, 2u8))
      }
      None => None,
    };

    if let Some(key) = state_key {
      let already_seen = !self.seen_states.lock().await.insert(key);
      if already_seen {
        return;
      }
    }

    let actions = match message.payload {
      Some(lycoris_proto::node::state_message::Payload::Suspect(suspect)) => {
        self
          .service
          .on_probe(
            &from,
            SwimMessage::Suspect {
              node_id: suspect.node_id,
              incarnation: suspect.incarnation,
            },
          )
          .await
      }
      Some(lycoris_proto::node::state_message::Payload::Leave(leave)) => {
        self
          .service
          .on_probe(
            &from,
            SwimMessage::Leave {
              node_id: leave.node_id,
              incarnation: leave.incarnation,
            },
          )
          .await
      }
      None => Vec::new(),
    };
    self.spawn_dispatch(actions).await;
  }
}
