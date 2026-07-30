//! Legacy node-facing gRPC topology tests were removed when membership moved
//! to the libp2p overlay. Membership admission and convergence now live in
//! `overlay_admission.rs`; control-plane authorization lives in
//! `control_plane.rs`. The final CLI topology matrix is tracked by
//! `specs/plans/libp2p-overlay.md`.
