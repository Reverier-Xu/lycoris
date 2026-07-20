//! Dev-only test fixtures shared by the workspace's test suites: cluster
//! certificate generation ([`certs`]), a mock HTTP/1.1 server ([`http`]),
//! and the wasm artifact builder the wasm end-to-end tests load ([`wasm`]).
//!
//! Every helper panics with remediation on failure — a broken fixture must
//! fail its test loudly, never degrade it into a silent pass.

pub mod certs;
pub mod http;
pub mod wasm;
