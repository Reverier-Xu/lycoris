#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Plugin engine layer.
//!
//! Hosts the plugin package model, the manifest model with its wire-format
//! validation, and the two sandboxed execution engines (WASM via wasmtime,
//! Lua via mlua) behind one common invocation contract. Payloads are JSON
//! bytes end-to-end.

mod engine;
mod error;
mod manifest;
mod package;

pub use engine::{EngineKind, EngineLimits, PluginEngine, PluginInstance};
pub use error::{PluginError, Result};
pub use manifest::{DEFAULT_ENTRY, HookDecl, HookErrorPolicy, PluginManifest};
pub use package::PluginPackage;
