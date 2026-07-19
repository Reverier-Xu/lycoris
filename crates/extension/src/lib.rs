#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Extension engine layer.
//!
//! Hosts the extension package model, the manifest model with its wire-format
//! validation, and the two sandboxed execution engines (WASM via wasmtime,
//! Lua via mlua) behind one common invocation contract. Payloads are JSON
//! bytes end-to-end.

mod engine;
mod error;
mod lua;
mod manifest;
mod package;
mod wasm;

pub use engine::{EngineKind, EngineLimits, ExtensionEngine, ExtensionInstance};
pub use error::{ExtensionError, Result};
pub use lua::LuaEngine;
pub use manifest::{ExtensionManifest, HookDecl, HookErrorPolicy};
pub use package::{DEFAULT_ENTRY, ExtensionPackage};
pub use wasm::WasmEngine;
