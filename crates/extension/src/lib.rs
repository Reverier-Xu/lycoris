#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Extension engine layer.
//!
//! Hosts the extension package model, the manifest model with its wire-format
//! validation, the two sandboxed execution engines (WASM via wasmtime, Lua
//! via mlua) behind one common invocation contract, and the typed LLM
//! provider contract built on top of it. Payloads are JSON bytes end-to-end.

mod engine;
mod error;
mod http;
mod llm;
mod lua;
mod manifest;
mod package;
mod wasm;

pub use engine::{CONFIGURE_METHOD, EngineKind, EngineLimits, ExtensionEngine, ExtensionInstance};
pub use error::{ExtensionError, Result};
pub use llm::{
  CHAT_METHOD, ChatMessage, ChatRequest, ChatResponse, Choice, EMBED_METHOD, EmbedRequest,
  EmbedResponse, Embedding, LlmError, LlmProvider, MODELS_METHOD, PROVIDES_LLM, Role, Usage,
  WireError, from_wire, to_wire,
};
pub use lua::LuaEngine;
pub use manifest::{ExtensionManifest, HookDecl, HookErrorPolicy};
pub use package::{DEFAULT_ENTRY, ExtensionPackage};
pub use wasm::WasmEngine;
