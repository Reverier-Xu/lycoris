mod actor;
mod config;
mod directory;
mod handle;
mod messaging;

pub use actor::LinkRuntime;
pub use config::LinkConfig;
pub(crate) use handle::LinkCommand;
pub use handle::{InboundEnvelope, InboundToken, LinkError, LinkHandle, LinkSnapshot};

#[cfg(test)]
mod tests;
