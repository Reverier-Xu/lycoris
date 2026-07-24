mod actor;
mod config;
mod handle;

pub use actor::LinkRuntime;
pub use config::LinkConfig;
pub(crate) use handle::LinkCommand;
pub use handle::{LinkError, LinkHandle, LinkSnapshot};

#[cfg(test)]
mod tests;
