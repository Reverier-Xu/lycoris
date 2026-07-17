use thiserror::Error;

/// Errors that can occur when loading, validating, or writing configuration.
///
/// Shared by the daemon and client configuration types: both read and write
/// TOML files and fail the same way, so there is one error type per crate,
/// not per configuration struct.
#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("parse error: {0}")]
  Parse(#[from] toml::de::Error),
  #[error("serialize error: {0}")]
  Serialize(#[from] toml::ser::Error),
  #[error("invalid node address: {source}")]
  InvalidNodeAddress {
    #[source]
    source: InvalidAddressError,
  },
  #[error("invalid bootstrap peer at index {index}: {source}")]
  InvalidPeerAddress {
    index: usize,
    #[source]
    source: InvalidAddressError,
  },
  #[error("no configuration file found in the default locations")]
  NotFound,
}

/// A cluster address that does not use the required `https://` scheme.
#[derive(Debug, Error)]
#[error("'{0}' must start with https://")]
pub struct InvalidAddressError(pub(crate) String);

impl From<InvalidAddressError> for ConfigError {
  fn from(source: InvalidAddressError) -> Self {
    Self::InvalidNodeAddress { source }
  }
}
