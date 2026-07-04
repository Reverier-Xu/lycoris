use redb::{TypeName, Value};

use crate::StorageError;

/// Serialize a value to a byte vector using `postcard`.
pub fn encode<T: serde::Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, StorageError> {
  Ok(postcard::to_stdvec(value)?)
}

/// Deserialize a value from a byte slice using `postcard`.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
  Ok(postcard::from_bytes(bytes)?)
}

/// Opaque byte-blob value type for redb tables.
///
/// Domain records are serialized into `Vec<u8>` (currently via `postcard`)
/// before being wrapped in `Bytes`. The newtype exists only to provide a safe,
/// infallible `redb::Value` implementation without forcing serialization errors
/// into the trait methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bytes(pub Vec<u8>);

impl Value for Bytes {
  type SelfType<'a> = Bytes;
  type AsBytes<'a> = Vec<u8>;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
  where
    Self: 'a, {
    Bytes(data.to_vec())
  }

  fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
    value.0.clone()
  }

  fn type_name() -> TypeName {
    TypeName::new("lycoris.storage.Bytes")
  }
}
