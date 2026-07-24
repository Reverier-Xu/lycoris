use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::MAX_FRAME_BYTES;

#[derive(Debug, Error)]
pub enum FrameError {
  #[error("payload is {actual} bytes; maximum is {maximum}")]
  PayloadTooLarge { actual: usize, maximum: usize },
  #[error("frame is {actual} bytes; maximum is {maximum}")]
  FrameTooLarge { actual: usize, maximum: usize },
  #[error("frame serialization failed: {0}")]
  Serialization(#[from] postcard::Error),
}

pub fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, FrameError> {
  let frame = postcard::to_stdvec(value)?;
  if frame.len() > MAX_FRAME_BYTES {
    return Err(FrameError::FrameTooLarge {
      actual: frame.len(),
      maximum: MAX_FRAME_BYTES,
    });
  }
  Ok(frame)
}

pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T, FrameError> {
  if frame.len() > MAX_FRAME_BYTES {
    return Err(FrameError::FrameTooLarge {
      actual: frame.len(),
      maximum: MAX_FRAME_BYTES,
    });
  }
  Ok(postcard::from_bytes(frame)?)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Envelope, MessageKind, ProtocolId, protocol::test_support};

  fn envelope() -> Envelope {
    let header = test_support::header(ProtocolId::Resource, MessageKind::Response, 10, 4);
    Envelope::new(header, b"resource page".to_vec()).unwrap()
  }

  #[test]
  fn frame_round_trip_preserves_the_envelope() {
    let expected = envelope();
    let encoded = encode_frame(&expected).unwrap();
    let decoded: Envelope = decode_frame(&encoded).unwrap();

    assert_eq!(decoded, expected);
  }

  #[test]
  fn decoder_rejects_oversized_and_malformed_frames() {
    let oversized = vec![0; MAX_FRAME_BYTES + 1];
    assert!(matches!(
      decode_frame::<Envelope>(&oversized),
      Err(FrameError::FrameTooLarge { .. })
    ));
    assert!(matches!(
      decode_frame::<Envelope>(b"not-postcard"),
      Err(FrameError::Serialization(_))
    ));
  }
}
