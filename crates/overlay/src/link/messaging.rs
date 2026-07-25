use std::io;

use async_trait::async_trait;
use futures_util::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response, swarm::StreamProtocol};

use crate::{Envelope, MAX_FRAME_BYTES, decode_frame, encode_frame};

pub(crate) const OVERLAY_STREAM_PROTOCOL: StreamProtocol =
  StreamProtocol::new("/lycoris/overlay/1");

#[derive(Debug, Clone, Default)]
pub(crate) struct EnvelopeCodec;

#[async_trait]
impl request_response::Codec for EnvelopeCodec {
  type Protocol = StreamProtocol;
  type Request = Envelope;
  type Response = Envelope;

  async fn read_request<T>(
    &mut self, _protocol: &Self::Protocol, io: &mut T,
  ) -> io::Result<Self::Request>
  where
    T: AsyncRead + Unpin + Send, {
    read_envelope(io).await
  }

  async fn read_response<T>(
    &mut self, _protocol: &Self::Protocol, io: &mut T,
  ) -> io::Result<Self::Response>
  where
    T: AsyncRead + Unpin + Send, {
    read_envelope(io).await
  }

  async fn write_request<T>(
    &mut self, _protocol: &Self::Protocol, io: &mut T, request: Self::Request,
  ) -> io::Result<()>
  where
    T: AsyncWrite + Unpin + Send, {
    write_envelope(io, &request).await
  }

  async fn write_response<T>(
    &mut self, _protocol: &Self::Protocol, io: &mut T, response: Self::Response,
  ) -> io::Result<()>
  where
    T: AsyncWrite + Unpin + Send, {
    write_envelope(io, &response).await
  }
}

async fn read_envelope<T: AsyncRead + Unpin + Send>(io: &mut T) -> io::Result<Envelope> {
  let mut bytes = Vec::new();
  io.take(MAX_FRAME_BYTES as u64 + 1)
    .read_to_end(&mut bytes)
    .await?;
  decode_frame(&bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn write_envelope<T: AsyncWrite + Unpin + Send>(
  io: &mut T, envelope: &Envelope,
) -> io::Result<()> {
  let bytes =
    encode_frame(envelope).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
  io.write_all(&bytes).await?;
  io.close().await
}
