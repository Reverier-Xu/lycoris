use std::{net::SocketAddr, time::Duration};

use lycoris_api::proto::cluster_server::ClusterServer;
use thiserror::Error;
use tonic::transport::ServerTlsConfig;

#[derive(Debug, Error)]
pub enum ApiError {
  #[error("invalid bind address: {0}")]
  InvalidAddress(#[from] std::net::AddrParseError),
  #[error("transport error: {0}")]
  Transport(#[from] tonic::transport::Error),
}

/// Start a gRPC API server serving the given cluster service over mTLS.
pub async fn serve<S>(
  bind_address: &str, tls: ServerTlsConfig, service: ClusterServer<S>,
) -> Result<(), ApiError>
where
  S: lycoris_api::proto::cluster_server::Cluster, {
  let addr: SocketAddr = bind_address.parse()?;
  let server = tonic::transport::Server::builder()
    .tls_config(tls)?
    .timeout(Duration::from_secs(30))
    .add_service(service)
    .serve(addr);

  server.await?;
  Ok(())
}
