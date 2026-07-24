use std::{num::NonZeroUsize, time::Duration};

use libp2p::Multiaddr;

const DEFAULT_COMMAND_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub struct LinkConfig {
  listen_addresses: Vec<Multiaddr>,
  command_capacity: usize,
  idle_timeout: Duration,
  ping_interval: Duration,
  ping_timeout: Duration,
}

impl LinkConfig {
  pub fn new(listen_addresses: Vec<Multiaddr>) -> Self {
    Self {
      listen_addresses,
      command_capacity: DEFAULT_COMMAND_CAPACITY,
      idle_timeout: Duration::from_secs(60),
      ping_interval: Duration::from_secs(2),
      ping_timeout: Duration::from_secs(3),
    }
  }

  pub fn with_command_capacity(mut self, capacity: NonZeroUsize) -> Self {
    self.command_capacity = capacity.get();
    self
  }

  pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
    self.idle_timeout = timeout;
    self
  }

  pub(crate) fn listen_addresses(&self) -> &[Multiaddr] {
    &self.listen_addresses
  }

  pub(crate) const fn command_capacity(&self) -> usize {
    self.command_capacity
  }

  pub(crate) const fn idle_timeout(&self) -> Duration {
    self.idle_timeout
  }

  pub(crate) const fn ping_interval(&self) -> Duration {
    self.ping_interval
  }

  pub(crate) const fn ping_timeout(&self) -> Duration {
    self.ping_timeout
  }
}
