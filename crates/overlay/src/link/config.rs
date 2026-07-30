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
  reconnect_interval: Duration,
  reconnect_min_delay: Duration,
  reconnect_max_delay: Duration,
  discovered_address_ttl: Duration,
  mdns_query_interval: Duration,
  lan_discovery: bool,
}

impl LinkConfig {
  pub fn new(listen_addresses: Vec<Multiaddr>) -> Self {
    Self {
      listen_addresses,
      command_capacity: DEFAULT_COMMAND_CAPACITY,
      idle_timeout: Duration::from_secs(60),
      ping_interval: Duration::from_secs(2),
      ping_timeout: Duration::from_secs(3),
      reconnect_interval: Duration::from_secs(1),
      reconnect_min_delay: Duration::from_secs(1),
      reconnect_max_delay: Duration::from_secs(30),
      discovered_address_ttl: Duration::from_secs(120),
      mdns_query_interval: Duration::from_secs(5),
      lan_discovery: true,
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

  /// Enable or disable LAN discovery. Daemons keep it enabled by default;
  /// topology tests and WAN-only deployments may pin explicit links.
  pub fn with_lan_discovery(mut self, enabled: bool) -> Self {
    self.lan_discovery = enabled;
    self
  }

  #[cfg(test)]
  pub(crate) fn with_reconnect_timing(
    mut self, interval: Duration, min_delay: Duration, max_delay: Duration,
  ) -> Self {
    self.reconnect_interval = interval;
    self.reconnect_min_delay = min_delay;
    self.reconnect_max_delay = max_delay;
    self
  }

  #[cfg(test)]
  pub(crate) fn with_discovery_timing(
    mut self, address_ttl: Duration, query_interval: Duration,
  ) -> Self {
    self.discovered_address_ttl = address_ttl;
    self.mdns_query_interval = query_interval;
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

  pub(crate) const fn reconnect_interval(&self) -> Duration {
    self.reconnect_interval
  }

  pub(crate) const fn reconnect_min_delay(&self) -> Duration {
    self.reconnect_min_delay
  }

  pub(crate) const fn reconnect_max_delay(&self) -> Duration {
    self.reconnect_max_delay
  }

  pub(crate) const fn discovered_address_ttl(&self) -> Duration {
    self.discovered_address_ttl
  }

  pub(crate) const fn mdns_query_interval(&self) -> Duration {
    self.mdns_query_interval
  }

  pub(crate) const fn lan_discovery(&self) -> bool {
    self.lan_discovery
  }
}
