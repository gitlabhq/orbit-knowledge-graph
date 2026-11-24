use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

pub const DEFAULT_PROBE_SERVER_PORT: u16 = 9394;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeServerConfig {
    pub bind_address: Option<SocketAddr>,
}

pub fn default_bind_address() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], DEFAULT_PROBE_SERVER_PORT))
}

pub fn bind_address_for_port(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}
