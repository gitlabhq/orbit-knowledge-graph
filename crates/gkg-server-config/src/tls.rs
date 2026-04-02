//! TLS configuration.
//!
//! The struct definition lives here; the async `load_tls_config()` method
//! that depends on `tonic` stays in `gkg-server`.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub cert_path: Option<String>,
    #[serde(default)]
    pub key_path: Option<String>,
}
