//! TLS configuration.
//!
//! The struct definition lives here; the async `load_tls_config()` method
//! that depends on `tonic` stays in `orbit-server`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}
