use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    pub jwt_secret: String,
    #[serde(default = "default_jwt_issuer")]
    pub jwt_issuer: String,
    #[serde(default = "default_jwt_audience")]
    pub jwt_audience: String,
    #[serde(default = "default_jwt_clock_skew_secs")]
    pub jwt_clock_skew_secs: u64,
}

fn default_bind_address() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_jwt_issuer() -> String {
    "gitlab".to_string()
}

fn default_jwt_audience() -> String {
    "gitlab-knowledge-graph".to_string()
}

fn default_jwt_clock_skew_secs() -> u64 {
    60
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            jwt_secret: String::new(),
            jwt_issuer: default_jwt_issuer(),
            jwt_audience: default_jwt_audience(),
            jwt_clock_skew_secs: default_jwt_clock_skew_secs(),
        }
    }
}
