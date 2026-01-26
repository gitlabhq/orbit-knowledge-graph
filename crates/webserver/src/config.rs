use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebserverConfig {
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
    "0.0.0.0:8080".to_string()
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

impl Default for WebserverConfig {
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

impl WebserverConfig {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn with_bind_address(mut self, address: impl Into<String>) -> Self {
        self.bind_address = address.into();
        self
    }

    pub fn with_jwt_secret(mut self, secret: impl Into<String>) -> Self {
        self.jwt_secret = secret.into();
        self
    }
}
