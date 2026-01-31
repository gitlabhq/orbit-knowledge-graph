use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitalyRepositoryConfig {
    pub address: String,
    #[serde(default = "default_storage")]
    pub storage: String,
    #[serde(default)]
    pub relative_path: String,
    pub token: Option<String>,
}

fn default_storage() -> String {
    "default".to_string()
}

impl GitalyRepositoryConfig {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn with_relative_path(mut self, path: impl Into<String>) -> Self {
        self.relative_path = path.into();
        self
    }
}
