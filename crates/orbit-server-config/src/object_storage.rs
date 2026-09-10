use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ObjectStorageConfig {
    pub enabled: bool,
    pub provider: ObjectStorageProvider,
    pub bucket: String,
    pub prefix: String,
    pub auth: ObjectStorageAuth,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub allow_http: bool,
    pub ca_cert_path: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub service_account_key: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStorageProvider {
    S3,
    Gcs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStorageAuth {
    Identity,
    Static,
}
