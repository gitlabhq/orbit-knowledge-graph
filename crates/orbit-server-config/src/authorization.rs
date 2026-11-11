use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The modules that answer the provider names packages use.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AuthorizationConfig {
    /// The one provider that identifies callers.
    pub identify: String,
    pub providers: BTreeMap<String, ProviderConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProviderConfig {
    pub module: String,
    /// The module's own options.
    #[serde(flatten)]
    pub options: serde_json::Map<String, serde_json::Value>,
}
