use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_staleness_threshold_secs() -> u64 {
    120
}
fn default_debounce_secs() -> u64 {
    10
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct GraphStatusConfig {
    #[serde(default = "default_staleness_threshold_secs")]
    pub staleness_threshold_secs: u64,

    #[serde(default = "default_debounce_secs")]
    pub debounce_secs: u64,
}

impl Default for GraphStatusConfig {
    fn default() -> Self {
        Self {
            staleness_threshold_secs: default_staleness_threshold_secs(),
            debounce_secs: default_debounce_secs(),
        }
    }
}
