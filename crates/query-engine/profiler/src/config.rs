use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProfilingConfig {
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub query_log: bool,
    #[serde(default)]
    pub processors: bool,
    #[serde(default)]
    pub instance_health: bool,
}
