//! Engine configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ETL engine configuration.
///
/// # Defaults
///
/// - `max_concurrent_workers`: 16
/// - `modules`: empty
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfiguration {
    /// Maximum concurrent message handlers across all modules. Defaults to 16.
    #[serde(default = "EngineConfiguration::default_max_concurrent_workers")]
    pub max_concurrent_workers: usize,

    /// Per-module overrides, keyed by [`Module::name`](crate::module::Module::name).
    #[serde(default)]
    pub modules: HashMap<String, ModuleConfiguration>,
}

/// Per-module configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModuleConfiguration {
    /// Maximum concurrent handlers for this module, independent of the global limit.
    #[serde(default)]
    pub max_concurrency: Option<usize>,
}

impl Default for EngineConfiguration {
    fn default() -> Self {
        EngineConfiguration {
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            modules: HashMap::new(),
        }
    }
}

impl EngineConfiguration {
    fn default_max_concurrent_workers() -> usize {
        16
    }
}
