//! Concurrency settings. Global limit via `max_concurrent_workers`, per-module limits in `modules`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the ETL engine.
///
/// Controls global settings like maximum concurrent workers and
/// per-module configuration overrides.
///
/// # Defaults
///
/// - `max_concurrent_workers`: 16
/// - `modules`: empty (no per-module limits)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfiguration {
    /// Maximum number of concurrent message handlers across all modules.
    ///
    /// This is the global concurrency limit. Individual modules can have
    /// lower limits via their module configuration.
    #[serde(default = "EngineConfiguration::default_max_concurrent_workers")]
    pub max_concurrent_workers: usize,

    /// Per-module configuration overrides.
    ///
    /// Keys are module names (as returned by [`Module::name`](crate::module::Module::name)).
    #[serde(default)]
    pub modules: HashMap<String, ModuleConfiguration>,
}

/// Configuration for a specific module.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModuleConfiguration {
    /// Maximum concurrent handlers for this module.
    ///
    /// When set, limits how many messages this module can process
    /// concurrently, independent of the global limit.
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
