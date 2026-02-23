//! Engine configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

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

    /// Maximum number of retry attempts before giving up on a message.
    /// When exceeded, the message is acked (accepted as lost) to prevent poison messages.
    /// When absent, the engine falls back to the current behavior (immediate nack, NATS
    /// `max_deliver` as the safety net).
    #[serde(default)]
    pub max_retry_attempts: Option<u32>,

    /// Delay in seconds between retry attempts. Used as the NATS nack delay.
    /// When absent, nacks use immediate redelivery.
    #[serde(default)]
    pub retry_interval_secs: Option<u64>,
}

impl ModuleConfiguration {
    /// Returns the retry interval as a [`Duration`], if configured.
    pub fn retry_interval(&self) -> Option<Duration> {
        self.retry_interval_secs.map(Duration::from_secs)
    }
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
