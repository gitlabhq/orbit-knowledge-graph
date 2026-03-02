//! Engine configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Per-handler engine configuration (retry policy, concurrency group).
///
/// Each handler embeds this via `#[serde(flatten)]` in its own typed config struct.
/// The engine reads it via `handler.engine_config()` — no string-keyed HashMap lookup.
///
/// Retries are opt-in: a handler with no retry config will ack on failure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HandlerConfiguration {
    /// Which concurrency group this handler belongs to.
    /// Maps to a named semaphore in `EngineConfiguration::concurrency_groups`.
    #[serde(default)]
    pub concurrency_group: Option<String>,

    /// Maximum total attempts (including the first delivery) before giving up.
    ///
    /// `max_attempts: 1` means the handler runs once with no retries — on failure the
    /// message is acked and lost.
    ///
    /// `max_attempts: 5` means 1 initial attempt + 4 retries.
    ///
    /// When absent, failures are acked immediately (retries are opt-in).
    #[serde(default)]
    pub max_attempts: Option<u32>,

    /// Delay in seconds between retry attempts. Used as the NATS nack delay.
    /// When absent, nacks use immediate redelivery.
    #[serde(default)]
    pub retry_interval_secs: Option<u64>,
}

impl HandlerConfiguration {
    /// Returns the retry interval as a [`Duration`], if configured.
    pub fn retry_interval(&self) -> Option<Duration> {
        self.retry_interval_secs.map(Duration::from_secs)
    }
}

/// ETL engine configuration.
///
/// # Defaults
///
/// - `max_concurrent_workers`: 16
/// - `concurrency_groups`: empty
/// - `handlers`: empty
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfiguration {
    /// Maximum concurrent message handlers across all modules. Defaults to 16.
    #[serde(default = "EngineConfiguration::default_max_concurrent_workers")]
    pub max_concurrent_workers: usize,

    /// Named concurrency groups with their limits.
    /// Handlers reference these by name via `HandlerConfiguration::concurrency_group`.
    #[serde(default)]
    pub concurrency_groups: HashMap<String, usize>,

    /// Per-handler configuration, keyed by handler name.
    /// Each value is raw JSON that modules deserialize into their typed config structs.
    #[serde(default)]
    pub handlers: HashMap<String, serde_json::Value>,
}

impl Default for EngineConfiguration {
    fn default() -> Self {
        EngineConfiguration {
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            concurrency_groups: HashMap::new(),
            handlers: HashMap::new(),
        }
    }
}

impl EngineConfiguration {
    fn default_max_concurrent_workers() -> usize {
        16
    }
}
