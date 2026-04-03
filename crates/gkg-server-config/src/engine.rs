//! Engine, handler, and scheduler configuration types for the indexer.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── Base config types ────────────────────────────────────────────────

/// Per-handler engine configuration (retry policy, concurrency group).
///
/// Each handler embeds this via `#[serde(flatten)]` in its own typed config struct.
/// The engine reads it via `handler.engine_config()`.
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
    /// `max_attempts: 1` means the handler runs once with no retries.
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

/// Per-task schedule configuration (cadence interval).
///
/// Each scheduled task embeds this via `#[serde(flatten)]` in its own typed config struct.
/// The scheduler loop reads it via `task.schedule()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScheduleConfiguration {
    /// Interval in seconds between task runs.
    /// When absent, the task runs every cycle.
    #[serde(default)]
    pub interval_secs: Option<u64>,
}

impl ScheduleConfiguration {
    pub fn interval(&self) -> Option<Duration> {
        self.interval_secs.map(Duration::from_secs)
    }
}

// ── Handler config types ─────────────────────────────────────────────

fn default_datalake_batch_size() -> u64 {
    1_000_000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,
}

impl Default for GlobalHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            datalake_batch_size: default_datalake_batch_size(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamespaceHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,
}

impl Default for NamespaceHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            datalake_batch_size: default_datalake_batch_size(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CodeIndexingTaskHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceDeletionHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,
}

/// Typed per-handler configuration for all registered handlers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HandlersConfiguration {
    #[serde(default)]
    pub global_handler: GlobalHandlerConfig,
    #[serde(default)]
    pub namespace_handler: NamespaceHandlerConfig,
    #[serde(default)]
    pub code_indexing_task: CodeIndexingTaskHandlerConfig,
    #[serde(default)]
    pub namespace_deletion: NamespaceDeletionHandlerConfig,
}

// ── Dispatcher / scheduler config types ──────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

fn default_dispatcher_batch_size() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiphonCodeIndexingTaskDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,

    #[serde(default = "default_dispatcher_batch_size")]
    pub batch_size: usize,
}

impl Default for SiphonCodeIndexingTaskDispatcherConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration::default(),
            events_stream_name: default_events_stream_name(),
            batch_size: default_dispatcher_batch_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceCodeBackfillDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,

    #[serde(default = "default_dispatcher_batch_size")]
    pub batch_size: usize,
}

impl Default for NamespaceCodeBackfillDispatcherConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration::default(),
            events_stream_name: default_events_stream_name(),
            batch_size: default_dispatcher_batch_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCleanupConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

impl Default for TableCleanupConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                interval_secs: Some(86400),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceDeletionSchedulerConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

impl Default for NamespaceDeletionSchedulerConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                interval_secs: Some(86400),
            },
        }
    }
}

/// Typed per-task configuration for all registered scheduled tasks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScheduledTasksConfiguration {
    #[serde(default)]
    pub global: GlobalDispatcherConfig,
    #[serde(default)]
    pub namespace: NamespaceDispatcherConfig,
    #[serde(default)]
    pub code_indexing_task: SiphonCodeIndexingTaskDispatcherConfig,
    #[serde(default)]
    pub namespace_code_backfill: NamespaceCodeBackfillDispatcherConfig,
    #[serde(default)]
    pub table_cleanup: TableCleanupConfig,
    #[serde(default)]
    pub namespace_deletion: NamespaceDeletionSchedulerConfig,
}

// ── Top-level engine config ──────────────────────────────────────────

/// ETL engine configuration.
///
/// # Defaults
///
/// - `max_concurrent_workers`: 16
/// - `concurrency_groups`: empty
/// - `handlers`: defaults for all handlers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfiguration {
    /// Maximum concurrent message handlers across all modules. Defaults to 16.
    #[serde(default = "EngineConfiguration::default_max_concurrent_workers")]
    pub max_concurrent_workers: usize,

    /// Named concurrency groups with their limits.
    /// Handlers reference these by name via `HandlerConfiguration::concurrency_group`.
    #[serde(default)]
    pub concurrency_groups: HashMap<String, usize>,

    /// Per-handler configuration.
    #[serde(default)]
    pub handlers: HandlersConfiguration,
}

impl Default for EngineConfiguration {
    fn default() -> Self {
        EngineConfiguration {
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            concurrency_groups: HashMap::new(),
            handlers: HandlersConfiguration::default(),
        }
    }
}

impl EngineConfiguration {
    fn default_max_concurrent_workers() -> usize {
        16
    }
}

/// Top-level schedule configuration.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScheduleConfig {
    #[serde(default)]
    pub tasks: ScheduledTasksConfiguration,
}
