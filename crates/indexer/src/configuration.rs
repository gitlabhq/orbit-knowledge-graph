//! Engine configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::modules::code::{
    CodeIndexingTaskHandlerConfig, NamespaceCodeBackfillDispatcherConfig,
    SiphonCodeIndexingTaskDispatcherConfig,
};
use crate::modules::namespace_deletion::{
    NamespaceDeletionHandlerConfig, NamespaceDeletionSchedulerConfig,
};
use crate::modules::sdlc::dispatch::{GlobalDispatcherConfig, NamespaceDispatcherConfig};
use crate::modules::sdlc::{GlobalHandlerConfig, NamespaceHandlerConfig};
use crate::scheduler::TableCleanupConfig;

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

/// Typed per-handler configuration for all registered handlers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// Typed per-task configuration for all registered scheduled tasks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// Configuration for the on-disk repository cache used by code indexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryCacheConfiguration {
    /// Maximum total disk bytes the cache may use. Defaults to 20 GB.
    #[serde(default = "RepositoryCacheConfiguration::default_disk_budget_bytes")]
    pub disk_budget_bytes: u64,

    /// Bytes reserved per code worker for in-flight archive extraction.
    /// Headroom = this value × code worker count. Defaults to 2 GB.
    #[serde(default = "RepositoryCacheConfiguration::default_headroom_per_worker_bytes")]
    pub headroom_per_worker_bytes: u64,

    /// Repos at or above this size are considered "large" and evicted last.
    /// Defaults to 100 MB.
    #[serde(default = "RepositoryCacheConfiguration::default_large_repo_threshold_bytes")]
    pub large_repo_threshold_bytes: u64,
}

impl Default for RepositoryCacheConfiguration {
    fn default() -> Self {
        Self {
            disk_budget_bytes: Self::default_disk_budget_bytes(),
            headroom_per_worker_bytes: Self::default_headroom_per_worker_bytes(),
            large_repo_threshold_bytes: Self::default_large_repo_threshold_bytes(),
        }
    }
}

impl RepositoryCacheConfiguration {
    const fn default_disk_budget_bytes() -> u64 {
        20 * 1024 * 1024 * 1024 // 20 GB
    }

    const fn default_headroom_per_worker_bytes() -> u64 {
        2 * 1024 * 1024 * 1024 // 2 GB
    }

    const fn default_large_repo_threshold_bytes() -> u64 {
        100 * 1024 * 1024 // 100 MB
    }

    /// Computes the usable cache budget after reserving headroom for workers.
    pub fn usable_budget(&self, code_worker_count: usize) -> u64 {
        self.disk_budget_bytes
            .saturating_sub(self.headroom_per_worker_bytes * code_worker_count as u64)
    }
}

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

    /// On-disk repository cache settings for code indexing.
    #[serde(default)]
    pub repository_cache: RepositoryCacheConfiguration,
}

impl Default for EngineConfiguration {
    fn default() -> Self {
        EngineConfiguration {
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            concurrency_groups: HashMap::new(),
            handlers: HandlersConfiguration::default(),
            repository_cache: RepositoryCacheConfiguration::default(),
        }
    }
}

impl EngineConfiguration {
    fn default_max_concurrent_workers() -> usize {
        16
    }

    /// Returns the concurrency limit for the "code" group, or 1 if not configured.
    pub fn code_worker_count(&self) -> usize {
        self.concurrency_groups.get("code").copied().unwrap_or(1)
    }
}
