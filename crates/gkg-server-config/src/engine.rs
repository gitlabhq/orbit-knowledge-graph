//! Engine, handler, and scheduler configuration types for the indexer.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use croner::Cron;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Base config types ────────────────────────────────────────────────

/// Per-subscription message processing policy (retry, concurrency, DLQ).
///
/// Lives under `engine.topics.<name>` in YAML. Applied to the `Subscription`
/// at handler registration time, so all handlers sharing a subscription
/// share the same processing policy.
///
/// Retries are opt-in: a subscription with no retry config will ack on failure.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SubscriptionConfig {
    /// Which concurrency group this subscription belongs to.
    /// Maps to a named semaphore in `EngineConfiguration::concurrency_groups`.
    #[serde(default)]
    pub concurrency_group: Option<String>,

    /// Maximum total attempts (including the first delivery) before giving up.
    ///
    /// `max_attempts: 1` means the message is processed once with no retries.
    /// `max_attempts: 5` means 1 initial attempt + 4 retries.
    ///
    /// When absent, failures are acked immediately (retries are opt-in).
    #[serde(default)]
    pub max_attempts: Option<u32>,

    /// Delay in seconds between retry attempts. Used as the NATS nack delay.
    /// When absent, nacks use immediate redelivery.
    #[serde(default)]
    pub retry_interval_secs: Option<u64>,

    /// Route exhausted retries to the dead letter queue.
    #[serde(default)]
    pub dead_letter_on_exhaustion: bool,

    /// Per-consumer cap on simultaneously-delivered-but-not-yet-acked messages.
    /// When absent, the NATS server default applies (currently 1000).
    #[serde(default)]
    pub max_ack_pending: Option<u32>,
}

impl SubscriptionConfig {
    /// Returns the retry interval as a [`Duration`], if configured.
    pub fn retry_interval(&self) -> Option<Duration> {
        self.retry_interval_secs.map(Duration::from_secs)
    }
}

const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);

/// Per-task schedule configuration.
///
/// Each scheduled task embeds this via `#[serde(flatten)]` in its own typed config struct.
/// The scheduler reads it via `task.schedule()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ScheduleConfiguration {
    /// Cron expression with seconds field (6-field: `sec min hour dom mon dow`).
    /// When absent, the task runs on a default 60-second interval.
    #[serde(default)]
    pub cron: Option<String>,
}

impl ScheduleConfiguration {
    /// Duration until the next fire time after `now`.
    /// Falls back to `DEFAULT_INTERVAL` when no cron expression is set.
    pub fn next_delay(&self, now: DateTime<Utc>) -> Duration {
        let Some(expr) = self.cron.as_deref() else {
            return DEFAULT_INTERVAL;
        };
        let Ok(cron) = Cron::from_str(expr) else {
            return DEFAULT_INTERVAL;
        };
        cron.find_next_occurrence(&now, false)
            .ok()
            .map(|next| {
                let delta = next - now;
                delta.to_std().unwrap_or(DEFAULT_INTERVAL)
            })
            .unwrap_or(DEFAULT_INTERVAL)
    }

    /// Approximate interval between consecutive firings (used as cadence lock TTL).
    /// Falls back to `DEFAULT_INTERVAL` when no cron expression is set.
    pub fn interval_hint(&self) -> Duration {
        let Some(expr) = self.cron.as_deref() else {
            return DEFAULT_INTERVAL;
        };
        let Ok(cron) = Cron::from_str(expr) else {
            return DEFAULT_INTERVAL;
        };
        let now = Utc::now();
        let first = cron.find_next_occurrence(&now, false).ok();
        let second = first.and_then(|t| cron.find_next_occurrence(&t, false).ok());
        match (first, second) {
            (Some(a), Some(b)) => (b - a).to_std().unwrap_or(DEFAULT_INTERVAL),
            _ => DEFAULT_INTERVAL,
        }
    }
}

// ── Handler config types ─────────────────────────────────────────────

fn default_datalake_batch_size() -> u64 {
    500_000
}

fn default_stream_block_size() -> u64 {
    65_536
}

fn default_system_notes_resolve_lookup_batch_size() -> usize {
    1_000
}

fn default_halving_initial_block_size() -> u64 {
    100_000
}

fn default_halving_min_block_size() -> u64 {
    1024
}

/// Tuning for the SDLC datalake extract retry loop.
///
/// The first attempt uses the datalake's configured `max_block_size`
/// (typically `datalake_batch_size`). After a failure, subsequent attempts
/// seed at `halving_initial_block_size` and halve on each retry, with
/// `halving_min_block_size` as the floor.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct DatalakeRetryConfig {
    /// Starting `max_block_size` (in rows) for the halving series after the
    /// first failure. Sized to stay safely under the Arrow String int32
    /// offset cap even on unexpectedly heavy text columns.
    #[serde(default = "default_halving_initial_block_size")]
    pub halving_initial_block_size: u64,

    /// Floor for the halving series. Prevents pathologically tiny scans
    /// after repeated retries.
    #[serde(default = "default_halving_min_block_size")]
    pub halving_min_block_size: u64,
}

impl Default for DatalakeRetryConfig {
    fn default() -> Self {
        Self {
            halving_initial_block_size: default_halving_initial_block_size(),
            halving_min_block_size: default_halving_min_block_size(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct EntityHandlerConfig {
    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,

    #[serde(default)]
    pub batch_size_overrides: HashMap<String, u64>,

    #[serde(default)]
    pub partition_overrides: HashMap<String, u32>,

    /// Rows per block streamed from the datalake (`max_block_size`). Larger blocks
    /// amortize per-batch write round-trips (more throughput) at the cost of peak
    /// memory per in-flight block.
    #[serde(default = "default_stream_block_size")]
    #[schemars(range(min = 1))]
    pub stream_block_size: u64,

    /// Maximum number of items bound into each SystemNote resolver lookup.
    #[serde(default = "default_system_notes_resolve_lookup_batch_size")]
    #[schemars(range(min = 1))]
    pub system_notes_resolve_lookup_batch_size: usize,
}

impl Default for EntityHandlerConfig {
    fn default() -> Self {
        Self {
            datalake_batch_size: default_datalake_batch_size(),
            batch_size_overrides: HashMap::new(),
            partition_overrides: HashMap::new(),
            stream_block_size: default_stream_block_size(),
            system_notes_resolve_lookup_batch_size: default_system_notes_resolve_lookup_batch_size(
            ),
        }
    }
}

fn default_fetch_concurrency() -> usize {
    6
}

fn default_code_indexing_max_file_size_bytes() -> u64 {
    5_000_000
}

fn default_code_indexing_max_files() -> usize {
    1_000_000
}

fn default_code_indexing_per_file_timeout_ms() -> u64 {
    2000
}

fn default_code_indexing_cross_file_resolve_timeout_ms() -> u64 {
    180_000
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CodeIndexingPipelineConfig {
    #[serde(default = "default_code_indexing_max_file_size_bytes")]
    pub max_file_size_bytes: u64,
    #[serde(default = "default_code_indexing_max_files")]
    pub max_files: usize,
    #[serde(default)]
    pub worker_threads: usize,
    #[serde(default)]
    pub max_concurrent_languages: usize,
    /// Global per-file resolution timeout in milliseconds.
    /// Applied to all languages unless the language's own DSL rules
    /// specify a different value. 0 = no global timeout.
    #[serde(default = "default_code_indexing_per_file_timeout_ms")]
    pub per_file_timeout_ms: u64,
    /// Wall-clock budget for the sequential cross-file resolution phase
    /// (import edges, call edges). 0 = no timeout.
    #[serde(default = "default_code_indexing_cross_file_resolve_timeout_ms")]
    pub cross_file_resolve_timeout_ms: u64,
    /// Maximum concurrent Gitaly repository fetch operations. Controls how
    /// many repositories can be downloaded simultaneously in the pipelined
    /// code indexer. 0 = no limit. Defaults to 6.
    #[serde(default = "default_fetch_concurrency")]
    pub fetch_concurrency: usize,
}

impl Default for CodeIndexingPipelineConfig {
    fn default() -> Self {
        Self {
            max_file_size_bytes: default_code_indexing_max_file_size_bytes(),
            max_files: default_code_indexing_max_files(),
            worker_threads: 0,
            max_concurrent_languages: 0,
            per_file_timeout_ms: default_code_indexing_per_file_timeout_ms(),
            cross_file_resolve_timeout_ms: default_code_indexing_cross_file_resolve_timeout_ms(),
            fetch_concurrency: default_fetch_concurrency(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CodeIndexingTaskHandlerConfig {
    #[serde(default)]
    pub pipeline: CodeIndexingPipelineConfig,
}

/// Typed per-handler domain configuration (batch sizes, pipeline settings).
///
/// Engine-level config (retry, concurrency, DLQ) lives in `topics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(deny_unknown_fields)]
pub struct HandlersConfiguration {
    #[serde(default)]
    pub entity_handler: EntityHandlerConfig,
    #[serde(default)]
    pub code_indexing_task: CodeIndexingTaskHandlerConfig,
}

// ── Dispatcher / scheduler config types ──────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct GlobalDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TableCleanupConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

impl Default for TableCleanupConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                cron: Some("0 0 3 * * *".into()),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceDeletionSchedulerConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

impl Default for NamespaceDeletionSchedulerConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                cron: Some("0 0 3 * * *".into()),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MigrationCompletionConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

impl Default for MigrationCompletionConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                cron: Some("0 */5 * * * *".into()),
            },
        }
    }
}

/// Mutable-FK edge kinds reconciled by default. Immutable FKs (project_id,
/// author_id, …) can't orphan, so sweeping them is wasted work; this allowlist
/// keeps per-run cost bounded. Other mutable kinds are eligible via config.
fn default_stale_edge_relationship_kinds() -> Vec<String> {
    [
        "HAS_LATEST_DIFF",
        "HAS_HEAD_PIPELINE",
        "LAST_EDITED_BY",
        "IN_MILESTONE",
    ]
    .iter()
    .map(|kind| kind.to_string())
    .collect()
}

/// Tombstones stale FK-derived "latest"/single-value edges whose endpoint no
/// longer matches the owner node's current FK column. ReplacingMergeTree keys
/// the edge on its (mutable) `target_id`, so an FK change orphans the old edge
/// instead of replacing it; this sweep reconciles them off the indexing path.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StaleEdgeReconciliationConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,

    /// Relationship kinds to reconcile. One `INSERT … SELECT` runs per
    /// `(kind, FK-owner)` variant, so an empty list disables the sweep.
    #[serde(default = "default_stale_edge_relationship_kinds")]
    pub relationship_kinds: Vec<String>,
}

impl Default for StaleEdgeReconciliationConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                cron: Some("0 */15 * * * *".into()),
            },
            relationship_kinds: default_stale_edge_relationship_kinds(),
        }
    }
}

/// Typed per-task configuration for all registered scheduled tasks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(deny_unknown_fields)]
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
    #[serde(default)]
    pub migration_completion: MigrationCompletionConfig,
    #[serde(default)]
    pub stale_edge_reconciliation: StaleEdgeReconciliationConfig,
}

// ── Top-level engine config ──────────────────────────────────────────

/// Indexer module selector. Each variant maps to a domain in `crates/indexer/src/modules/`.
///
/// An indexer process registers handlers only for the modules listed in
/// [`EngineConfiguration::modules`], letting operators run multiple specialised
/// indexer Deployments (e.g. a light SDLC pool and a beefy code pool) from the
/// same binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IndexerModule {
    /// SDLC entity handlers (one per ontology entity type, subscribing to global or namespace topics).
    Sdlc,
    /// Code indexing handler: clones repositories, runs tree-sitter, writes the code graph.
    Code,
    /// Namespace deletion handler.
    NamespaceDeletion,
}

impl IndexerModule {
    /// Full set of modules. Used as the default so existing deployments stay universal.
    pub fn all() -> Vec<IndexerModule> {
        vec![Self::Sdlc, Self::Code, Self::NamespaceDeletion]
    }
}

/// ETL engine configuration.
///
/// # Defaults
///
/// - `max_concurrent_workers`: 16
/// - `concurrency_groups`: empty
/// - `topics`: empty (no retry/DLQ by default)
/// - `handlers`: defaults for all handlers
/// - `modules`: all variants of [`IndexerModule`] (universal indexer)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct EngineConfiguration {
    /// Maximum concurrent message handlers across all modules. Defaults to 16.
    #[serde(default = "EngineConfiguration::default_max_concurrent_workers")]
    pub max_concurrent_workers: usize,

    /// Named concurrency groups with their limits.
    /// Subscriptions reference these by name via `SubscriptionConfig::concurrency_group`.
    #[serde(default)]
    pub concurrency_groups: HashMap<String, usize>,

    /// Per-subscription message processing policy (retry, concurrency, DLQ).
    /// Keyed by a human-readable label matching topic name constants.
    #[serde(default)]
    pub topics: HashMap<String, SubscriptionConfig>,

    /// Per-handler domain configuration (batch sizes, pipeline settings).
    #[serde(default)]
    pub handlers: HandlersConfiguration,

    /// Datalake retry tuning shared by all SDLC pipelines.
    #[serde(default)]
    pub datalake_retry: DatalakeRetryConfig,

    /// Modules whose handlers this indexer process should register. Defaults to all
    /// modules for backward compatibility (universal indexer). Set to a subset to
    /// run a specialised pool, e.g. `[code]` for a code-only Deployment.
    #[serde(default = "IndexerModule::all")]
    pub modules: Vec<IndexerModule>,
}

impl Default for EngineConfiguration {
    fn default() -> Self {
        EngineConfiguration {
            max_concurrent_workers: Self::default_max_concurrent_workers(),
            concurrency_groups: HashMap::new(),
            topics: HashMap::new(),
            handlers: HandlersConfiguration::default(),
            datalake_retry: DatalakeRetryConfig::default(),
            modules: IndexerModule::all(),
        }
    }
}

impl EngineConfiguration {
    fn default_max_concurrent_workers() -> usize {
        16
    }

    /// Returns whether `module` is enabled in this configuration.
    pub fn is_module_enabled(&self, module: IndexerModule) -> bool {
        self.modules.contains(&module)
    }

    /// Validates engine-level invariants that cannot be expressed in the type system.
    pub fn validate(&self) -> Result<(), EngineConfigError> {
        if self.modules.is_empty() {
            return Err(EngineConfigError::NoModulesEnabled);
        }
        let entity_handler = &self.handlers.entity_handler;
        if entity_handler.stream_block_size == 0 {
            return Err(EngineConfigError::ZeroStreamBlockSize);
        }
        if entity_handler.system_notes_resolve_lookup_batch_size == 0 {
            return Err(EngineConfigError::ZeroSystemNotesResolveLookupBatchSize);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineConfigError {
    #[error(
        "engine.modules must list at least one module; \
         leave it unset to register all modules (universal indexer)"
    )]
    NoModulesEnabled,

    #[error("engine.handlers.entity_handler.stream_block_size must be at least 1")]
    ZeroStreamBlockSize,

    #[error(
        "engine.handlers.entity_handler.system_notes_resolve_lookup_batch_size must be at least 1"
    )]
    ZeroSystemNotesResolveLookupBatchSize,
}

/// Top-level schedule configuration.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ScheduleConfig {
    #[serde(default)]
    pub tasks: ScheduledTasksConfiguration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_modules_are_universal() {
        let cfg = EngineConfiguration::default();
        assert_eq!(cfg.modules, IndexerModule::all());
        assert!(cfg.is_module_enabled(IndexerModule::Sdlc));
        assert!(cfg.is_module_enabled(IndexerModule::Code));
        assert!(cfg.is_module_enabled(IndexerModule::NamespaceDeletion));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn empty_modules_fails_validation() {
        let cfg = EngineConfiguration {
            modules: vec![],
            ..EngineConfiguration::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(EngineConfigError::NoModulesEnabled)
        ));
    }

    #[test]
    fn module_subset_only_enables_listed() {
        let cfg = EngineConfiguration {
            modules: vec![IndexerModule::Code],
            ..EngineConfiguration::default()
        };
        assert!(cfg.is_module_enabled(IndexerModule::Code));
        assert!(!cfg.is_module_enabled(IndexerModule::Sdlc));
        assert!(!cfg.is_module_enabled(IndexerModule::NamespaceDeletion));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn modules_deserialize_from_yaml() {
        let yaml = r#"
modules: [sdlc, namespace_deletion]
"#;
        let cfg: EngineConfiguration = serde_yaml::from_str(yaml).expect("valid yaml");
        assert_eq!(
            cfg.modules,
            vec![IndexerModule::Sdlc, IndexerModule::NamespaceDeletion]
        );
    }

    #[test]
    fn omitted_modules_field_uses_default() {
        let yaml = "max_concurrent_workers: 8\n";
        let cfg: EngineConfiguration = serde_yaml::from_str(yaml).expect("valid yaml");
        assert_eq!(cfg.modules, IndexerModule::all());
    }

    #[test]
    fn entity_handler_streaming_knobs_default_to_pre_tunable_constants() {
        let cfg = EntityHandlerConfig::default();
        assert_eq!(cfg.stream_block_size, 65_536);
        assert_eq!(cfg.system_notes_resolve_lookup_batch_size, 1_000);
    }

    #[test]
    fn entity_handler_streaming_knobs_override_from_yaml() {
        let yaml = "stream_block_size: 262144\nsystem_notes_resolve_lookup_batch_size: 2048\n";
        let cfg: EntityHandlerConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        assert_eq!(cfg.stream_block_size, 262_144);
        assert_eq!(cfg.system_notes_resolve_lookup_batch_size, 2_048);
    }

    #[test]
    fn zero_stream_block_size_fails_validation() {
        let mut cfg = EngineConfiguration::default();
        cfg.handlers.entity_handler.stream_block_size = 0;
        assert!(matches!(
            cfg.validate(),
            Err(EngineConfigError::ZeroStreamBlockSize)
        ));
    }

    #[test]
    fn zero_system_notes_resolve_lookup_batch_size_fails_validation() {
        let mut cfg = EngineConfiguration::default();
        cfg.handlers
            .entity_handler
            .system_notes_resolve_lookup_batch_size = 0;
        assert!(matches!(
            cfg.validate(),
            Err(EngineConfigError::ZeroSystemNotesResolveLookupBatchSize)
        ));
    }
}
