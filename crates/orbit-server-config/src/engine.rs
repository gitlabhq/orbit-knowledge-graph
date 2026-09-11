//! Engine, handler, and scheduler configuration types for the indexer.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use croner::Cron;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::resources::{ContainerResources, derive_code_indexing_slots};

// ── Base config types ────────────────────────────────────────────────

/// Sparse per-subscription policy override (retry, concurrency, DLQ).
///
/// The correct default policy for each topic is declared in Rust by the indexer
/// module that owns the topic (see `crates/indexer/src/modules/*`). A
/// `engine.topics.<name>` entry in YAML is a *field-wise override* layered on top
/// of that declared default via [`SubscriptionConfig::with_optional_override`]: only the
/// fields the entry sets change; every unset field keeps the module default. Each
/// field is therefore `Option`, so "unset" is distinguishable from an explicit
/// value (notably `dead_letter_on_exhaustion: false`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SubscriptionConfig {
    /// Which concurrency group this subscription belongs to.
    /// Maps to a named semaphore in `EngineConfiguration::concurrency_groups`.
    pub concurrency_group: Option<String>,

    /// Maximum total attempts (including the first delivery) before giving up.
    ///
    /// `max_attempts: 1` means the message is processed once with no retries.
    /// `max_attempts: 5` means 1 initial attempt + 4 retries.
    pub max_attempts: Option<u32>,

    /// Delay in seconds between retry attempts. Used as the NATS nack delay.
    /// When absent, nacks use immediate redelivery.
    pub retry_interval_secs: Option<u64>,

    /// Route exhausted retries to the dead letter queue.
    pub dead_letter_on_exhaustion: Option<bool>,

    /// Per-consumer cap on simultaneously-delivered-but-not-yet-acked messages.
    /// When absent, the NATS server default applies (currently 1000).
    pub max_ack_pending: Option<u32>,
}

impl SubscriptionConfig {
    /// Returns the retry interval as a [`Duration`], if configured.
    pub fn retry_interval(&self) -> Option<Duration> {
        self.retry_interval_secs.map(Duration::from_secs)
    }

    /// Field-wise merge: fields `overlay` sets win, unset fields keep `self`'s value.
    pub fn with_optional_override(
        &self,
        overlay: Option<&SubscriptionConfig>,
    ) -> SubscriptionConfig {
        let Some(overlay) = overlay else {
            return self.clone();
        };
        SubscriptionConfig {
            concurrency_group: overlay
                .concurrency_group
                .clone()
                .or_else(|| self.concurrency_group.clone()),
            max_attempts: overlay.max_attempts.or(self.max_attempts),
            retry_interval_secs: overlay.retry_interval_secs.or(self.retry_interval_secs),
            dead_letter_on_exhaustion: overlay
                .dead_letter_on_exhaustion
                .or(self.dead_letter_on_exhaustion),
            max_ack_pending: overlay.max_ack_pending.or(self.max_ack_pending),
        }
    }
}

/// Retry cadence when croner finds no occurrence after `now` (an expression
/// that can never fire again, e.g. a fixed past date).
const NO_OCCURRENCE_RETRY: Duration = Duration::from_secs(60);

/// Truncate sub-second precision from a [`DateTime`], snapping to the current
/// whole second. Works around croner 3.0.1 preserving sub-second fractions in
/// `find_next_occurrence`; becomes a harmless no-op if a future croner version
/// fixes the upstream fraction handling. See #905.
fn truncate_subsecond(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_nanosecond(0).unwrap_or(dt)
}

/// A six-field cron expression (`sec min hour dom mon dow`), validated when
/// the configuration is deserialized so an invalid schedule fails startup.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct CronSchedule {
    expression: String,
    #[serde(skip)]
    cron: Cron,
}

impl CronSchedule {
    pub fn expression(&self) -> &str {
        &self.expression
    }
}

impl TryFrom<String> for CronSchedule {
    type Error = croner::errors::CronError;

    fn try_from(expression: String) -> Result<Self, Self::Error> {
        let cron = Cron::from_str(&expression)?;
        Ok(Self { expression, cron })
    }
}

impl From<CronSchedule> for String {
    fn from(schedule: CronSchedule) -> Self {
        schedule.expression
    }
}

/// Per-task schedule configuration.
///
/// Each scheduled task embeds this via `#[serde(flatten)]` in its own typed config struct.
/// The scheduler reads it via `task.schedule()`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ScheduleConfiguration {
    pub cron: CronSchedule,
}

impl ScheduleConfiguration {
    /// Duration until the next fire time after `now`.
    pub fn next_delay(&self, now: DateTime<Utc>) -> Duration {
        // croner 3.0.1 preserves the sub-second fraction of `now` in the
        // returned occurrence, causing drift and periodic double-fires.
        // Truncating to whole seconds for the lookup pins every fire to
        // :00.000; the delta is still measured from the real `now` so the
        // caller sleeps exactly until that clean boundary.
        let truncated = truncate_subsecond(now);
        self.cron
            .cron
            .find_next_occurrence(&truncated, false)
            .ok()
            .and_then(|next| (next - now).to_std().ok())
            .unwrap_or(NO_OCCURRENCE_RETRY)
    }

    /// Approximate interval between consecutive firings (used as cadence lock TTL).
    pub fn interval_hint(&self) -> Duration {
        let now = truncate_subsecond(Utc::now());
        let cron = &self.cron.cron;
        let first = cron.find_next_occurrence(&now, false).ok();
        let second = first.and_then(|t| cron.find_next_occurrence(&t, false).ok());
        match (first, second) {
            (Some(a), Some(b)) => (b - a).to_std().unwrap_or(NO_OCCURRENCE_RETRY),
            _ => NO_OCCURRENCE_RETRY,
        }
    }
}

// ── Handler config types ─────────────────────────────────────────────

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
    pub halving_initial_block_size: u64,

    /// Floor for the halving series. Prevents pathologically tiny scans
    /// after repeated retries.
    pub halving_min_block_size: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct EntityHandlerConfig {
    /// Rows per SDLC datalake page (also the ClickHouse `max_block_size`). Unset =
    /// derived from the container memory limit (prod's 32 GiB sdlc pool anchors
    /// the tuned 500k page), floored at 100k.
    pub datalake_batch_size: Option<u64>,

    #[serde(default)]
    pub batch_size_overrides: HashMap<String, u64>,

    /// Maximum number of items bound into each SystemNote resolver lookup.
    #[schemars(range(min = 1))]
    pub system_notes_resolve_lookup_batch_size: usize,
}

impl EntityHandlerConfig {
    pub fn resolve_runtime_defaults(&mut self, resources: &ContainerResources) {
        if self.datalake_batch_size.is_some() {
            return;
        }
        let batch = resources.derive_datalake_batch_size();
        self.datalake_batch_size = Some(batch);
        info!(
            memory_limit_bytes = resources.memory_limit_bytes,
            value = batch,
            "derived engine.handlers.entity_handler.datalake_batch_size"
        );
    }

    /// Panics when [`Self::resolve_runtime_defaults`] has not run and the
    /// value is unset: there is no fallback constant.
    pub fn datalake_batch_size(&self) -> u64 {
        self.datalake_batch_size
            .expect("engine.handlers.entity_handler.datalake_batch_size unresolved")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CodeIndexingPipelineConfig {
    pub max_file_size_bytes: u64,
    pub max_files: usize,
    /// Post-filter retained bytes above which a repository is skipped entirely
    /// (indexed empty, then checkpointed). Bounds per-repo disk so fetch/index
    /// concurrency can rise without exhausting the pod volume. 0 = no limit.
    pub max_total_bytes: u64,
    pub worker_threads: usize,
    pub max_concurrent_languages: usize,
    /// Global per-file resolution timeout in milliseconds.
    /// Applied to all languages unless the language's own DSL rules
    /// specify a different value. 0 = no global timeout.
    pub per_file_timeout_ms: u64,
    pub per_file_parse_timeout_ms: u64,
    pub per_file_walk_timeout_ms: u64,
    pub per_file_ssa_timeout_ms: u64,
    /// Wall-clock budget for the sequential cross-file resolution phase
    /// (import edges, call edges). 0 = no timeout.
    pub cross_file_resolve_timeout_ms: u64,
    /// Hard wall-clock budget (seconds) for one repository job (fetch + index); exceeding it aborts and retries. May exceed `nats.ack_wait_secs`: the handler heartbeats `ack_progress` and renews the project lock so a long job is not redelivered. 0 = no timeout.
    pub job_timeout_secs: u64,
    /// Maximum concurrent Gitaly repository fetch operations. Controls how
    /// many repositories can be downloaded simultaneously in the pipelined
    /// code indexer. 0 = no limit.
    pub fetch_concurrency: usize,
    /// In-flight batches the streaming sink holds before back-pressuring the parser.
    pub write_channel_capacity: usize,
    /// Maximum rows per ClickHouse insert; larger batches are sliced before sending.
    pub write_slice_rows: usize,
    /// Soft-flush interval in seconds: the coalescer flushes a table on this tick only once it holds at least `write_min_flush_rows`, so a trickle of small repos pools into one part instead of one tiny part per tick.
    pub write_buffer_age_secs: u64,
    /// Minimum buffered rows a table needs before the soft tick flushes it. Below this, rows keep pooling across repos until the row count or the hard `write_max_flush_age_secs` cap is reached.
    pub write_min_flush_rows: usize,
    /// Hard cap in seconds on how long a table's oldest unflushed row may wait before it is force-flushed regardless of size, bounding the uncheckpointed-rows window. Keep below `nats.ack_wait_secs`.
    pub write_max_flush_age_secs: u64,
    /// Coalesced parts written to ClickHouse concurrently. Trades memory (up to this many `write_slice_rows`-sized parts in flight) for write throughput.
    pub write_max_concurrent: usize,
    /// Parsable source-file count (`Decision::Parse`) at or below which a repository runs on the small lane.
    pub small_repo_max_files: usize,
    /// Concurrent indexing slots for small repositories. Unset = derived from the container CPU count, capped by its memory limit.
    pub small_indexing_slots: Option<usize>,
    /// Concurrent indexing slots reserved for big repositories so small ones can't starve them. Unset = derived from the container CPU count, capped by its memory limit.
    pub big_indexing_slots: Option<usize>,
}

impl CodeIndexingPipelineConfig {
    pub fn resolve_runtime_defaults(&mut self, resources: &ContainerResources) {
        if self.small_indexing_slots.is_some() && self.big_indexing_slots.is_some() {
            return;
        }

        let slots = derive_code_indexing_slots(resources.derive_worker_budget());
        if self.small_indexing_slots.is_none() {
            self.small_indexing_slots = Some(slots.small_indexing_slots);
            info!(
                available_parallelism = resources.available_parallelism,
                memory_limit_bytes = resources.memory_limit_bytes,
                value = slots.small_indexing_slots,
                "derived code-indexing pipeline.small_indexing_slots"
            );
        }
        if self.big_indexing_slots.is_none() {
            self.big_indexing_slots = Some(slots.big_indexing_slots);
            info!(
                available_parallelism = resources.available_parallelism,
                memory_limit_bytes = resources.memory_limit_bytes,
                value = slots.big_indexing_slots,
                "derived code-indexing pipeline.big_indexing_slots"
            );
        }
    }

    /// Panics when [`Self::resolve_runtime_defaults`] has not run and the
    /// value is unset: there is no fallback constant.
    pub fn small_indexing_slots(&self) -> usize {
        self.small_indexing_slots
            .expect("code-indexing pipeline.small_indexing_slots unresolved")
    }

    /// Panics when [`Self::resolve_runtime_defaults`] has not run and the
    /// value is unset: there is no fallback constant.
    pub fn big_indexing_slots(&self) -> usize {
        self.big_indexing_slots
            .expect("code-indexing pipeline.big_indexing_slots unresolved")
    }

    /// Hard per-job timeout, or `None` when disabled (`job_timeout_secs == 0`).
    pub fn job_timeout(&self) -> Option<Duration> {
        (self.job_timeout_secs > 0).then(|| Duration::from_secs(self.job_timeout_secs))
    }

    pub fn write_buffer_age(&self) -> Duration {
        Duration::from_secs(self.write_buffer_age_secs.max(1))
    }

    pub fn write_max_flush_age(&self) -> Duration {
        Duration::from_secs(self.write_max_flush_age_secs.max(1))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CodeIndexingTaskHandlerConfig {
    pub pipeline: CodeIndexingPipelineConfig,
}

/// Typed per-handler domain configuration (batch sizes, pipeline settings).
///
/// Engine-level config (retry, concurrency, DLQ) lives in `topics`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(deny_unknown_fields)]
pub struct HandlersConfiguration {
    pub entity_handler: EntityHandlerConfig,
    pub code_indexing_task: CodeIndexingTaskHandlerConfig,
}

// ── Dispatcher / scheduler config types ──────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GlobalDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
    pub sweep_interval_secs: u64,
}

/// Drives the continuous Siphon CDC trigger: which JetStream the orchestrator
/// drains and how many messages per `consume_pending` call.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SiphonRouterConfig {
    pub events_stream_name: String,
    pub batch_size: usize,
}

/// Cadence for the coverage-driven code-backfill sweep and its publish batch size.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CodeBackfillSweepConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
    pub publish_window: usize,
}

/// Cadence and per-run namespace cap for the post-backfill code stale sweep.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CodeStaleSweepConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
    pub max_namespaces_per_run: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TableCleanupConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceDeletionSchedulerConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MigrationCompletionConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

/// Tombstones edges a node's pipeline stopped emitting, off the indexing path.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StaleEdgeReconciliationConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
    pub lookback_secs: u64,
}

impl StaleEdgeReconciliationConfig {
    pub fn lookback(&self) -> chrono::TimeDelta {
        chrono::TimeDelta::seconds(self.lookback_secs as i64)
    }
}

/// Typed per-task configuration for all registered scheduled tasks.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(deny_unknown_fields)]
pub struct ScheduledTasksConfiguration {
    pub global: GlobalDispatcherConfig,
    pub namespace: NamespaceDispatcherConfig,
    pub siphon: SiphonRouterConfig,
    pub code_backfill: CodeBackfillSweepConfig,
    pub code_stale_sweep: CodeStaleSweepConfig,
    pub table_cleanup: TableCleanupConfig,
    pub namespace_deletion: NamespaceDeletionSchedulerConfig,
    pub migration_completion: MigrationCompletionConfig,
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
    /// Full set of modules, as declared in `config/default.yaml`.
    pub fn all() -> Vec<IndexerModule> {
        vec![Self::Sdlc, Self::Code, Self::NamespaceDeletion]
    }

    /// Name of the concurrency group this module's handlers subscribe under.
    /// Single source for group names so derived caps can't drift from subscription groups.
    pub const fn concurrency_group(self) -> &'static str {
        match self {
            Self::Sdlc | Self::NamespaceDeletion => "sdlc",
            Self::Code => "code",
        }
    }
}

/// ETL engine configuration. Scale fields left unset derive from the
/// container's resources at startup; an explicit value always wins.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct EngineConfiguration {
    /// Maximum concurrent message handlers across all modules. Unset = derived
    /// from the container CPU count, capped by its memory limit.
    pub max_concurrent_workers: Option<usize>,

    /// Named concurrency groups with their limits. Empty = derived from the
    /// enabled `modules` and the worker cap.
    #[serde(default)]
    pub concurrency_groups: HashMap<String, usize>,

    /// Sparse per-subscription policy overrides, keyed by topic name. Each entry
    /// is layered field-wise over the module-declared default for that topic;
    /// omit a topic to use its declared default unchanged.
    #[serde(default)]
    pub topics: HashMap<String, SubscriptionConfig>,

    /// Per-handler domain configuration (batch sizes, pipeline settings).
    pub handlers: HandlersConfiguration,

    /// Datalake retry tuning shared by all SDLC pipelines.
    pub datalake_retry: DatalakeRetryConfig,

    /// Modules whose handlers this process registers.
    pub modules: Vec<IndexerModule>,
}

impl EngineConfiguration {
    /// Panics when [`Self::resolve_runtime_defaults`] has not run and the
    /// value is unset: there is no fallback constant.
    pub fn max_concurrent_workers(&self) -> usize {
        self.max_concurrent_workers
            .expect("engine.max_concurrent_workers unresolved")
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
        if entity_handler.system_notes_resolve_lookup_batch_size == 0 {
            return Err(EngineConfigError::ZeroSystemNotesResolveLookupBatchSize);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineConfigError {
    #[error("engine.modules must list at least one module")]
    NoModulesEnabled,

    #[error(
        "engine.handlers.entity_handler.system_notes_resolve_lookup_batch_size must be at least 1"
    )]
    ZeroSystemNotesResolveLookupBatchSize,
}

/// Top-level schedule configuration.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ScheduleConfig {
    pub tasks: ScheduledTasksConfiguration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppConfig;

    fn cron(expr: &str) -> CronSchedule {
        CronSchedule::try_from(expr.to_string()).unwrap()
    }

    fn declared_policy_fixture() -> SubscriptionConfig {
        SubscriptionConfig {
            concurrency_group: Some("code".into()),
            max_attempts: Some(5),
            retry_interval_secs: Some(60),
            dead_letter_on_exhaustion: Some(true),
            max_ack_pending: None,
        }
    }

    #[test]
    fn overlay_replaces_only_fields_the_override_sets() {
        let resolved =
            declared_policy_fixture().with_optional_override(Some(&SubscriptionConfig {
                max_attempts: Some(2),
                ..Default::default()
            }));
        assert_eq!(resolved.max_attempts, Some(2));
        assert_eq!(resolved.retry_interval_secs, Some(60));
        assert_eq!(resolved.dead_letter_on_exhaustion, Some(true));
        assert_eq!(resolved.concurrency_group.as_deref(), Some("code"));
    }

    #[test]
    fn overlay_can_turn_dead_letter_off() {
        let resolved =
            declared_policy_fixture().with_optional_override(Some(&SubscriptionConfig {
                dead_letter_on_exhaustion: Some(false),
                ..Default::default()
            }));
        assert_eq!(resolved.dead_letter_on_exhaustion, Some(false));
    }

    #[test]
    fn with_optional_override_none_returns_declared_policy() {
        let resolved = declared_policy_fixture().with_optional_override(None);
        assert_eq!(resolved.max_attempts, Some(5));
        assert_eq!(resolved.dead_letter_on_exhaustion, Some(true));
    }

    #[test]
    fn embedded_schedule_declares_every_task_cron() {
        let tasks = AppConfig::embedded_defaults().schedule.tasks;
        assert_eq!(tasks.global.schedule.cron.expression(), "0 */1 * * * *");
        assert_eq!(tasks.namespace.schedule.cron.expression(), "*/30 * * * * *");
        assert_eq!(tasks.namespace.sweep_interval_secs, 3600);
        assert_eq!(
            tasks.table_cleanup.schedule.cron.expression(),
            "0 0 3 * * 0"
        );
        assert_eq!(
            tasks.stale_edge_reconciliation.schedule.cron.expression(),
            "0 */30 * * * *"
        );
        assert_eq!(
            tasks.code_stale_sweep.schedule.cron.expression(),
            "0 */1 * * * *"
        );
        assert_eq!(tasks.code_stale_sweep.max_namespaces_per_run, 10);
    }

    #[test]
    fn task_without_cron_is_rejected() {
        let err = orbit_utils::yaml::from_str::<TableCleanupConfig>("{}").unwrap_err();
        assert!(err.to_string().contains("cron"), "{err}");
    }

    #[test]
    fn invalid_cron_is_rejected_at_deserialization() {
        let err = orbit_utils::yaml::from_str::<TableCleanupConfig>("cron: nonsense").unwrap_err();
        assert!(err.to_string().contains("Invalid pattern"), "{err}");
    }

    #[test]
    fn cron_round_trips_through_serialization() {
        let cfg: TableCleanupConfig = orbit_utils::yaml::from_str("cron: \"0 0 3 * * 0\"").unwrap();
        let json = serde_json::to_string(&cfg).unwrap();
        assert_eq!(json, r#"{"cron":"0 0 3 * * 0"}"#);
    }

    #[test]
    fn job_timeout_is_some_by_default_and_disabled_at_zero() {
        let cfg = AppConfig::embedded_defaults()
            .engine
            .handlers
            .code_indexing_task
            .pipeline;
        assert_eq!(cfg.job_timeout(), Some(Duration::from_secs(1500)));
        let disabled = CodeIndexingPipelineConfig {
            job_timeout_secs: 0,
            ..cfg
        };
        assert_eq!(disabled.job_timeout(), None);
    }

    #[test]
    fn default_modules_are_universal() {
        let cfg = AppConfig::embedded_defaults().engine;
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
            ..AppConfig::embedded_defaults().engine
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
            ..AppConfig::embedded_defaults().engine
        };
        assert!(cfg.is_module_enabled(IndexerModule::Code));
        assert!(!cfg.is_module_enabled(IndexerModule::Sdlc));
        assert!(!cfg.is_module_enabled(IndexerModule::NamespaceDeletion));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn modules_deserialize_from_yaml() {
        let modules: Vec<IndexerModule> =
            orbit_utils::yaml::from_str("[sdlc, namespace_deletion]").expect("valid yaml");
        assert_eq!(
            modules,
            vec![IndexerModule::Sdlc, IndexerModule::NamespaceDeletion]
        );
    }

    #[test]
    fn system_notes_lookup_batch_size_overrides_from_yaml() {
        let yaml = "system_notes_resolve_lookup_batch_size: 2048\n";
        let cfg: EntityHandlerConfig = orbit_utils::yaml::from_str(yaml).expect("valid yaml");
        assert_eq!(cfg.system_notes_resolve_lookup_batch_size, 2_048);
        assert!(cfg.batch_size_overrides.is_empty());
    }

    #[test]
    fn zero_system_notes_resolve_lookup_batch_size_fails_validation() {
        let mut cfg = AppConfig::embedded_defaults().engine;
        cfg.handlers
            .entity_handler
            .system_notes_resolve_lookup_batch_size = 0;
        assert!(matches!(
            cfg.validate(),
            Err(EngineConfigError::ZeroSystemNotesResolveLookupBatchSize)
        ));
    }

    #[test]
    #[should_panic(expected = "unresolved")]
    fn unresolved_worker_count_has_no_fallback() {
        let _ = AppConfig::embedded_defaults()
            .engine
            .max_concurrent_workers();
    }

    #[test]
    fn interval_hint_returns_exact_period() {
        let sched = ScheduleConfiguration {
            cron: cron("0 */1 * * * *"),
        };

        let hint = sched.interval_hint();

        // interval_hint computes (second_occurrence - first_occurrence).
        // Even without truncation the chained find_next_occurrence calls
        // carry the same sub-second fraction, so the difference cancels to
        // a clean 60s. The truncation is defensive; this test pins the
        // contract regardless.
        assert_eq!(hint, Duration::from_secs(60));
    }

    #[test]
    fn next_delay_snaps_to_whole_second_boundary() {
        use chrono::NaiveDate;

        let sched = ScheduleConfiguration {
            cron: cron("0 */1 * * * *"),
        };

        // 2026-01-15 10:05:00.700 UTC — 700ms into a matching second.
        // Without truncation croner returns :06:00.700, yielding ~60.0s delay.
        // With truncation the next occurrence is :06:00.000, yielding exactly 59.3s.
        let now = NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_milli_opt(10, 5, 0, 700)
            .unwrap()
            .and_utc();

        let delay = sched.next_delay(now);
        let secs = delay.as_secs_f64();

        // Must be < 60s (snapped to :06:00.000 → 59.3s), not ~60.0s or ~60.7s.
        assert!(
            (59.0..60.0).contains(&secs),
            "expected delay ~59.3s, got {secs:.3}s"
        );
    }
}
