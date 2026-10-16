use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use code_graph::v2::{CancellationToken, Pipeline, PipelineConfig, ProgressObserver};
use gitlab_client::GitlabClientError;
use orbit_server_config::CodeIndexingPipelineConfig;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use super::arrow_converter::{IndexerConverter, IndexerEnvelope};
use super::checkpoint::{CodeCheckpoint, CodeCheckpointStore};
use super::config::CodeTableNames;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository::resolver::Repository;
use super::repository::{
    ArchiveLeases, EmptyRepositoryReason, LeaseSettings, RepositoryResolver, ResolveError,
};
use super::stale_data_cleaner::StaleDataCleaner;
use crate::clickhouse::{ClickHouseWriter, WriteError, WriteReport};
use crate::handler::{HandlerContext, HandlerError};
use crate::locking::LockGuard;
use crate::nats::ProgressNotifier;
use crate::observer::{IndexingMode, IndexingObserver};
use orbit_utils::traversal_path::TraversalPath;

#[derive(Clone)]
pub struct IndexingRequest {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: TraversalPath,
    pub task_id: i64,
    pub commit_sha: Option<String>,
    pub checkpoint: CodeCheckpoint,
    /// Gitaly storage from the project lookup; `None` uses the tenant-wide archive budget.
    pub repository_storage: Option<String>,
}

/// Keeps the NATS message alive during long pipeline runs so it is not redelivered.
struct NatsHeartbeat {
    runtime: tokio::runtime::Handle,
    notifier: ProgressNotifier,
}

impl ProgressObserver for NatsHeartbeat {
    fn family_finished(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime.block_on(self.notifier.notify_in_progress());
        }));
    }
}

pub enum IndexOutcome {
    Indexed {
        rows_written: u64,
    },
    /// No repository content to index (404, a non-gateway 5xx, an empty archive, or over the size cap); already checkpointed.
    EmptyRepository,
}

impl IndexOutcome {
    pub fn metric_label(&self) -> &'static str {
        match self {
            IndexOutcome::Indexed { .. } => "indexed",
            IndexOutcome::EmptyRepository => "empty_repository",
        }
    }
}

enum Fetched {
    Repository(Repository),
    EmptyRepository,
    NoArchiveSlot { waited: Duration },
}

pub enum IndexError {
    BudgetExceeded { budget: Duration },
    Overloaded(GitlabClientError),
    NoLane { waited: Duration },
    Failed(HandlerError),
}

impl From<HandlerError> for IndexError {
    fn from(err: HandlerError) -> Self {
        Self::Failed(err)
    }
}

struct WorkClock {
    total: Option<Duration>,
    remaining: Duration,
}

impl WorkClock {
    fn new(budget: Option<Duration>) -> Self {
        Self {
            total: budget,
            remaining: budget.unwrap_or(Duration::ZERO),
        }
    }

    /// `None` when the budget ran out before the phase finished.
    async fn run<T>(&mut self, phase: impl Future<Output = T>) -> Option<T> {
        if self.total.is_none() {
            return Some(phase.await);
        }
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(self.remaining, phase).await.ok();
        self.remaining = self.remaining.saturating_sub(started.elapsed());
        result
    }
}

type InFlightInserts = JoinSet<Result<WriteReport, WriteError>>;

struct IndexedRun {
    inserts: InFlightInserts,
    indexed_at: DateTime<Utc>,
    rows_written: u64,
}

pub(super) struct IndexingReport {
    pub stats: code_graph::v2::pipeline::PipelineStats,
    pub errors: Vec<code_graph::v2::pipeline::PipelineError>,
    pub skipped: Vec<code_graph::v2::error::SkippedFile>,
    pub limited: Vec<code_graph::v2::error::SkippedFile>,
    pub faults: Vec<code_graph::v2::error::FaultedFile>,
}

impl From<code_graph::v2::PipelineResult> for IndexingReport {
    fn from(result: code_graph::v2::PipelineResult) -> Self {
        Self {
            stats: result.stats,
            errors: result.errors,
            skipped: result.skipped,
            limited: Vec::new(),
            faults: result.faults,
        }
    }
}

pub struct CodeIndexer {
    resolver: RepositoryResolver,
    writer: Arc<ClickHouseWriter>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    metrics: CodeMetrics,
    table_names: Arc<CodeTableNames>,
    ontology: Arc<ontology::Ontology>,
    pipeline_config: CodeIndexingPipelineConfig,
    fetch_concurrency: usize,
    small_repo_max_files: usize,
    fetch_slots: Option<Arc<Semaphore>>,
    small_indexing_slots: Option<Arc<Semaphore>>,
    big_indexing_slots: Option<Arc<Semaphore>>,
}

impl CodeIndexer {
    #[allow(
        clippy::too_many_arguments,
        reason = "pipeline constructor wires all collaborators explicitly; grouping into a struct would just move the arity"
    )]
    pub fn new(
        resolver: RepositoryResolver,
        writer: Arc<ClickHouseWriter>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
        table_names: Arc<CodeTableNames>,
        ontology: Arc<ontology::Ontology>,
        pipeline_config: CodeIndexingPipelineConfig,
    ) -> Self {
        let fc = pipeline_config.fetch_concurrency;
        let small = pipeline_config.small_indexing_slots();
        let big = pipeline_config.big_indexing_slots();
        Self {
            resolver,
            writer,
            checkpoint_store,
            stale_data_cleaner,
            metrics,
            table_names,
            ontology,
            fetch_concurrency: fc,
            small_repo_max_files: pipeline_config.small_repo_max_files,
            fetch_slots: sem(fc),
            small_indexing_slots: sem(small),
            big_indexing_slots: sem(big),
            pipeline_config,
        }
    }

    /// Derived inflight cap for the engine listen loop: a download can pre-fetch ahead of
    /// every indexing lane. Returns `None` when any limit is unbounded (0), so the global
    /// default applies.
    pub fn max_inflight(&self) -> Option<usize> {
        let small = self.pipeline_config.small_indexing_slots();
        let big = self.pipeline_config.big_indexing_slots();
        if self.fetch_concurrency == 0 || small == 0 || big == 0 {
            return None;
        }
        Some(self.fetch_concurrency + small + big)
    }

    /// Hard per-job wall-clock timeout, or `None` when disabled.
    pub fn job_timeout(&self) -> Option<std::time::Duration> {
        self.pipeline_config.job_timeout()
    }

    pub async fn index_project(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        observer: &mut dyn IndexingObserver,
        cancel: CancellationToken,
        lock: &LockGuard,
    ) -> Result<IndexOutcome, IndexError> {
        let budget = self.pipeline_config.job_timeout();
        let mut clock = WorkClock::new(budget);

        // Dropping the timed-out fetch is what stops it; nothing in that phase polls the token.
        let Some(fetched) = clock
            .run(self.fetch_repository(context, request, &cancel))
            .await
        else {
            return Err(IndexError::BudgetExceeded {
                budget: budget.unwrap_or_default(),
            });
        };
        let repository = match fetched? {
            Fetched::EmptyRepository => return Ok(IndexOutcome::EmptyRepository),
            Fetched::NoArchiveSlot { waited } => return Err(IndexError::NoLane { waited }),
            Fetched::Repository(repository) => repository,
        };

        // Fetch plus the longest allowed wait can reach ack_wait; refresh the delivery first.
        context.progress.notify_in_progress().await;
        let lane = self
            .acquire_indexing_lane(&repository, lock.time_left(), &cancel)
            .await?;

        let indexed = clock
            .run(self.index_fetched(
                context,
                request,
                &repository,
                lane,
                observer,
                cancel.clone(),
            ))
            .await;
        let run = match indexed {
            Some(result) => result.map_err(IndexError::Failed)?,
            None => {
                // The blocking parse outlives the dropped future; cancel makes it bail.
                cancel.cancel();
                return Err(IndexError::BudgetExceeded {
                    budget: budget.unwrap_or_default(),
                });
            }
        };

        drop(repository);
        context.work.release();
        await_inserts(run.inserts).await?;
        if cancel.is_cancelled() {
            return Err(HandlerError::Processing(
                "project lock lost before the checkpoint; retrying the task".to_string(),
            )
            .into());
        }
        self.tombstone_stale_rows(request, run.indexed_at).await;
        let mut checkpoint = request.checkpoint.clone();
        checkpoint.complete(request.task_id, request.commit_sha.clone(), run.indexed_at);
        self.save_checkpoint(&checkpoint).await?;
        info!(
            project_id = checkpoint.project_id,
            task_id = checkpoint.last_task_id,
            "completed code indexing"
        );
        Ok(IndexOutcome::Indexed {
            rows_written: run.rows_written,
        })
    }

    async fn tombstone_stale_rows(&self, request: &IndexingRequest, indexed_at: DateTime<Utc>) {
        let checkpoint = &request.checkpoint;
        if checkpoint.indexing_mode() != IndexingMode::Incremental {
            return;
        }
        if let Err(error) = self
            .stale_data_cleaner
            .delete_stale_data(
                &checkpoint.traversal_path,
                checkpoint.project_id,
                &checkpoint.branch,
                indexed_at,
            )
            .await
        {
            warn!(
                project_id = checkpoint.project_id,
                %error,
                "failed to delete stale data, will retry on next indexing"
            );
        }
    }

    async fn save_checkpoint(&self, checkpoint: &CodeCheckpoint) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save(checkpoint)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to set checkpoint: {e}")))
            .record_error_stage(&self.metrics, "checkpoint")
    }

    async fn fetch_repository(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        cancel: &CancellationToken,
    ) -> Result<Fetched, IndexError> {
        // Phase 1: Fetch — bounded by fetch_slots so we don't overwhelm
        // Gitaly with concurrent downloads while still pre-fetching ahead
        // of the processing phase.
        let fetch_queued = Instant::now();
        let _fetch_slot = acquire(&self.fetch_slots, "fetch").await?;
        self.metrics
            .record_slot_wait("fetch_slot_wait", fetch_queued.elapsed());

        let leases = ArchiveLeases::new(
            Arc::clone(&context.lock_service),
            LeaseSettings {
                storage_budget: self.pipeline_config.archive_storage_budget,
                tenant_budget: self.pipeline_config.archive_tenant_budget,
                ttl: Duration::from_secs(self.pipeline_config.archive_lease_ttl_secs.max(1)),
                max_wait: Duration::from_secs(self.pipeline_config.archive_lease_max_wait_secs),
            },
            cancel.clone(),
            self.metrics.clone(),
        );
        let fetch_start = Instant::now();
        let repository = match self
            .resolver
            .resolve(
                request.project_id,
                &request.branch,
                request.commit_sha.as_deref(),
                request.traversal_path.top_level_namespace_id(),
                &self.pipeline_config,
                &leases,
                request.repository_storage.as_deref(),
            )
            .await
        {
            Ok(repository) => {
                self.metrics.record_resolution_strategy("full_download");
                if let Repository::IncrementalEngine(files) = &repository {
                    for file in files.files() {
                        if let orbit_utils::vfs::Decision::List(reason) = file.decision() {
                            self.metrics
                                .record_archive_entry_skipped(reason, 1, file.size);
                        }
                    }
                    let dropped = files.usage().dropped_listed;
                    if dropped > 0 {
                        self.metrics
                            .record_archive_entry_skipped("file_limit", dropped as u64, 0);
                    }
                }
                repository
            }
            Err(ResolveError::EmptyRepository { reason, detail }) => {
                let redelivered = match reason {
                    EmptyRepositoryReason::EmptyArchive => context.redelivered_on_failure,
                    _ => !reason.is_permanent(),
                };
                if request.commit_sha.is_some() && redelivered {
                    self.metrics.record_stage_error("repository_fetch");
                    return Err(HandlerError::Processing(format!(
                        "archive empty ({reason}: {detail}) for task with commit_sha; retrying"
                    ))
                    .into());
                }
                warn!(
                    project_id = request.project_id,
                    branch = %request.branch,
                    reason = %reason,
                    detail,
                    "checkpointing repository as indexed-empty"
                );
                self.metrics.record_resolution_strategy("empty_repository");
                self.metrics
                    .record_empty_repository(reason.as_metric_label());
                self.metrics.record_fetch_duration(fetch_start.elapsed());
                let mut checkpoint = request.checkpoint.clone();
                checkpoint.complete_empty_repository(request.task_id);
                self.save_checkpoint(&checkpoint).await?;
                return Ok(Fetched::EmptyRepository);
            }
            Err(ResolveError::Overloaded(cause)) => {
                self.metrics.record_stage_error("repository_fetch_overload");
                return Err(IndexError::Overloaded(cause));
            }
            Err(ResolveError::NoArchiveSlot { waited }) => {
                return Ok(Fetched::NoArchiveSlot { waited });
            }
            Err(ResolveError::Other(err)) => {
                self.metrics.record_stage_error("repository_fetch");
                return Err(err.into());
            }
        };
        let fetch_duration = fetch_start.elapsed();
        self.metrics.record_fetch_duration(fetch_duration);
        info!(
            duration_ms = fetch_duration.as_millis() as u64,
            "repository extraction completed"
        );

        // Release the fetch slot before the caller queues for an indexing slot. This is the
        // pipelining point: freeing it lets another handler start its Gitaly download.
        drop(_fetch_slot);
        Ok(Fetched::Repository(repository))
    }

    async fn acquire_indexing_lane(
        &self,
        repository: &Repository,
        within: Duration,
        cancel: &CancellationToken,
    ) -> Result<Option<OwnedSemaphorePermit>, IndexError> {
        // A reserved big lane keeps a flood of small repos from starving monorepos.
        let lane = if repository.parseable_files() <= self.small_repo_max_files {
            &self.small_indexing_slots
        } else {
            &self.big_indexing_slots
        };
        let queued = Instant::now();
        let permit = tokio::select! {
            biased;
            permit = tokio::time::timeout(within, acquire(lane, "indexing")) => permit,
            () = lock_lost(cancel) => {
                return Err(IndexError::NoLane {
                    waited: queued.elapsed(),
                });
            }
        };
        match permit {
            Ok(permit) => {
                self.metrics
                    .record_slot_wait("indexing_lane_wait", queued.elapsed());
                permit.map_err(IndexError::Failed)
            }
            Err(_) => Err(IndexError::NoLane { waited: within }),
        }
    }

    async fn index_fetched(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        repository: &Repository,
        _lane: Option<OwnedSemaphorePermit>,
        observer: &mut dyn IndexingObserver,
        cancel: CancellationToken,
    ) -> Result<IndexedRun, HandlerError> {
        context.progress.notify_in_progress().await;

        let indexed_at = Utc::now();
        let indexing_result = self
            .run_indexing(context, request, repository, indexed_at, observer, cancel)
            .await;

        // `repository` owns a TempDir that removes the extraction tree on drop, so it is reclaimed
        // whether this returns, errors, or is dropped mid-run on the wall-clock timeout.
        self.metrics.record_cleanup("success");
        indexing_result
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "indexing stage threads its collaborators explicitly; a params struct would just move the arity"
    )]
    async fn run_indexing(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        repository: &Repository,
        indexed_at: DateTime<Utc>,
        observer: &mut dyn IndexingObserver,
        cancel: CancellationToken,
    ) -> Result<IndexedRun, HandlerError> {
        let indexing_start = Instant::now();
        let (result, inserts, metered_bytes) = self
            .build_code_graph(context, request, repository, indexed_at, cancel.clone())
            .await?;

        // A cancelled run yields a partial graph with no error, so the fatal-error guard can't catch it.
        if cancel.is_cancelled() {
            return Err(HandlerError::Processing(
                "code indexing cancelled mid-run; not checkpointing partial graph".to_string(),
            ));
        }

        context.progress.notify_in_progress().await;
        self.metrics
            .record_indexing_duration(indexing_start.elapsed());

        let rows_written = self.record_indexing_results(
            &result,
            observer,
            request,
            indexing_start,
            metered_bytes.load(Ordering::Relaxed),
        );

        if let Some(error) = result.errors.iter().find(|error| error.fatal) {
            return Err(HandlerError::dead_letter(format!(
                "fatal code indexing pipeline error during {} for {}: {}",
                error.stage, error.file_path, error.error
            )));
        }

        context.progress.notify_in_progress().await;
        Ok(IndexedRun {
            inserts,
            indexed_at,
            rows_written,
        })
    }

    fn build_pipeline_config(
        &self,
        context: &HandlerContext,
        cancel: CancellationToken,
    ) -> PipelineConfig {
        let to_timeout = |ms: u64| (ms > 0).then(|| std::time::Duration::from_millis(ms));
        let progress = Arc::new(NatsHeartbeat {
            runtime: tokio::runtime::Handle::current(),
            notifier: context.progress.clone(),
        });
        let phase_cpu_metrics = self.metrics.clone();
        let on_phase_cpu: Option<code_graph::v2::PhaseCpuObserver> =
            Some(std::sync::Arc::new(move |language, cpu| {
                phase_cpu_metrics.record_file_phase_cpu(language, cpu)
            }));
        PipelineConfig {
            cancel,
            max_files: self.pipeline_config.max_files,
            worker_threads: self.pipeline_config.worker_threads,
            max_concurrent_languages: self.pipeline_config.max_concurrent_languages,
            per_file_timeout: to_timeout(self.pipeline_config.per_file_timeout_ms),
            per_file_parse_timeout: to_timeout(self.pipeline_config.per_file_parse_timeout_ms),
            per_file_walk_timeout: to_timeout(self.pipeline_config.per_file_walk_timeout_ms),
            per_file_ssa_timeout: to_timeout(self.pipeline_config.per_file_ssa_timeout_ms),
            cross_file_resolve_timeout: to_timeout(
                self.pipeline_config.cross_file_resolve_timeout_ms,
            ),
            progress,
            on_phase_cpu,
            ..Default::default()
        }
    }

    async fn build_code_graph(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        repository: &Repository,
        indexed_at: DateTime<Utc>,
        cancel: CancellationToken,
    ) -> Result<(IndexingReport, InFlightInserts, Arc<AtomicU64>), HandlerError> {
        let limits = super::incremental::limits(&self.pipeline_config);
        let tracer = code_graph::v2::trace::Tracer::new(false);
        let envelope = IndexerEnvelope::new(
            request.traversal_path.clone(),
            request.project_id,
            request.branch.clone(),
            request.commit_sha.as_deref().unwrap_or("").to_string(),
            indexed_at,
        );

        let inserts = Arc::new(Mutex::new(InFlightInserts::new()));
        let metered_bytes = Arc::new(AtomicU64::new(0));
        let on_batch: Arc<code_graph::v2::OnBatch> = Arc::new({
            let (inserts, metered_bytes) = (inserts.clone(), metered_bytes.clone());
            let inserter = SliceInserter {
                writer: self.writer.clone(),
                slots: Arc::new(Semaphore::new(
                    self.pipeline_config.write_max_concurrent.max(1),
                )),
                max_rows: self.pipeline_config.write_slice_rows.max(1),
                runtime: tokio::runtime::Handle::current(),
            };
            move |table: &str,
                  batch: arrow::record_batch::RecordBatch|
                  -> Result<(), code_graph::v2::SinkError> {
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let batch_bytes = match orbit_arrow::logical_byte_size(&batch) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::error!(table, error = %e, "batch has no logical-byte-size rule; counting 0 bytes");
                        0
                    }
                };
                metered_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
                inserter.spawn_slices(&inserts, table, batch)
            }
        });

        let code_graph_start = Instant::now();
        let repository = repository.clone();
        let ontology = self.ontology.clone();
        let table_names = self.table_names.clone();
        let span = tracing::Span::current();
        let legacy_config = self.build_pipeline_config(context, cancel.clone());
        let progress = NatsHeartbeat {
            runtime: tokio::runtime::Handle::current(),
            notifier: context.progress.clone(),
        };
        let options = super::incremental::Options {
            limits,
            worker_threads: legacy_config.worker_threads,
            concurrent_families: legacy_config.concurrent_languages(),
            small_family_files: self.small_repo_max_files,
            batch_bytes: orbit_arrow::BATCH_BYTES,
            cancelled: Arc::new(move || cancel.is_cancelled()),
            family_finished: Arc::new(move || progress.family_finished()),
            phase_started: {
                let span = span.clone();
                Arc::new(move |family, phase, files| {
                    span.in_scope(|| info!(family, phase, files, "code-graph phase started"));
                })
            },
        };
        let parsed = tokio::task::spawn_blocking(move || {
            span.in_scope(|| match repository {
                Repository::IncrementalEngine(repository) => super::incremental::run(
                    repository,
                    options,
                    envelope,
                    ontology,
                    table_names,
                    on_batch,
                ),
                Repository::Extracted(repository) => Ok(Pipeline::run_with_tracer(
                    repository.path(),
                    repository.file_inventory.clone(),
                    legacy_config,
                    tracer,
                    Arc::new(IndexerConverter::new(envelope, &ontology, table_names)),
                    on_batch,
                )
                .into()),
            })
        })
        .await;
        let result = parsed
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()))
            .map_err(|e| HandlerError::Processing(format!("pipeline failed: {e}")))?;
        info!(
            duration_ms = code_graph_start.elapsed().as_millis() as u64,
            "code-graph building completed"
        );

        Ok((result, std::mem::take(&mut *inserts.lock()), metered_bytes))
    }

    fn record_indexing_results(
        &self,
        result: &IndexingReport,
        observer: &mut dyn IndexingObserver,
        request: &IndexingRequest,
        indexing_start: Instant,
        written_bytes: u64,
    ) -> u64 {
        let parsed_count = result
            .stats
            .files_parsed
            .saturating_sub(result.skipped.len() + result.faults.len());
        let skipped_count = result.stats.files_skipped + result.skipped.len();

        self.metrics
            .record_repository_source_size(result.stats.bytes_discovered);
        self.metrics
            .record_phase_timing(&result.stats.phase_timings);
        for lt in &result.stats.language_timings {
            self.metrics.record_language_timing(lt);
        }

        // Name the few genuinely-slow files so an engineer can open them; the
        // per-phase CPU distribution lives in the phase_cpu_duration histogram.
        const SLOW_FILE_LOG_THRESHOLD_MS: f64 = 500.0;
        const SLOW_FILE_LOG_MAX: usize = 10;
        for entry in result
            .stats
            .slowest_files
            .iter()
            .filter(|e| e.total_ms >= SLOW_FILE_LOG_THRESHOLD_MS)
            .take(SLOW_FILE_LOG_MAX)
        {
            info!(
                path = %entry.path,
                language = %entry.language,
                size_bytes = entry.size_bytes,
                parse_ms = entry.parse_ms,
                resolve_ms = entry.resolve_ms,
                total_ms = entry.total_ms,
                "slow file during code indexing"
            );
        }

        observer.record_source_bytes(result.stats.bytes_discovered);
        observer.files_processed(
            result.stats.files_discovered as u64,
            parsed_count as u64,
            skipped_count as u64,
        );
        observer.nodes_indexed("directory", result.stats.directories_indexed as u64);
        observer.nodes_indexed("file", result.stats.files_indexed as u64);
        observer.nodes_indexed("definition", result.stats.definitions_count as u64);
        observer.nodes_indexed("imported_symbol", result.stats.imports_count as u64);
        observer.nodes_indexed("edge", result.stats.edges_count as u64);

        let rows_written = (result.stats.directories_indexed
            + result.stats.files_indexed
            + result.stats.definitions_count
            + result.stats.imports_count
            + result.stats.edges_count) as u64;
        observer.record_graph_write("code_graph", rows_written, written_bytes);
        observer.record_duration(indexing_start.elapsed().as_millis() as u64);

        for skipped in &result.skipped {
            self.metrics.record_file_skipped(
                skipped.kind.as_metric_label(),
                &skipped.path,
                skipped.family.as_deref(),
            );
            if skipped.path.is_empty() {
                warn!(
                    project_id = request.project_id,
                    branch = %request.branch,
                    family = skipped.family.as_deref().unwrap_or_default(),
                    reason = skipped.kind.as_metric_label(),
                    detail = %skipped.detail,
                    "cross-file resolution stopped early during code indexing"
                );
            } else {
                debug!(
                    project_id = request.project_id,
                    branch = %request.branch,
                    path = %skipped.path,
                    reason = skipped.kind.as_metric_label(),
                    "file skipped during code indexing"
                );
            }
        }

        for limited in &result.limited {
            self.metrics.record_file_skipped(
                limited.kind.as_metric_label(),
                &limited.path,
                limited.family.as_deref(),
            );
            warn!(
                project_id = request.project_id,
                branch = %request.branch,
                path = %limited.path,
                reason = limited.kind.as_metric_label(),
                detail = %limited.detail,
                "file reached a limit during code indexing and was indexed in part"
            );
        }

        for fault in &result.faults {
            self.metrics
                .record_file_fault(fault.kind.as_metric_label(), &fault.path);
        }
        if !result.faults.is_empty() {
            warn!(
                project_id = request.project_id,
                branch = %request.branch,
                count = result.faults.len(),
                "files faulted during code indexing"
            );
            self.metrics
                .record_files_processed(result.faults.len() as u64, "errored");
        }

        for error in &result.errors {
            self.metrics.record_stage_error(error.stage);
        }

        rows_written
    }
}

fn sem(n: usize) -> Option<Arc<Semaphore>> {
    (n > 0).then(|| Arc::new(Semaphore::new(n)))
}

async fn acquire(
    slots: &Option<Arc<Semaphore>>,
    name: &str,
) -> Result<Option<OwnedSemaphorePermit>, HandlerError> {
    match slots {
        Some(s) => s
            .clone()
            .acquire_owned()
            .await
            .map(Some)
            .map_err(|e| HandlerError::Processing(format!("{name} slot closed: {e}"))),
        None => Ok(None),
    }
}

async fn lock_lost(cancel: &CancellationToken) {
    while !cancel.is_cancelled() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

struct SliceInserter {
    writer: Arc<ClickHouseWriter>,
    slots: Arc<Semaphore>,
    max_rows: usize,
    runtime: tokio::runtime::Handle,
}

impl SliceInserter {
    /// Blocks the parse thread for a slot, so a job never holds more than `write_max_concurrent` slices.
    fn spawn_slices(
        &self,
        inserts: &Mutex<InFlightInserts>,
        table: &str,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<(), code_graph::v2::SinkError> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = (batch.num_rows() - offset).min(self.max_rows);
            let slice = batch.slice(offset, len);
            let slot = self
                .runtime
                .block_on(self.slots.clone().acquire_owned())
                .map_err(|e| code_graph::v2::SinkError(e.to_string()))?;
            let (writer, table) = (self.writer.clone(), table.to_string());
            inserts.lock().spawn_on(
                async move {
                    let _slot = slot;
                    writer.write_durable(&table, slice).await
                },
                &self.runtime,
            );
            offset += len;
        }
        Ok(())
    }
}

/// The first failure drops the set, which aborts the inserts still in flight.
async fn await_inserts(mut inserts: InFlightInserts) -> Result<(), HandlerError> {
    while let Some(written) = inserts.join_next().await {
        written
            .map_err(|e| HandlerError::Processing(format!("write task failed: {e}")))?
            .map_err(|e| {
                HandlerError::Processing(format!("rows did not land; retrying the task: {e}"))
            })?;
    }
    Ok(())
}

#[cfg(test)]
impl CodeIndexer {
    pub(crate) async fn occupy_small_indexing_lanes(&self) -> OwnedSemaphorePermit {
        let lanes = self
            .small_indexing_slots
            .clone()
            .expect("small indexing lanes are bounded");
        let free = lanes.available_permits() as u32;
        lanes
            .acquire_many_owned(free)
            .await
            .expect("lanes are open")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn work_for(secs: u64) {
        tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn phases_share_one_work_budget() {
        let mut clock = WorkClock::new(Some(Duration::from_secs(250)));

        let started = tokio::time::Instant::now();
        assert!(clock.run(work_for(100)).await.is_some());
        assert!(
            clock.run(work_for(200)).await.is_none(),
            "100s + 200s must overrun a 250s budget"
        );
        assert_eq!(
            started.elapsed().as_secs(),
            250,
            "the second phase gets only what the first left"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_disabled_budget_never_times_out() {
        let mut clock = WorkClock::new(None);
        assert!(clock.run(work_for(10_000)).await.is_some());
    }
}
