use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::v2::{CancellationToken, Pipeline, PipelineConfig};
use gkg_server_config::CodeIndexingPipelineConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use super::arrow_converter::IndexerEnvelope;
use super::writer::{StreamWriter, WriteTotals};
use super::checkpoint::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::config::CodeTableNames;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository::cache::CachedRepository;
use super::repository::{RepositoryResolver, ResolveError};
use super::stale_data_cleaner::StaleDataCleaner;
use crate::handler::{HandlerContext, HandlerError};
use crate::observer::IndexingObserver;

pub struct IndexingRequest {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: String,
    pub task_id: i64,
    pub commit_sha: Option<String>,
    pub had_prior_checkpoint: bool,
}

/// Terminal outcome of `CodeIndexingPipeline::index_project`.
///
/// The handler records a single `events_processed` outcome label based on
/// this variant — keeping `indexed` and `empty_repository` mutually exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOutcome {
    /// Repository downloaded, parsed, written to the graph, and checkpointed.
    Indexed,
    /// Archive endpoint signalled no repository content (404 or 5xx); the
    /// checkpoint was still set so retries and DLQ are avoided.
    EmptyRepository,
}

/// Number of indexing slots derived from the concurrency group limit.
/// Used both for the semaphore and for the `max_inflight` calculation.
pub fn indexing_slot_count(concurrency_limit: usize) -> usize {
    concurrency_limit / 2
}

pub struct CodeIndexingPipeline {
    resolver: RepositoryResolver,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    metrics: CodeMetrics,
    table_names: Arc<CodeTableNames>,
    ontology: Arc<ontology::Ontology>,
    pipeline_config: CodeIndexingPipelineConfig,
    fetch_concurrency: usize,
    indexing_slot_count: usize,
    fetch_slots: Option<Arc<Semaphore>>,
    indexing_slots: Option<Arc<Semaphore>>,
}

impl CodeIndexingPipeline {
    #[allow(
        clippy::too_many_arguments,
        reason = "pipeline constructor wires all collaborators explicitly; grouping into a struct would just move the arity"
    )]
    pub fn new(
        resolver: RepositoryResolver,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
        table_names: Arc<CodeTableNames>,
        ontology: Arc<ontology::Ontology>,
        pipeline_config: CodeIndexingPipelineConfig,
        concurrency_limit: usize,
    ) -> Self {
        let fc = pipeline_config.fetch_concurrency;
        let ic = indexing_slot_count(concurrency_limit);
        let fetch_slots = sem(fc);
        let indexing_slots = sem(ic);
        Self {
            resolver,
            checkpoint_store,
            stale_data_cleaner,
            metrics,
            table_names,
            ontology,
            pipeline_config,
            fetch_concurrency: fc,
            indexing_slot_count: ic,
            fetch_slots,
            indexing_slots,
        }
    }

    /// Derived inflight cap for the engine listen loop. Returns `None` when
    /// either limit is unbounded (0), meaning the global default should apply.
    pub fn max_inflight(&self) -> Option<usize> {
        if self.fetch_concurrency == 0 || self.indexing_slot_count == 0 {
            return None;
        }
        Some(self.fetch_concurrency + self.indexing_slot_count)
    }

    /// Hard per-job wall-clock timeout, or `None` when disabled.
    pub fn job_timeout(&self) -> Option<std::time::Duration> {
        self.pipeline_config.job_timeout()
    }

    #[tracing::instrument(
        name = "code_indexing_project",
        skip_all,
        fields(
            project_id = request.project_id,
            namespace_id,
            traversal_path = %request.traversal_path,
            branch = %request.branch,
        )
    )]
    pub async fn index_project(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        observer: &mut dyn IndexingObserver,
        cancel: CancellationToken,
    ) -> Result<IndexOutcome, HandlerError> {
        let Some(namespace_id) =
            gkg_utils::traversal_path::top_level_namespace_id(&request.traversal_path)
        else {
            return Err(HandlerError::Processing(format!(
                "traversal_path {:?} has no namespace_id",
                request.traversal_path
            )));
        };
        tracing::Span::current().record("namespace_id", namespace_id);

        // Phase 1: Fetch — bounded by fetch_slots so we don't overwhelm
        // Gitaly with concurrent downloads while still pre-fetching ahead
        // of the processing phase.
        let _fetch_slot = acquire(&self.fetch_slots, "fetch").await?;

        let fetch_start = Instant::now();
        let repository = match self
            .resolver
            .resolve(
                request.project_id,
                &request.branch,
                request.commit_sha.as_deref(),
            )
            .await
        {
            Ok(path) => {
                self.metrics.record_resolution_strategy("full_download");
                path
            }
            Err(ResolveError::EmptyRepository { reason, detail }) => {
                warn!(
                    project_id = request.project_id,
                    branch = %request.branch,
                    reason = %reason,
                    detail,
                    "project has no repository content; checkpointing as indexed-empty"
                );
                self.metrics.record_resolution_strategy("empty_repository");
                self.metrics
                    .record_empty_repository(reason.as_metric_label());
                self.metrics.record_fetch_duration(fetch_start.elapsed());
                self.set_checkpoint(request, None, Utc::now()).await?;
                return Ok(IndexOutcome::EmptyRepository);
            }
            Err(ResolveError::Other(err)) => {
                self.metrics.record_stage_error("repository_fetch");
                return Err(err);
            }
        };
        let fetch_duration = fetch_start.elapsed();
        self.metrics.record_fetch_duration(fetch_duration);
        info!(
            duration_ms = fetch_duration.as_millis() as u64,
            "repository extraction completed"
        );

        // Release fetch slot before waiting for the indexing slot. This is
        // the pipelining point: freeing the fetch slot lets another handler
        // start its Gitaly download while we wait for an indexing slot.
        drop(_fetch_slot);

        // Phase 2: Process — bounded by indexing_slots (CPU-heavy analysis).
        let _indexing_slot = acquire(&self.indexing_slots, "indexing").await?;

        context.progress.notify_in_progress().await;

        let indexed_at = Utc::now();
        let indexing_result = self
            .run_indexing(context, request, &repository, indexed_at, observer, cancel)
            .await;

        if let Err(error) = self
            .resolver
            .cleanup(request.project_id, &request.branch)
            .await
        {
            self.metrics.record_cleanup("failure");
            warn!(
                project_id = request.project_id,
                branch = %request.branch,
                %error,
                "failed to clean up downloaded repository from disk"
            );
        } else {
            self.metrics.record_cleanup("success");
        }

        indexing_result?;

        self.set_checkpoint(request, request.commit_sha.as_deref(), indexed_at)
            .await?;

        Ok(IndexOutcome::Indexed)
    }

    async fn set_checkpoint(
        &self,
        request: &IndexingRequest,
        last_commit: Option<&str>,
        indexed_at: DateTime<Utc>,
    ) -> Result<(), HandlerError> {
        let checkpoint = CodeIndexingCheckpoint {
            traversal_path: request.traversal_path.clone(),
            project_id: request.project_id,
            branch: request.branch.clone(),
            last_task_id: request.task_id,
            last_commit: last_commit.map(|s| s.to_string()),
            indexed_at,
        };

        self.checkpoint_store
            .set_checkpoint(&checkpoint)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to set checkpoint: {e}")))
            .record_error_stage(&self.metrics, "checkpoint")?;

        info!(
            project_id = request.project_id,
            branch = %request.branch,
            commit = ?last_commit,
            task_id = request.task_id,
            "completed code indexing"
        );

        Ok(())
    }

    async fn run_indexing(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        repository: &CachedRepository,
        indexed_at: DateTime<Utc>,
        observer: &mut dyn IndexingObserver,
        cancel: CancellationToken,
    ) -> Result<(), HandlerError> {
        let indexing_start = Instant::now();
        let config = self.build_pipeline_config(context, cancel);
        let (result, per_table_writes) = self
            .build_code_graph(context, request, repository, indexed_at, config)
            .await?;

        context.progress.notify_in_progress().await;
        self.metrics
            .record_indexing_duration(indexing_start.elapsed());

        self.record_indexing_results(
            &result,
            &per_table_writes,
            observer,
            request,
            indexing_start,
        );

        if let Some(error) = result.errors.iter().find(|error| error.fatal) {
            return Err(HandlerError::Permanent {
                message: format!(
                    "fatal code indexing pipeline error during {} for {}: {}",
                    error.stage, error.file_path, error.error
                ),
                action: crate::handler::PermanentAction::DeadLetter,
            });
        }

        context.progress.notify_in_progress().await;
        self.run_stale_cleanup(request, indexed_at, &result).await;

        Ok(())
    }

    fn build_pipeline_config(
        &self,
        context: &HandlerContext,
        cancel: CancellationToken,
    ) -> PipelineConfig {
        let to_timeout = |ms: u64| (ms > 0).then(|| std::time::Duration::from_millis(ms));
        let handle = tokio::runtime::Handle::current();
        let progress = context.progress.clone();
        let on_progress: Option<std::sync::Arc<dyn Fn() + Send + Sync>> =
            Some(std::sync::Arc::new(move || {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handle.block_on(progress.notify_in_progress());
                }));
            }));
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
            on_progress,
            on_phase_cpu,
            ..Default::default()
        }
    }

    async fn build_code_graph(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        repository: &CachedRepository,
        indexed_at: DateTime<Utc>,
        config: PipelineConfig,
    ) -> Result<
        (
            code_graph::v2::PipelineResult,
            Vec<WriteTotals>,
        ),
        HandlerError,
    > {
        let tracer = code_graph::v2::trace::Tracer::new(false);
        let envelope = IndexerEnvelope::new(
            request.traversal_path.clone(),
            request.project_id,
            request.branch.clone(),
            request.commit_sha.as_deref().unwrap_or("").to_string(),
            indexed_at,
        );

        let converter: Arc<dyn code_graph::v2::GraphConverter> =
            Arc::new(arrow_converter::IndexerConverter::new(
                envelope,
                &self.ontology,
                self.table_names.clone(),
            ));
        let streaming_sink = Arc::new(StreamWriter::new(
            context.destination.clone(),
            self.pipeline_config.write_channel_capacity,
            self.pipeline_config.write_max_concurrent_writes,
            self.pipeline_config.write_slice_rows,
        ));
        let sink: Arc<dyn code_graph::v2::BatchSink> = streaming_sink.clone();

        let code_graph_start = Instant::now();
        let repo_dir = repository.path.clone();
        let file_inventory = repository.file_inventory.clone();
        let stream_reasons = repository.stream_reasons.clone();
        let result = tokio::task::spawn_blocking(move || {
            Pipeline::run_with_tracer(
                &repo_dir,
                file_inventory,
                config,
                &stream_reasons,
                tracer,
                converter,
                sink,
            )
        })
        .await
        .map_err(|e| HandlerError::Processing(format!("pipeline thread panicked: {e}")))?;
        let code_graph_duration = code_graph_start.elapsed();
        info!(
            duration_ms = code_graph_duration.as_millis() as u64,
            "code-graph building completed"
        );

        let flush_start = Instant::now();
        let per_table_writes = match streaming_sink.finish().await {
            Ok(totals) => totals,
            Err(e) => {
                return Err(HandlerError::Permanent {
                    message: format!(
                        "fatal code indexing pipeline error during flush for project {}: {e}",
                        request.project_id
                    ),
                    action: crate::handler::PermanentAction::DeadLetter,
                });
            }
        };
        let graph_write_duration = flush_start.elapsed();
        info!(
            duration_ms = graph_write_duration.as_millis() as u64,
            "graph writing completed"
        );

        Ok((result, per_table_writes))
    }

    fn record_indexing_results(
        &self,
        result: &code_graph::v2::PipelineResult,
        per_table_writes: &[WriteTotals],
        observer: &mut dyn IndexingObserver,
        request: &IndexingRequest,
        indexing_start: Instant,
    ) {
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

        for write in per_table_writes {
            observer.record_graph_write(&write.table, write.rows, write.bytes);
        }
        observer.record_duration(indexing_start.elapsed().as_millis() as u64);

        for skipped in &result.skipped {
            self.metrics
                .record_file_skipped(skipped.kind.as_metric_label());
            debug!(
                project_id = request.project_id,
                branch = %request.branch,
                path = %skipped.path,
                reason = skipped.kind.as_metric_label(),
                "file skipped during code indexing"
            );
        }

        for fault in &result.faults {
            self.metrics.record_file_fault(fault.kind.as_metric_label());
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
    }

    async fn run_stale_cleanup(
        &self,
        request: &IndexingRequest,
        indexed_at: DateTime<Utc>,
        result: &code_graph::v2::PipelineResult,
    ) {
        // A killed first-time run can leave partial rows that only this sweep removes.
        info!(
            project_id = request.project_id,
            branch = %request.branch,
            watermark = %indexed_at,
            definitions = result.stats.definitions_count,
            imports = result.stats.imports_count,
            files = result.stats.files_indexed,
            directories = result.stats.directories_indexed,
            files_discovered = result.stats.files_discovered,
            faulted = result.faults.len(),
            skipped = result.skipped.len(),
            "cleaning stale code data: tombstoning prior-version rows not re-emitted by this run"
        );
        let cleanup_start = Instant::now();
        if let Err(error) = self
            .stale_data_cleaner
            .delete_stale_data(
                &request.traversal_path,
                request.project_id,
                &request.branch,
                indexed_at,
            )
            .await
        {
            warn!(
                project_id = request.project_id,
                branch = %request.branch,
                %error,
                "failed to delete stale data, will retry on next indexing"
            );
        }
        let stale_data_cleanup_duration = cleanup_start.elapsed();
        info!(
            duration_ms = stale_data_cleanup_duration.as_millis() as u64,
            "stale data cleanup completed"
        );
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
