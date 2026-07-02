use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::v2::{CancellationToken, Pipeline, PipelineConfig};
use gkg_server_config::CodeIndexingPipelineConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use super::arrow_converter::{IndexerConverter, IndexerEnvelope};
use super::checkpoint::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::config::CodeTableNames;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository::cache::CachedRepository;
use super::repository::{RepositoryResolver, ResolveError};
use super::stale_data_cleaner::StaleDataCleaner;
use crate::clickhouse::{BufferedWriter, BufferedWriterConfig, ClickHouseWriter, FlushToken};
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
pub enum IndexOutcome {
    /// Parsed and streamed to the sink, which checkpoints it after the flush lands.
    Indexed,
    /// Archive endpoint signalled no repository content (404 or 5xx); already checkpointed.
    EmptyRepository,
}

impl IndexOutcome {
    pub fn metric_label(&self) -> &'static str {
        match self {
            IndexOutcome::Indexed => "indexed",
            IndexOutcome::EmptyRepository => "empty_repository",
        }
    }
}

/// Tracks one project's buffered rows across every table they span. The pipeline holds a +1
/// sentinel and increments `remaining` per submitted batch; the writer decrements via
/// [`FlushToken`] as each part lands. Whichever decrement reaches zero finalizes the project:
/// stale-clean its prior version then checkpoint, unless any part failed (then the sweep and
/// NATS redelivery retry it). This makes the checkpoint the single durable record, with no
/// watermark to track.
struct ProjectCommit {
    remaining: AtomicUsize,
    failed: AtomicBool,
    checkpoint: CodeIndexingCheckpoint,
    store: Arc<dyn CodeCheckpointStore>,
    cleaner: Arc<dyn StaleDataCleaner>,
    inflight: Arc<AtomicUsize>,
}

impl ProjectCommit {
    fn release(self: Arc<Self>) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        tokio::spawn(async move {
            self.finalize().await;
        });
    }

    async fn finalize(&self) {
        if self.failed.load(Ordering::Acquire) {
            warn!(
                project_id = self.checkpoint.project_id,
                "a buffered write failed; skipping checkpoint so the project is re-indexed",
            );
            return;
        }
        let cp = &self.checkpoint;
        if let Err(error) = self
            .cleaner
            .delete_stale_data(&cp.traversal_path, cp.project_id, &cp.branch, cp.indexed_at)
            .await
        {
            warn!(
                project_id = cp.project_id,
                %error,
                "failed to delete stale data, will retry on next indexing"
            );
        }
        match self.store.set_checkpoint(cp).await {
            Ok(()) => info!(
                project_id = cp.project_id,
                task_id = cp.last_task_id,
                "completed code indexing"
            ),
            Err(e) => warn!(
                project_id = cp.project_id,
                error = %e,
                "failed to checkpoint code indexing; project will be re-indexed",
            ),
        }
    }
}

impl FlushToken for ProjectCommit {
    fn on_flushed(self: Arc<Self>) {
        self.release();
    }
    fn on_failed(self: Arc<Self>) {
        self.failed.store(true, Ordering::Release);
        self.release();
    }
}

impl Drop for ProjectCommit {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct CodeIndexingPipeline {
    resolver: RepositoryResolver,
    writer: BufferedWriter,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    inflight: Arc<AtomicUsize>,
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

impl CodeIndexingPipeline {
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
        let small = pipeline_config.small_indexing_slots;
        let big = pipeline_config.big_indexing_slots;
        let writer = BufferedWriter::spawn(
            writer,
            BufferedWriterConfig {
                channel_capacity: pipeline_config.write_channel_capacity,
                max_rows: pipeline_config.write_slice_rows,
                flush_interval: pipeline_config.write_buffer_age(),
                min_flush_rows: pipeline_config.write_min_flush_rows,
                max_age: pipeline_config.write_max_flush_age(),
                max_concurrent: pipeline_config.write_max_concurrent,
            },
        );
        Self {
            resolver,
            writer,
            checkpoint_store,
            stale_data_cleaner,
            inflight: Arc::new(AtomicUsize::new(0)),
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
        let small = self.pipeline_config.small_indexing_slots;
        let big = self.pipeline_config.big_indexing_slots;
        if self.fetch_concurrency == 0 || small == 0 || big == 0 {
            return None;
        }
        Some(self.fetch_concurrency + small + big)
    }

    /// Hard per-job wall-clock timeout, or `None` when disabled.
    pub fn job_timeout(&self) -> Option<std::time::Duration> {
        self.pipeline_config.job_timeout()
    }

    /// Flush all buffered writes and wait until every project they made durable has been
    /// stale-cleaned and checkpointed. For tests and shutdown; steady state relies on the
    /// size/age flush and the per-project commit tokens.
    pub async fn flush(&self) -> Result<(), HandlerError> {
        self.writer
            .flush()
            .await
            .map_err(|e| HandlerError::Processing(format!("flush failed: {e}")))?;
        while self.inflight.load(Ordering::Acquire) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
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
                // No rows to flush, so checkpoint directly rather than through the sink.
                self.checkpoint_store
                    .set_checkpoint(&CodeIndexingCheckpoint {
                        traversal_path: request.traversal_path.clone(),
                        project_id: request.project_id,
                        branch: request.branch.clone(),
                        last_task_id: request.task_id,
                        last_commit: None,
                        indexed_at: Utc::now(),
                    })
                    .await
                    .map_err(|e| HandlerError::Processing(format!("failed to set checkpoint: {e}")))
                    .record_error_stage(&self.metrics, "checkpoint")?;
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

        let extraction_guard = self.resolver.extraction_guard(repository.path.clone());

        // Release fetch slot before waiting for the indexing slot. This is
        // the pipelining point: freeing the fetch slot lets another handler
        // start its Gitaly download while we wait for an indexing slot.
        drop(_fetch_slot);

        // A reserved big lane keeps a flood of small repos from starving monorepos.
        let parseable = code_graph::v2::inventory::parseable_file_count(&repository.file_inventory);
        let lane = if parseable <= self.small_repo_max_files {
            &self.small_indexing_slots
        } else {
            &self.big_indexing_slots
        };
        let _indexing_slot = acquire(lane, "indexing").await?;

        context.progress.notify_in_progress().await;

        let indexed_at = Utc::now();
        let indexing_result = self
            .run_indexing(context, request, &repository, indexed_at, observer, cancel)
            .await;

        if let Err(error) = self.resolver.cleanup(&repository.path).await {
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
        extraction_guard.disarm();

        let commit = indexing_result?;

        // Drop the pipeline's sentinel hold. If every submitted batch has already flushed, this
        // is the decrement that finalizes; otherwise the writer's last flush will.
        commit.release();

        Ok(IndexOutcome::Indexed)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "indexing stage threads its collaborators explicitly; a params struct would just move the arity"
    )]
    async fn run_indexing(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        repository: &CachedRepository,
        indexed_at: DateTime<Utc>,
        observer: &mut dyn IndexingObserver,
        cancel: CancellationToken,
    ) -> Result<Arc<ProjectCommit>, HandlerError> {
        let indexing_start = Instant::now();
        let config = self.build_pipeline_config(context, cancel);
        let (result, commit) = self
            .build_code_graph(request, repository, indexed_at, config)
            .await?;

        context.progress.notify_in_progress().await;
        self.metrics
            .record_indexing_duration(indexing_start.elapsed());

        self.record_indexing_results(&result, observer, request, indexing_start);

        if let Some(error) = result.errors.iter().find(|error| error.fatal) {
            // Some batches may already have flushed; mark the commit failed and drop the
            // sentinel so it never checkpoints and the project is re-indexed.
            commit.failed.store(true, Ordering::Release);
            commit.release();
            return Err(HandlerError::dead_letter(format!(
                "fatal code indexing pipeline error during {} for {}: {}",
                error.stage, error.file_path, error.error
            )));
        }

        context.progress.notify_in_progress().await;
        Ok(commit)
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

    /// Parse the repository, stream its batches to the writer under one project commit, return it.
    async fn build_code_graph(
        &self,
        request: &IndexingRequest,
        repository: &CachedRepository,
        indexed_at: DateTime<Utc>,
        config: PipelineConfig,
    ) -> Result<(code_graph::v2::PipelineResult, Arc<ProjectCommit>), HandlerError> {
        let tracer = code_graph::v2::trace::Tracer::new(false);
        let envelope = IndexerEnvelope::new(
            request.traversal_path.clone(),
            request.project_id,
            request.branch.clone(),
            request.commit_sha.as_deref().unwrap_or("").to_string(),
            indexed_at,
        );

        let converter: Arc<dyn code_graph::v2::GraphConverter> = Arc::new(IndexerConverter::new(
            envelope,
            &self.ontology,
            self.table_names.clone(),
        ));

        // remaining starts at 1: a sentinel the pipeline releases after the parse finishes, so
        // the commit can't finalize mid-stream even if every flushed part drains first.
        self.inflight.fetch_add(1, Ordering::AcqRel);
        let commit = Arc::new(ProjectCommit {
            remaining: AtomicUsize::new(1),
            failed: AtomicBool::new(false),
            checkpoint: CodeIndexingCheckpoint {
                traversal_path: request.traversal_path.clone(),
                project_id: request.project_id,
                branch: request.branch.clone(),
                last_task_id: request.task_id,
                last_commit: request.commit_sha.clone(),
                indexed_at,
            },
            store: self.checkpoint_store.clone(),
            cleaner: self.stale_data_cleaner.clone(),
            inflight: self.inflight.clone(),
        });

        let writer = self.writer.clone();
        let max_rows = self.pipeline_config.write_slice_rows.max(1);
        let token = commit.clone();
        let on_batch: Arc<code_graph::v2::OnBatch> = Arc::new(
            move |table: &str, batch: arrow::record_batch::RecordBatch| {
                // The converter emits zero-row node tables for ParsedOnly graphs; skip them so we
                // never buffer an empty part or add a phantom commit token.
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let table = table.to_string();
                let mut offset = 0;
                while offset < batch.num_rows() {
                    let len = (batch.num_rows() - offset).min(max_rows);
                    token.remaining.fetch_add(1, Ordering::AcqRel);
                    let token: Arc<dyn FlushToken> = token.clone();
                    writer
                        .submit(table.clone(), batch.slice(offset, len), token)
                        .map_err(|e| code_graph::v2::SinkError(e.to_string()))?;
                    offset += len;
                }
                Ok(())
            },
        );

        let code_graph_start = Instant::now();
        let repo_dir = repository.path.clone();
        let file_inventory = repository.file_inventory.clone();
        let stream_reasons = repository.stream_reasons.clone();
        let parsed = tokio::task::spawn_blocking(move || {
            Pipeline::run_with_tracer(
                &repo_dir,
                file_inventory,
                config,
                &stream_reasons,
                tracer,
                converter,
                on_batch,
            )
        })
        .await;
        let result = match parsed {
            Ok(result) => result,
            Err(e) => {
                // The parse thread panicked; drop the sentinel so the leaked commit can finalize
                // (as failed) instead of pinning the inflight count forever.
                commit.failed.store(true, Ordering::Release);
                commit.release();
                return Err(HandlerError::Processing(format!(
                    "pipeline thread panicked: {e}"
                )));
            }
        };
        info!(
            duration_ms = code_graph_start.elapsed().as_millis() as u64,
            "code-graph building completed"
        );

        Ok((result, commit))
    }

    fn record_indexing_results(
        &self,
        result: &code_graph::v2::PipelineResult,
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

        // Writes are deferred to the sink, so row count comes from the parse result, not writes.
        let rows_written = (result.stats.directories_indexed
            + result.stats.files_indexed
            + result.stats.definitions_count
            + result.stats.imports_count
            + result.stats.edges_count) as u64;
        observer.record_graph_write("code_graph", rows_written, 0);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::checkpoint::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use chrono::Utc;
    use std::time::Duration;

    fn commit(
        store: Arc<dyn CodeCheckpointStore>,
        cleaner: Arc<dyn StaleDataCleaner>,
        inflight: Arc<AtomicUsize>,
        batches: usize,
    ) -> Arc<ProjectCommit> {
        inflight.fetch_add(1, Ordering::AcqRel);
        Arc::new(ProjectCommit {
            remaining: AtomicUsize::new(1 + batches),
            failed: AtomicBool::new(false),
            checkpoint: CodeIndexingCheckpoint {
                traversal_path: "1/7/".into(),
                project_id: 7,
                branch: "main".into(),
                last_task_id: 7,
                last_commit: None,
                indexed_at: Utc::now(),
            },
            store,
            cleaner,
            inflight,
        })
    }

    async fn settle(inflight: &AtomicUsize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while inflight.load(Ordering::Acquire) > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("commit finalize stuck");
    }

    #[tokio::test]
    async fn checkpoints_once_every_batch_and_the_sentinel_release() {
        let store = Arc::new(MockCodeCheckpointStore::new());
        let cleaner = Arc::new(MockStaleDataCleaner::default());
        let inflight = Arc::new(AtomicUsize::new(0));
        let commit = commit(store.clone(), cleaner, inflight.clone(), 2);

        commit.clone().release();
        commit.clone().release();
        assert!(
            store
                .get_checkpoint("1/7/", 7, "main")
                .await
                .unwrap()
                .is_none(),
            "two batches drained but the sentinel still holds the commit",
        );

        commit.release();
        settle(&inflight).await;
        assert!(
            store
                .get_checkpoint("1/7/", 7, "main")
                .await
                .unwrap()
                .is_some(),
            "sentinel release was the last decrement and must checkpoint",
        );
    }

    #[tokio::test]
    async fn a_failed_batch_skips_the_checkpoint() {
        let store = Arc::new(MockCodeCheckpointStore::new());
        let cleaner = Arc::new(MockStaleDataCleaner::default());
        let inflight = Arc::new(AtomicUsize::new(0));
        let commit = commit(store.clone(), cleaner, inflight.clone(), 1);

        commit.failed.store(true, Ordering::Release);
        commit.clone().release();
        commit.release();
        settle(&inflight).await;
        assert!(
            store
                .get_checkpoint("1/7/", 7, "main")
                .await
                .unwrap()
                .is_none(),
            "a failed flush must leave the project un-checkpointed for re-indexing",
        );
    }

    #[tokio::test]
    async fn abandoned_sentinel_drains_inflight_without_checkpointing() {
        let store = Arc::new(MockCodeCheckpointStore::new());
        let cleaner = Arc::new(MockStaleDataCleaner::default());
        let inflight = Arc::new(AtomicUsize::new(0));
        let commit = commit(store.clone(), cleaner, inflight.clone(), 2);

        // Dropping `commit` without a third `release()` mimics a run future dropped on timeout:
        // the sentinel is never released, yet the slot must still be reclaimed.
        commit.clone().release();
        commit.clone().release();
        drop(commit);

        settle(&inflight).await;
        assert!(
            store
                .get_checkpoint("1/7/", 7, "main")
                .await
                .unwrap()
                .is_none(),
            "an abandoned sentinel must not checkpoint; the sweep re-indexes the project",
        );
    }
}
