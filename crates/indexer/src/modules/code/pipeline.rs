use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::v2::{Pipeline, PipelineConfig};
use gkg_server_config::CodeIndexingPipelineConfig;
use tracing::{info, warn};

use super::arrow_converter::{self, IndexerEnvelope};
use super::checkpoint::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::config::CodeTableNames;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository::{RepositoryResolver, ResolveError};
use super::stale_data_cleaner::StaleDataCleaner;
use crate::handler::{HandlerContext, HandlerError};
use opentelemetry::KeyValue;

pub struct IndexingRequest {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: String,
    pub task_id: i64,
    pub commit_sha: Option<String>,
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

pub struct CodeIndexingPipeline {
    resolver: RepositoryResolver,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    metrics: CodeMetrics,
    table_names: Arc<CodeTableNames>,
    ontology: Arc<ontology::Ontology>,
    pipeline_config: CodeIndexingPipelineConfig,
}

impl CodeIndexingPipeline {
    pub fn new(
        resolver: RepositoryResolver,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
        table_names: Arc<CodeTableNames>,
        ontology: Arc<ontology::Ontology>,
        pipeline_config: CodeIndexingPipelineConfig,
    ) -> Self {
        Self {
            resolver,
            checkpoint_store,
            stale_data_cleaner,
            metrics,
            table_names,
            ontology,
            pipeline_config,
        }
    }

    pub async fn index_project(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
    ) -> Result<IndexOutcome, HandlerError> {
        let fetch_start = Instant::now();
        let repo_path = match self
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
                self.metrics
                    .repository_fetch_duration
                    .record(fetch_start.elapsed().as_secs_f64(), &[]);
                self.set_checkpoint(
                    &request.traversal_path,
                    request.project_id,
                    &request.branch,
                    request.task_id,
                    None,
                    Utc::now(),
                )
                .await?;
                return Ok(IndexOutcome::EmptyRepository);
            }
            Err(ResolveError::Other(err)) => {
                self.metrics
                    .errors
                    .add(1, &[KeyValue::new("stage", "repository_fetch")]);
                return Err(err);
            }
        };
        self.metrics
            .repository_fetch_duration
            .record(fetch_start.elapsed().as_secs_f64(), &[]);

        context.progress.notify_in_progress().await;

        let indexed_at = Utc::now();
        let commit_sha = request.commit_sha.as_deref().unwrap_or("");
        let indexing_result = self
            .run_indexing(
                context,
                request.project_id,
                &request.branch,
                commit_sha,
                &request.traversal_path,
                indexed_at,
                &repo_path,
            )
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

        self.set_checkpoint(
            &request.traversal_path,
            request.project_id,
            &request.branch,
            request.task_id,
            request.commit_sha.as_deref(),
            indexed_at,
        )
        .await?;

        Ok(IndexOutcome::Indexed)
    }

    async fn set_checkpoint(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
        task_id: i64,
        last_commit: Option<&str>,
        indexed_at: DateTime<Utc>,
    ) -> Result<(), HandlerError> {
        let checkpoint = CodeIndexingCheckpoint {
            traversal_path: traversal_path.to_string(),
            project_id,
            branch: branch.to_string(),
            last_task_id: task_id,
            last_commit: last_commit.map(|s| s.to_string()),
            indexed_at,
        };

        self.checkpoint_store
            .set_checkpoint(&checkpoint)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to set checkpoint: {e}")))
            .record_error_stage(&self.metrics, "checkpoint")?;

        info!(
            project_id,
            branch = %branch,
            commit = ?last_commit,
            task_id,
            "completed code indexing"
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_indexing(
        &self,
        context: &HandlerContext,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        traversal_path: &str,
        indexed_at: DateTime<Utc>,
        repo_dir: &Path,
    ) -> Result<(), HandlerError> {
        let indexing_start = Instant::now();
        let per_file_timeout = if self.pipeline_config.per_file_timeout_ms > 0 {
            Some(std::time::Duration::from_millis(
                self.pipeline_config.per_file_timeout_ms,
            ))
        } else {
            None
        };
        let config = PipelineConfig {
            max_file_size: self.pipeline_config.max_file_size_bytes,
            max_files: self.pipeline_config.max_files,
            respect_gitignore: self.pipeline_config.respect_gitignore,
            worker_threads: self.pipeline_config.worker_threads,
            max_concurrent_languages: self.pipeline_config.max_concurrent_languages,
            per_file_timeout,
            ..Default::default()
        };
        let tracer = code_graph::v2::trace::Tracer::new(false);

        let envelope = IndexerEnvelope::new(
            traversal_path.to_string(),
            project_id,
            branch.to_string(),
            commit_sha.to_string(),
            indexed_at,
        );

        let converter: Arc<dyn code_graph::v2::GraphConverter> =
            Arc::new(arrow_converter::IndexerConverter {
                envelope,
                ontology: self.ontology.clone(),
                table_names: self.table_names.clone(),
            });
        let sink: Arc<dyn code_graph::v2::BatchSink> =
            Arc::new(arrow_converter::ClickHouseSink::new(
                context.destination.clone(),
                tokio::runtime::Handle::current(),
            ));

        // Run the synchronous pipeline on a blocking thread so the tokio
        // worker is freed. The writer thread inside the pipeline calls
        // runtime.block_on() which would deadlock a single-threaded
        // tokio runtime if we blocked the worker here.
        let repo_dir_owned = repo_dir.to_path_buf();
        let result = tokio::task::spawn_blocking(move || {
            Pipeline::run_with_tracer(&repo_dir_owned, config, tracer, converter, sink)
        })
        .await
        .map_err(|e| HandlerError::Processing(format!("pipeline thread panicked: {e}")))?;
        self.metrics
            .indexing_duration
            .record(indexing_start.elapsed().as_secs_f64(), &[]);

        // The pipeline orchestrator increments `stats.files_parsed` by
        // the full batch size for each language whose `process_files`
        // returns `Ok(())` — including files that recorded a per-file
        // skip or fault. Subtract those out here so the reported parsed
        // count is the truly successful one.
        let parsed_count = result
            .stats
            .files_parsed
            .saturating_sub(result.skipped.len() + result.faults.len());
        let skipped_count = result.stats.files_skipped + result.skipped.len();

        self.metrics
            .record_files_processed(result.stats.files_discovered as u64, "discovered");
        self.metrics
            .record_repository_source_size(result.stats.bytes_discovered);
        self.metrics
            .record_files_processed(parsed_count as u64, "parsed");
        self.metrics
            .record_files_processed(skipped_count as u64, "skipped");
        self.metrics
            .record_nodes_indexed(result.stats.definitions_count as u64, "definition");
        self.metrics
            .record_nodes_indexed(result.stats.imports_count as u64, "imported_symbol");
        self.metrics
            .record_nodes_indexed(result.stats.edges_count as u64, "edge");

        for skipped in &result.skipped {
            self.metrics
                .record_file_skipped(skipped.kind.as_metric_label());
        }

        for fault in &result.faults {
            self.metrics.record_file_fault(fault.kind.as_metric_label());
        }
        if !result.faults.is_empty() {
            warn!(
                project_id,
                branch = %branch,
                count = result.faults.len(),
                "files faulted during code indexing"
            );
            self.metrics
                .record_files_processed(result.faults.len() as u64, "errored");
        }

        if let Some(error) = result.errors.iter().find(|error| error.fatal) {
            self.metrics
                .errors
                .add(1, &[KeyValue::new("stage", error.stage)]);
            return Err(HandlerError::Permanent {
                message: format!(
                    "fatal code indexing pipeline error during {} for {}: {}",
                    error.stage, error.file_path, error.error
                ),
                action: crate::handler::PermanentAction::DeadLetter,
            });
        }

        context.progress.notify_in_progress().await;

        if let Err(error) = self
            .stale_data_cleaner
            .delete_stale_data(traversal_path, project_id, branch, indexed_at)
            .await
        {
            warn!(
                project_id,
                branch = %branch,
                %error,
                "failed to delete stale data, will retry on next indexing"
            );
        }

        Ok(())
    }
}
