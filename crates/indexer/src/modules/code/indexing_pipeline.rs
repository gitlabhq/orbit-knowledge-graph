use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::v2::{Pipeline, PipelineConfig};
use tracing::{debug, info, warn};

use super::arrow_converter::{self, IndexerEnvelope};
use super::checkpoint_store::{CodeCheckpointStore, CodeIndexingCheckpoint};
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
}

impl CodeIndexingPipeline {
    pub fn new(
        resolver: RepositoryResolver,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
        table_names: Arc<CodeTableNames>,
        ontology: Arc<ontology::Ontology>,
    ) -> Self {
        Self {
            resolver,
            checkpoint_store,
            stale_data_cleaner,
            metrics,
            table_names,
            ontology,
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
        let config = PipelineConfig {
            max_file_size: 5_000_000,
            ..Default::default()
        };
        let tracer = code_graph::v2::trace::Tracer::new(false);
        let result = Pipeline::run_with_tracer(repo_dir, config, tracer);
        self.metrics
            .indexing_duration
            .record(indexing_start.elapsed().as_secs_f64(), &[]);

        self.metrics
            .record_files_processed(result.stats.files_skipped as u64, "skipped");

        if !result.errors.is_empty() {
            warn!(
                project_id,
                branch = %branch,
                count = result.errors.len(),
                "some files failed to parse during code indexing"
            );
            self.metrics
                .record_files_processed(result.errors.len() as u64, "errored");
        }

        context.progress.notify_in_progress().await;

        if result.graphs.is_empty() {
            debug!(project_id, branch = %branch, "indexing produced no graph data, skipping write");
            return Ok(());
        }

        let envelope = IndexerEnvelope::new(
            traversal_path.to_string(),
            project_id,
            branch.to_string(),
            commit_sha.to_string(),
            indexed_at,
        );

        for graph in &result.graphs {
            self.metrics
                .record_files_processed(graph.files().count() as u64, "parsed");

            let converted = arrow_converter::convert_code_graph(graph, &envelope, &self.ontology)
                .map_err(|e| HandlerError::Processing(format!("arrow conversion failed: {e}")))
                .record_error_stage(&self.metrics, "arrow_conversion")?;

            self.write_data(context, &converted).await?;
        }

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

    async fn write_data(
        &self,
        ctx: &HandlerContext,
        data: &arrow_converter::ConvertedGraphData,
    ) -> Result<(), HandlerError> {
        self.write_batch(ctx, &self.table_names.branch, &data.branch)
            .await?;
        self.write_batch(ctx, &self.table_names.directory, &data.directories)
            .await?;
        self.write_batch(ctx, &self.table_names.file, &data.files)
            .await?;
        self.write_batch(ctx, &self.table_names.definition, &data.definitions)
            .await?;
        self.write_batch(
            ctx,
            &self.table_names.imported_symbol,
            &data.imported_symbols,
        )
        .await?;
        self.write_edge_batches(ctx, &data.edges).await?;
        Ok(())
    }

    /// Split an edges RecordBatch by `relationship_kind` and write each
    /// group to the ontology-resolved table.
    async fn write_edge_batches(
        &self,
        ctx: &HandlerContext,
        edges: &arrow::record_batch::RecordBatch,
    ) -> Result<(), HandlerError> {
        use arrow::array::AsArray;
        use std::collections::HashMap;

        if edges.num_rows() == 0 {
            return Ok(());
        }

        let rel_col = edges
            .column_by_name("relationship_kind")
            .expect("edges batch must have relationship_kind column");
        let rel_array = rel_col.as_string::<i32>();

        // Group row indices by destination table.
        let mut table_rows: HashMap<&str, Vec<u32>> = HashMap::new();
        for i in 0..edges.num_rows() {
            let rel_kind = rel_array.value(i);
            let table = self.table_names.edge_table_for(rel_kind);
            table_rows.entry(table).or_default().push(i as u32);
        }

        for (table, indices) in &table_rows {
            let idx_array = arrow::array::UInt32Array::from(indices.clone());
            let batch = arrow::compute::take_record_batch(edges, &idx_array)
                .map_err(|e| HandlerError::Processing(format!("edge batch split failed: {e}")))?;
            self.write_batch(ctx, table, &batch).await?;
        }

        Ok(())
    }

    async fn write_batch(
        &self,
        ctx: &HandlerContext,
        table: &str,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<(), HandlerError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let writer = ctx
            .destination
            .new_batch_writer(table)
            .await
            .map_err(|e| HandlerError::Processing(format!("writer creation failed: {e}")))
            .record_error_stage(&self.metrics, "write")?;

        writer
            .write_batch(std::slice::from_ref(batch))
            .await
            .map_err(|e| HandlerError::Processing(format!("write to {table} failed: {e}")))
            .record_error_stage(&self.metrics, "write")
    }
}
