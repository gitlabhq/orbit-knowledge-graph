use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::analysis::types::GraphData;
use code_graph::indexer::{IndexingConfig, RepositoryIndexer};
use code_graph::loading::DirectoryFileSource;
use tempfile::TempDir;
use tracing::{debug, info, warn};

use super::archive;
use super::arrow_converter::ArrowConverter;
use super::checkpoint_store::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::config::CodeTableNames;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository_service::RepositoryService;
use super::stale_data_cleaner::StaleDataCleaner;
use crate::handler::{HandlerContext, HandlerError};

pub struct IndexingRequest {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: String,
    pub task_id: i64,
    pub commit_sha: Option<String>,
}

pub struct CodeIndexingPipeline {
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    metrics: CodeMetrics,
    table_names: Arc<CodeTableNames>,
}

impl CodeIndexingPipeline {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
        table_names: Arc<CodeTableNames>,
    ) -> Self {
        Self {
            repository_service,
            checkpoint_store,
            stale_data_cleaner,
            metrics,
            table_names,
        }
    }

    pub async fn index_project(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
    ) -> Result<(), HandlerError> {
        let temp_dir = TempDir::new()
            .map_err(|e| HandlerError::Processing(format!("failed to create temp dir: {e}")))?;

        let fetch_start = Instant::now();
        let ref_name = request.commit_sha.as_deref().unwrap_or(&request.branch);
        let archive_bytes = self
            .repository_service
            .download_archive(request.project_id, ref_name)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download archive: {e}")))
            .record_error_stage(&self.metrics, "repository_fetch")?;
        self.metrics
            .repository_fetch_duration
            .record(fetch_start.elapsed().as_secs_f64(), &[]);

        context.progress.notify_in_progress().await;

        let extract_start = Instant::now();
        archive::extract_tar_gz(&archive_bytes, temp_dir.path())
            .map_err(|e| HandlerError::Processing(format!("failed to extract archive: {e}")))
            .record_error_stage(&self.metrics, "repository_extract")?;
        self.metrics
            .repository_extract_duration
            .record(extract_start.elapsed().as_secs_f64(), &[]);

        context.progress.notify_in_progress().await;

        let indexed_at = Utc::now();
        self.run_indexing(
            context,
            request.project_id,
            &request.branch,
            &request.traversal_path,
            indexed_at,
            temp_dir.path(),
        )
        .await?;

        self.set_checkpoint(
            &request.traversal_path,
            request.project_id,
            &request.branch,
            request.task_id,
            request.commit_sha.as_deref(),
            indexed_at,
        )
        .await
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

    async fn run_indexing(
        &self,
        context: &HandlerContext,
        project_id: i64,
        branch: &str,
        traversal_path: &str,
        indexed_at: DateTime<Utc>,
        repo_dir: &Path,
    ) -> Result<(), HandlerError> {
        let repo_path = repo_dir.to_string_lossy().to_string();
        let indexer = RepositoryIndexer::with_graph_identity(
            format!("project-{project_id}"),
            repo_path.clone(),
            project_id,
            branch.to_string(),
        );
        let file_source = DirectoryFileSource::new(repo_path);

        let indexing_start = Instant::now();
        let result = indexer
            .index_files(file_source, &IndexingConfig::default())
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to index code: {e}")))
            .record_error_stage(&self.metrics, "indexing")?;
        self.metrics
            .indexing_duration
            .record(indexing_start.elapsed().as_secs_f64(), &[]);

        self.metrics
            .record_files_processed(result.skipped_files.len() as u64, "skipped");
        self.metrics
            .record_files_processed(result.errored_files.len() as u64, "errored");

        if !result.errored_files.is_empty() {
            warn!(
                project_id,
                branch = %branch,
                count = result.errored_files.len(),
                "some files failed to parse during code indexing"
            );
        }

        context.progress.notify_in_progress().await;

        let Some(graph_data) = result.graph_data else {
            debug!(project_id, branch = %branch, "indexing produced no graph data, skipping write");
            return Ok(());
        };

        self.metrics
            .record_files_processed(graph_data.file_nodes.len() as u64, "parsed");
        self.metrics.record_node_counts(&graph_data);

        self.write_graph_data(
            context,
            project_id,
            branch,
            traversal_path,
            indexed_at,
            &graph_data,
        )
        .await?;

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

    async fn write_graph_data(
        &self,
        ctx: &HandlerContext,
        project_id: i64,
        branch: &str,
        traversal_path: &str,
        indexed_at: DateTime<Utc>,
        graph_data: &GraphData,
    ) -> Result<(), HandlerError> {
        let converter = ArrowConverter::new(
            traversal_path.to_string(),
            project_id,
            branch.to_string(),
            indexed_at,
        );

        let converted = converter
            .convert_all(graph_data)
            .map_err(|e| HandlerError::Processing(format!("arrow conversion failed: {e}")))
            .record_error_stage(&self.metrics, "arrow_conversion")?;

        self.write_batch(ctx, &self.table_names.directory, &converted.directories)
            .await?;
        self.write_batch(ctx, &self.table_names.file, &converted.files)
            .await?;
        self.write_batch(ctx, &self.table_names.definition, &converted.definitions)
            .await?;
        self.write_batch(
            ctx,
            &self.table_names.imported_symbol,
            &converted.imported_symbols,
        )
        .await?;
        self.write_batch(ctx, &self.table_names.edge, &converted.edges)
            .await?;

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
