use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::analysis::types::GraphData;
use code_graph::indexer::{IndexingConfig, RepositoryIndexer};
use code_graph::loading::DirectoryFileSource;
use ontology::EDGE_TABLE;
use tempfile::TempDir;
use tracing::{debug, info, warn};

use super::arrow_converter::ArrowConverter;
use super::config::tables;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository_service::RepositoryService;
use super::stale_data_cleaner::StaleDataCleaner;
use super::watermark_store::{CodeIndexingWatermark, CodeWatermarkStore};
use crate::module::{HandlerContext, HandlerError};
use gitlab_client::RepositoryInfo;

pub struct IndexingRequest {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: String,
    pub event_id: i64,
    pub commit_sha: String,
    pub repository: RepositoryInfo,
}

pub struct CodeIndexingPipeline {
    repository_service: Arc<dyn RepositoryService>,
    watermark_store: Arc<dyn CodeWatermarkStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    metrics: CodeMetrics,
}

impl CodeIndexingPipeline {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        watermark_store: Arc<dyn CodeWatermarkStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            repository_service,
            watermark_store,
            stale_data_cleaner,
            metrics,
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
        self.repository_service
            .extract_repository(&request.repository, temp_dir.path(), &request.commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to extract repository: {e}")))
            .record_error_stage(&self.metrics, "repository_extract")?;
        self.metrics
            .repository_fetch_duration
            .record(fetch_start.elapsed().as_secs_f64(), &[]);

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

        self.set_watermark(
            request.project_id,
            &request.branch,
            request.event_id,
            &request.commit_sha,
            indexed_at,
        )
        .await
    }

    async fn set_watermark(
        &self,
        project_id: i64,
        branch: &str,
        event_id: i64,
        commit_sha: &str,
        indexed_at: DateTime<Utc>,
    ) -> Result<(), HandlerError> {
        let watermark = CodeIndexingWatermark {
            project_id,
            branch: branch.to_string(),
            last_event_id: event_id,
            last_commit: commit_sha.to_string(),
            indexed_at,
        };

        self.watermark_store
            .set_watermark(&watermark)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to set watermark: {e}")))
            .record_error_stage(&self.metrics, "watermark")?;

        info!(
            project_id,
            branch = %branch,
            commit = %commit_sha,
            event_id,
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
        let indexer = RepositoryIndexer::new(format!("project-{project_id}"), repo_path.clone());
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

        let Some(mut graph_data) = result.graph_data else {
            debug!(project_id, branch = %branch, "indexing produced no graph data, skipping write");
            return Ok(());
        };

        // TODO: This should be done on construction of the GraphData struct.
        graph_data.assign_node_ids(project_id, branch);

        self.metrics
            .record_files_processed(graph_data.file_nodes.len() as u64, "parsed");
        self.metrics.record_node_counts(&graph_data);

        let write_start = Instant::now();
        self.write_graph_data(
            context,
            project_id,
            branch,
            traversal_path,
            indexed_at,
            &graph_data,
        )
        .await?;
        self.metrics
            .write_duration
            .record(write_start.elapsed().as_secs_f64(), &[]);

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

        self.write_batch(ctx, tables::GL_DIRECTORY, &converted.directories)
            .await?;
        self.write_batch(ctx, tables::GL_FILE, &converted.files)
            .await?;
        self.write_batch(ctx, tables::GL_DEFINITION, &converted.definitions)
            .await?;
        self.write_batch(ctx, tables::GL_IMPORTED_SYMBOL, &converted.imported_symbols)
            .await?;
        self.write_batch(ctx, EDGE_TABLE, &converted.edges).await?;

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
