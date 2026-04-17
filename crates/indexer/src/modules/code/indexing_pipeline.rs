use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use code_graph::linker::analysis::types::GraphData;
use code_graph::linker::indexer::{IndexingConfig, RepositoryIndexer};
use code_graph::linker::loading::DirectoryFileSource;
use tracing::{debug, info, warn};

use super::arrow_converter::ArrowConverter;
use super::checkpoint_store::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::config::CodeTableNames;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository::RepositoryResolver;
use super::stale_data_cleaner::StaleDataCleaner;
use crate::handler::{HandlerContext, HandlerError};
use crate::progress::CodeProgressWriter;

pub struct IndexingRequest {
    pub project_id: i64,
    pub branch: String,
    pub traversal_path: String,
    pub task_id: i64,
    pub commit_sha: Option<String>,
}

pub struct CodeIndexingPipeline {
    resolver: RepositoryResolver,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    stale_data_cleaner: Arc<dyn StaleDataCleaner>,
    metrics: CodeMetrics,
    table_names: Arc<CodeTableNames>,
    code_progress: Option<Arc<CodeProgressWriter>>,
}

impl CodeIndexingPipeline {
    pub fn new(
        resolver: RepositoryResolver,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        stale_data_cleaner: Arc<dyn StaleDataCleaner>,
        metrics: CodeMetrics,
        table_names: Arc<CodeTableNames>,
        code_progress: Option<Arc<CodeProgressWriter>>,
    ) -> Self {
        Self {
            resolver,
            checkpoint_store,
            stale_data_cleaner,
            metrics,
            table_names,
            code_progress,
        }
    }

    pub async fn index_project(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
    ) -> Result<(), HandlerError> {
        let fetch_start = Instant::now();
        let repo_path = self
            .resolver
            .resolve(
                request.project_id,
                &request.branch,
                request.commit_sha.as_deref(),
            )
            .await
            .record_error_stage(&self.metrics, "repository_fetch")?;
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

        self.write_progress(context, request, indexed_at).await;

        Ok(())
    }

    async fn write_progress(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
        indexed_at: DateTime<Utc>,
    ) {
        let Some(progress) = self.code_progress.as_ref() else {
            return;
        };
        let commit = request.commit_sha.as_deref().unwrap_or("");

        if let Err(e) = progress
            .write_project_progress(
                context.nats.as_ref(),
                request.project_id,
                &request.traversal_path,
                &request.branch,
                commit,
                indexed_at,
            )
            .await
        {
            warn!(
                project_id = request.project_id,
                branch = %request.branch,
                error = %e,
                "failed to write code project progress to KV (non-fatal)"
            );
        }

        let Some(namespace_id) = namespace_id_from_traversal_path(&request.traversal_path) else {
            debug!(
                traversal_path = %request.traversal_path,
                "unable to derive namespace_id from traversal path, skipping namespace meta update"
            );
            return;
        };
        let namespace_traversal_path = namespace_traversal_prefix(&request.traversal_path)
            .unwrap_or_else(|| request.traversal_path.clone());

        if let Err(e) = progress
            .update_namespace_code_meta(
                context.nats.as_ref(),
                namespace_id,
                &namespace_traversal_path,
                indexed_at,
            )
            .await
        {
            warn!(
                namespace_id,
                error = %e,
                "failed to update namespace code meta (non-fatal)"
            );
        }
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
            commit_sha,
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

    #[allow(clippy::too_many_arguments)]
    async fn write_graph_data(
        &self,
        ctx: &HandlerContext,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        traversal_path: &str,
        indexed_at: DateTime<Utc>,
        graph_data: &GraphData,
    ) -> Result<(), HandlerError> {
        let converter = ArrowConverter::new(
            traversal_path.to_string(),
            project_id,
            branch.to_string(),
            commit_sha.to_string(),
            indexed_at,
        );

        let converted = converter
            .convert_all(graph_data)
            .map_err(|e| HandlerError::Processing(format!("arrow conversion failed: {e}")))
            .record_error_stage(&self.metrics, "arrow_conversion")?;

        self.write_batch(ctx, &self.table_names.branch, &converted.branch)
            .await?;
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
        // TODO(multi-edge-tables, #454): when gl_code_edge is declared, split converted.edges
        // by destination table if code edges span multiple tables. Currently all code
        // edges share one table, so a single write_batch is correct.
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

/// Returns the namespace id from a traversal path of form
/// `{org}/{namespace_id}/...`.
fn namespace_id_from_traversal_path(traversal_path: &str) -> Option<i64> {
    traversal_path
        .trim_start_matches('/')
        .split('/')
        .nth(1)
        .and_then(|s| s.parse::<i64>().ok())
}

/// Returns the traversal path truncated to `{org}/{namespace_id}/`.
fn namespace_traversal_prefix(traversal_path: &str) -> Option<String> {
    let mut parts = traversal_path.trim_end_matches('/').split('/');
    let org = parts.next()?;
    let ns = parts.next()?;
    if org.is_empty() || ns.is_empty() {
        return None;
    }
    Some(format!("{org}/{ns}/"))
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn namespace_id_from_traversal_path_parses_second_segment() {
        assert_eq!(namespace_id_from_traversal_path("1/9970/proj/"), Some(9970));
        assert_eq!(
            namespace_id_from_traversal_path("1/9970/55154808/95754906/"),
            Some(9970)
        );
    }

    #[test]
    fn namespace_id_from_traversal_path_returns_none_when_malformed() {
        assert_eq!(namespace_id_from_traversal_path(""), None);
        assert_eq!(namespace_id_from_traversal_path("1"), None);
        assert_eq!(namespace_id_from_traversal_path("1/abc/"), None);
    }

    #[test]
    fn namespace_traversal_prefix_takes_first_two_segments() {
        assert_eq!(
            namespace_traversal_prefix("1/9970/proj/"),
            Some("1/9970/".to_string())
        );
        assert_eq!(
            namespace_traversal_prefix("1/9970/55154808/95754906/"),
            Some("1/9970/".to_string())
        );
    }

    #[test]
    fn namespace_traversal_prefix_returns_none_when_malformed() {
        assert!(namespace_traversal_prefix("").is_none());
        assert!(namespace_traversal_prefix("1").is_none());
        assert!(namespace_traversal_prefix("1/").is_none());
    }
}
