use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use etl_engine::destination::BatchWriter;
use etl_engine::module::HandlerError;
use futures::StreamExt;
use serde::Serialize;

use super::datalake::{DatalakeQuery, ToQueryParams};
use super::namespace_handler::{NamespacedEntityContext, NamespacedEntityHandler};
use super::watermark_store::TIMESTAMP_FORMAT;

const PROJECT_QUERY: &str = r#"
SELECT
    project.id AS id,
    project.name AS name,
    project.description AS description,
    project.visibility_level AS visibility_level,
    project.path AS path,
    project.namespace_id AS namespace_id,
    project.creator_id AS creator_id,
    project.created_at AS created_at,
    project.updated_at AS updated_at,
    project.archived AS archived,
    project.star_count AS star_count,
    project.last_activity_at AS last_activity_at,
    traversal_paths.traversal_path AS traversal_path,
    project._siphon_deleted AS deleted
FROM siphon_projects project
INNER JOIN project_namespace_traversal_paths traversal_paths ON project.id = traversal_paths.id
WHERE project.id IN (SELECT id FROM project_namespace_traversal_paths WHERE startsWith(traversal_path, {traversal_path:String}))
    AND project._siphon_replicated_at > {last_watermark:String}
    AND project._siphon_replicated_at <= {watermark:String}
"#;

const PROJECT_TRANSFORM_SQL: &str = r#"
SELECT
    id,
    name,
    description,
    CASE visibility_level
        WHEN 0 THEN 'private'
        WHEN 10 THEN 'internal'
        WHEN 20 THEN 'public'
        ELSE 'unknown'
    END AS visibility_level,
    path,
    created_at,
    updated_at,
    archived,
    star_count,
    last_activity_at,
    traversal_path,
    deleted
FROM source_data
"#;

const CREATOR_EDGE_SQL: &str = r#"
SELECT
    creator_id AS source_id,
    'User' AS source_kind,
    'creator' AS relationship_kind,
    id AS target_id,
    'Project' AS target_kind
FROM source_data
WHERE creator_id IS NOT NULL
"#;

const CONTAINS_EDGE_SQL: &str = r#"
SELECT
    namespace_id AS source_id,
    'Group' AS source_kind,
    'contains' AS relationship_kind,
    id AS target_id,
    'Project' AS target_kind
FROM source_data
WHERE namespace_id IS NOT NULL
"#;

#[derive(Clone, Serialize)]
pub struct ProjectQueryParams {
    pub traversal_path: String,
    pub last_watermark: String,
    pub watermark: String,
}

impl ProjectQueryParams {
    pub fn from_context(context: &NamespacedEntityContext) -> Self {
        Self {
            traversal_path: format!(
                "{}/{}/",
                context.payload.organization, context.payload.namespace
            ),
            last_watermark: context.last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: context
                .payload
                .watermark
                .format(TIMESTAMP_FORMAT)
                .to_string(),
        }
    }
}

pub struct ProjectHandler {
    datalake: Arc<dyn DatalakeQuery>,
}

impl ProjectHandler {
    pub fn new(datalake: Arc<dyn DatalakeQuery>) -> Self {
        Self { datalake }
    }

    async fn transform_and_write_batch(
        batch: RecordBatch,
        project_writer: &dyn BatchWriter,
        edge_writer: &dyn BatchWriter,
    ) -> Result<(), HandlerError> {
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| HandlerError::Processing(format!("failed to create mem table: {e}")))?;

        session
            .register_table("source_data", Arc::new(mem_table))
            .map_err(|e| HandlerError::Processing(format!("failed to register table: {e}")))?;

        let projects = Self::execute_query(&session, PROJECT_TRANSFORM_SQL).await?;
        project_writer
            .write_batch(&[projects])
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to write projects: {e}")))?;

        let creator_edges = Self::execute_query(&session, CREATOR_EDGE_SQL).await?;
        if creator_edges.num_rows() > 0 {
            edge_writer
                .write_batch(&[creator_edges])
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to write creator edges: {e}"))
                })?;
        }

        let contains_edges = Self::execute_query(&session, CONTAINS_EDGE_SQL).await?;
        if contains_edges.num_rows() > 0 {
            edge_writer
                .write_batch(&[contains_edges])
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to write contains edges: {e}"))
                })?;
        }

        Ok(())
    }

    async fn execute_query(
        session: &SessionContext,
        sql: &str,
    ) -> Result<RecordBatch, HandlerError> {
        let dataframe = session
            .sql(sql)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to execute sql: {e}")))?;

        let schema = Arc::new(dataframe.schema().as_arrow().clone());

        let batches = dataframe
            .collect()
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to collect results: {e}")))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches)
            .map_err(|e| HandlerError::Processing(format!("failed to concat batches: {e}")))
    }
}

#[async_trait]
impl NamespacedEntityHandler for ProjectHandler {
    fn name(&self) -> &'static str {
        "project-handler"
    }

    async fn handle(&self, context: &NamespacedEntityContext) -> Result<(), HandlerError> {
        let project_writer = context
            .handler_context
            .destination
            .new_batch_writer("projects")
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to create projects writer: {e}"))
            })?;

        let edge_writer = context
            .handler_context
            .destination
            .new_batch_writer("edges")
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to create edges writer: {e}")))?;

        let params = ProjectQueryParams::from_context(context);
        let mut stream = self
            .datalake
            .query_arrow(PROJECT_QUERY, params.to_query_params())
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to query projects: {e}")))?;

        while let Some(result) = stream.next().await {
            let source_batch = result
                .map_err(|e| HandlerError::Processing(format!("failed to read batch: {e}")))?;
            if source_batch.num_rows() == 0 {
                continue;
            }

            Self::transform_and_write_batch(
                source_batch,
                project_writer.as_ref(),
                edge_writer.as_ref(),
            )
            .await?;
        }

        Ok(())
    }
}
