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

const GROUP_QUERY: &str = r#"
SELECT
    namespace.id AS id,
    namespace.name AS name,
    namespace_details.description AS description,
    namespace.visibility_level AS visibility_level,
    namespace.path AS path,
    namespace.parent_id AS parent_id,
    namespace.owner_id AS owner_id,
    namespace.created_at AS created_at,
    namespace.updated_at AS updated_at,
    traversal_paths.traversal_path AS traversal_path,
    namespace._siphon_deleted AS deleted
FROM siphon_namespaces namespace
INNER JOIN siphon_namespace_details namespace_details ON namespace.id = namespace_details.namespace_id
INNER JOIN namespace_traversal_paths traversal_paths ON namespace.id = traversal_paths.id
WHERE namespace.id IN (SELECT id FROM namespace_traversal_paths WHERE startsWith(traversal_path, {traversal_path:String}))
    AND namespace._siphon_replicated_at > {last_watermark:String}
    AND namespace._siphon_replicated_at <= {watermark:String}
"#;

const GROUP_TRANSFORM_SQL: &str = r#"
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
    traversal_path,
    deleted
FROM source_data
"#;

const OWNER_EDGE_SQL: &str = r#"
SELECT
    owner_id AS source_id,
    'User' AS source_kind,
    'owner' AS relationship_kind,
    id AS target_id,
    'Group' AS target_kind
FROM source_data
WHERE owner_id IS NOT NULL
"#;

const PARENT_EDGE_SQL: &str = r#"
SELECT
    parent_id AS source_id,
    'Group' AS source_kind,
    'contains' AS relationship_kind,
    id AS target_id,
    'Group' AS target_kind
FROM source_data
WHERE parent_id IS NOT NULL
"#;

#[derive(Clone, Serialize)]
pub struct GroupQueryParams {
    pub traversal_path: String,
    pub last_watermark: String,
    pub watermark: String,
}

impl GroupQueryParams {
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

pub struct GroupHandler {
    datalake: Arc<dyn DatalakeQuery>,
}

impl GroupHandler {
    pub fn new(datalake: Arc<dyn DatalakeQuery>) -> Self {
        Self { datalake }
    }

    async fn transform_and_write_batch(
        batch: RecordBatch,
        group_writer: &dyn BatchWriter,
        edge_writer: &dyn BatchWriter,
    ) -> Result<(), HandlerError> {
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| HandlerError::Processing(format!("failed to create mem table: {e}")))?;

        session
            .register_table("source_data", Arc::new(mem_table))
            .map_err(|e| HandlerError::Processing(format!("failed to register table: {e}")))?;

        let groups = Self::execute_query(&session, GROUP_TRANSFORM_SQL).await?;
        group_writer
            .write_batch(&[groups])
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to write groups: {e}")))?;

        let owner_edges = Self::execute_query(&session, OWNER_EDGE_SQL).await?;
        if owner_edges.num_rows() > 0 {
            edge_writer.write_batch(&[owner_edges]).await.map_err(|e| {
                HandlerError::Processing(format!("failed to write owner edges: {e}"))
            })?;
        }

        let parent_edges = Self::execute_query(&session, PARENT_EDGE_SQL).await?;
        if parent_edges.num_rows() > 0 {
            edge_writer
                .write_batch(&[parent_edges])
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to write parent edges: {e}"))
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
impl NamespacedEntityHandler for GroupHandler {
    fn name(&self) -> &'static str {
        "group-handler"
    }

    async fn handle(&self, context: &NamespacedEntityContext) -> Result<(), HandlerError> {
        let group_writer = context
            .handler_context
            .destination
            .new_batch_writer("groups")
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to create groups writer: {e}"))
            })?;

        let edge_writer = context
            .handler_context
            .destination
            .new_batch_writer("edges")
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to create edges writer: {e}")))?;

        let params = GroupQueryParams::from_context(context);
        let mut stream = self
            .datalake
            .query_arrow(GROUP_QUERY, params.to_query_params())
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to query groups: {e}")))?;

        while let Some(result) = stream.next().await {
            let source_batch = result
                .map_err(|e| HandlerError::Processing(format!("failed to read batch: {e}")))?;
            if source_batch.num_rows() == 0 {
                continue;
            }

            Self::transform_and_write_batch(
                source_batch,
                group_writer.as_ref(),
                edge_writer.as_ref(),
            )
            .await?;
        }

        Ok(())
    }
}
