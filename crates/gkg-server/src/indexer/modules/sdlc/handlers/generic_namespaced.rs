//! Generic namespaced entity handler.
//!
//! This handler processes entities with namespaced scope using ontology definitions.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use etl_engine::module::HandlerError;
use futures::StreamExt;
use ontology::NodeEntity;
use serde::Serialize;

use super::HandlerCreationError;
use crate::indexer::modules::sdlc::datalake::{DatalakeQuery, ToQueryParams};
use crate::indexer::modules::sdlc::namespace_handler::{
    NamespacedEntityContext, NamespacedEntityHandler,
};
use crate::indexer::modules::sdlc::transform::TransformEngine;
use crate::indexer::modules::sdlc::watermark_store::TIMESTAMP_FORMAT;

/// Query parameters for namespaced entity queries.
#[derive(Clone, Serialize)]
struct NamespacedQueryParams {
    traversal_path: String,
    last_watermark: String,
    watermark: String,
}

impl NamespacedQueryParams {
    fn from_context(context: &NamespacedEntityContext) -> Self {
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

/// Generic handler for namespaced entities.
///
/// This handler uses ontology definitions to dynamically generate SQL
/// and process entities within a namespace context.
pub struct GenericNamespacedHandler {
    node: NodeEntity,
    handler_name: String,
    datalake: Arc<dyn DatalakeQuery>,
    transform_sql: String,
    source_query: String,
    edge_queries: Vec<String>,
}

impl GenericNamespacedHandler {
    /// Create a new generic namespaced handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the node has no ETL configuration, source query,
    /// or if the query is missing required parameters.
    pub fn new(
        node: NodeEntity,
        datalake: Arc<dyn DatalakeQuery>,
    ) -> Result<Self, HandlerCreationError> {
        let etl = node.etl.as_ref().ok_or_else(|| HandlerCreationError {
            node_name: node.name.clone(),
            reason: "node has no ETL configuration".to_string(),
        })?;

        let missing_params = etl.validate_query_parameters();
        if !missing_params.is_empty() {
            return Err(HandlerCreationError {
                node_name: node.name.clone(),
                reason: format!("query missing required parameters: {}", missing_params.join(", ")),
            });
        }

        let transform_sql = TransformEngine::build_transform_sql(&node);
        let source_query = TransformEngine::build_source_query(&node).ok_or_else(|| {
            HandlerCreationError {
                node_name: node.name.clone(),
                reason: "failed to build source query".to_string(),
            }
        })?;

        let edge_queries: Vec<String> = node
            .edge_generation
            .iter()
            .map(|edge| TransformEngine::build_edge_sql(&node.name, edge))
            .collect();

        let handler_name = format!("generic-{}-handler", node.name.to_lowercase());

        Ok(Self {
            node,
            handler_name,
            datalake,
            transform_sql,
            source_query,
            edge_queries,
        })
    }

    async fn transform_and_write_batch(
        &self,
        batch: RecordBatch,
        entity_writer: &dyn etl_engine::destination::BatchWriter,
        edge_writer: &dyn etl_engine::destination::BatchWriter,
    ) -> Result<(), HandlerError> {
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| HandlerError::Processing(format!("failed to create mem table: {e}")))?;

        session
            .register_table("source_data", Arc::new(mem_table))
            .map_err(|e| HandlerError::Processing(format!("failed to register table: {e}")))?;

        let entities = Self::execute_query(&session, &self.transform_sql).await?;
        entity_writer
            .write_batch(&[entities])
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to write entities: {e}")))?;

        for edge_query in &self.edge_queries {
            let edges = Self::execute_query(&session, edge_query).await?;
            if edges.num_rows() > 0 {
                edge_writer
                    .write_batch(&[edges])
                    .await
                    .map_err(|e| HandlerError::Processing(format!("failed to write edges: {e}")))?;
            }
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
impl NamespacedEntityHandler for GenericNamespacedHandler {
    fn name(&self) -> &str {
        &self.handler_name
    }

    async fn handle(&self, context: &NamespacedEntityContext) -> Result<(), HandlerError> {
        let entity_writer = context
            .handler_context
            .destination
            .new_batch_writer(&self.node.destination_table)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to create entity writer: {e}"))
            })?;

        let edge_writer = context
            .handler_context
            .destination
            .new_batch_writer("edges")
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to create edge writer: {e}")))?;

        let params = NamespacedQueryParams::from_context(context);
        let mut stream = self
            .datalake
            .query_arrow(&self.source_query, params.to_query_params())
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to query source: {e}")))?;

        while let Some(result) = stream.next().await {
            let source_batch = result
                .map_err(|e| HandlerError::Processing(format!("failed to read batch: {e}")))?;
            if source_batch.num_rows() == 0 {
                continue;
            }

            self.transform_and_write_batch(
                source_batch,
                entity_writer.as_ref(),
                edge_writer.as_ref(),
            )
            .await?;
        }

        Ok(())
    }
}
