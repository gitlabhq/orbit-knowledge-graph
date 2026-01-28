use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use etl_engine::destination::Destination;
use etl_engine::module::HandlerError;
use futures::StreamExt;
use ontology::NodeEntity;

use super::HandlerCreationError;
use crate::indexer::modules::sdlc::datalake::{DatalakeQuery, ToQueryParams};
use crate::indexer::modules::sdlc::transform::TransformEngine;

pub struct OntologyEntityPipeline {
    pub entity_name: String,
    destination_table: String,
    extract: String,
    transform: String,
    edge_transforms: Vec<String>,
    datalake: Arc<dyn DatalakeQuery>,
}

impl OntologyEntityPipeline {
    pub fn from_node(
        node: &NodeEntity,
        datalake: Arc<dyn DatalakeQuery>,
    ) -> Result<Self, HandlerCreationError> {
        let etl = node.etl.as_ref().ok_or_else(|| HandlerCreationError {
            handler_name: node.name.clone(),
            reason: "node has no ETL configuration".to_string(),
        })?;

        let missing_params = etl.validate_query_parameters();
        if !missing_params.is_empty() {
            return Err(HandlerCreationError {
                handler_name: node.name.clone(),
                reason: format!(
                    "query missing required parameters: {}",
                    missing_params.join(", ")
                ),
            });
        }

        let extract =
            TransformEngine::build_source_query(node).ok_or_else(|| HandlerCreationError {
                handler_name: node.name.clone(),
                reason: "failed to build source query".to_string(),
            })?;

        let transform = TransformEngine::build_transform_sql(node);

        let edge_transforms: Vec<String> = node
            .edges
            .iter()
            .map(|edge| TransformEngine::build_edge_sql(&node.name, edge))
            .collect();

        Ok(Self {
            entity_name: node.name.clone(),
            destination_table: node.destination_table.clone(),
            extract,
            transform,
            edge_transforms,
            datalake,
        })
    }

    pub async fn run(
        &self,
        params: impl ToQueryParams,
        destination: Arc<dyn Destination>,
    ) -> Result<(), HandlerError> {
        let entity_writer = destination
            .new_batch_writer(&self.destination_table)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to create entity writer: {e}"))
            })?;

        let edge_writer = destination
            .new_batch_writer("edges")
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to create edge writer: {e}")))?;

        let mut stream = self
            .datalake
            .query_arrow(&self.extract, params.to_query_params())
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to query source: {e}")))?;

        while let Some(result) = stream.next().await {
            let batch = result
                .map_err(|e| HandlerError::Processing(format!("failed to read batch: {e}")))?;

            if batch.num_rows() == 0 {
                continue;
            }

            self.transform_and_load(batch, entity_writer.as_ref(), edge_writer.as_ref())
                .await?;
        }

        Ok(())
    }

    async fn transform_and_load(
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

        let entities = self.execute_sql(&session, &self.transform).await?;
        entity_writer
            .write_batch(&[entities])
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to write entities: {e}")))?;

        for edge_sql in &self.edge_transforms {
            let edges = self.execute_sql(&session, edge_sql).await?;
            if edges.num_rows() > 0 {
                edge_writer
                    .write_batch(&[edges])
                    .await
                    .map_err(|e| HandlerError::Processing(format!("failed to write edges: {e}")))?;
            }
        }

        Ok(())
    }

    async fn execute_sql(
        &self,
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
