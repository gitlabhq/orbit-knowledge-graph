//! Ontology-driven ETL pipeline for entity processing.
//!
//! The pipeline handles extracting data from the datalake, transforming it
//! according to the ontology definition, and writing to the destination.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use etl_engine::destination::{BatchWriter, Destination};
use etl_engine::module::HandlerError;
use futures::StreamExt;
use ontology::{EDGE_TABLE, NodeEntity};
use serde_json::Value;

use super::datalake::DatalakeQuery;
use super::transform::{
    SOURCE_DATA_TABLE, build_all_edge_sql, build_source_query, build_transform_sql,
};

pub struct OntologyEntityPipeline {
    entity_name: String,
    destination_table: String,
    extract_query: String,
    transform_sql: String,
    edge_transforms: Vec<String>,
    datalake: Arc<dyn DatalakeQuery>,
}

impl OntologyEntityPipeline {
    pub fn from_node(node: &NodeEntity, datalake: Arc<dyn DatalakeQuery>) -> Option<Self> {
        let extract_query = build_source_query(node)?;
        let transform_sql = build_transform_sql(node);
        let edge_transforms = build_all_edge_sql(node);

        Some(Self {
            entity_name: node.name.clone(),
            destination_table: node.destination_table.clone(),
            extract_query,
            transform_sql,
            edge_transforms,
            datalake,
        })
    }

    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    pub async fn process(
        &self,
        params: Value,
        destination: &dyn Destination,
    ) -> Result<(), HandlerError> {
        let entity_writer = destination
            .new_batch_writer(&self.destination_table)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to create {} writer: {e}",
                    self.entity_name
                ))
            })?;

        let edge_writer = destination
            .new_batch_writer(EDGE_TABLE)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to create edge writer for {}: {e}",
                    self.entity_name
                ))
            })?;

        let mut stream = self
            .datalake
            .query_arrow(&self.extract_query, params)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to query {} data: {e}", self.entity_name))
            })?;

        while let Some(result) = stream.next().await {
            let source_batch = result.map_err(|e| {
                HandlerError::Processing(format!("failed to read {} batch: {e}", self.entity_name))
            })?;

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

    async fn transform_and_write_batch(
        &self,
        batch: RecordBatch,
        entity_writer: &dyn BatchWriter,
        edge_writer: &dyn BatchWriter,
    ) -> Result<(), HandlerError> {
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]]).map_err(|e| {
            HandlerError::Processing(format!(
                "failed to create mem table for {}: {e}",
                self.entity_name
            ))
        })?;

        session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to register table for {}: {e}",
                    self.entity_name
                ))
            })?;

        // Transform and write node data
        let transformed = self.execute_query(&session, &self.transform_sql).await?;
        entity_writer
            .write_batch(&[transformed])
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to write {}: {e}", self.entity_name))
            })?;

        // Transform and write edges
        for edge_sql in &self.edge_transforms {
            let edges = self.execute_query(&session, edge_sql).await?;
            if edges.num_rows() > 0 {
                edge_writer.write_batch(&[edges]).await.map_err(|e| {
                    HandlerError::Processing(format!(
                        "failed to write edges for {}: {e}",
                        self.entity_name
                    ))
                })?;
            }
        }

        Ok(())
    }

    async fn execute_query(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<RecordBatch, HandlerError> {
        let dataframe = session.sql(sql).await.map_err(|e| {
            HandlerError::Processing(format!(
                "failed to execute sql for {}: {e}",
                self.entity_name
            ))
        })?;

        let schema = Arc::new(dataframe.schema().as_arrow().clone());

        let batches = dataframe.collect().await.map_err(|e| {
            HandlerError::Processing(format!(
                "failed to collect results for {}: {e}",
                self.entity_name
            ))
        })?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches).map_err(|e| {
            HandlerError::Processing(format!(
                "failed to concat batches for {}: {e}",
                self.entity_name
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use async_trait::async_trait;
    use futures::stream;
    use ontology::{DataType, EtlConfig, EtlScope, Field};
    use std::collections::BTreeMap;

    struct MockDatalake;

    #[async_trait]
    impl DatalakeQuery for MockDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }
    }

    fn create_test_node() -> NodeEntity {
        NodeEntity {
            name: "User".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                },
                Field {
                    name: "username".to_string(),
                    source: "username".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                },
            ],
            primary_keys: vec!["id".to_string()],
            destination_table: "gl_users".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: "siphon_users".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges: BTreeMap::new(),
            }),
        }
    }

    #[test]
    fn from_node_creates_pipeline_with_etl_config() {
        let node = create_test_node();
        let datalake = Arc::new(MockDatalake);

        let pipeline = OntologyEntityPipeline::from_node(&node, datalake);

        assert!(pipeline.is_some());
        let pipeline = pipeline.unwrap();
        assert_eq!(pipeline.entity_name(), "User");
    }

    #[test]
    fn from_node_returns_none_without_etl_config() {
        let node = NodeEntity {
            name: "NoEtl".to_string(),
            fields: vec![],
            primary_keys: vec!["id".to_string()],
            destination_table: "test".to_string(),
            etl: None,
        };
        let datalake = Arc::new(MockDatalake);

        let pipeline = OntologyEntityPipeline::from_node(&node, datalake);

        assert!(pipeline.is_none());
    }
}
