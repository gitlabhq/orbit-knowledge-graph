use std::sync::Arc;
use std::time::Instant;

use crate::destination::{BatchWriter, Destination};
use crate::handler::HandlerError;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::StreamExt;
use ontology::constants::EDGE_TABLE;
use ontology::{EdgeSourceEtlConfig, NodeEntity, Ontology};
use serde_json::Value;
use tracing::{debug, info};

use super::datalake::DatalakeQuery;
use super::metrics::SdlcMetrics;
use super::prepare::{PreparedEdgeEtl, PreparedEtlConfig};
use super::transform::{
    SOURCE_DATA_TABLE, build_all_edge_sql, build_edge_etl_transform_sql, build_transform_sql,
};

pub struct OntologyEntityPipeline {
    entity_name: String,
    destination_table: String,
    extract_query: String,
    transform_sql: String,
    edge_transforms: Vec<String>,
    datalake: Arc<dyn DatalakeQuery>,
    metrics: SdlcMetrics,
}

impl OntologyEntityPipeline {
    pub fn from_node(
        node: &NodeEntity,
        ontology: &Ontology,
        datalake: Arc<dyn DatalakeQuery>,
        metrics: SdlcMetrics,
    ) -> Option<Self> {
        let config = PreparedEtlConfig::from_node(node, ontology)?;
        let transform_sql = build_transform_sql(&config);
        let edge_transforms = build_all_edge_sql(&config);

        Some(Self {
            entity_name: config.node_kind,
            destination_table: config.destination_table,
            extract_query: config.extract_query,
            transform_sql,
            edge_transforms,
            datalake,
            metrics,
        })
    }

    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    pub async fn process(
        &self,
        params: Value,
        destination: &dyn Destination,
    ) -> Result<u64, HandlerError> {
        let started_at = Instant::now();

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

        debug!(
            entity = %self.entity_name,
            %params,
            "querying datalake for entity data"
        );

        let query_start = Instant::now();
        let mut stream = self
            .datalake
            .query_arrow(&self.extract_query, params)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to query {} data: {e}", self.entity_name))
            })?;
        self.metrics
            .record_datalake_query_duration(&self.entity_name, query_start.elapsed().as_secs_f64());

        let mut batch_count: u64 = 0;
        let mut total_rows: u64 = 0;
        let mut total_edges: u64 = 0;

        while let Some(result) = stream.next().await {
            let source_batch = result.map_err(|e| {
                HandlerError::Processing(format!("failed to read {} batch: {e}", self.entity_name))
            })?;

            if source_batch.num_rows() == 0 {
                continue;
            }

            batch_count += 1;
            let batch_rows = source_batch.num_rows() as u64;
            total_rows += batch_rows;

            let edges_written = self
                .transform_and_write_batch(
                    source_batch,
                    entity_writer.as_ref(),
                    edge_writer.as_ref(),
                )
                .await?;
            total_edges += edges_written as u64;
        }

        let elapsed = started_at.elapsed();

        self.metrics.record_pipeline_completion(
            &self.entity_name,
            elapsed.as_secs_f64(),
            total_rows,
            total_edges,
            batch_count,
        );

        if total_rows == 0 {
            debug!(
                entity = %self.entity_name,
                elapsed_ms = elapsed.as_millis() as u64,
                "entity pipeline processing complete"
            );
        } else {
            info!(
                entity = %self.entity_name,
                batches_processed = batch_count,
                total_rows,
                total_edges,
                elapsed_ms = elapsed.as_millis() as u64,
                "entity pipeline processing complete"
            );
        }

        Ok(total_rows)
    }

    async fn transform_and_write_batch(
        &self,
        batch: RecordBatch,
        entity_writer: &dyn BatchWriter,
        edge_writer: &dyn BatchWriter,
    ) -> Result<usize, HandlerError> {
        let transform_start = Instant::now();
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

        let transformed = self.execute_query(&session, &self.transform_sql).await?;
        let rows_transformed = transformed.num_rows();
        entity_writer
            .write_batch(&[transformed])
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to write {}: {e}", self.entity_name))
            })?;

        debug!(
            entity = %self.entity_name,
            rows = rows_transformed,
            "entity batch transform and write complete"
        );

        let mut edges_written = 0;
        for edge_sql in &self.edge_transforms {
            let edges = self.execute_query(&session, edge_sql).await?;
            let edge_count = edges.num_rows();
            if edge_count > 0 {
                edge_writer.write_batch(&[edges]).await.map_err(|e| {
                    HandlerError::Processing(format!(
                        "failed to write edges for {}: {e}",
                        self.entity_name
                    ))
                })?;
                edges_written += edge_count;
            }
        }

        self.metrics
            .record_transform_duration(&self.entity_name, transform_start.elapsed().as_secs_f64());

        Ok(edges_written)
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

/// Pipeline for processing edge ETL from join tables.
///
/// Unlike `OntologyEntityPipeline`, this only produces edges (no nodes).
pub struct OntologyEdgePipeline {
    relationship_kind: String,
    extract_query: String,
    transform_sql: String,
    datalake: Arc<dyn DatalakeQuery>,
    metrics: SdlcMetrics,
}

impl OntologyEdgePipeline {
    pub fn from_config(
        relationship_kind: &str,
        config: &EdgeSourceEtlConfig,
        ontology: &Ontology,
        datalake: Arc<dyn DatalakeQuery>,
        metrics: SdlcMetrics,
    ) -> Self {
        let prepared = PreparedEdgeEtl::from_config(relationship_kind, config, ontology);
        let transform_sql = build_edge_etl_transform_sql(&prepared);

        Self {
            relationship_kind: relationship_kind.to_string(),
            extract_query: prepared.extract_query,
            transform_sql,
            datalake,
            metrics,
        }
    }

    pub fn relationship_kind(&self) -> &str {
        &self.relationship_kind
    }

    pub async fn process(
        &self,
        params: Value,
        destination: &dyn Destination,
    ) -> Result<u64, HandlerError> {
        let started_at = Instant::now();

        let edge_writer = destination
            .new_batch_writer(EDGE_TABLE)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to create edge writer for {}: {e}",
                    self.relationship_kind
                ))
            })?;

        debug!(
            edge = %self.relationship_kind,
            %params,
            "querying datalake for edge data"
        );

        let query_start = Instant::now();
        let mut stream = self
            .datalake
            .query_arrow(&self.extract_query, params)
            .await
            .map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to query {} edge data: {e}",
                    self.relationship_kind
                ))
            })?;
        self.metrics.record_datalake_query_duration(
            &self.relationship_kind,
            query_start.elapsed().as_secs_f64(),
        );

        let mut batch_count: u64 = 0;
        let mut total_rows: u64 = 0;
        let mut total_edges_written: u64 = 0;

        while let Some(result) = stream.next().await {
            let source_batch = result.map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to read {} edge batch: {e}",
                    self.relationship_kind
                ))
            })?;

            if source_batch.num_rows() == 0 {
                continue;
            }

            batch_count += 1;
            let batch_rows = source_batch.num_rows() as u64;
            total_rows += batch_rows;

            let edges_written = self
                .transform_and_write_batch(source_batch, edge_writer.as_ref())
                .await?;
            total_edges_written += edges_written as u64;
        }

        let elapsed = started_at.elapsed();

        self.metrics.record_pipeline_completion(
            &self.relationship_kind,
            elapsed.as_secs_f64(),
            total_rows,
            total_edges_written,
            batch_count,
        );

        if total_rows == 0 {
            debug!(
                edge = %self.relationship_kind,
                elapsed_ms = elapsed.as_millis() as u64,
                "edge pipeline processing complete"
            );
        } else {
            info!(
                edge = %self.relationship_kind,
                batches_processed = batch_count,
                source_rows = total_rows,
                edges_written = total_edges_written,
                elapsed_ms = elapsed.as_millis() as u64,
                "edge pipeline processing complete"
            );
        }

        Ok(total_rows)
    }

    async fn transform_and_write_batch(
        &self,
        batch: RecordBatch,
        edge_writer: &dyn BatchWriter,
    ) -> Result<usize, HandlerError> {
        let transform_start = Instant::now();
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]]).map_err(|e| {
            HandlerError::Processing(format!(
                "failed to create mem table for {} edges: {e}",
                self.relationship_kind
            ))
        })?;

        session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to register table for {} edges: {e}",
                    self.relationship_kind
                ))
            })?;

        let edges = self.execute_query(&session, &self.transform_sql).await?;
        let edges_count = edges.num_rows();
        if edges_count > 0 {
            edge_writer.write_batch(&[edges]).await.map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to write {} edges: {e}",
                    self.relationship_kind
                ))
            })?;

            debug!(
                edge = %self.relationship_kind,
                edges_written = edges_count,
                "edge batch transform and write complete"
            );
        }

        self.metrics.record_transform_duration(
            &self.relationship_kind,
            transform_start.elapsed().as_secs_f64(),
        );

        Ok(edges_count)
    }

    async fn execute_query(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<RecordBatch, HandlerError> {
        let dataframe = session.sql(sql).await.map_err(|e| {
            HandlerError::Processing(format!(
                "failed to execute sql for {} edges: {e}",
                self.relationship_kind
            ))
        })?;

        let schema = Arc::new(dataframe.schema().as_arrow().clone());

        let batches = dataframe.collect().await.map_err(|e| {
            HandlerError::Processing(format!(
                "failed to collect results for {} edges: {e}",
                self.relationship_kind
            ))
        })?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches).map_err(|e| {
            HandlerError::Processing(format!(
                "failed to concat batches for {} edges: {e}",
                self.relationship_kind
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use async_trait::async_trait;
    use futures::stream;
    use ontology::{DataType, EtlConfig, EtlScope, Field};
    use std::collections::BTreeMap;

    fn test_metrics() -> SdlcMetrics {
        let provider = opentelemetry::global::meter_provider();
        let meter = provider.meter("test");
        SdlcMetrics::with_meter(&meter)
    }

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
            domain: "core".to_string(),
            label: "username".to_string(),
            fields: vec![
                Field {
                    name: "id".to_string(),
                    source: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
                Field {
                    name: "username".to_string(),
                    source: "username".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    enum_values: None,
                    enum_type: ontology::EnumType::default(),
                },
            ],
            destination_table: "gl_user".to_string(),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: "siphon_users".to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges: BTreeMap::new(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn from_node_creates_pipeline() {
        let node = create_test_node();
        let ontology = Ontology::new();
        let datalake = Arc::new(MockDatalake);

        let pipeline =
            OntologyEntityPipeline::from_node(&node, &ontology, datalake, test_metrics());

        assert!(pipeline.is_some());
        assert_eq!(pipeline.unwrap().entity_name(), "User");
    }

    #[test]
    fn from_node_returns_none_without_etl() {
        let node = NodeEntity {
            name: "NoEtl".to_string(),
            destination_table: "test".to_string(),
            ..Default::default()
        };
        let ontology = Ontology::new();
        let datalake = Arc::new(MockDatalake);

        let pipeline =
            OntologyEntityPipeline::from_node(&node, &ontology, datalake, test_metrics());

        assert!(pipeline.is_none());
    }
}
