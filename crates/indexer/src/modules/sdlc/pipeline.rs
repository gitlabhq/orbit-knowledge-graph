use std::sync::Arc;
use std::time::Instant;

use crate::destination::{BatchWriter, Destination};
use crate::handler::HandlerError;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::StreamExt;
use ontology::{EdgeSourceEtlConfig, NodeEntity, Ontology};
use serde_json::Value;
use tracing::{debug, info};

use super::cursor_paginator::{CursorPaginator, CursorValue, cursor_params};
use super::datalake::DatalakeQuery;
use super::metrics::SdlcMetrics;
use super::prepare::{PreparedEdgeEtl, PreparedEtlConfig};
use super::transform::{
    SOURCE_DATA_TABLE, build_all_edge_sql, build_edge_etl_transform_sql, build_transform_sql,
};
use super::watermark_store::CursorReporter;

const DEFAULT_PAGE_SIZE: u64 = 100_000;

pub struct OntologyEntityPipeline {
    entity_name: String,
    destination_table: String,
    edge_table: String,
    extract_query: String,
    transform_sql: String,
    edge_transforms: Vec<String>,
    datalake: Arc<dyn DatalakeQuery>,
    metrics: SdlcMetrics,
    cursor_columns: Vec<String>,
    page_size: u64,
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
            edge_table: ontology.edge_table().to_string(),
            extract_query: config.extract_query,
            transform_sql,
            edge_transforms,
            datalake,
            metrics,
            cursor_columns: config.cursor_columns,
            page_size: DEFAULT_PAGE_SIZE,
        })
    }

    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    pub async fn process(
        &self,
        params: Value,
        destination: &dyn Destination,
        cursor_reporter: &dyn CursorReporter,
    ) -> Result<u64, HandlerError> {
        let started_at = Instant::now();

        let entity_writer = destination
            .new_batch_writer(&self.destination_table)
            .await
            .map_err(|e| self.error(format!("failed to create writer: {e}")))?;

        let edge_writer = destination
            .new_batch_writer(&self.edge_table)
            .await
            .map_err(|e| self.error(format!("failed to create edge writer: {e}")))?;

        let mut paginator =
            CursorPaginator::new(self.cursor_columns.clone(), self.page_size);
        if let Some(starting) = extract_starting_cursor(&params) {
            paginator = paginator.with_cursor(starting);
        }

        let mut total_rows: u64 = 0;
        let mut total_edges: u64 = 0;
        let mut total_batches: u64 = 0;

        loop {
            let page_query = paginator.build_page_query(&self.extract_query);
            let page_params = merge_cursor_params(&params, &paginator);

            let query_start = Instant::now();
            let mut stream = self
                .datalake
                .query_arrow(&page_query, page_params)
                .await
                .map_err(|e| self.error(format!("failed to query data: {e}")))?;
            self.metrics
                .record_datalake_query_duration(&self.entity_name, query_start.elapsed().as_secs_f64());

            let mut page_rows: u64 = 0;
            let mut last_cursor: Option<Vec<CursorValue>> = None;

            while let Some(result) = stream.next().await {
                let batch = result
                    .map_err(|e| self.error(format!("failed to read batch: {e}")))?;
                if batch.num_rows() == 0 {
                    continue;
                }

                total_batches += 1;
                page_rows += batch.num_rows() as u64;
                last_cursor = paginator.advance(&batch);

                let edges = self
                    .transform_and_write_batch(batch, entity_writer.as_ref(), edge_writer.as_ref())
                    .await?;
                total_edges += edges as u64;
            }

            total_rows += page_rows;

            if let Some(ref cursor) = last_cursor {
                cursor_reporter.on_page_complete(cursor).await?;
            }

            if paginator.is_last_page(page_rows) {
                break;
            }
        }

        self.log_completion(started_at, total_rows, total_edges, total_batches);
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

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| self.error(format!("failed to create mem table: {e}")))?;

        session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|e| self.error(format!("failed to register table: {e}")))?;

        let transformed = self.execute_query(&session, &self.transform_sql).await?;
        entity_writer
            .write_batch(&[transformed])
            .await
            .map_err(|e| self.error(format!("failed to write entities: {e}")))?;

        let mut edges_written = 0;
        for edge_sql in &self.edge_transforms {
            let edges = self.execute_query(&session, edge_sql).await?;
            if edges.num_rows() > 0 {
                edge_writer
                    .write_batch(std::slice::from_ref(&edges))
                    .await
                    .map_err(|e| self.error(format!("failed to write edges: {e}")))?;
                edges_written += edges.num_rows();
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
        let dataframe = session
            .sql(sql)
            .await
            .map_err(|e| self.error(format!("failed to execute sql: {e}")))?;

        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let batches = dataframe
            .collect()
            .await
            .map_err(|e| self.error(format!("failed to collect results: {e}")))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches)
            .map_err(|e| self.error(format!("failed to concat batches: {e}")))
    }

    fn error(&self, message: String) -> HandlerError {
        HandlerError::Processing(format!("{}: {message}", self.entity_name))
    }

    fn log_completion(&self, started_at: Instant, total_rows: u64, total_edges: u64, total_batches: u64) {
        let elapsed = started_at.elapsed();
        self.metrics.record_pipeline_completion(
            &self.entity_name,
            elapsed.as_secs_f64(),
            total_rows,
            total_edges,
            total_batches,
        );

        if total_rows == 0 {
            debug!(entity = %self.entity_name, elapsed_ms = elapsed.as_millis() as u64, "entity pipeline complete");
        } else {
            info!(
                entity = %self.entity_name, batches_processed = total_batches,
                total_rows, total_edges, elapsed_ms = elapsed.as_millis() as u64,
                "entity pipeline complete"
            );
        }
    }
}

/// Pipeline for edge ETL from join tables. Produces edges only, no nodes.
pub struct OntologyEdgePipeline {
    relationship_kind: String,
    edge_table: String,
    extract_query: String,
    transform_sql: String,
    datalake: Arc<dyn DatalakeQuery>,
    metrics: SdlcMetrics,
    cursor_columns: Vec<String>,
    page_size: u64,
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
            edge_table: ontology.edge_table().to_string(),
            extract_query: prepared.extract_query,
            transform_sql,
            datalake,
            metrics,
            cursor_columns: prepared.cursor_columns,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    pub fn relationship_kind(&self) -> &str {
        &self.relationship_kind
    }

    pub async fn process(
        &self,
        params: Value,
        destination: &dyn Destination,
        cursor_reporter: &dyn CursorReporter,
    ) -> Result<u64, HandlerError> {
        let started_at = Instant::now();

        let edge_writer = destination
            .new_batch_writer(&self.edge_table)
            .await
            .map_err(|e| self.error(format!("failed to create edge writer: {e}")))?;

        let mut paginator =
            CursorPaginator::new(self.cursor_columns.clone(), self.page_size);
        if let Some(starting) = extract_starting_cursor(&params) {
            paginator = paginator.with_cursor(starting);
        }

        let mut total_rows: u64 = 0;
        let mut total_edges: u64 = 0;
        let mut total_batches: u64 = 0;

        loop {
            let page_query = paginator.build_page_query(&self.extract_query);
            let page_params = merge_cursor_params(&params, &paginator);

            let query_start = Instant::now();
            let mut stream = self
                .datalake
                .query_arrow(&page_query, page_params)
                .await
                .map_err(|e| self.error(format!("failed to query data: {e}")))?;
            self.metrics.record_datalake_query_duration(
                &self.relationship_kind,
                query_start.elapsed().as_secs_f64(),
            );

            let mut page_rows: u64 = 0;
            let mut last_cursor: Option<Vec<CursorValue>> = None;

            while let Some(result) = stream.next().await {
                let batch = result
                    .map_err(|e| self.error(format!("failed to read batch: {e}")))?;
                if batch.num_rows() == 0 {
                    continue;
                }

                total_batches += 1;
                page_rows += batch.num_rows() as u64;
                last_cursor = paginator.advance(&batch);

                let edges = self
                    .transform_and_write_batch(batch, edge_writer.as_ref())
                    .await?;
                total_edges += edges as u64;
            }

            total_rows += page_rows;

            if let Some(ref cursor) = last_cursor {
                cursor_reporter.on_page_complete(cursor).await?;
            }

            if paginator.is_last_page(page_rows) {
                break;
            }
        }

        self.log_completion(started_at, total_rows, total_edges, total_batches);
        Ok(total_rows)
    }

    async fn transform_and_write_batch(
        &self,
        batch: RecordBatch,
        edge_writer: &dyn BatchWriter,
    ) -> Result<usize, HandlerError> {
        let transform_start = Instant::now();
        let session = SessionContext::new();

        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| self.error(format!("failed to create mem table: {e}")))?;

        session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|e| self.error(format!("failed to register table: {e}")))?;

        let edges = self.execute_query(&session, &self.transform_sql).await?;
        let count = edges.num_rows();
        if count > 0 {
            edge_writer
                .write_batch(&[edges])
                .await
                .map_err(|e| self.error(format!("failed to write edges: {e}")))?;
        }

        self.metrics.record_transform_duration(
            &self.relationship_kind,
            transform_start.elapsed().as_secs_f64(),
        );

        Ok(count)
    }

    async fn execute_query(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<RecordBatch, HandlerError> {
        let dataframe = session
            .sql(sql)
            .await
            .map_err(|e| self.error(format!("failed to execute sql: {e}")))?;

        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let batches = dataframe
            .collect()
            .await
            .map_err(|e| self.error(format!("failed to collect results: {e}")))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches)
            .map_err(|e| self.error(format!("failed to concat batches: {e}")))
    }

    fn error(&self, message: String) -> HandlerError {
        HandlerError::Processing(format!("{}: {message}", self.relationship_kind))
    }

    fn log_completion(&self, started_at: Instant, total_rows: u64, total_edges: u64, total_batches: u64) {
        let elapsed = started_at.elapsed();
        self.metrics.record_pipeline_completion(
            &self.relationship_kind,
            elapsed.as_secs_f64(),
            total_rows,
            total_edges,
            total_batches,
        );

        if total_rows == 0 {
            debug!(edge = %self.relationship_kind, elapsed_ms = elapsed.as_millis() as u64, "edge pipeline complete");
        } else {
            info!(
                edge = %self.relationship_kind, batches_processed = total_batches,
                source_rows = total_rows, edges_written = total_edges,
                elapsed_ms = elapsed.as_millis() as u64, "edge pipeline complete"
            );
        }
    }
}

fn extract_starting_cursor(params: &Value) -> Option<Vec<CursorValue>> {
    params
        .get("__starting_cursor")
        .and_then(|v| v.as_str())
        .and_then(|s| super::cursor_paginator::deserialize_cursor(s).ok())
}

fn merge_cursor_params(base_params: &Value, paginator: &CursorPaginator) -> Value {
    let mut params = base_params.clone();
    if let Some(cursor_values) = paginator.cursor_values()
        && let Value::Object(ref mut map) = params
    {
        for (key, value) in cursor_params(cursor_values) {
            map.insert(key, value);
        }
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use async_trait::async_trait;
    use futures::stream;
    use ontology::{DataType, EtlConfig, EtlScope, Field, constants::GL_TABLE_PREFIX};
    use std::collections::BTreeMap;

    fn test_metrics() -> SdlcMetrics {
        SdlcMetrics::with_meter(&crate::testkit::test_meter())
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
            destination_table: format!("{GL_TABLE_PREFIX}user"),
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
