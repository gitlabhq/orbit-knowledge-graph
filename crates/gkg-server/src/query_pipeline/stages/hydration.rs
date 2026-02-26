use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::{QueryType, SecurityContext};

use crate::redaction::{ColumnValue, QueryResult};

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{HydrationOutput, RedactionOutput};

type PropertyMap = HashMap<(String, i64), HashMap<String, ColumnValue>>;

pub struct HydrationStage {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
}

impl HydrationStage {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>) -> Self {
        Self { ontology, client }
    }

    pub async fn execute(
        &self,
        input: RedactionOutput,
        security_context: &SecurityContext,
        obs: &mut PipelineObserver,
    ) -> Result<HydrationOutput, PipelineError> {
        let t = Instant::now();
        let mut query_result = input.query_result;
        let result_context = query_result.ctx().clone();

        if !matches!(result_context.query_type, Some(QueryType::Neighbors)) {
            obs.hydrated(t.elapsed());
            return Ok(HydrationOutput {
                query_result,
                result_context,
                redacted_count: input.redacted_count,
            });
        }

        let refs = self.extract_entity_refs(&query_result);
        if refs.is_empty() {
            obs.hydrated(t.elapsed());
            return Ok(HydrationOutput {
                query_result,
                result_context,
                redacted_count: input.redacted_count,
            });
        }

        let property_map = obs.check(self.fetch_all_properties(&refs, security_context).await)?;
        self.merge_properties(&mut query_result, &property_map);
        obs.hydrated(t.elapsed());
        Ok(HydrationOutput {
            query_result,
            result_context,
            redacted_count: input.redacted_count,
        })
    }

    fn extract_entity_refs(&self, result: &QueryResult) -> HashMap<String, Vec<i64>> {
        let mut refs: HashMap<String, Vec<i64>> = HashMap::new();

        for row in result.authorized_rows() {
            if let (Some(id), Some(entity_type)) = (
                row.get_column_i64("_gkg_neighbor_id"),
                row.get_column_string("_gkg_neighbor_type"),
            ) {
                refs.entry(entity_type).or_default().push(id);
            }
        }

        for ids in refs.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }

        refs
    }

    async fn fetch_all_properties(
        &self,
        refs: &HashMap<String, Vec<i64>>,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let mut result = HashMap::new();

        for (entity_type, ids) in refs {
            if ids.is_empty() {
                continue;
            }

            let props = self
                .fetch_entity_properties(entity_type, ids, security_context)
                .await?;
            result.extend(props);
        }

        Ok(result)
    }

    async fn fetch_entity_properties(
        &self,
        entity_type: &str,
        ids: &[i64],
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let query_json = self.build_search_query(entity_type, ids);
        let compiled = query_engine::compile(&query_json, &self.ontology, security_context)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let mut query = self.client.query(&compiled.sql);
        for (key, value) in &compiled.params {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        self.parse_property_batches(entity_type, &batches)
    }

    fn build_search_query(&self, entity_type: &str, ids: &[i64]) -> String {
        serde_json::json!({
            "query_type": "search",
            "node": {
                "id": "n",
                "entity": entity_type,
                "columns": "*",
                "node_ids": ids
            },
            "limit": 1000
        })
        .to_string()
    }

    fn parse_property_batches(
        &self,
        entity_type: &str,
        batches: &[RecordBatch],
    ) -> Result<PropertyMap, PipelineError> {
        let mut result = HashMap::new();

        for batch in batches {
            let schema = batch.schema();
            let id_idx = schema.index_of("_gkg_n_id").ok();

            let Some(id_idx) = id_idx else {
                continue;
            };

            let id_col = batch.column(id_idx).as_any().downcast_ref::<Int64Array>();
            let Some(ids) = id_col else {
                continue;
            };

            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    continue;
                }
                let id = ids.value(row);

                let mut props = HashMap::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let name = field.name();
                    if name.starts_with("_gkg_") {
                        continue;
                    }
                    if let Some(value) = self.column_value_to_column(batch.column(col_idx), row) {
                        props.insert(name.clone(), value);
                    }
                }

                result.insert((entity_type.to_string(), id), props);
            }
        }

        Ok(result)
    }

    fn column_value_to_column(&self, col: &dyn Array, row: usize) -> Option<ColumnValue> {
        if col.is_null(row) {
            return Some(ColumnValue::Null);
        }
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            return Some(ColumnValue::String(arr.value(row).to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return Some(ColumnValue::Int64(arr.value(row)));
        }
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            return Some(ColumnValue::String(arr.value(row).to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            return Some(ColumnValue::String(arr.value(row).to_string()));
        }
        None
    }

    fn merge_properties(&self, result: &mut QueryResult, property_map: &PropertyMap) {
        for row in result.authorized_rows_mut() {
            let id = row.get_column_i64("_gkg_neighbor_id");
            let entity_type = row.get_column_string("_gkg_neighbor_type");

            if let (Some(id), Some(entity_type)) = (id, entity_type)
                && let Some(props) = property_map.get(&(entity_type, id))
            {
                for (key, value) in props {
                    row.set_column(key.clone(), value.clone());
                }
            }
        }
    }
}
