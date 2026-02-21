use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::{HydrationPlan, HydrationTemplate, QueryType, ResultContext, SecurityContext};

use crate::redaction::{ColumnValue, QueryResult};

use super::super::error::PipelineError;

type PropertyMap = HashMap<(String, i64), HashMap<String, ColumnValue>>;

pub struct HydrationStage {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
}

impl HydrationStage {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>) -> Self {
        Self { ontology, client }
    }

    /// Pre-auth hydration: for dynamic nodes with indirect authorization
    /// (owner_entity is set), fetch the auth_id_column value from the entity table
    /// so authorization can resolve without relying on edge columns.
    pub async fn resolve_auth_ids(
        &self,
        result: &mut QueryResult,
        result_context: &ResultContext,
        security_context: &SecurityContext,
    ) -> Result<(), PipelineError> {
        let mut indirect_refs: HashMap<String, Vec<i64>> = HashMap::new();
        let mut auth_columns: HashMap<String, String> = HashMap::new();

        for row in result.rows() {
            for node_ref in row.dynamic_nodes() {
                let Some(auth) = result_context.get_entity_auth(&node_ref.entity_type) else {
                    continue;
                };
                if auth.owner_entity.is_none() {
                    continue;
                }
                indirect_refs
                    .entry(node_ref.entity_type.clone())
                    .or_default()
                    .push(node_ref.id);
                auth_columns
                    .entry(node_ref.entity_type.clone())
                    .or_insert_with(|| auth.auth_id_column.clone());
            }
        }

        for ids in indirect_refs.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }

        if indirect_refs.is_empty() {
            return Ok(());
        }

        let mut overrides = HashMap::new();
        for (entity_type, ids) in &indirect_refs {
            let auth_col = &auth_columns[entity_type];
            let resolved = self
                .fetch_auth_ids(entity_type, ids, auth_col, security_context)
                .await?;
            overrides.extend(resolved);
        }

        result.set_auth_id_overrides(overrides);
        Ok(())
    }

    pub async fn execute(
        &self,
        mut result: QueryResult,
        result_context: &ResultContext,
        security_context: &SecurityContext,
        hydration_plan: &HydrationPlan,
    ) -> Result<QueryResult, PipelineError> {
        match hydration_plan {
            HydrationPlan::None => return Ok(result),
            HydrationPlan::Static(templates) => {
                let property_map = self
                    .hydrate_static(&result, templates, result_context, security_context)
                    .await?;
                self.merge_static_properties(&mut result, &property_map, result_context);
            }
            HydrationPlan::Dynamic => {
                let refs = self.extract_entity_refs(&result, result_context);
                if refs.is_empty() {
                    return Ok(result);
                }
                let property_map = self.fetch_all_properties(&refs, security_context).await?;
                self.merge_dynamic_properties(&mut result, &property_map, result_context);
            }
        }
        Ok(result)
    }

    /// Hydrate using pre-compiled templates (Traversal/Search).
    /// Extracts entity IDs from `_gkg_{alias}_pk` or `_gkg_{alias}_id` columns,
    /// then fetches full properties via search queries.
    async fn hydrate_static(
        &self,
        result: &QueryResult,
        templates: &[HydrationTemplate],
        result_context: &ResultContext,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let mut all_props = HashMap::new();

        for template in templates {
            let redaction_node = result_context.get(&template.node_alias);
            let Some(rn) = redaction_node else { continue };

            // Prefer _gkg_{alias}_pk (true primary key) for hydration lookup;
            // fall back to _gkg_{alias}_id which may be the auth ID.
            let mut ids: Vec<i64> = result
                .authorized_rows()
                .filter_map(|row| {
                    row.get_column_i64(&rn.pk_column)
                        .or_else(|| row.get_column_i64(&rn.id_column))
                })
                .collect();
            ids.sort_unstable();
            ids.dedup();

            if ids.is_empty() {
                continue;
            }

            let props = self
                .fetch_entity_properties(&template.entity_type, &ids, security_context)
                .await?;
            all_props.extend(props);
        }

        Ok(all_props)
    }

    /// Merge hydrated properties into rows for static (Traversal/Search) queries.
    fn merge_static_properties(
        &self,
        result: &mut QueryResult,
        property_map: &PropertyMap,
        result_context: &ResultContext,
    ) {
        for row in result.authorized_rows_mut() {
            for rn in result_context.nodes() {
                let pk = row
                    .get_column_i64(&rn.pk_column)
                    .or_else(|| row.get_column_i64(&rn.id_column));
                let Some(pk) = pk else { continue };

                if let Some(props) = property_map.get(&(rn.entity_type.clone(), pk)) {
                    for (key, value) in props {
                        row.set_column(key.clone(), value.clone());
                    }
                }
            }
        }
    }

    /// Extract entity references from dynamic query results (Neighbors or PathFinding).
    fn extract_entity_refs(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
    ) -> HashMap<String, Vec<i64>> {
        let mut refs: HashMap<String, Vec<i64>> = HashMap::new();

        let is_neighbors = matches!(result_context.query_type, Some(QueryType::Neighbors));

        for row in result.authorized_rows() {
            if is_neighbors
                && let (Some(id), Some(entity_type)) = (
                    row.get_column_i64("_gkg_neighbor_id"),
                    row.get_column_string("_gkg_neighbor_type"),
                )
            {
                refs.entry(entity_type).or_default().push(id);
            }

            // PathFinding and Neighbors both carry dynamic_nodes
            for node_ref in row.dynamic_nodes() {
                refs.entry(node_ref.entity_type.clone())
                    .or_default()
                    .push(node_ref.id);
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
        let compiled =
            query_engine::compile_with_columns(&query_json, &self.ontology, security_context)
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

    /// Fetch just the auth_id_column for a set of entity IDs. Returns a map
    /// of (entity_type, entity_pk) -> auth_id for use in pre-auth resolution.
    async fn fetch_auth_ids(
        &self,
        entity_type: &str,
        ids: &[i64],
        auth_id_column: &str,
        security_context: &SecurityContext,
    ) -> Result<HashMap<(String, i64), i64>, PipelineError> {
        let query_json = self.build_auth_id_query(entity_type, ids, auth_id_column);
        let compiled =
            query_engine::compile_with_columns(&query_json, &self.ontology, security_context)
                .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let mut query = self.client.query(&compiled.sql);
        for (key, value) in &compiled.params {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        let mut result = HashMap::new();
        for batch in &batches {
            let schema = batch.schema();
            let pk_idx = schema.index_of("_gkg_n_id").ok();
            let auth_idx = schema.index_of(auth_id_column).ok();

            let (Some(pk_idx), Some(auth_idx)) = (pk_idx, auth_idx) else {
                continue;
            };

            let pk_col = batch.column(pk_idx).as_any().downcast_ref::<Int64Array>();
            let auth_col = batch.column(auth_idx).as_any().downcast_ref::<Int64Array>();

            let (Some(pks), Some(auths)) = (pk_col, auth_col) else {
                continue;
            };

            for row in 0..batch.num_rows() {
                if pks.is_null(row) || auths.is_null(row) {
                    continue;
                }
                result.insert((entity_type.to_string(), pks.value(row)), auths.value(row));
            }
        }

        Ok(result)
    }

    fn build_auth_id_query(&self, entity_type: &str, ids: &[i64], auth_id_column: &str) -> String {
        serde_json::json!({
            "query_type": "search",
            "node": {
                "id": "n",
                "entity": entity_type,
                "columns": [auth_id_column],
                "node_ids": ids
            },
            "limit": 1000
        })
        .to_string()
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

    /// Merge hydrated properties into rows for dynamic (Neighbors/PathFinding) queries.
    fn merge_dynamic_properties(
        &self,
        result: &mut QueryResult,
        property_map: &PropertyMap,
        result_context: &ResultContext,
    ) {
        let is_neighbors = matches!(result_context.query_type, Some(QueryType::Neighbors));

        for row in result.authorized_rows_mut() {
            // For Neighbors: merge using _gkg_neighbor_id/_gkg_neighbor_type
            if is_neighbors
                && let Some(id) = row.get_column_i64("_gkg_neighbor_id")
                && let Some(entity_type) = row.get_column_string("_gkg_neighbor_type")
                && let Some(props) = property_map.get(&(entity_type, id))
            {
                for (key, value) in props {
                    row.set_column(key.clone(), value.clone());
                }
            }

            // For PathFinding: merge using dynamic_nodes
            // Properties are attached per-node with a prefix to avoid column name collisions.
            // Paths have multiple nodes per row so we store them indexed.
            // TODO: define the path result format (array of hydrated node objects vs flat columns)
        }
    }
}
