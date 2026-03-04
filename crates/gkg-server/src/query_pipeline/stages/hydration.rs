use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use futures::future::try_join_all;
use ontology::Ontology;
use query_engine::{HydrationPlan, HydrationTemplate, SecurityContext, compile};

use crate::redaction::{ColumnValue, QueryResult};

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{HydrationOutput, RedactionOutput};

use query_engine::constants::{GKG_COLUMN_PREFIX, HYDRATION_NODE_ALIAS, redaction_id_column};

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
        hydration_plan: &HydrationPlan,
        security_context: &SecurityContext,
        obs: &mut PipelineObserver,
    ) -> Result<HydrationOutput, PipelineError> {
        let t = Instant::now();
        let mut query_result = input.query_result;
        let result_context = query_result.ctx().clone();

        match hydration_plan {
            HydrationPlan::None => {
                obs.hydrated(t.elapsed());
                return Ok(HydrationOutput {
                    query_result,
                    result_context,
                    redacted_count: input.redacted_count,
                });
            }
            HydrationPlan::Static(templates) => {
                let property_map = obs.check(
                    self.hydrate_static(templates, &query_result, security_context)
                        .await,
                )?;
                if !property_map.is_empty() {
                    merge_static_properties(&mut query_result, &property_map, templates);
                }
            }
            HydrationPlan::Dynamic => {
                let refs = extract_dynamic_refs(&query_result);
                if !refs.is_empty() {
                    let property_map =
                        obs.check(self.hydrate_dynamic(&refs, security_context).await)?;
                    merge_dynamic_properties(&mut query_result, &property_map);
                }
            }
        }

        obs.hydrated(t.elapsed());
        Ok(HydrationOutput {
            query_result,
            result_context,
            redacted_count: input.redacted_count,
        })
    }

    /// Static hydration: use pre-built templates from compile time.
    /// Collects IDs from `_gkg_{alias}_id` columns, injects them into
    /// template query JSON, compiles and executes concurrently.
    async fn hydrate_static(
        &self,
        templates: &[HydrationTemplate],
        query_result: &QueryResult,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let futures: Vec<_> = templates
            .iter()
            .filter_map(|template| {
                let ids = collect_static_ids(query_result, template);
                if ids.is_empty() {
                    return None;
                }
                let query_json = template.with_ids(&ids);
                Some(self.compile_and_fetch(&template.entity_type, query_json, security_context))
            })
            .collect();

        let results = try_join_all(futures).await?;
        let mut merged = HashMap::new();
        for props in results {
            merged.extend(props);
        }
        Ok(merged)
    }

    /// Dynamic hydration: build search queries from scratch at runtime.
    /// Entity types are discovered from dynamic_nodes after redaction.
    /// All entity-type queries execute concurrently.
    async fn hydrate_dynamic(
        &self,
        refs: &HashMap<String, Vec<i64>>,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let futures: Vec<_> = refs
            .iter()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(entity_type, ids)| {
                let query_json = build_dynamic_search_query(entity_type, ids, &self.ontology)?;
                Ok(self.compile_and_fetch(entity_type, query_json, security_context))
            })
            .collect::<Result<Vec<_>, PipelineError>>()?;

        let results = try_join_all(futures).await?;
        let mut merged = HashMap::new();
        for props in results {
            merged.extend(props);
        }
        Ok(merged)
    }

    /// Compile a hydration query JSON string, execute it, and parse the results.
    async fn compile_and_fetch(
        &self,
        entity_type: &str,
        query_json: String,
        security_context: &SecurityContext,
    ) -> Result<PropertyMap, PipelineError> {
        let compiled = compile(&query_json, &self.ontology, security_context)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let mut query = self.client.query(&compiled.base.sql);
        for (key, value) in &compiled.base.params {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        parse_property_batches(entity_type, &batches)
    }
}

/// Collect entity IDs for a static template from `_gkg_{alias}_id` columns.
fn collect_static_ids(result: &QueryResult, template: &HydrationTemplate) -> Vec<i64> {
    let id_column = redaction_id_column(&template.node_alias);
    let mut ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|row| row.get_column_i64(&id_column))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Merge static hydration results back into rows as flat columns.
/// The hydration template uses the same node alias as the base query,
/// so columns already come back with the correct prefix (e.g. "u_username").
fn merge_static_properties(
    result: &mut QueryResult,
    property_map: &PropertyMap,
    templates: &[HydrationTemplate],
) {
    for row in result.authorized_rows_mut() {
        for template in templates {
            let id = row.get_column_i64(&redaction_id_column(&template.node_alias));
            if let Some(id) = id
                && let Some(props) = property_map.get(&(template.entity_type.clone(), id))
            {
                for (key, value) in props {
                    row.set_column(key.clone(), value.clone());
                }
            }
        }
    }
}

/// Collect unique entity (type, id) pairs from dynamic_nodes across all authorized rows.
fn extract_dynamic_refs(result: &QueryResult) -> HashMap<String, Vec<i64>> {
    let mut refs: HashMap<String, Vec<i64>> = HashMap::new();

    for row in result.authorized_rows() {
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

/// Merge dynamic hydration results into NodeRef.properties on dynamic_nodes.
/// Works uniformly for both PathFinding (multiple nodes per row) and
/// Neighbors (single node per row).
fn merge_dynamic_properties(result: &mut QueryResult, property_map: &PropertyMap) {
    for row in result.authorized_rows_mut() {
        for node_ref in row.dynamic_nodes_mut() {
            if let Some(props) = property_map.get(&(node_ref.entity_type.clone(), node_ref.id)) {
                node_ref.properties = props.clone();
            }
        }
    }
}

/// Build a search query JSON from scratch for dynamic hydration.
/// Only used when entity types are discovered at runtime (PathFinding, Neighbors).
fn build_dynamic_search_query(
    entity_type: &str,
    ids: &[i64],
    ontology: &ontology::Ontology,
) -> Result<String, PipelineError> {
    if ontology.get_node(entity_type).is_none() {
        return Err(PipelineError::Execution(format!(
            "entity type not found in ontology during dynamic hydration: {entity_type}"
        )));
    }

    let query_json = serde_json::json!({
        "query_type": "search",
        "node": {
            "id": HYDRATION_NODE_ALIAS,
            "entity": entity_type,
            "columns": "*",
            "node_ids": ids
        },
        "limit": 1000
    })
    .to_string();

    Ok(query_json)
}

fn parse_property_batches(
    entity_type: &str,
    batches: &[RecordBatch],
) -> Result<PropertyMap, PipelineError> {
    let mut result = HashMap::new();
    let id_column = redaction_id_column(HYDRATION_NODE_ALIAS);
    let alias_prefix = format!("{HYDRATION_NODE_ALIAS}_");

    for batch in batches {
        let schema = batch.schema();
        let id_idx = schema.index_of(&id_column).ok();

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
                if name.starts_with(GKG_COLUMN_PREFIX) {
                    continue;
                }
                let clean_name = name.strip_prefix(&alias_prefix).unwrap_or(name).to_string();
                if let Some(value) = column_value_from_arrow(batch.column(col_idx), row) {
                    props.insert(clean_name, value);
                }
            }

            result.insert((entity_type.to_string(), id), props);
        }
    }

    Ok(result)
}

fn column_value_from_arrow(col: &dyn Array, row: usize) -> Option<ColumnValue> {
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
