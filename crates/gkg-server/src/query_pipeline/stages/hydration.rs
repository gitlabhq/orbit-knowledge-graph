use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use futures::future::try_join_all;
use query_engine::{DynamicColumnMode, HydrationPlan, HydrationTemplate, QueryType, compile};

use gkg_utils::arrow::{ArrowUtils, ColumnValue};
use querying_types::QueryResult;

use querying_pipeline::{
    HydrationOutput, PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
    RedactionOutput,
};

use query_engine::constants::{
    GKG_COLUMN_PREFIX, HYDRATION_NODE_ALIAS, MAX_DYNAMIC_HYDRATION_RESULTS, redaction_id_column,
};

type PropertyMap = HashMap<(String, i64), HashMap<String, ColumnValue>>;

#[derive(Clone)]
pub struct HydrationStage;

impl HydrationStage {
    fn client(ctx: &QueryPipelineContext) -> Result<&Arc<ArrowClickHouseClient>, PipelineError> {
        ctx.extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))
    }

    async fn hydrate_static(
        ctx: &QueryPipelineContext,
        templates: &[HydrationTemplate],
        query_result: &QueryResult,
    ) -> Result<PropertyMap, PipelineError> {
        let futures: Vec<_> = templates
            .iter()
            .filter_map(|template| {
                let ids = Self::collect_static_ids(query_result, template);
                if ids.is_empty() {
                    return None;
                }
                let query_json = template.with_ids(&ids);
                Some(Self::compile_and_fetch(
                    ctx,
                    &template.entity_type,
                    query_json,
                ))
            })
            .collect();

        let results = try_join_all(futures).await?;
        let mut merged = HashMap::new();
        for props in results {
            merged.extend(props);
        }
        Ok(merged)
    }

    async fn hydrate_dynamic(
        ctx: &QueryPipelineContext,
        refs: &HashMap<String, Vec<i64>>,
    ) -> Result<PropertyMap, PipelineError> {
        let futures: Vec<_> = refs
            .iter()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(entity_type, ids)| {
                let query_json = Self::build_dynamic_search_query(ctx, entity_type, ids)?;
                Ok(Self::compile_and_fetch(ctx, entity_type, query_json))
            })
            .collect::<Result<Vec<_>, PipelineError>>()?;

        let results = try_join_all(futures).await?;
        let mut merged = HashMap::new();
        for props in results {
            merged.extend(props);
        }
        Ok(merged)
    }

    async fn compile_and_fetch(
        ctx: &QueryPipelineContext,
        entity_type: &str,
        query_json: String,
    ) -> Result<PropertyMap, PipelineError> {
        let client = Self::client(ctx)?;
        let compiled = compile(&query_json, &ctx.ontology, ctx.security_context()?)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let mut query = client.query(&compiled.base.sql);
        for (key, param) in &compiled.base.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        Self::parse_property_batches(entity_type, &batches)
    }

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

    fn merge_dynamic_properties(result: &mut QueryResult, property_map: &PropertyMap) {
        for row in result.authorized_rows_mut() {
            for node_ref in row.dynamic_nodes_mut() {
                if let Some(props) = property_map.get(&(node_ref.entity_type.clone(), node_ref.id))
                {
                    node_ref.properties = props.clone();
                }
            }
        }
    }

    fn build_dynamic_search_query(
        ctx: &QueryPipelineContext,
        entity_type: &str,
        ids: &[i64],
    ) -> Result<String, PipelineError> {
        let input = &ctx.compiled()?.input;
        let node = ctx.ontology.get_node(entity_type).ok_or_else(|| {
            PipelineError::Execution(format!(
                "entity type not found in ontology during dynamic hydration: {entity_type}"
            ))
        })?;

        let columns: serde_json::Value = match input.options.dynamic_columns {
            DynamicColumnMode::All => serde_json::json!("*"),
            DynamicColumnMode::Default => {
                if node.default_columns.is_empty() {
                    return Err(PipelineError::Execution(format!(
                        "no default_columns defined for {entity_type}"
                    )));
                }
                serde_json::json!(node.default_columns)
            }
        };

        let query_json = serde_json::json!({
            "query_type": QueryType::Search.to_string(),
            "node": {
                "id": HYDRATION_NODE_ALIAS,
                "entity": entity_type,
                "columns": columns,
                "node_ids": ids
            },
            "limit": ids.len().min(MAX_DYNAMIC_HYDRATION_RESULTS)
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
            for row in 0..batch.num_rows() {
                let Some(id) = ArrowUtils::get_column::<Int64Type>(batch, &id_column, row) else {
                    continue;
                };

                let props: HashMap<String, ColumnValue> = ArrowUtils::extract_row(batch, row)
                    .into_iter()
                    .filter(|(name, _)| !name.starts_with(GKG_COLUMN_PREFIX))
                    .map(|(name, value)| {
                        let clean = name
                            .strip_prefix(&alias_prefix)
                            .unwrap_or(&name)
                            .to_string();
                        (clean, value)
                    })
                    .collect();

                result.insert((entity_type.to_string(), id), props);
            }
        }

        Ok(result)
    }
}

impl PipelineStage for HydrationStage {
    type Input = RedactionOutput;
    type Output = HydrationOutput;

    async fn execute(
        &self,
        input: Self::Input,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let mut query_result = input.query_result;
        let result_context = query_result.ctx().clone();

        match &ctx.compiled()?.hydration {
            HydrationPlan::None => {
                obs.hydrated(t.elapsed());
                return Ok(HydrationOutput {
                    query_result,
                    result_context,
                    redacted_count: input.redacted_count,
                });
            }
            HydrationPlan::Static(templates) => {
                let property_map = Self::hydrate_static(ctx, templates, &query_result)
                    .await
                    .inspect_err(|e| obs.record_error(e))?;
                if !property_map.is_empty() {
                    Self::merge_static_properties(&mut query_result, &property_map, templates);
                }
            }
            HydrationPlan::Dynamic => {
                let refs = Self::extract_dynamic_refs(&query_result);
                if !refs.is_empty() {
                    let property_map = Self::hydrate_dynamic(ctx, &refs)
                        .await
                        .inspect_err(|e| obs.record_error(e))?;
                    Self::merge_dynamic_properties(&mut query_result, &property_map);
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
}
