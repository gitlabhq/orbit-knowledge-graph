use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use futures::future::try_join_all;
use query_engine::compiler::{
    DynamicColumnMode, HydrationPlan, HydrationTemplate, QueryType, compile,
};

use gkg_utils::arrow::{ArrowUtils, ColumnValue};
use query_engine::types::QueryResult;

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::RedactionOutput;
use query_engine::shared::{
    DebugQuery, HydrationOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};

use query_engine::compiler::constants::{
    GKG_COLUMN_PREFIX, HYDRATION_NODE_ALIAS, MAX_DYNAMIC_HYDRATION_RESULTS, redaction_id_column,
};

type PropertyMap = HashMap<(String, i64), HashMap<String, ColumnValue>>;

#[derive(Clone)]
pub struct HydrationStage;

impl HydrationStage {
    /// Retrieve the ClickHouse client from server extensions.
    fn client(ctx: &QueryPipelineContext) -> Result<&Arc<ArrowClickHouseClient>, PipelineError> {
        ctx.server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))
    }

    /// Static hydration: use pre-built templates from compile time.
    async fn hydrate_static(
        ctx: &QueryPipelineContext,
        templates: &[HydrationTemplate],
        query_result: &QueryResult,
    ) -> Result<(PropertyMap, Vec<DebugQuery>, Vec<QueryExecution>), PipelineError> {
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
        let mut debug_queries = Vec::new();
        let mut executions = Vec::new();
        for (props, debug, execution) in results {
            merged.extend(props);
            debug_queries.push(debug);
            executions.push(execution);
        }
        Ok((merged, debug_queries, executions))
    }

    /// Dynamic hydration: build search queries from scratch at runtime.
    async fn hydrate_dynamic(
        ctx: &QueryPipelineContext,
        refs: &HashMap<String, Vec<i64>>,
    ) -> Result<(PropertyMap, Vec<DebugQuery>, Vec<QueryExecution>), PipelineError> {
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
        let mut debug_queries = Vec::new();
        let mut executions = Vec::new();
        for (props, debug, execution) in results {
            merged.extend(props);
            debug_queries.push(debug);
            executions.push(execution);
        }
        Ok((merged, debug_queries, executions))
    }

    /// Compile a hydration query JSON string, execute it, and parse the results.
    async fn compile_and_fetch(
        ctx: &QueryPipelineContext,
        entity_type: &str,
        query_json: String,
    ) -> Result<(PropertyMap, DebugQuery, QueryExecution), PipelineError> {
        let client = Self::client(ctx)?;
        let compiled = compile(&query_json, &ctx.ontology, ctx.security_context()?)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let rendered_sql = compiled.base.render();
        let debug = DebugQuery {
            sql: compiled.base.sql.clone(),
            rendered: rendered_sql.clone(),
        };
        let http_params: Vec<(String, String)> = compiled
            .base
            .params
            .iter()
            .map(|(k, v)| (k.clone(), v.render_http_param()))
            .collect();

        let t = std::time::Instant::now();
        let (batches, query_stats) = client
            .profiler()
            .execute_with_stats(&compiled.base.sql, &http_params, &[])
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;

        let execution = QueryExecution {
            label: format!("hydration:{entity_type}"),
            rendered_sql,
            query_id: query_stats.query_id.clone(),
            elapsed_ms: t.elapsed().as_secs_f64() * 1000.0,
            stats: QueryExecutionStats {
                read_rows: query_stats.read_rows,
                read_bytes: query_stats.read_bytes,
                result_rows: query_stats.result_rows,
                result_bytes: query_stats.result_bytes,
                elapsed_ns: query_stats.elapsed_ns,
                memory_usage: query_stats.memory_usage,
            },
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };

        let props = Self::parse_property_batches(entity_type, &batches)?;
        Ok((props, debug, execution))
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

    /// Build a search query JSON from scratch for dynamic hydration.
    /// Reads `input.options.dynamic_columns` to decide whether to fetch all columns
    /// or only the entity's `default_columns` from the ontology.
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
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx.phases.get::<RedactionOutput>().ok_or_else(|| {
            PipelineError::Execution("RedactionOutput not found in phases".into())
        })?;
        let t = Instant::now();
        let mut query_result = input.query_result.clone();
        let redacted_count = input.redacted_count;
        let result_context = query_result.ctx().clone();
        let mut hydration_queries = Vec::new();
        let hydration_plan = ctx.compiled()?.hydration.clone();

        match &hydration_plan {
            HydrationPlan::None => {}
            HydrationPlan::Static(templates) => {
                let (property_map, debug, executions) =
                    Self::hydrate_static(ctx, templates, &query_result)
                        .await
                        .inspect_err(|e| obs.record_error(e))?;
                hydration_queries = debug;
                for exec in &executions {
                    obs.query_executed(
                        &exec.label,
                        exec.stats.read_rows,
                        exec.stats.read_bytes,
                        exec.stats.memory_usage,
                    );
                }
                ctx.phases
                    .get_or_insert_default::<QueryExecutionLog>()
                    .0
                    .extend(executions);
                if !property_map.is_empty() {
                    Self::merge_static_properties(&mut query_result, &property_map, templates);
                }
            }
            HydrationPlan::Dynamic => {
                let refs = Self::extract_dynamic_refs(&query_result);
                if !refs.is_empty() {
                    let (property_map, debug, executions) = Self::hydrate_dynamic(ctx, &refs)
                        .await
                        .inspect_err(|e| obs.record_error(e))?;
                    hydration_queries = debug;
                    for exec in &executions {
                        obs.query_executed(
                            &exec.label,
                            exec.stats.read_rows,
                            exec.stats.read_bytes,
                            exec.stats.memory_usage,
                        );
                    }
                    ctx.phases
                        .get_or_insert_default::<QueryExecutionLog>()
                        .0
                        .extend(executions);
                    Self::merge_dynamic_properties(&mut query_result, &property_map);
                }
            }
        }

        obs.hydrated(t.elapsed());
        Ok(HydrationOutput {
            query_result,
            result_context,
            redacted_count,
            hydration_queries,
        })
    }
}
