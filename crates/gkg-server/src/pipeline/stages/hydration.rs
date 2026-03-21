use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ProfilingConfig};
use futures::future::try_join_all;
use query_engine::compiler::{DynamicColumnMode, HydrationPlan, HydrationTemplate, compile};

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

const CONSOLIDATED_ENTITY_TYPE_COL: &str = "_gkg_entity_type";
const CONSOLIDATED_PROPS_COL: &str = "_gkg_props";

#[derive(Clone)]
pub struct HydrationStage;

impl HydrationStage {
    fn client(ctx: &QueryPipelineContext) -> Result<&Arc<ArrowClickHouseClient>, PipelineError> {
        ctx.server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))
    }

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

    /// Consolidated dynamic hydration: builds a single UNION ALL query across all
    /// entity types, bypassing the full compilation pipeline. Each arm uses
    /// `id IN (...)` for primary-key point lookups and `Map(String, String)` for
    /// uniform column alignment.
    async fn hydrate_dynamic_consolidated(
        ctx: &QueryPipelineContext,
        refs: &HashMap<String, Vec<i64>>,
    ) -> Result<(PropertyMap, Vec<DebugQuery>, Vec<QueryExecution>), PipelineError> {
        let client = Self::client(ctx)?;
        let profiling = ctx
            .server_extensions
            .get::<ProfilingConfig>()
            .cloned()
            .unwrap_or_default();
        let security = ctx.security_context()?;
        let input = &ctx.compiled()?.input;

        let mut arms: Vec<String> = Vec::new();

        for (entity_type, ids) in refs {
            if ids.is_empty() {
                continue;
            }
            let node = match ctx.ontology.get_node(entity_type) {
                Some(n) => n,
                None => continue,
            };

            let columns = match input.options.dynamic_columns {
                DynamicColumnMode::All => node
                    .fields
                    .iter()
                    .filter(|f| f.name != "_version" && f.name != "_deleted")
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>(),
                DynamicColumnMode::Default => {
                    if node.default_columns.is_empty() {
                        continue;
                    }
                    node.default_columns.iter().map(|s| s.as_str()).collect()
                }
            };

            let id_list = ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            let map_entries = columns
                .iter()
                .filter(|&&c| c != "id")
                .map(|&col| format!("'{col}', toString({col})"))
                .collect::<Vec<_>>()
                .join(", ");

            let table = &node.destination_table;

            let traversal_filter =
                if node.has_traversal_path && !security.traversal_paths.is_empty() {
                    let path = &security.traversal_paths[0];
                    format!(" AND startsWith(traversal_path, '{path}')")
                } else {
                    String::new()
                };

            let limit = ids.len().min(MAX_DYNAMIC_HYDRATION_RESULTS);

            arms.push(format!(
                "SELECT id, '{entity_type}' AS {CONSOLIDATED_ENTITY_TYPE_COL}, \
                 toJSONString(map({map_entries})) AS {CONSOLIDATED_PROPS_COL} \
                 FROM {table} \
                 WHERE id IN ({id_list}){traversal_filter} \
                 LIMIT {limit}"
            ));
        }

        if arms.is_empty() {
            return Ok((HashMap::new(), Vec::new(), Vec::new()));
        }

        let sql = arms.join(" UNION ALL ");

        let debug = DebugQuery {
            sql: sql.clone(),
            rendered: sql.clone(),
        };

        let (batches, execution) = if profiling.enabled {
            let t = Instant::now();
            let (batches, query_stats) = client
                .profiler()
                .execute_with_stats(&sql, &[], &[])
                .await
                .map_err(|e| PipelineError::Execution(e.to_string()))?;
            let elapsed = t.elapsed();

            let mut execution = QueryExecution {
                label: "hydration:consolidated".into(),
                rendered_sql: sql.clone(),
                query_id: query_stats.query_id.clone(),
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
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

            if profiling.explain {
                execution.explain_plan = client.profiler().explain_plan(&sql).await.ok();
            }

            (batches, execution)
        } else {
            let t = Instant::now();
            let batches = client
                .query(&sql)
                .fetch_arrow()
                .await
                .map_err(|e| PipelineError::Execution(e.to_string()))?;
            let elapsed = t.elapsed();
            let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

            let execution = QueryExecution {
                label: "hydration:consolidated".into(),
                rendered_sql: sql,
                query_id: String::new(),
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                stats: QueryExecutionStats {
                    result_rows,
                    elapsed_ns: elapsed.as_nanos() as u64,
                    ..Default::default()
                },
                explain_plan: None,
                explain_pipeline: None,
                query_log: None,
                processors: None,
            };

            (batches, execution)
        };

        let props = Self::parse_consolidated_batches(&batches)?;
        Ok((props, vec![debug], vec![execution]))
    }

    fn parse_consolidated_batches(batches: &[RecordBatch]) -> Result<PropertyMap, PipelineError> {
        let mut result = HashMap::new();

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let Some(id) = ArrowUtils::get_column::<Int64Type>(batch, "id", row_idx) else {
                    continue;
                };

                let row_data = ArrowUtils::extract_row(batch, row_idx);

                let entity_type = row_data
                    .iter()
                    .find(|(name, _)| name.as_str() == CONSOLIDATED_ENTITY_TYPE_COL)
                    .and_then(|(_, v)| v.as_string().cloned());

                let Some(entity_type) = entity_type else {
                    continue;
                };

                let props = row_data
                    .iter()
                    .find(|(name, _)| name.as_str() == CONSOLIDATED_PROPS_COL)
                    .and_then(|(_, v)| v.as_string())
                    .and_then(|json_str| {
                        serde_json::from_str::<HashMap<String, String>>(json_str).ok()
                    })
                    .map(|m| {
                        m.into_iter()
                            .map(|(k, v)| (k, ColumnValue::String(v)))
                            .collect()
                    })
                    .unwrap_or_default();

                result.insert((entity_type, id), props);
            }
        }

        Ok(result)
    }

    async fn compile_and_fetch(
        ctx: &QueryPipelineContext,
        entity_type: &str,
        query_json: String,
    ) -> Result<(PropertyMap, DebugQuery, QueryExecution), PipelineError> {
        let client = Self::client(ctx)?;
        let profiling = ctx
            .server_extensions
            .get::<ProfilingConfig>()
            .cloned()
            .unwrap_or_default();
        let compiled = compile(&query_json, &ctx.ontology, ctx.security_context()?)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let rendered_sql = compiled.base.render();
        let debug = DebugQuery {
            sql: compiled.base.sql.clone(),
            rendered: rendered_sql.clone(),
        };
        let label = format!("hydration:{entity_type}");

        let (batches, execution) = if profiling.enabled {
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
            let elapsed = t.elapsed();

            let mut execution = QueryExecution {
                label,
                rendered_sql,
                query_id: query_stats.query_id.clone(),
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
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

            if profiling.explain {
                execution.explain_plan = client.profiler().explain_plan(&debug.rendered).await.ok();
            }

            (batches, execution)
        } else {
            let t = std::time::Instant::now();
            let mut query = client.query(&compiled.base.sql);
            for (key, param) in &compiled.base.params {
                query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
            }
            let batches = query
                .fetch_arrow()
                .await
                .map_err(|e| PipelineError::Execution(e.to_string()))?;
            let elapsed = t.elapsed();
            let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

            let execution = QueryExecution {
                label,
                rendered_sql,
                query_id: String::new(),
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                stats: QueryExecutionStats {
                    result_rows,
                    elapsed_ns: elapsed.as_nanos() as u64,
                    ..Default::default()
                },
                explain_plan: None,
                explain_pipeline: None,
                query_log: None,
                processors: None,
            };

            (batches, execution)
        };

        let props = Self::parse_property_batches(entity_type, &batches)?;
        Ok((props, debug, execution))
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
                    let (property_map, debug, executions) =
                        Self::hydrate_dynamic_consolidated(ctx, &refs)
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
