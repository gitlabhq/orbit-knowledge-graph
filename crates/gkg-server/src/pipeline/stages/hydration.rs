use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ProfilingConfig};
use query_engine::compiler::{
    ColumnSelection, DynamicEntityColumns, HydrationPlan, HydrationTemplate, Input, InputNode,
    QueryType, compile_input,
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
    HYDRATION_NODE_ALIAS, MAX_DYNAMIC_HYDRATION_RESULTS, redaction_id_column,
};

type PropertyMap = HashMap<(String, i64), HashMap<String, ColumnValue>>;

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
        let mut nodes = Vec::new();
        let mut total_ids: usize = 0;

        for template in templates {
            if template.columns.is_empty() {
                continue;
            }

            let ids = Self::collect_static_ids(query_result, template);
            if ids.is_empty() {
                continue;
            }

            total_ids += ids.len();
            nodes.push(InputNode {
                id: HYDRATION_NODE_ALIAS.to_string(),
                entity: Some(template.entity_type.clone()),
                table: Some(template.destination_table.clone()),
                columns: Some(ColumnSelection::List(template.columns.clone())),
                node_ids: ids,
                ..InputNode::default()
            });
        }

        Self::execute_hydration(ctx, nodes, total_ids).await
    }

    /// Dynamic hydration: builds an `Input` with one node per
    /// discovered entity type using pre-resolved column specs from the
    /// compilation plan. No ontology lookups at runtime.
    async fn hydrate_dynamic(
        ctx: &QueryPipelineContext,
        entity_specs: &[DynamicEntityColumns],
        refs: &HashMap<String, Vec<i64>>,
    ) -> Result<(PropertyMap, Vec<DebugQuery>, Vec<QueryExecution>), PipelineError> {
        let mut nodes = Vec::new();
        let mut total_ids: usize = 0;

        for (entity_type, ids) in refs {
            if ids.is_empty() {
                continue;
            }

            let spec = match entity_specs.iter().find(|s| s.entity_type == *entity_type) {
                Some(s) => s,
                None => continue,
            };

            if spec.columns.is_empty() {
                continue;
            }

            let capped_ids: Vec<i64> = ids
                .iter()
                .copied()
                .take(MAX_DYNAMIC_HYDRATION_RESULTS)
                .collect();
            total_ids += capped_ids.len();

            nodes.push(InputNode {
                id: HYDRATION_NODE_ALIAS.to_string(),
                entity: Some(entity_type.clone()),
                table: Some(spec.destination_table.clone()),
                columns: Some(ColumnSelection::List(spec.columns.clone())),
                node_ids: capped_ids,
                ..InputNode::default()
            });
        }

        Self::execute_hydration(ctx, nodes, total_ids).await
    }

    /// Compile a `QueryType::Hydration` input and execute the single UNION ALL
    /// query against ClickHouse. Shared by both static and dynamic hydration.
    async fn execute_hydration(
        ctx: &QueryPipelineContext,
        nodes: Vec<InputNode>,
        total_ids: usize,
    ) -> Result<(PropertyMap, Vec<DebugQuery>, Vec<QueryExecution>), PipelineError> {
        if nodes.is_empty() {
            return Ok((HashMap::new(), Vec::new(), Vec::new()));
        }

        let client = Self::client(ctx)?;
        let profiling = ctx
            .server_extensions
            .get::<ProfilingConfig>()
            .cloned()
            .unwrap_or_default();

        let hydration_input = Input {
            query_type: QueryType::Hydration,
            nodes,
            limit: total_ids as u32,
            ..Input::default()
        };

        let compiled = compile_input(hydration_input, ctx.security_context()?)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;

        let rendered_sql = compiled.base.render();
        let debug = DebugQuery {
            sql: compiled.base.sql.clone(),
            rendered: rendered_sql.clone(),
        };

        let (batches, execution) = if profiling.enabled {
            let http_params: Vec<(String, String)> = compiled
                .base
                .params
                .iter()
                .map(|(k, v)| (k.clone(), v.render_http_param()))
                .collect();

            let t = Instant::now();
            let (batches, query_stats) = client
                .profiler()
                .execute_with_stats(&compiled.base.sql, &http_params, &[])
                .await
                .map_err(|e| PipelineError::Execution(e.to_string()))?;
            let elapsed = t.elapsed();

            let mut execution = QueryExecution {
                label: "hydration:dynamic".into(),
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
            let t = Instant::now();
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
                label: "hydration:dynamic".into(),
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

        let props = Self::parse_dynamic_batches(&batches)?;
        Ok((props, vec![debug], vec![execution]))
    }

    fn parse_dynamic_batches(batches: &[RecordBatch]) -> Result<PropertyMap, PipelineError> {
        let alias = HYDRATION_NODE_ALIAS;
        let entity_type_col = format!("{alias}_entity_type");
        let props_col = format!("{alias}_props");
        let id_col = format!("{alias}_id");

        let mut result = HashMap::new();

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let Some(id) = ArrowUtils::get_column::<Int64Type>(batch, &id_col, row_idx) else {
                    continue;
                };

                let row_data = ArrowUtils::extract_row(batch, row_idx);

                let entity_type = row_data
                    .iter()
                    .find(|(name, _)| name.as_str() == entity_type_col)
                    .and_then(|(_, v)| v.as_string().cloned());

                let Some(entity_type) = entity_type else {
                    continue;
                };

                let props: HashMap<String, ColumnValue> = row_data
                    .iter()
                    .find(|(name, _)| name.as_str() == props_col)
                    .and_then(|(_, v)| v.as_string())
                    .and_then(|json_str| {
                        serde_json::from_str::<HashMap<String, serde_json::Value>>(json_str).ok()
                    })
                    .map(|m| {
                        m.into_iter()
                            .filter_map(|(k, v)| {
                                let cv = ColumnValue::from(v);
                                if cv == ColumnValue::Null {
                                    None
                                } else {
                                    Some((k, cv))
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                result.insert((entity_type, id), props);
            }
        }

        Ok(result)
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
                        // Prefix with the node alias so entity_properties("u", ..)
                        // finds "u_username" when the hydration returned "username".
                        let col_name = format!("{}_{key}", template.node_alias);
                        row.set_column(col_name, value.clone());
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
            HydrationPlan::Dynamic(entity_specs) => {
                let refs = Self::extract_dynamic_refs(&query_result);
                if !refs.is_empty() {
                    let (property_map, debug, executions) =
                        Self::hydrate_dynamic(ctx, entity_specs, &refs)
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
