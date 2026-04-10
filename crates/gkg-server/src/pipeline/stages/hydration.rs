use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::ProfilingConfig;
use query_engine::compiler::{HydrationPlan, InputNode, compile_input};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::RedactionOutput;
use query_engine::shared::content::{
    ColumnResolverRegistry, EntityVirtualColumns, PropertyMap, ResolverContext,
    resolve_virtual_columns,
};
use query_engine::shared::hydration as hydration_helpers;
use query_engine::shared::{
    DebugQuery, HydrationOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};

#[derive(Clone)]
pub struct HydrationStage;

impl HydrationStage {
    fn client(ctx: &QueryPipelineContext) -> Result<&Arc<ArrowClickHouseClient>, PipelineError> {
        ctx.server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))
    }

    /// Build a [`ResolverContext`] and call the shared resolution loop.
    async fn resolve(
        ctx: &QueryPipelineContext,
        entity_virtual_columns: &[EntityVirtualColumns<'_>],
        property_map: &mut PropertyMap,
    ) -> Result<(), PipelineError> {
        let has_work = entity_virtual_columns.iter().any(|(_, vc)| !vc.is_empty());
        let registry = match ctx.server_extensions.get::<ColumnResolverRegistry>() {
            Some(r) => r,
            None if has_work => {
                return Err(PipelineError::ContentResolution(
                    "virtual columns requested but no ColumnResolverRegistry available".into(),
                ));
            }
            None => return Ok(()),
        };

        let resolver_ctx = ResolverContext {
            security_context: Some(ctx.security_context()?.clone()),
        };

        resolve_virtual_columns(
            registry,
            &resolver_ctx,
            entity_virtual_columns,
            property_map,
        )
        .await
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

        let hydration_input = hydration_helpers::build_hydration_input(nodes, total_ids);

        let compiled = compile_input(hydration_input, ctx.security_context()?, &ctx.table_prefix)
            .map_err(|e| PipelineError::Compile {
            client_safe: e.is_client_safe(),
            message: e.to_string(),
        })?;

        let rendered_sql = compiled.base.render();
        let debug = DebugQuery {
            sql: compiled.base.sql.clone(),
            rendered: rendered_sql.clone(),
        };

        let profiling_id = if profiling.enabled {
            Some(uuid::Uuid::new_v4().to_string())
        } else {
            None
        };

        let start = Instant::now();
        let mut query = client.query(&compiled.base.sql);
        if let Some(ref pid) = profiling_id {
            let log_comment = format!("gkg;hydration;profiling_id={pid}");
            query = query.with_setting("log_comment", log_comment);
        }
        for (key, param) in &compiled.base.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let (batches, summary) = query
            .fetch_arrow_with_summary()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))?;
        let elapsed = start.elapsed();
        let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

        let stats = super::execution::apply_summary(
            QueryExecutionStats {
                result_rows,
                elapsed_ns: elapsed.as_nanos() as u64,
                ..Default::default()
            },
            summary.as_ref(),
        );

        let mut execution = QueryExecution {
            label: "hydration:dynamic".into(),
            rendered_sql,
            query_id: String::new(),
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats,
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };

        if let Some(ref pid) = profiling_id {
            if profiling.explain {
                execution.explain_plan = client.explain_plan(&debug.rendered).await.ok();
            }
            if let Ok(Some(entry)) = client.fetch_query_log(pid).await {
                execution.query_id = entry.query_id.clone();
                execution.stats.read_rows = entry.read_rows;
                execution.stats.read_bytes = entry.read_bytes;
                execution.stats.result_rows = entry.result_rows;
                execution.stats.result_bytes = entry.result_bytes;
                execution.stats.memory_usage = entry.memory_usage as i64;
            }
        }

        let props = hydration_helpers::parse_hydration_batches(&batches)?;
        Ok((props, vec![debug], vec![execution]))
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
                let (nodes, ids_count) =
                    hydration_helpers::hydrate_static(templates, &query_result)?;
                let (property_map, debug, executions) =
                    Self::execute_hydration(ctx, nodes, ids_count)
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
                let mut property_map = property_map;
                let entity_virtuals: Vec<EntityVirtualColumns<'_>> = templates
                    .iter()
                    .map(|t| (t.entity_type.as_str(), t.virtual_columns.as_slice()))
                    .collect();
                Self::resolve(ctx, &entity_virtuals, &mut property_map).await?;

                hydration_helpers::strip_injected_columns(
                    &mut property_map,
                    templates
                        .iter()
                        .map(|t| (t.entity_type.as_str(), &t.injected_columns)),
                );

                if !property_map.is_empty() {
                    hydration_helpers::merge_static_properties(
                        &mut query_result,
                        &property_map,
                        templates,
                    );
                }
            }
            HydrationPlan::Dynamic(entity_specs) => {
                let refs = hydration_helpers::extract_dynamic_refs(&query_result);
                if !refs.is_empty() {
                    let (nodes, ids_count) =
                        hydration_helpers::hydrate_dynamic(entity_specs, &refs)?;
                    let (property_map, debug, executions) =
                        Self::execute_hydration(ctx, nodes, ids_count)
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
                    let mut property_map = property_map;
                    let entity_virtuals: Vec<EntityVirtualColumns<'_>> = entity_specs
                        .iter()
                        .map(|s| (s.entity_type.as_str(), s.virtual_columns.as_slice()))
                        .collect();
                    Self::resolve(ctx, &entity_virtuals, &mut property_map).await?;

                    hydration_helpers::strip_injected_columns(
                        &mut property_map,
                        entity_specs
                            .iter()
                            .map(|s| (s.entity_type.as_str(), &s.injected_columns)),
                    );

                    hydration_helpers::merge_dynamic_properties(&mut query_result, &property_map);
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
