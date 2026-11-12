use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use orbit_log_fields::warn;
use query_engine::compiler::{
    HydrationArm, HydrationCompileOptions, HydrationPlan, HydrationRequest, QueryType,
};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::RedactionOutput;
use query_engine::shared::content::{
    ColumnResolverRegistry, EntityVirtualColumns, PropertyMap, ResolverContext,
    resolve_virtual_columns,
};
use query_engine::shared::hydration as hydration_helpers;
use query_engine::shared::{DebugQuery, HydrationOutput, QueryExecution, QueryExecutionLog};
use query_engine::types::QueryResult;

#[derive(Clone)]
pub struct HydrationStage;

impl HydrationStage {
    /// Reads the key properties the row checks need that the base query did not return.
    pub async fn resolve_keys(
        ctx: &mut QueryPipelineContext,
        query_result: &mut QueryResult,
    ) -> Result<(), PipelineError> {
        let lookups = query_result.key_lookups();
        if lookups.is_empty() {
            return Ok(());
        }
        let data_model = ctx
            .server_extensions
            .get::<Arc<query_data_model::Catalog<query_data_model::ClickHouse>>>()
            .ok_or_else(|| PipelineError::custom("query data model missing"))?;
        let limits = ctx.limits()?;
        let grants = ctx.grants()?;
        let mut arms = Vec::new();
        for mut lookup in lookups {
            if lookup.ids.len() > limits.hydration_rows {
                warn!(
                    safe(ENTITY) = lookup.entity.as_str(),
                    safe(COUNT) = lookup.ids.len(),
                    safe(LIMIT) = limits.hydration_rows,
                    "key lookup is capped, so nodes past the limit are redacted"
                );
                lookup.ids.truncate(limits.hydration_rows);
            }
            let scope =
                hydration_helpers::granted_paths(grants, data_model.model(), &lookup.entity)?;
            arms.push(hydration_helpers::hydration_arm(
                data_model.model(),
                limits,
                &lookup.entity,
                &lookup.ids,
                &lookup.properties,
                scope,
            )?);
        }

        let (property_map, _, executions) = Self::execute_hydration(ctx, arms, "owner").await?;
        ctx.phases
            .get_or_insert_default::<QueryExecutionLog>()
            .0
            .extend(executions);
        query_result.set_lookup_keys(&property_map);
        Ok(())
    }

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
        let requested_rows = ctx.compiled()?.shape.limit as usize;

        resolve_virtual_columns(
            registry,
            &resolver_ctx,
            entity_virtual_columns,
            property_map,
            requested_rows,
        )
        .await
    }

    async fn execute_hydration(
        ctx: &QueryPipelineContext,
        arms: Vec<HydrationArm>,
        kind: &str,
    ) -> Result<(PropertyMap, Vec<DebugQuery>, Vec<QueryExecution>), PipelineError> {
        if arms.is_empty() {
            return Ok((HashMap::new(), Vec::new(), Vec::new()));
        }
        let options = HydrationCompileOptions {
            dynamic: matches!(
                ctx.compiled()?.query_type,
                QueryType::Neighbors | QueryType::PathFinding
            ),
            path_segment_budget: Some(ctx.limits()?.hydration_path_segments),
        };
        let request = HydrationRequest { arms, options };
        let (query, executed) = super::dag::hydrate(ctx, request, kind).await?;
        let debug = match ctx.compiled()?.debug {
            true => vec![DebugQuery {
                sql: query.sql.clone(),
                rendered: query.render(),
            }],
            false => Vec::new(),
        };
        let data_model = ctx
            .server_extensions
            .get::<Arc<query_data_model::Catalog<query_data_model::ClickHouse>>>()
            .ok_or_else(|| PipelineError::custom("query data model missing"))?;
        let props =
            hydration_helpers::parse_hydration_batches(&executed.batches, data_model.model())?;
        Ok((props, debug, executed.log))
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
        let input = ctx
            .phases
            .remove::<RedactionOutput>()
            .ok_or_else(|| PipelineError::Execution("RedactionOutput not found in phases".into()))
            .inspect_err(|e| obs.record_error(e))?;
        let t = Instant::now();
        let mut query_result = input.query_result;
        let redacted_count = input.redacted_count;
        let result_context = query_result.ctx().clone();
        let mut hydration_queries = Vec::new();
        let hydration_plan = ctx
            .compiled()
            .inspect_err(|e| obs.record_error(e))?
            .hydration
            .clone();

        match &hydration_plan {
            HydrationPlan::None => {}
            HydrationPlan::Static(templates) => {
                let data_model = ctx
                    .server_extensions
                    .get::<Arc<query_data_model::Catalog<query_data_model::ClickHouse>>>()
                    .ok_or_else(|| PipelineError::custom("query data model missing"))
                    .inspect_err(|e| obs.record_error(e))?;
                let limits = ctx.limits().inspect_err(|e| obs.record_error(e))?;
                let arms = hydration_helpers::hydrate_static(
                    data_model.model(),
                    limits,
                    templates,
                    &query_result,
                )
                .inspect_err(|e| obs.record_error(e))?;
                let (property_map, debug, executions) =
                    Self::execute_hydration(ctx, arms, "static")
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
                Self::resolve(ctx, &entity_virtuals, &mut property_map)
                    .await
                    .inspect_err(|e| obs.record_error(e))?;

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
                hydration_helpers::apply_virtual_filters_static(&mut query_result, templates);
                hydration_helpers::strip_filter_injected_static(&mut query_result, templates);
            }
            HydrationPlan::Dynamic(entity_specs) => {
                let static_nodes: Vec<_> = result_context.nodes().cloned().collect();
                let refs = hydration_helpers::extract_dynamic_refs(&query_result, &static_nodes);
                if !refs.is_empty() {
                    let grants = ctx.grants().inspect_err(|e| obs.record_error(e))?;
                    let ref_paths =
                        hydration_helpers::collect_dynamic_ref_paths(&query_result, &static_nodes);
                    let data_model = ctx
                        .server_extensions
                        .get::<Arc<query_data_model::Catalog<query_data_model::ClickHouse>>>()
                        .ok_or_else(|| PipelineError::custom("query data model missing"))
                        .inspect_err(|e| obs.record_error(e))?;
                    let limits = ctx.limits().inspect_err(|e| obs.record_error(e))?;
                    let arms = hydration_helpers::hydrate_dynamic(
                        data_model.model(),
                        limits,
                        entity_specs,
                        &refs,
                        grants,
                        &ref_paths,
                    )
                    .inspect_err(|e| obs.record_error(e))?;
                    let (property_map, debug, executions) =
                        Self::execute_hydration(ctx, arms, "dynamic")
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
                    Self::resolve(ctx, &entity_virtuals, &mut property_map)
                        .await
                        .inspect_err(|e| obs.record_error(e))?;

                    hydration_helpers::strip_injected_columns(
                        &mut property_map,
                        entity_specs
                            .iter()
                            .map(|s| (s.entity_type.as_str(), &s.injected_columns)),
                    );

                    hydration_helpers::merge_dynamic_properties(&mut query_result, &property_map);
                    hydration_helpers::merge_static_node_properties(
                        &mut query_result,
                        &property_map,
                        &static_nodes,
                    );
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
