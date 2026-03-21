use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clickhouse_client::profiler::QueryProfiler;
use compiler::{CompiledQueryContext, HydrationPlan, SecurityContext, compile};
use ontology::Ontology;
use shared::{QueryExecution, QueryExecutionStats};
use types::{QueryResult, ResourceAuthorization};

pub struct ProfilerOptions {
    pub explain: bool,
    pub profile: bool,
    pub processors: bool,
    pub settings: Vec<(String, String)>,
}

pub struct ProfilerResult {
    pub compiled: Arc<CompiledQueryContext>,
    pub executions: Vec<QueryExecution>,
    pub result_rows: usize,
}

pub async fn execute_profiled_query(
    profiler: &QueryProfiler,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
    query_json: &str,
    opts: &ProfilerOptions,
) -> Result<ProfilerResult> {
    let compiled =
        Arc::new(compile(query_json, ontology, security_ctx).context("query compilation failed")?);

    let mut executions = Vec::new();
    let rendered_sql = compiled.base.render();

    let extra_settings: Vec<(&str, &str)> = opts
        .settings
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let t = Instant::now();
    let (batches, query_stats) = profiler
        .execute_with_stats(&rendered_sql, &[], &extra_settings)
        .await
        .context("base query execution failed")?;

    let mut base_exec = build_execution("base", &rendered_sql, &query_stats, t.elapsed());
    enrich_execution(profiler, &mut base_exec, &rendered_sql, opts).await;
    executions.push(base_exec);

    let mut query_result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let checks = query_result.resource_checks();
    let authorizations: Vec<ResourceAuthorization> = checks
        .iter()
        .map(|check| ResourceAuthorization {
            resource_type: check.resource_type.clone(),
            authorized: check.ids.iter().map(|id| (*id, true)).collect(),
        })
        .collect();
    query_result.apply_authorizations(&authorizations);

    let result_rows = query_result.authorized_count();

    match &compiled.hydration {
        HydrationPlan::None => {}
        HydrationPlan::Static(templates) => {
            for template in templates {
                let ids: Vec<i64> = query_result
                    .authorized_rows()
                    .filter_map(|row| {
                        row.get_column_i64(&compiler::constants::redaction_id_column(
                            &template.node_alias,
                        ))
                    })
                    .collect();

                if ids.is_empty() {
                    continue;
                }

                let hydration_json = template.with_ids(&ids);
                let hydration_compiled = compile(&hydration_json, ontology, security_ctx)
                    .context("hydration compilation failed")?;

                let label = format!("hydration:{}", template.entity_type);
                let exec =
                    execute_and_profile(profiler, &label, &hydration_compiled.base.render(), opts)
                        .await?;
                executions.push(exec);
            }
        }
        HydrationPlan::Dynamic => {
            let mut refs: HashMap<String, Vec<i64>> = HashMap::new();
            for row in query_result.authorized_rows() {
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

            for (entity_type, ids) in &refs {
                if ids.is_empty() {
                    continue;
                }

                let node = ontology
                    .get_node(entity_type)
                    .context(format!("entity type not found: {entity_type}"))?;

                let columns = if node.default_columns.is_empty() {
                    serde_json::json!("*")
                } else {
                    serde_json::json!(node.default_columns)
                };

                let hydration_json = serde_json::json!({
                    "query_type": "search",
                    "node": {
                        "id": "h",
                        "entity": entity_type,
                        "columns": columns,
                        "node_ids": ids
                    },
                    "limit": ids.len().min(1000)
                })
                .to_string();

                let hydration_compiled = compile(&hydration_json, ontology, security_ctx)
                    .context("dynamic hydration compilation failed")?;

                let label = format!("hydration:{entity_type}");
                let exec =
                    execute_and_profile(profiler, &label, &hydration_compiled.base.render(), opts)
                        .await?;
                executions.push(exec);
            }
        }
    }

    Ok(ProfilerResult {
        compiled,
        executions,
        result_rows,
    })
}

async fn execute_and_profile(
    profiler: &QueryProfiler,
    label: &str,
    rendered_sql: &str,
    opts: &ProfilerOptions,
) -> Result<QueryExecution> {
    let extra_settings: Vec<(&str, &str)> = opts
        .settings
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let t = Instant::now();
    let (_batches, query_stats) = profiler
        .execute_with_stats(rendered_sql, &[], &extra_settings)
        .await
        .context(format!("{label} execution failed"))?;

    let mut exec = build_execution(label, rendered_sql, &query_stats, t.elapsed());
    enrich_execution(profiler, &mut exec, rendered_sql, opts).await;
    Ok(exec)
}

fn build_execution(
    label: &str,
    rendered_sql: &str,
    stats: &clickhouse_client::QueryStats,
    elapsed: std::time::Duration,
) -> QueryExecution {
    QueryExecution {
        label: label.into(),
        rendered_sql: rendered_sql.into(),
        query_id: stats.query_id.clone(),
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        stats: QueryExecutionStats {
            read_rows: stats.read_rows,
            read_bytes: stats.read_bytes,
            result_rows: stats.result_rows,
            result_bytes: stats.result_bytes,
            elapsed_ns: stats.elapsed_ns,
            memory_usage: stats.memory_usage,
        },
        explain_plan: None,
        explain_pipeline: None,
        query_log: None,
        processors: None,
    }
}

async fn enrich_execution(
    profiler: &QueryProfiler,
    exec: &mut QueryExecution,
    rendered_sql: &str,
    opts: &ProfilerOptions,
) {
    if opts.explain {
        exec.explain_plan = profiler.explain_plan(rendered_sql).await.ok();
        exec.explain_pipeline = profiler.explain_pipeline(rendered_sql).await.ok();
    }

    if opts.profile
        && let Ok(Some(entry)) = profiler.fetch_query_log(&exec.query_id).await
    {
        exec.query_log = Some(serde_json::to_value(&entry).unwrap_or_default());
    }

    if opts.processors
        && let Ok(profiles) = profiler.fetch_processors_profile(&exec.query_id).await
        && !profiles.is_empty()
    {
        exec.processors = Some(serde_json::to_value(&profiles).unwrap_or_default());
    }
}
