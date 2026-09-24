//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use std::sync::Arc;

use ontology::Ontology;
use orbit_server_config::QueryConfig;

/// Pathfinding hard ceilings. Config can tighten but never exceed these.
/// Kept in sync with config/default.yaml `path_finding:` block.
const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360; // 15 GiB
const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 100_000;

use crate::ast::{Node, Query, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::{
    check, codegen, cursor, enforce, hydrate, logical_v3, lower_v3, normalize, physical_v3,
    restrict, security, settings, validate,
};

#[derive(Debug, Clone)]
struct QueryPlan {
    physical: Option<PhysicalPlan>,
    node_sources: std::collections::HashMap<String, (String, String)>,
    hop_count: usize,
    has_semi_joins: bool,
    explain: String,
}

#[derive(Debug, Clone)]
enum PhysicalPlan {
    ClickHouse(physical_v3::PhysicalPlan<physical_v3::ClickHouse>),
    DuckDb(physical_v3::PhysicalPlan<physical_v3::DuckDb>),
}
use crate::types::SecurityContext;

fn require<T>(opt: Option<T>, field: &str) -> Result<T> {
    opt.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

compiler_pipeline_macros::define_compiler_ctx! {
    env {
        pub ontology: Arc<Ontology>,
        pub security_ctx: SecurityContext,
    }

    state {
        pub raw: String,
        pub input: Input,
        pub query_plan: QueryPlan,
        pub node: Node,
        pub result_ctx: ResultContext,
        pub query_config: QueryConfig,
        pub hydration_plan: HydrationPlan,
        pub output: CompiledQueryContext,
    }

    phases {
        json_dsl_parse {
            reads_env: [ontology]
            mutates: [raw, input]
        }
        gql_parse {
            mutates: [raw, input]
        }
        validate {
            reads_env: [ontology]
            mutates: [input]
        }
        validate_local {
            reads_env: [ontology]
            mutates: [input]
        }
        normalize {
            reads_env: [ontology]
            mutates: [input]
        }
        restrict {
            reads_env: [ontology, security_ctx]
            mutates: [input]
        }
        plan_clickhouse {
            mutates: [input, query_plan]
        }
        plan_duckdb {
            mutates: [input, query_plan]
        }
        lower {
            reads_state: [input]
            mutates: [query_plan, node]
        }
        enforce {
            reads_state: [input]
            mutates: [query_plan, node, result_ctx]
        }
        security {
            reads_env: [security_ctx, ontology]
            reads_state: [input]
            mutates: [node]
        }
        cursor {
            mutates: [input, node]
        }
        check {
            reads_env: [security_ctx, ontology]
            reads_state: [node]
        }
        hydrate_plan {
            reads_env: [ontology, security_ctx]
            reads_state: [input, node]
            mutates: [hydration_plan]
        }
        settings {
            reads_state: [input, node]
            mutates: [query_plan, query_config]
        }
        codegen {
            reads_state: [node, input, query_plan]
            mutates: [result_ctx, query_config, hydration_plan, output]
        }
        duckdb_codegen {
            reads_state: [node, input, query_plan]
            mutates: [result_ctx, hydration_plan, output]
        }
    }

    pipelines {
        clickhouse_json_dsl {
            env: [ontology, security_ctx]
            state: [raw, input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            phases: [json_dsl_parse, validate, normalize, restrict, plan_clickhouse, lower, enforce, security, cursor, check, hydrate_plan, settings, codegen]
        }
        clickhouse_gql {
            env: [ontology, security_ctx]
            state: [raw, input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            phases: [gql_parse, validate, normalize, restrict, plan_clickhouse, lower, enforce, security, cursor, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            phases: [restrict, plan_clickhouse, lower, enforce, settings, codegen]
        }
        duckdb_json_dsl {
            env: [ontology]
            state: [raw, input, query_plan, node, result_ctx, hydration_plan, output]
            phases: [json_dsl_parse, validate_local, normalize, plan_duckdb, lower, enforce, duckdb_codegen]
        }
        duckdb_gql {
            env: [ontology]
            state: [raw, input, query_plan, node, result_ctx, hydration_plan, output]
            phases: [gql_parse, validate_local, normalize, plan_duckdb, lower, enforce, duckdb_codegen]
        }
        validate_normalize {
            env: [ontology]
            state: [raw, input]
            phases: [json_dsl_parse, validate, normalize]
        }
    }
}

fn json_dsl_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let raw = require(ctx.take_raw(), "raw")?;
    ctx.set_input(frontend::json_dsl::parse(&raw, ctx.ontology())?);
    Ok(())
}

fn gql_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    if let Some(raw) = ctx.take_raw() {
        ctx.set_input(frontend::gql::parse(&raw)?);
    }
    Ok(())
}

fn validate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(ctx.take_input(), "input")?;
    let v = validate::Validator::new(ctx.ontology());
    v.check_shape(&input)?;
    if let Some(c) = &mut input.cursor
        && let Some(after) = &c.after
    {
        if input.compiler.query_hash == 0 {
            return Err(QueryError::PaginationError(
                "cursor binding requires a query hash from the frontend".into(),
            ));
        }
        c.seek = Some(cursor::decode(after, input.compiler.query_hash)?);
    }
    v.check_references(&input)?;
    v.annotate_filter_types(&mut input);
    ctx.set_input(input);
    Ok(())
}

fn validate_local(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(ctx.take_input(), "input")?;
    let v =
        validate::Validator::new(ctx.ontology()).with_skip(validate::Skip { selectivity: true });
    v.check_shape(&input)?;
    v.check_references(&input)?;
    v.annotate_filter_types(&mut input);
    ctx.set_input(input);
    Ok(())
}

fn normalize(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    ctx.set_input(normalize::normalize(input, ctx.ontology())?);
    Ok(())
}

fn restrict(ctx: &mut impl CompilerCtx) -> Result<()> {
    let ontology = ctx.ontology().clone();
    let security_ctx = ctx.security_ctx().clone();
    let mut input = require(ctx.take_input(), "input")?;
    restrict::restrict(&mut input, &ontology, &security_ctx)?;
    ctx.set_input(input);
    Ok(())
}

fn plan_clickhouse(ctx: &mut impl CompilerCtx) -> Result<()> {
    plan_for(ctx, crate::Backend::ClickHouse)
}

fn plan_duckdb(ctx: &mut impl CompilerCtx) -> Result<()> {
    plan_for(ctx, crate::Backend::DuckDb)
}

fn plan_for(ctx: &mut impl CompilerCtx, backend: crate::Backend) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let logical = logical_v3::plan(&input);
    let catalog = physical_v3::PhysicalCatalog::new(&logical, &input, ctx.ontology());
    let (physical, node_sources) = match backend {
        crate::Backend::ClickHouse => {
            let plan = <physical_v3::ClickHouse as physical_v3::Backend>::optimize(
                physical_v3::plan_clickhouse(logical.clone(), &catalog),
                &catalog,
            );
            (PhysicalPlan::ClickHouse(plan.clone()), catalog.node_sources(&plan))
        }
        crate::Backend::DuckDb => {
            let plan = physical_v3::plan_duckdb(logical.clone(), &catalog);
            (PhysicalPlan::DuckDb(plan.clone()), catalog.node_sources(&plan))
        }
    };
    let query_plan = QueryPlan {
        node_sources,
        hop_count: input.relationships.len(),
        has_semi_joins: false,
        explain: logical.root.explain(),
        physical: Some(physical),
    };
    ctx.set_input(input);
    ctx.set_query_plan(query_plan);
    Ok(())
}

fn lower(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let physical = query_plan
        .physical
        .take()
        .ok_or_else(|| QueryError::PipelineInvariant("physical plan not set".into()))?;
    let node = match physical {
        PhysicalPlan::ClickHouse(plan) => lower_v3::clickhouse(plan)?,
        PhysicalPlan::DuckDb(plan) => lower_v3::duckdb(plan)?,
    };
    ctx.set_query_plan(query_plan);
    ctx.set_node(node);
    Ok(())
}

fn enforce(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let node_edge_col = query_plan.node_sources.clone();
    ctx.set_query_plan(query_plan);
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let result_context = enforce::enforce_return(&mut node, &input, &node_edge_col)?;
    ctx.set_node(node);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn security(ctx: &mut impl CompilerCtx) -> Result<()> {
    let security_ctx = ctx.security_ctx().clone();
    let ontology = ctx.ontology().clone();
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().as_ref(), "input")?;
    let security_ctx = security_ctx.with_scope_prefixes(input.compiler.scope_prefixes.clone());
    security::apply_security_context(&mut node, &security_ctx, &ontology)?;
    ctx.set_node(node);
    Ok(())
}

fn cursor(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(ctx.take_input(), "input")?;
    let mut node = require(ctx.take_node(), "node")?;
    cursor::apply(&mut node, &mut input)?;
    ctx.set_input(input);
    ctx.set_node(node);
    Ok(())
}

fn check(ctx: &mut impl CompilerCtx) -> Result<()> {
    let node = require(ctx.node().clone(), "node")?;
    check::check_ast(&node, ctx.security_ctx(), ctx.ontology())
}

fn hydrate_plan(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    let emitted = require(ctx.node().as_ref(), "node")?;
    let plan = hydrate::generate_hydration_plan(input, emitted, ctx.ontology(), ctx.security_ctx());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().clone(), "input")?;
    let query_type: &str = input.query_type.into();
    let mut config = settings::resolve(query_type);

    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let node = require(ctx.node().clone(), "node")?;
    if let Node::Query(q) = &node {
        let derived = &mut config.compiler_derived;
        derived.enable_materialized_cte = q.ctes.iter().any(|c| c.materialized);
        derived.optimize_move_to_prewhere_if_final =
            scans_final(q) || q.ctes.iter().any(|c| scans_final(&c.query));
        if !q.ctes.is_empty() || query_plan.has_semi_joins {
            derived.use_index_for_in_with_subqueries_max_values =
                Some(IN_SUBQUERY_INDEX_MAX_VALUES);
        }
    }

    if query_plan.hop_count >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    // Pathfinding safety net: enforce hard limits on fan-out-prone queries
    // regardless of config. These are compiler-side floors; the config can
    // only tighten them further.
    if input.query_type == QueryType::PathFinding {
        if config.max_execution_time.is_none()
            || config.max_execution_time > Some(PATHFINDING_MAX_EXECUTION_TIME)
        {
            config.max_execution_time = Some(PATHFINDING_MAX_EXECUTION_TIME);
        }
        if config.max_memory_usage.is_none()
            || config.max_memory_usage > Some(PATHFINDING_MAX_MEMORY_USAGE)
        {
            config.max_memory_usage = Some(PATHFINDING_MAX_MEMORY_USAGE);
        }
    }
    ctx.set_query_plan(query_plan);
    ctx.set_query_config(config);
    Ok(())
}

fn scans_final(q: &Query) -> bool {
    matches!(q.from, TableRef::Scan { final_: true, .. })
}

fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.node().clone(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let base = codegen::codegen(&node, result_context, query_config)?;
    let query_type = input.query_type;
    let plan = ctx
        .query_plan()
        .as_ref()
        .map(|p| p.explain.clone())
        .unwrap_or_default();
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
        plan,
    });
    Ok(())
}

fn duckdb_codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.node().clone(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let base = codegen::duckdb::codegen(&node, result_context)?;
    let query_type = input.query_type;
    let plan = ctx
        .query_plan()
        .as_ref()
        .map(|p| p.explain.clone())
        .unwrap_or_default();
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
        plan,
    });
    Ok(())
}
