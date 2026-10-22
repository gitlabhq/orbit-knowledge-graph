//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use orbit_server_config::QueryConfig;

/// Pathfinding hard ceilings. Config can tighten but never exceed these.
/// Kept in sync with config/default.yaml `path_finding:` block.
const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360; // 15 GiB
const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 1_000_000;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Node, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::lower::LoweredMetadata;
use crate::passes::plan::HydrationCompileOptions;
use crate::passes::plan::QueryPlan;
use crate::passes::{
    check, codegen, enforce, hydrate, lower, normalize, page, plan, relationships, response_policy,
    restrict, security, settings, validate,
};
use crate::types::SecurityContext;

fn require<T>(opt: Option<T>, field: &str) -> Result<T> {
    opt.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

compiler_pipeline_macros::define_compiler_ctx! {
    env {
        pub security_ctx: SecurityContext,
    }

    state {
        pub raw: String,
        pub input: Input,
        pub scope_proofs: crate::scope::QueryScope,
        pub hydration_options: HydrationCompileOptions,
        pub query_plan: QueryPlan,
        pub node: Node,
        pub lowered_metadata: LoweredMetadata,
        pub result_ctx: ResultContext,
        pub query_config: QueryConfig,
        pub hydration_plan: HydrationPlan,
        pub output: CompiledQueryContext,
    }

    phases {
        json_dsl_parse {
            reads_env: [data_model]
            mutates: [raw, input]
        }
        gql_parse {
            mutates: [raw, input]
        }
        validate_relationships {
            reads_env: [data_model]
            reads_state: [input]
        }
        validate_conditions {
            reads_env: [data_model]
            reads_state: [input]
        }
        validate {
            reads_env: [data_model]
            mutates: [input]
        }
        validate_local {
            reads_env: [data_model]
            mutates: [input]
        }
        normalize {
            reads_env: [data_model]
            mutates: [input]
        }
        restrict {
            reads_env: [data_model, security_ctx]
            mutates: [input, scope_proofs]
        }
        plan_clickhouse {
            reads_env: [data_model]
            reads_state: [hydration_options]
            mutates: [input, query_plan]
        }
        plan_duckdb {
            reads_env: [data_model]
            reads_state: [hydration_options]
            mutates: [input, query_plan]
        }
        lower {
            reads_state: [input, scope_proofs]
            mutates: [query_plan, node, lowered_metadata]
        }
        response_policy {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node]
        }
        enforce {
            reads_env: [data_model]
            reads_state: [input, scope_proofs]
            mutates: [node, lowered_metadata, result_ctx]
        }
        enforce_local {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node, lowered_metadata, result_ctx]
        }
        security {
            reads_env: [security_ctx, data_model]
            mutates: [node]
        }
        paginate {
            reads_state: [input, lowered_metadata]
            mutates: [node]
        }
        check {
            reads_env: [security_ctx, data_model]
            reads_state: [node]
        }
        hydrate_plan {
            reads_env: [security_ctx, data_model]
            reads_state: [input, node]
            mutates: [hydration_plan]
        }
        settings {
            reads_state: [input, node]
            mutates: [query_plan, query_config]
        }
        codegen {
            reads_state: [input]
            mutates: [node, result_ctx, query_config, hydration_plan, output]
        }
        duckdb_codegen {
            reads_state: [input]
            mutates: [node, result_ctx, hydration_plan, output]
        }
    }

    pipelines {
        clickhouse_json_dsl {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [json_dsl_parse, validate, normalize, restrict, plan_clickhouse, lower, response_policy, enforce, security, paginate, check, hydrate_plan, settings, codegen]
        }
        clickhouse_gql {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [gql_parse, validate, validate_relationships, normalize, restrict, plan_clickhouse, lower, response_policy, enforce, security, paginate, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [input, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [validate_conditions, restrict, plan_clickhouse, lower, response_policy, enforce, security, check, settings, codegen]
        }
        duckdb_json_dsl {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [json_dsl_parse, validate_local, normalize, plan_duckdb, lower, enforce_local, paginate, duckdb_codegen]
        }
        duckdb_gql {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [gql_parse, validate_local, validate_relationships, normalize, plan_duckdb, lower, enforce_local, paginate, duckdb_codegen]
        }
        validate_normalize_gql {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, input]
            phases: [gql_parse, validate, validate_relationships, normalize]
        }
        validate_normalize {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, input]
            phases: [json_dsl_parse, validate, normalize]
        }
    }
}

fn json_dsl_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let raw = require(ctx.take_raw(), "raw")?;
    let input = frontend::json_dsl::parse(&raw, ctx.data_model())?;
    ctx.set_input(input);
    Ok(())
}

fn gql_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    if let Some(raw) = ctx.take_raw() {
        frontend::gql::parse_query(&raw)?.load(ctx);
    }
    Ok(())
}

fn validate_conditions(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    validate::Validator::new(ctx.data_model()).check_filter_types(input)
}

fn validate_relationships(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    relationships::validate_relationships(input, ctx.data_model())
}

fn validate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let v = validate::Validator::new(ctx.data_model());
    v.check_shape(&input)?;
    v.check_references(&input)?;
    ctx.set_input(input);
    Ok(())
}

fn validate_local(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let v =
        validate::Validator::new(ctx.data_model()).with_skip(validate::Skip { selectivity: true });
    v.check_shape(&input)?;
    v.check_references(&input)?;
    ctx.set_input(input);
    Ok(())
}

fn normalize(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let input = normalize::normalize(input, ctx.data_model())?;
    ctx.set_input(input);
    Ok(())
}

fn restrict<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut input = require(ctx.take_input(), "input")?;
    let node_proofs = restrict::restrict(&mut input, ctx.data_model(), &security_ctx)?;
    let scope_proofs = crate::scope::QueryScope::new(&input, node_proofs, ctx.data_model());
    ctx.set_input(input);
    ctx.set_scope_proofs(scope_proofs);
    Ok(())
}

fn plan_clickhouse(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    plan_with(ctx, plan::plan_clickhouse)
}

fn plan_duckdb(
    ctx: &mut impl CompilerCtx<Model = query_data_model::DuckDbDataModel>,
) -> Result<()> {
    plan_with(ctx, plan::plan_duckdb)
}

fn plan_with<C>(
    ctx: &mut C,
    build: impl FnOnce(&Input, &C::Model, HydrationCompileOptions) -> Result<QueryPlan>,
) -> Result<()>
where
    C: CompilerCtx,
{
    let input = require(ctx.take_input(), "input")?;
    let hydration_options = ctx
        .hydration_options()
        .as_ref()
        .copied()
        .unwrap_or_default();
    let query_plan = build(&input, ctx.data_model(), hydration_options)?;
    ctx.set_input(input);
    ctx.set_query_plan(query_plan);
    Ok(())
}

fn lower(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let input = require(ctx.input().clone(), "input")?;
    let lowered = lower::emit(&query_plan, &input, ctx.scope_proofs().as_ref())?;
    ctx.set_query_plan(query_plan);
    ctx.set_node(lowered.ast);
    ctx.set_lowered_metadata(lowered.metadata);
    Ok(())
}

fn response_policy(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().clone(), "input")?;
    let mut node = require(ctx.take_node(), "node")?;
    response_policy::apply_text_excerpts(&mut node, &input, ctx.data_model());
    ctx.set_node(node);
    Ok(())
}

fn enforce<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let metadata = require(ctx.take_lowered_metadata(), "lowered_metadata")?;
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let scope = ctx.scope_proofs().as_ref();
    enforce::enforce_role_scans(&mut node, &input, &metadata, ctx.data_model(), scope)?;
    let result_context =
        enforce::enforce_lowered_return(&mut node, &input, &metadata, ctx.data_model(), scope)?;
    ctx.set_node(node);
    ctx.set_lowered_metadata(metadata);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn enforce_local<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let metadata = require(ctx.take_lowered_metadata(), "lowered_metadata")?;
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let result_context =
        enforce::enforce_local_return(&mut node, &input, &metadata, ctx.data_model())?;
    ctx.set_node(node);
    ctx.set_lowered_metadata(metadata);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn security<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut node = require(ctx.take_node(), "node")?;
    security::apply_security_context(&mut node, &security_ctx, ctx.data_model())?;
    ctx.set_node(node);
    Ok(())
}

fn paginate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().as_ref(), "input")?;
    let metadata = require(ctx.lowered_metadata().as_ref(), "lowered_metadata")?;
    page::apply(&mut node, input, metadata);
    ctx.set_node(node);
    Ok(())
}

fn check<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let node = require(ctx.node().clone(), "node")?;
    check::check_ast(&node, ctx.security_ctx(), ctx.data_model())
}

fn hydrate_plan<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let input = require(ctx.input().as_ref(), "input")?;
    let emitted = require(ctx.node().as_ref(), "node")?;
    let plan =
        hydrate::generate_hydration_plan(input, emitted, ctx.data_model(), ctx.security_ctx());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().clone(), "input")?;
    let query_type: &str = input.query_type.into();
    let mut config = settings::resolve(query_type);

    let node = require(ctx.node().clone(), "node")?;
    if let Node::Query(q) = &node {
        let derived = &mut config.compiler_derived;
        derived.null_in_is_unknown = input.has_negation();
        visit_queries(q, &mut |query| {
            derived.enable_materialized_cte |= query.ctes.iter().any(|cte| cte.materialized);
            visit_relations(&query.from, &mut |relation| {
                derived.optimize_move_to_prewhere_if_final |=
                    matches!(relation, TableRef::Scan { final_: true, .. });
            });
            if !query.ctes.is_empty() {
                derived.use_index_for_in_with_subqueries_max_values =
                    Some(IN_SUBQUERY_INDEX_MAX_VALUES);
            }
            Ok(())
        })?;
    }

    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    if query_plan.relations().len() >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    if query_plan.is_point_lookup() && config.use_query_cache == Some(true) {
        config.use_query_cache = Some(false);
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

fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let base = codegen::codegen(&node, result_context, query_config)?;
    let query_type = input.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        ast: node,
        base,
        hydration,
        input,
        has_virtual_columns,
    });
    Ok(())
}

fn duckdb_codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let base = codegen::duckdb::codegen(&node, result_context)?;
    let query_type = input.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        ast: node,
        base,
        hydration,
        input,
        has_virtual_columns,
    });
    Ok(())
}

fn hydration_has_virtuals(plan: &HydrationPlan) -> bool {
    match plan {
        HydrationPlan::None => false,
        HydrationPlan::Static(templates) => templates
            .iter()
            .any(|template| !template.virtual_columns.is_empty()),
        HydrationPlan::Dynamic(entities) => entities
            .iter()
            .any(|entity| !entity.virtual_columns.is_empty()),
    }
}
