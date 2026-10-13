//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use orbit_server_config::QueryConfig;

/// Pathfinding hard ceilings. Config can tighten but never exceed these.
/// Kept in sync with config/default.yaml `path_finding:` block.
const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360; // 15 GiB
const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 100_000;

use crate::ast::visit::{visit_relations, visit_selects};
use crate::ast::{Ast, TableRef};
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::codegen::PaginationContext;
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::lower::LoweredMetadata;
use crate::passes::plan::HydrationCompileOptions;
use crate::passes::plan::QueryPlan;
use crate::passes::{
    bind, check, codegen, cursor, enforce, hydrate, lower, plan, response_policy, restrict,
    security, settings,
};
use crate::query::{Query, QueryType};
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
        pub query: Query,
        pub pagination: PaginationContext,
        pub scope_proofs: crate::scope::QueryScope,
        pub hydration_options: HydrationCompileOptions,
        pub query_plan: QueryPlan,
        pub ast: Ast<ModelSchema>,
        pub lowered_metadata: LoweredMetadata,
        pub result_ctx: ResultContext,
        pub query_config: QueryConfig,
        pub hydration_plan: HydrationPlan,
        pub output: CompiledQueryContext,
    }

    phases {
        json_dsl_parse {
            reads_env: [data_model]
            mutates: [raw, input, pagination]
        }
        gql_parse {
            mutates: [raw, input, pagination]
        }
        bind {
            reads_env: [data_model]
            mutates: [input, query, pagination]
        }
        bind_local {
            reads_env: [data_model]
            mutates: [input, query]
        }
        bind_hydration {
            reads_env: [data_model]
            mutates: [input, query]
        }
        restrict {
            reads_env: [data_model, security_ctx]
            mutates: [query, scope_proofs]
        }
        plan_clickhouse {
            reads_env: [data_model]
            reads_state: [scope_proofs, hydration_options]
            mutates: [query, query_plan]
        }
        plan_duckdb {
            reads_env: [data_model]
            reads_state: [scope_proofs, hydration_options]
            mutates: [query, query_plan]
        }
        lower {
            reads_state: [query]
            mutates: [query_plan, ast, lowered_metadata]
        }
        scope_requirements {
            reads_env: [data_model]
            reads_state: [scope_proofs]
            mutates: [ast]
        }
        response_policy {
            reads_env: [data_model]
            reads_state: [query]
            mutates: [ast]
        }
        enforce {
            reads_env: [data_model]
            reads_state: [query]
            mutates: [ast, lowered_metadata, result_ctx]
        }
        enforce_local {
            reads_env: [data_model]
            reads_state: [query]
            mutates: [ast, lowered_metadata, result_ctx]
        }
        security {
            reads_env: [security_ctx, data_model]
            mutates: [ast]
        }
        cursor {
            reads_state: [lowered_metadata]
            mutates: [query, pagination, ast]
        }
        check {
            reads_env: [security_ctx, data_model]
            reads_state: [ast]
        }
        hydrate_plan {
            reads_env: [security_ctx, data_model]
            reads_state: [query, ast]
            mutates: [hydration_plan]
        }
        settings {
            reads_state: [query, ast, pagination]
            mutates: [query_plan, query_config]
        }
        codegen {
            reads_state: [ast, query, pagination]
            mutates: [result_ctx, query_config, hydration_plan, pagination, output]
        }
        duckdb_codegen {
            reads_state: [ast, query, pagination]
            mutates: [result_ctx, hydration_plan, pagination, output]
        }
    }

    pipelines {
        clickhouse_json_dsl {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, query, pagination, scope_proofs, hydration_options, query_plan, ast, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [json_dsl_parse, bind, restrict, plan_clickhouse, lower, response_policy, enforce, scope_requirements, security, cursor, check, hydrate_plan, settings, codegen]
        }
        clickhouse_gql {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, query, pagination, scope_proofs, hydration_options, query_plan, ast, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [gql_parse, bind, restrict, plan_clickhouse, lower, response_policy, enforce, scope_requirements, security, cursor, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [input, query, pagination, scope_proofs, hydration_options, query_plan, ast, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [bind_hydration, restrict, plan_clickhouse, lower, scope_requirements, response_policy, enforce, settings, codegen]
        }
        duckdb_json_dsl {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, query, pagination, scope_proofs, hydration_options, query_plan, ast, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [json_dsl_parse, bind_local, plan_duckdb, lower, enforce_local, cursor, duckdb_codegen]
        }
        duckdb_gql {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, query, pagination, scope_proofs, hydration_options, query_plan, ast, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [gql_parse, bind_local, plan_duckdb, lower, enforce_local, cursor, duckdb_codegen]
        }
        validate_normalize_gql {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, input, query, pagination]
            phases: [gql_parse, bind]
        }
        validate_normalize {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, input, query, pagination]
            phases: [json_dsl_parse, bind]
        }
    }
}

fn json_dsl_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let raw = require(ctx.take_raw(), "raw")?;
    let (input, query_hash) = frontend::json_dsl::parse(&raw, ctx.data_model())?;
    ctx.set_input(input);
    ctx.set_pagination(PaginationContext {
        query_hash,
        ..Default::default()
    });
    Ok(())
}

fn gql_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    if let Some(raw) = ctx.take_raw() {
        frontend::gql::parse_with_hash(&raw)?.load(ctx);
    }
    Ok(())
}

fn bind(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(ctx.take_input(), "input")?;
    if let Some(c) = &mut input.cursor
        && let Some(after) = &c.after
    {
        let query_hash = ctx
            .pagination()
            .as_ref()
            .map_or(0, |pagination| pagination.query_hash);
        if query_hash == 0 {
            return Err(QueryError::PaginationError(
                "cursor binding requires a query hash from the frontend".into(),
            ));
        }
        let values = cursor::decode(after, query_hash)?;
        c.after = Some(cursor::encode(query_hash, &values));
    }
    let query = bind::bind(input, ctx.data_model(), bind::Mode::Public)?;
    ctx.set_query(query);
    Ok(())
}

fn bind_local(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let query = bind::bind(input, ctx.data_model(), bind::Mode::Local)?;
    ctx.set_query(query);
    Ok(())
}

fn bind_hydration(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let query = bind::bind(input, ctx.data_model(), bind::Mode::Hydration)?;
    ctx.set_query(query);
    Ok(())
}

fn restrict<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut query = require(ctx.take_query(), "query")?;
    let scope_proofs = restrict::restrict(&mut query, ctx.data_model(), &security_ctx)?;
    let scope_proofs = crate::scope::prepare_bound(&mut query, scope_proofs, ctx.data_model());
    ctx.set_query(query);
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
    build: impl FnOnce(
        &Query,
        &C::Model,
        HydrationCompileOptions,
        &std::collections::HashSet<String>,
    ) -> Result<QueryPlan>,
) -> Result<()>
where
    C: CompilerCtx,
{
    let query = require(ctx.take_query(), "query")?;
    let hydration_options = ctx
        .hydration_options()
        .as_ref()
        .copied()
        .unwrap_or_default();
    let no_table_scans = std::collections::HashSet::new();
    let table_scans = ctx
        .scope_proofs()
        .as_ref()
        .map_or(&no_table_scans, crate::scope::QueryScope::table_scans);
    let query_plan = build(&query, ctx.data_model(), hydration_options, table_scans)?;
    ctx.set_query(query);
    ctx.set_query_plan(query_plan);
    Ok(())
}

fn lower<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
{
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let query = require(ctx.query().clone(), "query")?;
    let lowered = lower::emit(&query_plan, &query, ctx.data_model())?;
    ctx.set_query_plan(query_plan);
    ctx.set_ast(lowered.ast);
    ctx.set_lowered_metadata(lowered.metadata);
    Ok(())
}

fn scope_requirements(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut ast = require(ctx.take_ast(), "ast")?;
    if let Some(scope) = ctx.scope_proofs() {
        crate::scope::apply(&mut ast, scope, ctx.data_model())?;
    }
    ctx.set_ast(ast);
    Ok(())
}

fn response_policy(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query = require(ctx.query().clone(), "query")?;
    let mut ast = require(ctx.take_ast(), "ast")?;
    response_policy::apply_text_excerpts(&mut ast, &query, ctx.data_model());
    ctx.set_ast(ast);
    Ok(())
}

fn enforce<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let metadata = require(ctx.take_lowered_metadata(), "lowered_metadata")?;
    let mut ast = require(ctx.take_ast(), "ast")?;
    let query = require(ctx.query().clone(), "query")?;
    enforce::enforce_role_scans(&mut ast, &query, &metadata, ctx.data_model())?;
    let result_context =
        enforce::enforce_lowered_return(&mut ast, &query, &metadata, ctx.data_model())?;
    ctx.set_ast(ast);
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
    let mut ast = require(ctx.take_ast(), "ast")?;
    let query = require(ctx.query().clone(), "query")?;
    let result_context =
        enforce::enforce_local_return(&mut ast, &query, &metadata, ctx.data_model())?;
    ctx.set_ast(ast);
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
    let mut ast = require(ctx.take_ast(), "ast")?;
    security::apply_security_context(&mut ast, &security_ctx, ctx.data_model())?;
    ctx.set_ast(ast);
    Ok(())
}

fn cursor(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query = require(ctx.take_query(), "query")?;
    let mut ast = require(ctx.take_ast(), "ast")?;
    let metadata = require(ctx.lowered_metadata().clone(), "lowered_metadata")?;
    let mut pagination = ctx.take_pagination().unwrap_or_default();
    pagination.key_count = cursor::apply(&mut ast, &query, &metadata, pagination.query_hash)?;
    ctx.set_query(query);
    ctx.set_pagination(pagination);
    ctx.set_ast(ast);
    Ok(())
}

fn check<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let ast = require(ctx.ast().clone(), "ast")?;
    check::check_ast(&ast, ctx.security_ctx(), ctx.data_model())
}

fn hydrate_plan<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::OrbitQueryModel,
{
    let query = require(ctx.query().as_ref(), "query")?;
    let emitted = require(ctx.ast().as_ref(), "ast")?;
    let plan =
        hydrate::generate_hydration_plan(query, emitted, ctx.data_model(), ctx.security_ctx());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query = require(ctx.query().clone(), "query")?;
    let query_type: &str = query.query_type.into();
    let mut config = settings::resolve(query_type);

    let ast = require(ctx.ast().clone(), "ast")?;
    if let Some(q) = ast.select_ref() {
        let derived = &mut config.compiler_derived;
        visit_selects(q, &mut |query| {
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
    if query_plan.hops().len() >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    // Pathfinding safety net: enforce hard limits on fan-out-prone queries
    // regardless of config. These are compiler-side floors; the config can
    // only tighten them further.
    if query.query_type == QueryType::PathFinding {
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

fn codegen(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let ast = require(ctx.ast().clone(), "ast")?;
    let query = require(ctx.query().clone(), "query")?;
    let pagination = ctx.take_pagination().unwrap_or_default();
    let base = codegen::codegen(&ast, result_context, query_config)?;
    let query_type = query.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        query,
        pagination,
        has_virtual_columns,
    });
    Ok(())
}

fn duckdb_codegen(
    ctx: &mut impl CompilerCtx<Model = query_data_model::DuckDbDataModel>,
) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let ast = require(ctx.ast().clone(), "ast")?;
    let query = require(ctx.query().clone(), "query")?;
    let pagination = ctx.take_pagination().unwrap_or_default();
    let base = codegen::duckdb::codegen(&ast, result_context)?;
    let query_type = query.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        query,
        pagination,
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
