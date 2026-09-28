//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

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
use crate::passes::codegen::PaginationContext;
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::plan::HydrationCompileOptions;
use crate::passes::planner::{self, LoweredMetadata};
use crate::passes::{
    check, codegen, cursor, enforce, hydrate, normalize, relationships, response_policy, restrict,
    security, settings, validate,
};
use crate::types::SecurityContext;
use query_data_model::QueryDataModel;

enum QueryPlan {
    ClickHouse {
        bound: planner::BoundCatalog<query_data_model::ClickHouseDataModel>,
        candidate: Option<planner::Candidate<planner::ClickHouse>>,
        scope_requirements: Vec<crate::scope::ScopeProof>,
    },
    DuckDb {
        bound: planner::BoundCatalog<query_data_model::DuckDbDataModel>,
        candidate: Option<planner::Candidate<planner::DuckDb>>,
    },
}

impl QueryPlan {
    fn hop_count(&self) -> usize {
        match self {
            Self::ClickHouse { bound, .. } => bound.input.relationships.len(),
            Self::DuckDb { bound, .. } => bound.input.relationships.len(),
        }
    }
}

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
        pub pagination: PaginationContext,
        pub scope_proofs: std::collections::HashMap<String, crate::scope::ScopeProof>,
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
            mutates: [raw, input, pagination]
        }
        gql_parse {
            mutates: [raw, input, pagination]
        }
        validate_relationships {
            reads_env: [data_model]
            reads_state: [input]
        }
        validate {
            reads_env: [data_model]
            mutates: [input, pagination]
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
            reads_state: [scope_proofs, hydration_options]
            mutates: [input, query_plan]
        }
        plan_duckdb {
            reads_env: [data_model]
            reads_state: [scope_proofs, hydration_options]
            mutates: [input, query_plan]
        }
        lower {
            reads_state: [input]
            mutates: [query_plan, node, lowered_metadata]
        }
        scope_requirements {
            reads_state: [input]
            mutates: [query_plan, node]
        }
        response_policy {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node]
        }
        enforce {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node, lowered_metadata, result_ctx]
        }
        enforce_local {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node, lowered_metadata, result_ctx]
        }
        security {
            reads_env: [security_ctx, data_model]
            reads_state: [input, scope_proofs]
            mutates: [node]
        }
        cursor {
            reads_state: [lowered_metadata]
            mutates: [input, pagination, node]
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
            reads_state: [input, node, pagination]
            mutates: [query_plan, query_config]
        }
        codegen {
            reads_state: [node, input, pagination]
            mutates: [result_ctx, query_config, hydration_plan, pagination, output]
        }
        duckdb_codegen {
            reads_state: [node, input, pagination]
            mutates: [result_ctx, hydration_plan, pagination, output]
        }
    }

    pipelines {
        clickhouse_json_dsl {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, pagination, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [json_dsl_parse, validate, normalize, restrict, plan_clickhouse, lower, scope_requirements, response_policy, enforce, security, cursor, check, hydrate_plan, settings, codegen]
        }
        clickhouse_gql {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, pagination, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [gql_parse, validate, validate_relationships, normalize, restrict, plan_clickhouse, lower, scope_requirements, response_policy, enforce, security, cursor, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [input, pagination, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [restrict, plan_clickhouse, lower, scope_requirements, response_policy, enforce, settings, codegen]
        }
        duckdb_json_dsl {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, pagination, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [json_dsl_parse, validate_local, normalize, plan_duckdb, lower, enforce_local, cursor, duckdb_codegen]
        }
        duckdb_gql {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, pagination, scope_proofs, hydration_options, query_plan, node, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [gql_parse, validate_local, validate_relationships, normalize, plan_duckdb, lower, enforce_local, cursor, duckdb_codegen]
        }
        validate_normalize_gql {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, input, pagination]
            phases: [gql_parse, validate, validate_relationships, normalize]
        }
        validate_normalize {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, input, pagination]
            phases: [json_dsl_parse, validate, normalize]
        }
    }
}

fn json_dsl_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let raw = require(ctx.take_raw(), "raw")?;
    let (input, query_hash) = frontend::json_dsl::parse(&raw, ctx.data_model().ontology())?;
    ctx.set_input(input);
    ctx.set_pagination(PaginationContext {
        query_hash,
        ..Default::default()
    });
    Ok(())
}

fn gql_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    if let Some(raw) = ctx.take_raw() {
        let (input, query_hash) = frontend::gql::parse_with_hash(&raw)?;
        ctx.set_input(input);
        ctx.set_pagination(PaginationContext {
            query_hash,
            ..Default::default()
        });
    }
    Ok(())
}

fn validate_relationships(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    relationships::validate_relationships(input, ctx.data_model())
}

fn validate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(ctx.take_input(), "input")?;
    let v = validate::Validator::new(ctx.data_model());
    v.check_shape(&input)?;
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
    C::Model: query_data_model::QueryDataModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut input = require(ctx.take_input(), "input")?;
    let scope_proofs = restrict::restrict(&mut input, ctx.data_model(), &security_ctx)?;
    ctx.set_input(input);
    ctx.set_scope_proofs(scope_proofs);
    Ok(())
}

fn plan_clickhouse(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let hydration_options = ctx
        .hydration_options()
        .as_ref()
        .copied()
        .unwrap_or_default();
    let scope_proofs = ctx.scope_proofs().as_ref().cloned().unwrap_or_default();
    let planned = planner::clickhouse(
        input.clone(),
        ctx.data_model_arc(),
        hydration_options,
        &scope_proofs,
    )?;
    ctx.set_input(input);
    ctx.set_query_plan(QueryPlan::ClickHouse {
        bound: planned.bound,
        candidate: Some(planned.candidate),
        scope_requirements: planned.scope_requirements,
    });
    Ok(())
}

fn plan_duckdb(
    ctx: &mut impl CompilerCtx<Model = query_data_model::DuckDbDataModel>,
) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let planned = planner::duckdb(input.clone(), ctx.data_model_arc())?;
    ctx.set_input(input);
    ctx.set_query_plan(QueryPlan::DuckDb {
        bound: planned.bound,
        candidate: Some(planned.candidate),
    });
    Ok(())
}

fn lower(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let lowered = match query_plan {
        QueryPlan::ClickHouse {
            bound,
            candidate,
            scope_requirements,
        } => {
            let lowered = planner::lower_clickhouse(
                &bound,
                planner::SelectedPlan {
                    candidate: require(candidate, "physical candidate")?,
                },
            )?;
            ctx.set_query_plan(QueryPlan::ClickHouse {
                bound,
                candidate: None,
                scope_requirements,
            });
            lowered
        }
        QueryPlan::DuckDb { bound, candidate } => {
            let candidate = require(candidate, "physical candidate")?;
            let lowered = planner::lower_duckdb(&bound, planner::SelectedPlan { candidate })?;
            ctx.set_query_plan(QueryPlan::DuckDb {
                bound,
                candidate: None,
            });
            lowered
        }
    };
    ctx.set_node(lowered.ast);
    ctx.set_lowered_metadata(lowered.metadata);
    Ok(())
}

fn scope_requirements(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let mut node = require(ctx.take_node(), "node")?;
    if let Node::Query(query) = &mut node
        && let QueryPlan::ClickHouse {
            scope_requirements, ..
        } = &query_plan
    {
        for requirement in scope_requirements {
            let guard = crate::scope::resolved_scope_guard(requirement);
            query.where_clause = Some(match query.where_clause.take() {
                Some(existing) => crate::ast::Expr::and(existing, guard),
                None => guard,
            });
        }
    }
    ctx.set_query_plan(query_plan);
    ctx.set_node(node);
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
    C::Model: query_data_model::QueryDataModel,
{
    let mut metadata = require(ctx.take_lowered_metadata(), "lowered_metadata")?;
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    enforce::enforce_role_scans(&mut node, &input, &mut metadata, ctx.data_model())?;
    let result_context =
        enforce::enforce_lowered_return(&mut node, &input, &metadata, ctx.data_model())?;
    ctx.set_node(node);
    ctx.set_lowered_metadata(metadata);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn enforce_local<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::QueryDataModel,
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
    C::Model: query_data_model::QueryDataModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut node = require(ctx.take_node(), "node")?;
    let security_ctx =
        security_ctx.with_scope_proofs(ctx.scope_proofs().as_ref().cloned().unwrap_or_default());
    security::apply_security_context(&mut node, &security_ctx, ctx.data_model())?;
    ctx.set_node(node);
    Ok(())
}

fn cursor(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let mut node = require(ctx.take_node(), "node")?;
    let metadata = require(ctx.lowered_metadata().clone(), "lowered_metadata")?;
    let mut pagination = ctx.take_pagination().unwrap_or_default();
    pagination.key_count = cursor::apply(&mut node, &input, &metadata, pagination.query_hash)?;
    ctx.set_input(input);
    ctx.set_pagination(pagination);
    ctx.set_node(node);
    Ok(())
}

fn check<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::QueryDataModel,
{
    let node = require(ctx.node().clone(), "node")?;
    check::check_ast(&node, ctx.security_ctx(), ctx.data_model())
}

fn hydrate_plan<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::QueryDataModel,
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
        derived.enable_materialized_cte = q.ctes.iter().any(|c| c.materialized);
        derived.optimize_move_to_prewhere_if_final =
            scans_final(q) || q.ctes.iter().any(|c| scans_final(&c.query));
        if !q.ctes.is_empty() || contains_in_select(q) {
            derived.use_index_for_in_with_subqueries_max_values =
                Some(IN_SUBQUERY_INDEX_MAX_VALUES);
        }
    }

    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    if query_plan.hop_count() >= 3 {
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
    fn table_scans_final(table: &TableRef) -> bool {
        match table {
            TableRef::Scan { final_, .. } => *final_,
            TableRef::Join { left, right, .. } => {
                table_scans_final(left) || table_scans_final(right)
            }
            TableRef::Union { queries, .. } => queries.iter().any(scans_final),
            TableRef::Subquery { query, .. } => scans_final(query),
        }
    }

    table_scans_final(&q.from)
}

fn contains_in_select(query: &Query) -> bool {
    fn expression_has_in_select(expression: &crate::ast::Expr) -> bool {
        match expression {
            crate::ast::Expr::InSelect { .. } => true,
            crate::ast::Expr::BinaryOp { left, right, .. } => {
                expression_has_in_select(left) || expression_has_in_select(right)
            }
            crate::ast::Expr::UnaryOp { expr, .. } => expression_has_in_select(expr),
            crate::ast::Expr::FuncCall { args, .. } => args.iter().any(expression_has_in_select),
            crate::ast::Expr::Lambda { body, .. } => expression_has_in_select(body),
            _ => false,
        }
    }

    fn table_contains_in_select(table: &TableRef) -> bool {
        match table {
            TableRef::Scan { .. } => false,
            TableRef::Join { left, right, on, .. } => {
                expression_has_in_select(on)
                    || table_contains_in_select(left)
                    || table_contains_in_select(right)
            }
            TableRef::Union { queries, .. } => queries.iter().any(contains_in_select),
            TableRef::Subquery { query, .. } => contains_in_select(query),
        }
    }

    query
        .where_clause
        .as_ref()
        .is_some_and(expression_has_in_select)
        || table_contains_in_select(&query.from)
        || query.ctes.iter().any(|cte| contains_in_select(&cte.query))
}

fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.node().clone(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let pagination = ctx.take_pagination().unwrap_or_default();
    let base = codegen::codegen(&node, result_context, query_config)?;
    let query_type = input.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
        pagination,
        has_virtual_columns,
    });
    Ok(())
}

fn duckdb_codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.node().clone(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let pagination = ctx.take_pagination().unwrap_or_default();
    let base = codegen::duckdb::codegen(&node, result_context)?;
    let query_type = input.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
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
