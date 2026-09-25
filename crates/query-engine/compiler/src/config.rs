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
use crate::passes::codegen::PaginationContext;
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::lower::LoweredMetadata;
use crate::passes::plan::QueryPlan;
use crate::passes::{
    check, codegen, cursor, enforce, hydrate, lower, normalize, plan, relationships,
    response_policy, restrict, security, settings, validate,
};
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
        pub pagination: PaginationContext,
        pub scope_proofs: std::collections::HashMap<String, crate::scope::ScopeProof>,
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
            reads_env: [ontology]
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
        plan {
            reads_env: [data_model]
            reads_state: [scope_proofs]
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
            env: [ontology, security_ctx]
            state: [raw, input, pagination, scope_proofs, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [json_dsl_parse, validate, normalize, restrict, plan, lower, scope_requirements, response_policy, enforce, security, cursor, check, hydrate_plan, settings, codegen]
        }
        clickhouse_gql {
            model: query_data_model::ClickHouseDataModel
            env: [ontology, security_ctx]
            state: [raw, input, pagination, scope_proofs, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [gql_parse, validate, validate_relationships, normalize, restrict, plan, lower, scope_requirements, response_policy, enforce, security, cursor, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            model: query_data_model::ClickHouseDataModel
            env: [ontology, security_ctx]
            state: [input, pagination, scope_proofs, query_plan, node, lowered_metadata, result_ctx, query_config, hydration_plan, output]
            phases: [restrict, plan, lower, scope_requirements, response_policy, enforce, settings, codegen]
        }
        duckdb_json_dsl {
            model: query_data_model::DuckDbDataModel
            env: [ontology]
            state: [raw, input, pagination, scope_proofs, query_plan, node, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [json_dsl_parse, validate_local, normalize, plan, lower, enforce_local, cursor, duckdb_codegen]
        }
        duckdb_gql {
            model: query_data_model::DuckDbDataModel
            env: [ontology]
            state: [raw, input, pagination, scope_proofs, query_plan, node, lowered_metadata, result_ctx, hydration_plan, output]
            phases: [gql_parse, validate_local, validate_relationships, normalize, plan, lower, enforce_local, cursor, duckdb_codegen]
        }
        validate_normalize_gql {
            model: query_data_model::ClickHouseDataModel
            env: [ontology]
            state: [raw, input, pagination]
            phases: [gql_parse, validate, validate_relationships, normalize]
        }
        validate_normalize {
            model: query_data_model::ClickHouseDataModel
            env: [ontology]
            state: [raw, input, pagination]
            phases: [json_dsl_parse, validate, normalize]
        }
    }
}

fn json_dsl_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let raw = require(ctx.take_raw(), "raw")?;
    let (input, query_hash) = frontend::json_dsl::parse(&raw, ctx.ontology())?;
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
    C::Model: crate::data_model::AuthorizationModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut input = require(ctx.take_input(), "input")?;
    let scope_proofs = restrict::restrict(&mut input, ctx.data_model(), &security_ctx)?;
    ctx.set_input(input);
    ctx.set_scope_proofs(scope_proofs);
    Ok(())
}

fn plan(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    let scope_proofs = ctx.scope_proofs().as_ref().cloned().unwrap_or_default();
    let query_plan = plan::plan(&input, &scope_proofs, ctx.data_model())?;
    ctx.set_input(input);
    ctx.set_query_plan(query_plan);
    Ok(())
}

fn lower(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let input = require(ctx.input().clone(), "input")?;
    let lowered = lower::emit(&query_plan, &input)?;
    ctx.set_query_plan(query_plan);
    ctx.set_node(lowered.ast);
    ctx.set_lowered_metadata(lowered.metadata);
    Ok(())
}

fn scope_requirements(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let mut node = require(ctx.take_node(), "node")?;
    if let Node::Query(query) = &mut node {
        for requirement in &query_plan.scope_requirements {
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
    C::Model: crate::data_model::AuthorizationModel,
{
    let metadata = require(ctx.take_lowered_metadata(), "lowered_metadata")?;
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    enforce::enforce_role_scans(&mut node, &input, &metadata, ctx.data_model())?;
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
    C::Model: crate::data_model::QueryModel,
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
    C::Model: crate::data_model::SecurityModel,
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
    C::Model: crate::data_model::SecurityModel,
{
    let node = require(ctx.node().clone(), "node")?;
    check::check_ast(&node, ctx.security_ctx(), ctx.data_model())
}

fn hydrate_plan<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: crate::data_model::AuthorizationModel,
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
        if !q.ctes.is_empty() {
            derived.use_index_for_in_with_subqueries_max_values =
                Some(IN_SUBQUERY_INDEX_MAX_VALUES);
        }
    }

    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    if query_plan.hops.len() >= 3 {
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
