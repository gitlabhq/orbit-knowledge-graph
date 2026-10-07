//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use orbit_server_config::QueryConfig;

/// Pathfinding hard ceilings. Config can tighten but never exceed these.
/// Kept in sync with config/default.yaml `path_finding:` block.
const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360; // 15 GiB
const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 100_000;

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::codegen::PaginationContext;
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::lower::LoweredMetadata;
use crate::passes::plan::HydrationCompileOptions;
use crate::passes::plan::QueryPlan;
use crate::passes::{
    check, codegen, cursor, enforce, hydrate, lower, normalize, plan, relationships,
    response_policy, restrict, security, settings, validate,
};
use crate::types::SecurityContext;
use query_data_model::QueryDataModel;

fn require<T>(opt: Option<T>, field: &str) -> Result<T> {
    opt.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

pub enum GraphStage<'graph, 'catalog> {
    Logical(&'graph Input),
    Planned(
        &'graph crate::query_graph::QueryGraph<
            'catalog,
            query_data_model::ClickHouseDataModel,
            crate::query_graph::Expression<'catalog>,
            crate::query_graph::PhysicalOperation<'catalog>,
        >,
        crate::query_graph::BlockId,
    ),
    Emitted(
        &'graph crate::query_graph::QueryGraph<
            'catalog,
            query_data_model::ClickHouseDataModel,
            crate::query_graph::Expression<'catalog>,
            crate::query_graph::LoweredOperation<'catalog>,
        >,
        crate::query_graph::BlockId,
    ),
}

pub fn compile_graph(
    raw: &str,
    frontend: crate::Frontend,
    model: &std::sync::Arc<query_data_model::ClickHouseDataModel>,
    security_context: &SecurityContext,
) -> Result<CompiledQueryContext> {
    compile_graph_observed(raw, frontend, model, security_context, |_| Ok(()))
}

pub fn compile_graph_observed(
    raw: &str,
    frontend: crate::Frontend,
    model: &std::sync::Arc<query_data_model::ClickHouseDataModel>,
    security_context: &SecurityContext,
    observe: impl FnMut(GraphStage<'_, '_>) -> Result<()>,
) -> Result<CompiledQueryContext> {
    let mut context =
        ClickhouseJsonDslCtx::new(security_context.clone(), std::sync::Arc::clone(model));
    context.set_raw(raw.into());
    match frontend {
        crate::Frontend::JsonDsl => json_dsl_parse(&mut context)?,
        crate::Frontend::Gql => gql_parse(&mut context)?,
    }
    compile_graph_context(context, model, frontend, observe)
}

pub(crate) fn compile_graph_input(
    input: Input,
    model: &std::sync::Arc<query_data_model::ClickHouseDataModel>,
    security_context: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut context =
        ClickhouseJsonDslCtx::new(security_context.clone(), std::sync::Arc::clone(model));
    context.set_input(input);
    context.set_pagination(PaginationContext::default());
    compile_graph_context(context, model, crate::Frontend::Gql, |_| Ok(()))
}

fn compile_graph_context(
    mut context: ClickhouseJsonDslCtx,
    model: &std::sync::Arc<query_data_model::ClickHouseDataModel>,
    frontend: crate::Frontend,
    mut observe: impl FnMut(GraphStage<'_, '_>) -> Result<()>,
) -> Result<CompiledQueryContext> {
    use crate::query_graph::{Expression, PhysicalOperation, QueryGraph};
    let security_context = context.security_ctx().clone();
    validate(&mut context)?;
    if matches!(frontend, crate::Frontend::Gql) {
        validate_relationships(&mut context)?;
    }
    normalize(&mut context)?;
    observe(GraphStage::Logical(require(
        context.input().as_ref(),
        "input",
    )?))?;
    restrict(&mut context)?;
    let input = require(context.take_input(), "input")?;
    let scope = require(context.take_scope_proofs(), "scope_proofs")?;
    let mut pagination = require(context.take_pagination(), "pagination")?;
    let mut graph = QueryGraph::<_, Expression<'_>, PhysicalOperation<'_>>::new(model.as_ref());
    let root = graph.plan(&input)?;
    observe(GraphStage::Planned(&graph, root))?;
    let mut graph = graph.lower_operations()?;
    observe(GraphStage::Emitted(&graph, root))?;
    response_policy::apply_graph_excerpts(&mut graph, root, &input)?;
    let result_context = enforce::enforce_graph_return(&mut graph, root, &input)?;
    crate::scope::apply_graph(&mut graph, &scope, &input)?;
    if !security_context.scope_proofs.is_empty() {
        let scope = crate::scope::QueryScope::nodes(security_context.scope_proofs.clone());
        crate::scope::apply_graph(&mut graph, &scope, &input)?;
    }
    security::apply_graph_security(&mut graph, root, &security_context)?;
    let (root, key_count) = cursor::apply_graph(&mut graph, root, &input, pagination.query_hash)?;
    pagination.key_count = key_count;
    check::check_graph(&graph, root, &security_context)?;
    let hydration = hydrate::generate_graph_hydration(&input, &graph, root, &security_context);
    let mut query_config = settings::resolve(input.query_type.into());
    for block in graph.blocks() {
        query_config
            .compiler_derived
            .optimize_move_to_prewhere_if_final |= graph
            .operation(block)
            .is_ok_and(|operation| operation.reads_current());
        if graph.definitions(block)?.next().is_some() {
            query_config
                .compiler_derived
                .use_index_for_in_with_subqueries_max_values = Some(IN_SUBQUERY_INDEX_MAX_VALUES);
        }
    }
    if input.relationships.len() >= 3 {
        query_config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    apply_query_limits(&mut query_config, input.query_type);
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    let base = codegen::clickhouse::codegen_graph(graph, root, result_context, query_config)?;
    Ok(CompiledQueryContext {
        query_type: input.query_type,
        base,
        hydration,
        input,
        pagination,
        has_virtual_columns,
    })
}

compiler_pipeline_macros::define_compiler_ctx! {
    env {
        pub security_ctx: SecurityContext,
    }

    state {
        pub raw: String,
        pub input: Input,
        pub pagination: PaginationContext,
        pub scope_proofs: crate::scope::QueryScope,
        pub hydration_options: HydrationCompileOptions,
        pub query_plan: QueryPlan,
        pub node: Node,
        pub lowered_metadata: LoweredMetadata,
        pub result_ctx: ResultContext,
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
        plan_duckdb {
            reads_env: [data_model]
            reads_state: [scope_proofs, hydration_options]
            mutates: [input, query_plan]
        }
        lower {
            reads_state: [input]
            mutates: [query_plan, node, lowered_metadata]
        }
        enforce_local {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node, lowered_metadata, result_ctx]
        }
        cursor {
            reads_state: [lowered_metadata]
            mutates: [input, pagination, node]
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
            state: [raw, input, pagination, scope_proofs]
            phases: [json_dsl_parse, validate, normalize, restrict]
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
    let scope_proofs = crate::scope::prepare(&mut input, scope_proofs, ctx.data_model());
    ctx.set_input(input);
    ctx.set_scope_proofs(scope_proofs);
    Ok(())
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
    let lowered = lower::emit(&query_plan, &input)?;
    ctx.set_query_plan(query_plan);
    ctx.set_node(lowered.ast);
    ctx.set_lowered_metadata(lowered.metadata);
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

fn apply_query_limits(config: &mut QueryConfig, query_type: QueryType) {
    if query_type == QueryType::PathFinding {
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
