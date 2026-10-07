use orbit_server_config::QueryConfig;
use query_data_model::QueryDataModel;
use std::convert::Infallible;

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::codegen::{CompiledQueryContext, PaginationContext};
use crate::passes::enforce::ResultContext;
use crate::passes::frontend;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::lower::LoweredMetadata;
use crate::passes::plan::{HydrationCompileOptions, QueryPlan};
use crate::passes::{
    check, codegen, cursor, enforce, hydrate, lower, normalize, plan, relationships,
    response_policy, restrict, security, settings, validate,
};
use crate::query_graph::{BlockId, LatestRows, QueryGraph};
use crate::types::SecurityContext;

const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360;
const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 100_000;

fn require<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

pub enum GraphStage<'graph, 'catalog> {
    Logical(&'graph Input),
    Planned(
        &'graph QueryGraph<'catalog, query_data_model::ClickHouseDataModel, LatestRows<'catalog>>,
        BlockId,
    ),
    Emitted(
        &'graph QueryGraph<'catalog, query_data_model::ClickHouseDataModel, Infallible>,
        BlockId,
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
    let mut graph = QueryGraph::<_, LatestRows<'_>>::new(model.as_ref());
    let root = graph.plan(&input)?;
    observe(GraphStage::Planned(&graph, root))?;
    let mut graph = graph.lower_operations();
    observe(GraphStage::Emitted(&graph, root))?;
    response_policy::apply_graph_excerpts(&mut graph, root, &input)?;
    let (graph, result_context) = enforce::enforce_graph_return(graph, root, &input)?;
    let mut graph = crate::scope::apply_graph(graph, root, &scope, &input)?;
    if !security_context.scope_proofs.is_empty() {
        let scope = crate::scope::QueryScope::nodes(security_context.scope_proofs.clone());
        graph = crate::scope::apply_graph(graph, root, &scope, &input)?;
    }
    let graph = security::apply_graph_security(graph, root, &security_context)?;
    let (graph, root, key_count) = cursor::apply_graph(graph, root, &input, pagination.query_hash)?;
    pagination.key_count = key_count;
    check::check_graph(&graph, root, &security_context)?;
    let hydration = hydrate::generate_graph_hydration(&input, &graph, root, &security_context);
    let query_config = graph_settings(&graph, &input)?;
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

fn graph_settings(
    graph: &QueryGraph<'_, query_data_model::ClickHouseDataModel, Infallible>,
    input: &Input,
) -> Result<QueryConfig> {
    let mut config = settings::resolve(input.query_type.into());
    for block in graph.blocks() {
        config.compiler_derived.optimize_move_to_prewhere_if_final |= graph
            .operation(block)
            .is_ok_and(|operation| operation.reads_current());
        if graph.definitions(block)?.next().is_some() {
            config
                .compiler_derived
                .use_index_for_in_with_subqueries_max_values = Some(IN_SUBQUERY_INDEX_MAX_VALUES);
        }
    }
    if input.relationships.len() >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    if input.query_type == QueryType::PathFinding {
        config.max_execution_time = Some(
            config
                .max_execution_time
                .unwrap_or(PATHFINDING_MAX_EXECUTION_TIME)
                .min(PATHFINDING_MAX_EXECUTION_TIME),
        );
        config.max_memory_usage = Some(
            config
                .max_memory_usage
                .unwrap_or(PATHFINDING_MAX_MEMORY_USAGE)
                .min(PATHFINDING_MAX_MEMORY_USAGE),
        );
    }
    Ok(config)
}

compiler_pipeline_macros::define_compiler_ctx! {
    env { pub security_ctx: SecurityContext, }
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
        json_dsl_parse { reads_env: [data_model] mutates: [raw, input, pagination] }
        gql_parse { mutates: [raw, input, pagination] }
        validate_relationships { reads_env: [data_model] reads_state: [input] }
        validate { reads_env: [data_model] mutates: [input, pagination] }
        validate_local { reads_env: [data_model] mutates: [input] }
        normalize { reads_env: [data_model] mutates: [input] }
        restrict { reads_env: [data_model, security_ctx] mutates: [input, scope_proofs] }
        plan_duckdb { reads_env: [data_model] reads_state: [scope_proofs, hydration_options] mutates: [input, query_plan] }
        lower { reads_state: [input] mutates: [query_plan, node, lowered_metadata] }
        enforce_local { reads_env: [data_model] reads_state: [input] mutates: [node, lowered_metadata, result_ctx] }
        cursor { reads_state: [lowered_metadata] mutates: [input, pagination, node] }
        duckdb_codegen { reads_state: [node, input, pagination] mutates: [result_ctx, hydration_plan, pagination, output] }
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

fn json_dsl_parse(context: &mut impl CompilerCtx) -> Result<()> {
    let raw = require(context.take_raw(), "raw")?;
    let (input, query_hash) = frontend::json_dsl::parse(&raw, context.data_model().ontology())?;
    context.set_input(input);
    context.set_pagination(PaginationContext {
        query_hash,
        ..Default::default()
    });
    Ok(())
}

fn gql_parse(context: &mut impl CompilerCtx) -> Result<()> {
    if let Some(raw) = context.take_raw() {
        let (input, query_hash) = frontend::gql::parse_with_hash(&raw)?;
        context.set_input(input);
        context.set_pagination(PaginationContext {
            query_hash,
            ..Default::default()
        });
    }
    Ok(())
}

fn validate_relationships(context: &mut impl CompilerCtx) -> Result<()> {
    relationships::validate_relationships(
        require(context.input().as_ref(), "input")?,
        context.data_model(),
    )
}

fn validate(context: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(context.take_input(), "input")?;
    let validator = validate::Validator::new(context.data_model());
    validator.check_shape(&input)?;
    if let Some(cursor) = &mut input.cursor
        && let Some(after) = &cursor.after
    {
        let hash = context
            .pagination()
            .as_ref()
            .map_or(0, |pagination| pagination.query_hash);
        if hash == 0 {
            return Err(QueryError::PaginationError(
                "cursor binding requires a query hash from the frontend".into(),
            ));
        }
        cursor.after = Some(crate::passes::cursor::encode(
            hash,
            &crate::passes::cursor::decode(after, hash)?,
        ));
    }
    validator.check_references(&input)?;
    context.set_input(input);
    Ok(())
}

fn validate_local(context: &mut impl CompilerCtx) -> Result<()> {
    let input = require(context.take_input(), "input")?;
    let validator = validate::Validator::new(context.data_model())
        .with_skip(validate::Skip { selectivity: true });
    validator.check_shape(&input)?;
    validator.check_references(&input)?;
    context.set_input(input);
    Ok(())
}

fn normalize(context: &mut impl CompilerCtx) -> Result<()> {
    let input = require(context.take_input(), "input")?;
    context.set_input(normalize::normalize(input, context.data_model())?);
    Ok(())
}

fn restrict<C: CompilerCtx>(context: &mut C) -> Result<()>
where
    C::Model: QueryDataModel,
{
    let security = context.security_ctx().clone();
    let mut input = require(context.take_input(), "input")?;
    let proofs = restrict::restrict(&mut input, context.data_model(), &security)?;
    let scope = crate::scope::prepare(&mut input, proofs, context.data_model());
    context.set_input(input);
    context.set_scope_proofs(scope);
    Ok(())
}

fn plan_duckdb(
    context: &mut impl CompilerCtx<Model = query_data_model::DuckDbDataModel>,
) -> Result<()> {
    let input = require(context.take_input(), "input")?;
    let options = context
        .hydration_options()
        .as_ref()
        .copied()
        .unwrap_or_default();
    let plan = plan::plan_duckdb(&input, context.data_model(), options)?;
    context.set_input(input);
    context.set_query_plan(plan);
    Ok(())
}

fn lower(context: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(context.take_query_plan(), "query_plan")?;
    let input = require(context.input().as_ref(), "input")?;
    let lowered = lower::emit(&plan, input)?;
    context.set_query_plan(plan);
    context.set_node(lowered.ast);
    context.set_lowered_metadata(lowered.metadata);
    Ok(())
}

fn enforce_local<C: CompilerCtx>(context: &mut C) -> Result<()>
where
    C::Model: QueryDataModel,
{
    let metadata = require(context.take_lowered_metadata(), "lowered_metadata")?;
    let mut node = require(context.take_node(), "node")?;
    let input = require(context.input().as_ref(), "input")?;
    let result = enforce::enforce_local_return(&mut node, input, &metadata, context.data_model())?;
    context.set_node(node);
    context.set_lowered_metadata(metadata);
    context.set_result_ctx(result);
    Ok(())
}

fn cursor(context: &mut impl CompilerCtx) -> Result<()> {
    let input = require(context.take_input(), "input")?;
    let mut node = require(context.take_node(), "node")?;
    let metadata = require(context.lowered_metadata().as_ref(), "lowered_metadata")?;
    let mut pagination = context.pagination().clone().unwrap_or_default();
    pagination.key_count = cursor::apply(&mut node, &input, metadata, pagination.query_hash)?;
    context.set_input(input);
    context.set_pagination(pagination);
    context.set_node(node);
    Ok(())
}

fn duckdb_codegen(context: &mut impl CompilerCtx) -> Result<()> {
    let result = require(context.take_result_ctx(), "result_ctx")?;
    let hydration = context.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(context.node().as_ref(), "node")?;
    let base = codegen::duckdb::codegen(node, result)?;
    let input = require(context.input().clone(), "input")?;
    let pagination = context.take_pagination().unwrap_or_default();
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    context.set_output(CompiledQueryContext {
        query_type: input.query_type,
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
