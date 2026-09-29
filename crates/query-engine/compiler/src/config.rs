//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use orbit_server_config::QueryConfig;

/// Pathfinding hard ceilings. Config can tighten but never exceed these.
/// Kept in sync with config/default.yaml `path_finding:` block.
const PATHFINDING_MAX_EXECUTION_TIME: u64 = 15;
const PATHFINDING_MAX_MEMORY_USAGE: u64 = 16_106_127_360; // 15 GiB
const IN_SUBQUERY_INDEX_MAX_VALUES: u64 = 100_000;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Node, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::lowering::EmitOperation;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::codegen::PaginationContext;
use crate::passes::enforce::{ResultBindings, ResultContext, ReturnRequirements};
use crate::passes::frontend;
use crate::passes::hydrate::{HydrationCompileOptions, HydrationPlan};
use crate::passes::{
    check, codegen, cursor, enforce, hydrate, normalize, relationships, response_policy, restrict,
    security, settings, validate,
};
use crate::planning::bind::Source;
use crate::planning::generic::{Node as PlanNode, Values};
use crate::planning::physical::Scalar;
use crate::types::SecurityContext;
use query_data_model::QueryDataModel;
use std::convert::Infallible;

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
        pub scope_proofs: crate::scope::QueryScope,
        pub hydration_options: HydrationCompileOptions,
        pub node: Node,
        pub lowered_metadata: ResultBindings,
        pub return_requirements: ReturnRequirements,
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
        plan_local {
            reads_env: [data_model]
            reads_state: [input, return_requirements]
            mutates: [node, lowered_metadata]
        }
        plan_remote {
            reads_env: [data_model]
            reads_state: [input, return_requirements]
            mutates: [node, lowered_metadata]
        }
        plan_hydration {
            reads_env: [data_model]
            reads_state: [input, hydration_options]
            mutates: [node]
        }
        prepare_return {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [return_requirements]
        }
        enforce_return {
            reads_env: [data_model]
            reads_state: [input, return_requirements]
            mutates: [node, result_ctx]
        }
        restrict {
            reads_env: [data_model, security_ctx]
            mutates: [input, scope_proofs]
        }
        scope_requirements {
            reads_env: [data_model]
            reads_state: [scope_proofs, lowered_metadata]
            mutates: [node]
        }
        response_policy {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [node]
        }
        security {
            reads_env: [security_ctx, data_model]
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
            mutates: [query_config]
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
            state: [raw, input, pagination, scope_proofs, hydration_options, node, lowered_metadata, return_requirements, result_ctx, query_config, hydration_plan, output]
            phases: [json_dsl_parse, validate, normalize, restrict, prepare_return, plan_remote, enforce_return, response_policy, scope_requirements, security, cursor, check, hydrate_plan, settings, codegen]
        }
        clickhouse_gql {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, input, pagination, scope_proofs, hydration_options, node, lowered_metadata, return_requirements, result_ctx, query_config, hydration_plan, output]
            phases: [gql_parse, validate, validate_relationships, normalize, restrict, prepare_return, plan_remote, enforce_return, response_policy, scope_requirements, security, cursor, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [input, pagination, scope_proofs, hydration_options, node, lowered_metadata, return_requirements, result_ctx, query_config, hydration_plan, output]
            phases: [restrict, prepare_return, plan_hydration, enforce_return, settings, codegen]
        }
        duckdb_json_dsl {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, pagination, scope_proofs, hydration_options, node, lowered_metadata, return_requirements, result_ctx, hydration_plan, output]
            phases: [json_dsl_parse, validate_local, normalize, prepare_return, plan_local, enforce_return, cursor, duckdb_codegen]
        }
        duckdb_gql {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, input, pagination, scope_proofs, hydration_options, node, lowered_metadata, return_requirements, result_ctx, hydration_plan, output]
            phases: [gql_parse, validate_local, validate_relationships, normalize, prepare_return, plan_local, enforce_return, cursor, duckdb_codegen]
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
    let scope_proofs = crate::scope::prepare(&input, scope_proofs, ctx.data_model());
    ctx.set_input(input);
    ctx.set_scope_proofs(scope_proofs);
    Ok(())
}

fn scope_requirements(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut node = require(ctx.take_node(), "node")?;
    if let Some(scope) = ctx.scope_proofs() {
        let metadata = require(ctx.lowered_metadata().as_ref(), "lowered_metadata")?;
        crate::scope::apply_with_bindings(
            &mut node,
            scope,
            ctx.data_model(),
            &metadata.source_bindings,
        )?;
    }
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

fn plan_local(ctx: &mut impl CompilerCtx<Model = query_data_model::DuckDbDataModel>) -> Result<()> {
    use crate::planning::physical::{CurrentRows, select_source};

    plan_query(
        ctx,
        |source, model, values| select_source(source, model, CurrentRows::Snapshot, values),
        &[],
    )
}

fn plan_remote(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    plan_query(
        ctx,
        crate::planning::backends::clickhouse::select,
        &[
            crate::planning::backends::clickhouse::realize_foreign_key,
            crate::planning::backends::clickhouse::fuse_holder,
        ],
    )
}

fn plan_hydration(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    let options = ctx
        .hydration_options()
        .as_ref()
        .copied()
        .unwrap_or_default();
    let node = crate::planning::hydration::plan(input, options, ctx.data_model())?;
    ctx.set_node(node);
    Ok(())
}

fn prepare_return(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    let requirements = ReturnRequirements::prepare(input, ctx.data_model())?;
    ctx.set_return_requirements(requirements);
    Ok(())
}

fn enforce_return(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().as_ref(), "input")?;
    let requirements = require(ctx.return_requirements().as_ref(), "return_requirements")?;
    let result = enforce::enforce_lowered_return(&mut node, input, requirements, ctx.data_model())?;
    ctx.set_node(node);
    ctx.set_result_ctx(result);
    Ok(())
}

fn plan_query<C: CompilerCtx, S: EmitOperation + Clone + PartialEq>(
    ctx: &mut C,
    mut select_source: impl FnMut(
        Source,
        &C::Model,
        &mut Values,
    ) -> Result<PlanNode<S, Scalar, Infallible>>,
    source_rules: &[crate::planning::optimize::Rule<S, Scalar, Infallible>],
) -> Result<()> {
    use crate::ast::OrderExpr;
    use crate::input::OrderDirection;
    use crate::lowering::{Context, lower_program, scalar};
    use crate::planning::{aggregation, graph, optimize};

    let input = require(ctx.input().clone(), "input")?;
    let mut context = Context::default();
    let aggregate = input.query_type == QueryType::Aggregation;
    if input.query_type == QueryType::PathFinding {
        let (node, metadata) =
            crate::planning::path_finding::plan(&input, ctx.data_model(), |source, values| {
                select_source(source, ctx.data_model(), values)
            })?;
        ctx.set_node(node);
        ctx.set_lowered_metadata(metadata);
        return Ok(());
    }
    let requirements = require(ctx.return_requirements().as_ref(), "return_requirements")?;
    if input.query_type == QueryType::Neighbors {
        let (node, metadata) = crate::planning::neighbors::plan(
            &input,
            requirements,
            ctx.data_model(),
            |source, values| select_source(source, ctx.data_model(), values),
        )?;
        ctx.set_node(node);
        ctx.set_lowered_metadata(metadata);
        return Ok(());
    }
    let mut bound = if aggregate {
        let grouped_outputs = requirements
            .redactions
            .iter()
            .map(|(_, position)| requirements.required[*position].clone())
            .collect::<Vec<_>>();
        aggregation::bind(&input, ctx.data_model(), &grouped_outputs, || {
            context.alias()
        })?
    } else {
        graph::traversal(
            &input,
            ctx.data_model(),
            &requirements.required,
            &requirements.edge_outputs,
        )?
    };
    let physical = bound
        .root
        .expand_sources(&mut |source| select_source(source, ctx.data_model(), &mut bound.values))?;
    let mut rules = source_rules.to_vec();
    rules.extend(crate::planning::rules::registered());
    let candidates = optimize::candidates(physical, bound.values, &rules)?;
    let selected = optimize::select(candidates, |program| {
        optimize::estimated_work(program, |_| 1)
    })?
    .ok_or_else(|| QueryError::Lowering("no valid physical candidates".into()))?;
    let fragment = lower_program(
        &selected.program,
        &selected.values,
        &mut context,
        &scalar::emit,
    )?;

    let resolve = |value| {
        fragment
            .exports
            .iter()
            .find(|(id, _)| *id == value)
            .map(|(_, expression)| expression.clone())
            .ok_or_else(|| QueryError::Lowering("required result value was not exported".into()))
    };
    let identities = if aggregate {
        bound
            .required
            .iter()
            .map(|value| resolve(*value).map(OrderExpr::asc))
            .collect::<Result<Vec<_>>>()?
    } else {
        input
            .nodes
            .iter()
            .zip(&bound.required)
            .filter(|(node, _)| {
                !input.order_by.as_ref().is_some_and(|order| {
                    order.node == node.id && order.property == node.id_property
                })
            })
            .map(|(_, value)| resolve(*value).map(OrderExpr::asc))
            .collect::<Result<Vec<_>>>()?
    };
    let order = if aggregate {
        input
            .aggregation
            .sort
            .as_ref()
            .map(|order| {
                let index = bound
                    .outputs
                    .iter()
                    .position(|name| name.name() == Some(order.column.as_str()))
                    .ok_or_else(|| {
                        QueryError::ReferenceError("aggregate sort output is missing".into())
                    })?;
                Ok::<_, QueryError>(OrderExpr {
                    expr: fragment.exports[index].1.clone(),
                    desc: order.direction == OrderDirection::Desc,
                })
            })
            .transpose()?
    } else {
        input
            .order_by
            .as_ref()
            .map(|order| {
                Ok::<_, QueryError>(OrderExpr {
                    expr: resolve(bound.required[input.nodes.len()])?,
                    desc: order.direction == OrderDirection::Desc,
                })
            })
            .transpose()?
    };

    let mut query = fragment.into_query(&bound.outputs)?;
    query.order_by = order.into_iter().collect();

    ctx.set_node(Node::Query(Box::new(query)));
    ctx.set_lowered_metadata(ResultBindings {
        source_bindings: context.source_bindings,
        stable_order: identities,
    });
    Ok(())
}

fn security<C>(ctx: &mut C) -> Result<()>
where
    C: CompilerCtx,
    C::Model: query_data_model::QueryDataModel,
{
    let security_ctx = ctx.security_ctx().clone();
    let mut node = require(ctx.take_node(), "node")?;
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
    ctx.set_query_config(config);
    Ok(())
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
