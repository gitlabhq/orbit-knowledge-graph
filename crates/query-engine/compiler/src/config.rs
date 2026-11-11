//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use orbit_server_config::{QueryConfig, QueryLimits};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::passes::codegen::{CompiledQueryContext, ParameterizedQuery};
use crate::passes::hydrate::HydrationPlan;
use crate::passes::plan::PlanMetadata;
use crate::passes::plan::Request;
use crate::passes::result_contract::ResultContext;
use crate::passes::{
    admit, codegen, hydrate, normalize, page, response_policy, restrict, result_contract, security,
    security_check, settings,
};
use crate::surface;
use crate::types::QueryType;
use orbit_authorization::Grants;
use query_data_model::surface::Statement;

fn require<T>(opt: Option<T>, field: &str) -> Result<T> {
    opt.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

compiler_pipeline_macros::define_compiler_ctx! {
    env {
        pub grants: Grants,
        pub limits: QueryLimits,
    }

    state {
        pub raw: String,
        pub statement: Statement,
        pub tp_bounds: crate::tp_bounds::TpBounds,
        pub hydration: HydrationRequest,
        pub plan_metadata: PlanMetadata,
        pub result_ctx: ResultContext,
        pub query_config: QueryConfig,
        pub hydration_plan: HydrationPlan,
        pub relational: query_data_model::implementations::relational::plan::RelationalPlan<Self::Backend>,
        pub output: CompiledQueryContext,
        pub hydration_query: ParameterizedQuery,
    }

    phases {
        gql_parse {
            reads_env: [grants, data_model, limits]
            mutates: [raw, statement]
        }
        gql_parse_trusted {
            reads_env: [data_model, limits]
            mutates: [raw, statement]
        }
        gql_parse_local {
            reads_env: [data_model, limits]
            mutates: [raw, statement]
        }
        admit {
            reads_env: [data_model, limits]
            reads_state: [statement]
        }
        admit_local {
            reads_env: [data_model, limits]
            reads_state: [statement]
        }
        normalize {
            reads_env: [data_model, limits]
            mutates: [statement]
        }
        restrict {
            reads_env: [data_model, grants]
            reads_state: [statement]
        }
        restrict_hydration {
            reads_env: [data_model, grants]
            reads_state: [hydration]
        }
        tp_bounds {
            reads_env: [data_model]
            reads_state: [statement]
            mutates: [tp_bounds]
        }
        plan {
            reads_env: [data_model, limits]
            reads_state: [statement, tp_bounds, hydration]
            mutates: [relational, plan_metadata]
        }
        paginate {
            reads_env: [data_model]
            reads_state: [statement, plan_metadata]
            mutates: [relational]
        }
        response_policy {
            reads_env: [data_model]
            reads_state: [statement]
            mutates: [relational]
        }
        result_contract {
            reads_env: [data_model]
            reads_state: [statement, plan_metadata]
            mutates: [relational, result_ctx]
        }
        hydrate_plan {
            reads_env: [grants, data_model]
            reads_state: [statement, relational]
            mutates: [hydration_plan]
        }
        materialize {
            reads_env: [data_model]
            mutates: [relational]
        }
        settings {
            reads_env: [data_model, limits]
            reads_state: [statement, hydration, plan_metadata]
            mutates: [query_config, relational]
        }
        security {
            reads_env: [grants, data_model]
            mutates: [relational]
        }
        security_check {
            reads_env: [grants, data_model]
            reads_state: [relational]
        }
        codegen {
            reads_env: [data_model]
            reads_state: [statement]
            mutates: [relational, result_ctx, query_config, hydration_plan, output]
        }
        render_hydration {
            reads_env: [data_model]
            reads_state: [relational]
            mutates: [query_config, hydration_query]
        }
    }

    pipelines {
        clickhouse_gql {
            backend: query_data_model::ClickHouse
            env: [grants, limits]
            state: [raw, statement, tp_bounds, hydration, plan_metadata, result_ctx, query_config, hydration_plan, relational, output]
            phases: [gql_parse, normalize, restrict, admit, tp_bounds, plan, response_policy, result_contract, paginate, hydrate_plan, materialize, settings, security, security_check, codegen]
        }
        ch_hydration {
            backend: query_data_model::ClickHouse
            env: [grants, limits]
            state: [tp_bounds, hydration, plan_metadata, query_config, relational, hydration_query]
            phases: [restrict_hydration, plan, settings, security, security_check, render_hydration]
        }
        duckdb_gql {
            backend: query_data_model::DuckDb
            env: [limits]
            state: [raw, statement, tp_bounds, hydration, plan_metadata, result_ctx, query_config, hydration_plan, relational, output]
            phases: [gql_parse_local, normalize, admit_local, plan, result_contract, paginate, materialize, codegen]
        }
        check_gql {
            backend: query_data_model::ClickHouse
            env: [limits]
            state: [raw, statement]
            phases: [gql_parse_trusted, normalize, admit]
        }
    }
}

fn parse<C: CompilerCtx>(ctx: &mut C, admin: bool, all_columns: bool) -> Result<()> {
    if let Some(raw) = ctx.take_raw() {
        let catalog = surface::catalog(ctx.data_model(), ctx.limits(), admin);
        let catalog = match all_columns {
            true => catalog.with_all_columns(),
            false => catalog,
        };
        ctx.set_statement(surface::parse(&raw, catalog)?);
    }
    Ok(())
}

fn gql_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let admin = ctx.grants().caller().admin();
    parse(ctx, admin, false)
}

fn gql_parse_trusted(ctx: &mut impl CompilerCtx) -> Result<()> {
    parse(ctx, true, false)
}

/// The local CLI returns every property of a node that lists no columns.
fn gql_parse_local(ctx: &mut impl CompilerCtx) -> Result<()> {
    parse(ctx, true, true)
}

fn admit_with(ctx: &mut impl CompilerCtx, selectivity: bool) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    let query = crate::logical::Query::new(&statement.plan, model.model());
    admit::admit(query, model, ctx.limits(), selectivity)
}

fn admit(ctx: &mut impl CompilerCtx) -> Result<()> {
    admit_with(ctx, true)
}

fn admit_local(ctx: &mut impl CompilerCtx) -> Result<()> {
    admit_with(ctx, false)
}

fn normalize(ctx: &mut impl CompilerCtx) -> Result<()> {
    let statement = require(ctx.take_statement(), "statement")?;
    let model = ctx.data_model();
    let plan = normalize::normalize(statement.plan, model.model(), model, ctx.limits())?;
    ctx.set_statement(Statement { plan, ..statement });
    Ok(())
}

fn restrict(ctx: &mut impl CompilerCtx) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    restrict::restrict(&statement.plan, model.model(), model, ctx.grants())
}

fn restrict_hydration(ctx: &mut impl CompilerCtx) -> Result<()> {
    let request = require(ctx.hydration().as_ref(), "hydration")?;
    let model = ctx.data_model();
    restrict::restrict_hydration(request, model.model(), model, ctx.grants())
}

fn tp_bounds(ctx: &mut impl CompilerCtx) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    let query = crate::logical::Query::new(&statement.plan, model.model());
    ctx.set_tp_bounds(crate::tp_bounds::TpBounds::derive(query, model));
    Ok(())
}

fn plan(ctx: &mut impl CompilerCtx) -> Result<()> {
    let model = ctx.data_model();
    let request = match ctx.hydration().as_ref() {
        Some(hydration) => Request::Hydration(hydration),
        None => {
            let statement = require(ctx.statement().as_ref(), "statement")?;
            let query = crate::logical::Query::new(&statement.plan, model.model());
            Request::Query(query)
        }
    };
    let planned =
        crate::passes::plan::plan(request, ctx.tp_bounds().as_ref(), model, ctx.limits())?;
    ctx.set_relational(planned.plan);
    ctx.set_plan_metadata(planned.metadata);
    Ok(())
}

fn materialize(ctx: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let plan = codegen::materialize(plan, ctx.data_model())?;
    ctx.set_relational(plan);
    Ok(())
}

fn response_policy(ctx: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let model = ctx.data_model();
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let query = crate::logical::Query::new(&statement.plan, model.model());
    let plan = response_policy::apply_text_excerpts(plan, model, query)?;
    ctx.set_relational(plan);
    Ok(())
}

fn result_contract(ctx: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let model = ctx.data_model();
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let query = crate::logical::Query::new(&statement.plan, model.model());
    let metadata = require(ctx.plan_metadata().as_ref(), "plan_metadata")?;
    let (plan, result_context) = result_contract::apply(plan, query, metadata, model)?;
    ctx.set_relational(plan);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn security(ctx: &mut impl CompilerCtx<Backend = query_data_model::ClickHouse>) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let model = ctx.data_model();
    let plan = security::apply_security_context(plan, model, ctx.grants())?;
    ctx.set_relational(plan);
    Ok(())
}

fn paginate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let model = ctx.data_model();
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let query = crate::logical::Query::new(&statement.plan, model.model());
    let metadata = require(ctx.plan_metadata().as_ref(), "plan_metadata")?;
    let plan = page::apply(plan, model, query, metadata)?;
    ctx.set_relational(plan);
    Ok(())
}

fn security_check(
    ctx: &mut impl CompilerCtx<Backend = query_data_model::ClickHouse>,
) -> Result<()> {
    let plan = require(ctx.relational().as_ref(), "relational")?;
    let model = ctx.data_model();
    security_check::verify(plan, model, ctx.grants())
}

fn hydrate_plan(ctx: &mut impl CompilerCtx<Backend = query_data_model::ClickHouse>) -> Result<()> {
    let model = ctx.data_model();
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let query = crate::logical::Query::new(&statement.plan, model.model());
    let relational = &require(ctx.relational().as_ref(), "relational")?.plan;
    let projected: Vec<&str> = relational
        .outputs(relational.root())
        .filter_map(|output| relational.name(output))
        .collect();
    let plan = hydrate::generate_hydration_plan(query, &projected, model, ctx.grants());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings(ctx: &mut impl CompilerCtx<Backend = query_data_model::ClickHouse>) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let (query_type, negation) = match ctx.hydration() {
        Some(_) => (QueryType::Hydration, false),
        None => {
            let statement = require(ctx.statement().as_ref(), "statement")?;
            let query = crate::logical::Query::new(&statement.plan, ctx.data_model().model());
            let negated = query.leaves().iter().any(|leaf| leaf.negated);
            (QueryType::from(query.kind()), negated)
        }
    };
    let metadata = require(ctx.plan_metadata().as_ref(), "plan_metadata")?;
    let config = settings::resolve(
        query_type,
        negation,
        &plan,
        metadata.edges.len(),
        metadata.point_lookup,
        ctx.limits(),
    );
    let plan = codegen::configure(plan, ctx.data_model(), &config)?;
    ctx.set_relational(plan);
    ctx.set_query_config(config);
    Ok(())
}

fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    let query = crate::logical::Query::new(&statement.plan, model.model());
    let query_type = QueryType::from(query.kind());
    let shape = crate::passes::result_contract::ResultShape::new(query)?;
    let stats = crate::analytics::QueryStats::new(query, model);
    let plan = require(ctx.relational().as_ref(), "relational")?;
    let base = codegen::render(plan, model, result_context, query_config);
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        shape,
        stats,
        debug: statement.debug,
        has_virtual_columns,
    });
    Ok(())
}

fn render_hydration(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_config = ctx.take_query_config().unwrap_or_default();
    let plan = require(ctx.relational().as_ref(), "relational")?;
    let result_context = ResultContext::new().with_query_type(QueryType::Hydration);
    let query = codegen::render(plan, ctx.data_model(), result_context, query_config);
    ctx.set_hydration_query(query);
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
