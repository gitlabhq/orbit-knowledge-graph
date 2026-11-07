//! SSOT declaration for the compiler's env fields, state fields, phase grants,
//! and pipeline presets. The macro generates the `CompilerCtx` trait,
//! per-pipeline context structs, and runner functions.

use orbit_server_config::QueryConfig;

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::input::{Input, QueryType};
use crate::passes::codegen::{CompiledQueryContext, ParameterizedQuery, RelationalModel as _};
use crate::passes::hydrate::HydrationPlan;
use crate::passes::lower::LoweredMetadata;
use crate::passes::plan::Request;
use crate::passes::result_contract::ResultContext;
use crate::passes::{
    admit, codegen, hydrate, normalize, page, response_policy, restrict, result_contract, security,
    security_check, settings,
};
use crate::surface::{self, Frontend};
use crate::types::SecurityContext;
use query_data_model::surface::Statement;

fn require<T>(opt: Option<T>, field: &str) -> Result<T> {
    opt.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

compiler_pipeline_macros::define_compiler_ctx! {
    env {
        pub security_ctx: SecurityContext,
    }

    state {
        pub raw: String,
        pub statement: Statement,
        pub input: Input,
        pub tp_bounds: crate::tp_bounds::TpBounds,
        pub hydration: HydrationRequest,
        pub lowered_metadata: LoweredMetadata,
        pub result_ctx: ResultContext,
        pub query_config: QueryConfig,
        pub hydration_plan: HydrationPlan,
        pub relational: crate::passes::codegen::PlanOf<Self::Model>,
        pub output: CompiledQueryContext,
        pub hydration_query: ParameterizedQuery,
    }

    phases {
        json_dsl_parse {
            reads_env: [security_ctx, data_model]
            mutates: [raw, statement]
        }
        gql_parse {
            reads_env: [security_ctx, data_model]
            mutates: [raw, statement]
        }
        json_dsl_parse_trusted {
            reads_env: [data_model]
            mutates: [raw, statement]
        }
        gql_parse_trusted {
            reads_env: [data_model]
            mutates: [raw, statement]
        }
        bridge {
            reads_env: [data_model]
            reads_state: [statement]
            mutates: [input]
        }
        admit {
            reads_env: [data_model]
            reads_state: [statement]
        }
        admit_local {
            reads_env: [data_model]
            reads_state: [statement]
        }
        normalize {
            reads_env: [data_model]
            mutates: [statement]
        }
        restrict {
            reads_env: [data_model, security_ctx]
            reads_state: [statement]
        }
        restrict_hydration {
            reads_env: [data_model, security_ctx]
            reads_state: [hydration]
        }
        tp_bounds {
            reads_env: [data_model]
            reads_state: [statement]
            mutates: [tp_bounds]
        }
        plan {
            reads_env: [data_model]
            reads_state: [statement, tp_bounds, hydration]
            mutates: [relational, lowered_metadata]
        }
        paginate {
            reads_env: [data_model]
            reads_state: [input, lowered_metadata]
            mutates: [relational]
        }
        response_policy {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [relational]
        }
        result_contract {
            reads_env: [data_model]
            reads_state: [input, lowered_metadata]
            mutates: [relational, result_ctx]
        }
        hydrate_plan {
            reads_env: [security_ctx, data_model]
            reads_state: [input, relational]
            mutates: [hydration_plan]
        }
        materialize {
            reads_env: [data_model]
            mutates: [relational]
        }
        settings {
            reads_env: [data_model]
            reads_state: [input, hydration, lowered_metadata]
            mutates: [query_config, relational]
        }
        security {
            reads_env: [security_ctx, data_model]
            mutates: [relational]
        }
        security_check {
            reads_env: [security_ctx, data_model]
            reads_state: [relational]
        }
        codegen {
            reads_env: [data_model]
            reads_state: [input]
            mutates: [relational, result_ctx, query_config, hydration_plan, output]
        }
        render_hydration {
            reads_env: [data_model]
            reads_state: [relational]
            mutates: [query_config, hydration_query]
        }
    }

    pipelines {
        clickhouse_json_dsl {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, statement, input, tp_bounds, hydration, lowered_metadata, result_ctx, query_config, hydration_plan, relational, output]
            phases: [json_dsl_parse, normalize, restrict, bridge, admit, tp_bounds, plan, response_policy, result_contract, paginate, hydrate_plan, materialize, settings, security, security_check, codegen]
        }
        clickhouse_gql {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [raw, statement, input, tp_bounds, hydration, lowered_metadata, result_ctx, query_config, hydration_plan, relational, output]
            phases: [gql_parse, normalize, restrict, bridge, admit, tp_bounds, plan, response_policy, result_contract, paginate, hydrate_plan, materialize, settings, security, security_check, codegen]
        }
        ch_hydration {
            model: query_data_model::ClickHouseDataModel
            env: [security_ctx]
            state: [tp_bounds, hydration, lowered_metadata, query_config, relational, hydration_query]
            phases: [restrict_hydration, plan, settings, security, security_check, render_hydration]
        }
        duckdb_json_dsl {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, statement, input, tp_bounds, hydration, lowered_metadata, result_ctx, query_config, hydration_plan, relational, output]
            phases: [json_dsl_parse_trusted, normalize, bridge, admit_local, plan, result_contract, paginate, materialize, codegen]
        }
        duckdb_gql {
            model: query_data_model::DuckDbDataModel
            env: []
            state: [raw, statement, input, tp_bounds, hydration, lowered_metadata, result_ctx, query_config, hydration_plan, relational, output]
            phases: [gql_parse_trusted, normalize, bridge, admit_local, plan, result_contract, paginate, materialize, codegen]
        }
        check_gql {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, statement]
            phases: [gql_parse_trusted, normalize, admit]
        }
        check_json_dsl {
            model: query_data_model::ClickHouseDataModel
            env: []
            state: [raw, statement]
            phases: [json_dsl_parse_trusted, normalize, admit]
        }
    }
}

fn parse<C: CompilerCtx>(ctx: &mut C, frontend: Frontend, admin: bool) -> Result<()> {
    if let Some(raw) = ctx.take_raw() {
        let catalog = surface::catalog(ctx.data_model().catalog(), admin);
        ctx.set_statement(surface::parse(&raw, frontend, catalog)?);
    }
    Ok(())
}

fn json_dsl_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let admin = ctx.security_ctx().admin;
    parse(ctx, Frontend::JsonDsl, admin)
}

fn gql_parse(ctx: &mut impl CompilerCtx) -> Result<()> {
    let admin = ctx.security_ctx().admin;
    parse(ctx, Frontend::Gql, admin)
}

fn json_dsl_parse_trusted(ctx: &mut impl CompilerCtx) -> Result<()> {
    parse(ctx, Frontend::JsonDsl, true)
}

fn gql_parse_trusted(ctx: &mut impl CompilerCtx) -> Result<()> {
    parse(ctx, Frontend::Gql, true)
}

fn bridge(ctx: &mut impl CompilerCtx) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    let input = crate::bridge::input(statement, model.catalog().model(), model)?;
    ctx.set_input(input);
    Ok(())
}

fn admit_with(ctx: &mut impl CompilerCtx, selectivity: bool) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    let query = crate::logical::Query::new(&statement.plan, model.catalog().model());
    admit::admit(query, model, selectivity)
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
    let plan = normalize::normalize(statement.plan, model.catalog().model(), model)?;
    ctx.set_statement(Statement { plan, ..statement });
    Ok(())
}

fn restrict(ctx: &mut impl CompilerCtx) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    restrict::restrict(
        &statement.plan,
        model.catalog().model(),
        model,
        ctx.security_ctx(),
    )
}

fn restrict_hydration(ctx: &mut impl CompilerCtx) -> Result<()> {
    let request = require(ctx.hydration().as_ref(), "hydration")?;
    let model = ctx.data_model();
    restrict::restrict_hydration(request, model.catalog().model(), model, ctx.security_ctx())
}

fn tp_bounds(ctx: &mut impl CompilerCtx) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let model = ctx.data_model();
    let query = crate::logical::Query::new(&statement.plan, model.catalog().model());
    ctx.set_tp_bounds(crate::tp_bounds::TpBounds::derive(query, model));
    Ok(())
}

fn plan(ctx: &mut impl CompilerCtx) -> Result<()> {
    let model = ctx.data_model();
    let request = match ctx.hydration().as_ref() {
        Some(hydration) => Request::Hydration(hydration),
        None => {
            let statement = require(ctx.statement().as_ref(), "statement")?;
            let query = crate::logical::Query::new(&statement.plan, model.catalog().model());
            Request::Query(query)
        }
    };
    let planned = crate::passes::plan::plan(request, ctx.tp_bounds().as_ref(), model)?;
    ctx.set_relational(planned.plan);
    ctx.set_lowered_metadata(planned.metadata);
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
    let input = require(ctx.input().as_ref(), "input")?;
    let plan = response_policy::apply_text_excerpts(plan, ctx.data_model(), input)?;
    ctx.set_relational(plan);
    Ok(())
}

fn result_contract(ctx: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let input = require(ctx.input().as_ref(), "input")?;
    let metadata = require(ctx.lowered_metadata().as_ref(), "lowered_metadata")?;
    let (plan, result_context) = result_contract::apply(plan, input, metadata, ctx.data_model())?;
    ctx.set_relational(plan);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn security(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let model = ctx.data_model();
    let plan = security::apply_security_context(plan, model.catalog(), ctx.security_ctx())?;
    ctx.set_relational(plan);
    Ok(())
}

fn paginate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let input = require(ctx.input().as_ref(), "input")?;
    let metadata = require(ctx.lowered_metadata().as_ref(), "lowered_metadata")?;
    let plan = page::apply(plan, ctx.data_model(), input, metadata)?;
    ctx.set_relational(plan);
    Ok(())
}

fn security_check(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let plan = require(ctx.relational().as_ref(), "relational")?;
    let model = ctx.data_model();
    security_check::verify(plan, model.catalog(), ctx.security_ctx())
}

fn hydrate_plan(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let input = require(ctx.input().as_ref(), "input")?;
    let relational = &require(ctx.relational().as_ref(), "relational")?.plan;
    let projected: Vec<&str> = relational
        .outputs(relational.root())
        .filter_map(|output| relational.name(output))
        .collect();
    let plan =
        hydrate::generate_hydration_plan(input, &projected, ctx.data_model(), ctx.security_ctx());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings(
    ctx: &mut impl CompilerCtx<Model = query_data_model::ClickHouseDataModel>,
) -> Result<()> {
    let plan = require(ctx.take_relational(), "relational")?;
    let (query_type, negation) = match ctx.hydration() {
        Some(_) => (QueryType::Hydration, false),
        None => {
            let input = require(ctx.input().as_ref(), "input")?;
            (input.query_type, input.has_negation())
        }
    };
    let metadata = require(ctx.lowered_metadata().as_ref(), "lowered_metadata")?;
    let config = settings::resolve(
        query_type,
        negation,
        &plan,
        metadata.edges.len(),
        metadata.point_lookup,
    );
    let plan = codegen::configure(plan, ctx.data_model().catalog(), &config)?;
    ctx.set_relational(plan);
    ctx.set_query_config(config);
    Ok(())
}

fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let input = require(ctx.input().clone(), "input")?;
    let plan = require(ctx.relational().as_ref(), "relational")?;
    let base = codegen::render(plan, ctx.data_model(), result_context, query_config);
    let query_type = input.query_type;
    let has_virtual_columns = hydration_has_virtuals(&hydration);
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
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
