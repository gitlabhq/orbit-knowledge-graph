//! Compiles one stage of a DAG through the sub-pipeline registered for its frontend and the
//! backend of its connection, once the runner has the inputs the stage reads before it plans.

use std::sync::Arc;

use orbit_authorization::Grants;
use orbit_server_config::QueryLimits;
use query_data_model::{Catalog, ClickHouse, DuckDb, Model};

use super::{Connections, Peer, Request, Stage, StageId, StageQuery, describe};
use crate::config::{self, CompilerCtx as _};
use crate::error::{QueryError, Result};
use crate::metrics::CountErr;
use crate::passes::plan::remote::Target;
use crate::passes::remote_security::Admitted;
use crate::passes::result_contract::ResultContext;

/// The top-level pipeline a deployment parses and splits queries with, and the relational
/// catalog its frontend rules read: the server's, or the local CLI's.
#[derive(Debug, Clone)]
pub enum Frontend {
    Server(Arc<Catalog<ClickHouse>>),
    Local(Arc<Catalog<DuckDb>>),
}

impl Frontend {
    pub fn shared_model(&self) -> &Arc<Model> {
        match self {
            Self::Server(catalog) => catalog.shared_model(),
            Self::Local(catalog) => catalog.shared_model(),
        }
    }

    pub fn model(&self) -> &Model {
        self.shared_model()
    }
}

/// The frontend, the served connections and the caller every stage of one query compiles
/// against.
pub struct Env<'a> {
    pub frontend: &'a Frontend,
    /// Every connection the deployment serves, the frontend's included; each stage runs on one.
    pub connections: &'a Connections,
    pub grants: &'a Grants,
    pub limits: &'a QueryLimits,
    /// Keep each stage's planned and emitted plan for an observer.
    pub trace: bool,
}

/// What a stage reads before it plans, besides the key sets `bind` puts in its statement: what
/// its remote reads' policies admit, and the result contexts of the stages the Combine stage joins.
#[derive(Debug, Default)]
pub struct Inputs {
    pub admitted: Option<Admitted>,
    pub contexts: Vec<(StageId, ResultContext)>,
}

#[derive(Debug)]
pub struct Compiled {
    pub planned: Vec<String>,
    /// Whether the stage's security check proved every read carries its policy.
    pub proved: bool,
    pub emitted: Vec<String>,
    pub request: Request,
}

fn produced<T>(value: Option<T>) -> Result<T> {
    value.ok_or_else(|| QueryError::PipelineInvariant("pipeline did not produce output".into()))
}

pub fn compile(stage: Stage, mut inputs: Inputs, env: &Env<'_>) -> Result<Compiled> {
    let contract = stage.contract;
    let (grants, limits, trace) = (env.grants.clone(), *env.limits, env.trace);
    let statement = match stage.query {
        StageQuery::Gql(statement) => statement,
        StageQuery::Combine(query, shape) => {
            let joined = |at: &StageId| {
                stage
                    .inputs
                    .iter()
                    .any(|input| matches!(input, super::Input::Rows { from } if from == at))
            };
            inputs.contexts.retain(|(at, _)| joined(at));
            return combine(query, *shape, inputs, env);
        }
        StageQuery::Hydration(_) => {
            return Err(QueryError::PipelineInvariant(
                "a hydration stage compiles through compile_hydration".into(),
            ));
        }
    };
    let super::Runs::On { connection, stores } = stage.runs else {
        return Err(QueryError::PipelineInvariant(
            "only the Combine stage runs in the query engine".into(),
        ));
    };
    let peer = env.connections.get(connection).ok_or_else(|| {
        QueryError::PipelineInvariant(
            "a stage runs on a connection the deployment does not serve".into(),
        )
    })?;
    match peer {
        Peer::ClickHouse(catalog) => {
            let mut ctx = config::GqlClickhouseCtx::new(grants, limits, trace, Arc::clone(catalog));
            ctx.set_statement(statement);
            if let Some(contract) = contract {
                ctx.set_contract(contract);
            }
            config::run_gql_clickhouse(&mut ctx).count_err()?;
            let emitted = trace_relational(trace, ctx.relational().as_ref());
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: ctx.take_proved().unwrap_or_default(),
                emitted,
                request: Request::Query(Box::new(produced(ctx.take_output())?)),
            })
        }
        Peer::DuckDb(catalog) => {
            let mut ctx = config::GqlDuckdbCtx::new(grants, limits, trace, Arc::clone(catalog));
            ctx.set_statement(statement);
            if let Some(contract) = contract {
                ctx.set_contract(contract);
            }
            config::run_gql_duckdb(&mut ctx).count_err()?;
            let emitted = trace_relational(trace, ctx.relational().as_ref());
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: ctx.take_proved().unwrap_or_default(),
                emitted,
                request: Request::Query(Box::new(produced(ctx.take_output())?)),
            })
        }
        Peer::Remote(Target::Code(code)) => {
            let mut ctx = config::GqlOrbitCodeCtx::new(grants, limits, trace, Arc::clone(code));
            ctx.set_statement(statement);
            ctx.set_placed(stores);
            if let Some(admitted) = inputs.admitted {
                ctx.set_admitted(admitted);
            }
            config::run_gql_orbit_code(&mut ctx).count_err()?;
            let emitted = trace.then(|| ctx.code().as_ref().map(describe)).flatten();
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: ctx.take_proved().unwrap_or_default(),
                emitted: emitted.unwrap_or_default(),
                request: produced(ctx.take_request())?,
            })
        }
        Peer::Remote(Target::Live(live)) => {
            let mut ctx = config::GqlLiveCtx::new(grants, limits, trace, Arc::clone(live));
            ctx.set_statement(statement);
            ctx.set_placed(stores);
            if let Some(admitted) = inputs.admitted {
                ctx.set_admitted(admitted);
            }
            config::run_gql_live(&mut ctx).count_err()?;
            let emitted = trace.then(|| ctx.live().as_ref().map(describe)).flatten();
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: ctx.take_proved().unwrap_or_default(),
                emitted: emitted.unwrap_or_default(),
                request: produced(ctx.take_request())?,
            })
        }
    }
}

/// The `(gql, combine)` stage: the join of the other stages' rows, shaped as the response.
fn combine(
    query: super::CombineQuery,
    response: super::Response,
    inputs: Inputs,
    env: &Env<'_>,
) -> Result<Compiled> {
    let trace = env.trace;
    let shared = Arc::clone(env.frontend.shared_model());
    let mut ctx = config::GqlCombineCtx::new(*env.limits, trace, shared);
    ctx.set_combine_query(query);
    ctx.set_stage_contexts(inputs.contexts);
    config::run_gql_combine(&mut ctx).count_err()?;
    let plan = produced(ctx.take_combine())?;
    let described = describe(&plan);
    let compiled = crate::CompiledQueryContext {
        query_type: crate::QueryType::Traversal,
        base: crate::ParameterizedQuery {
            sql: described.join("\n"),
            params: Default::default(),
            columns: Vec::new(),
            result_context: produced(ctx.take_result_ctx())?,
            query_config: Default::default(),
            dialect: crate::SqlDialect::ClickHouse,
        },
        hydration: crate::HydrationPlan::None,
        shape: response.shape,
        stats: response.stats,
        debug: response.debug,
        has_virtual_columns: false,
    };
    Ok(Compiled {
        planned: ctx.take_planned().unwrap_or_default(),
        proved: false,
        emitted: if trace { described } else { Vec::new() },
        request: Request::Combine {
            plan,
            compiled: Box::new(compiled),
        },
    })
}

fn trace_relational<B>(
    trace: bool,
    plan: Option<&query_data_model::implementations::relational::plan::RelationalPlan<B>>,
) -> Vec<String> {
    match (trace, plan) {
        (true, Some(plan)) => describe(&plan.plan),
        _ => Vec::new(),
    }
}

/// What the top-level pipeline reads: query text, or a statement a surface already parsed.
pub enum Source<'a> {
    Text(&'a str),
    Parsed(query_data_model::surface::Statement),
}

/// The deployment's top-level pipeline: parse, normalize, restrict, admit, split into stages
/// across its served connections.
pub fn split_query(source: Source<'_>, env: &Env<'_>) -> Result<super::Dag> {
    let connections = env.connections.clone();
    let grants = env.grants.clone();
    match env.frontend {
        Frontend::Server(catalog) => {
            let mut ctx = config::GqlQueryCtx::new(
                grants,
                *env.limits,
                connections,
                env.trace,
                Arc::clone(catalog),
            );
            match source {
                Source::Text(raw) => ctx.set_raw(raw.to_string()),
                Source::Parsed(statement) => ctx.set_statement(statement),
            }
            config::run_gql_query(&mut ctx)
                .and_then(|()| produced(ctx.take_dag()))
                .count_err()
        }
        Frontend::Local(catalog) => {
            let mut ctx = config::GqlLocalCtx::new(
                grants,
                *env.limits,
                connections,
                env.trace,
                Arc::clone(catalog),
            );
            match source {
                Source::Text(raw) => ctx.set_raw(raw.to_string()),
                Source::Parsed(statement) => ctx.set_statement(statement),
            }
            config::run_gql_local(&mut ctx)
                .and_then(|()| produced(ctx.take_dag()))
                .count_err()
        }
    }
}

/// The `(hydration, clickhouse)` stage: the rows a relational query's checks kept, read by key
/// on the server's graph connection.
pub fn compile_hydration(
    request: crate::hydration::HydrationRequest,
    env: &Env<'_>,
) -> Result<Compiled> {
    let Frontend::Server(catalog) = env.frontend else {
        return Err(QueryError::PipelineInvariant(
            "hydration runs on the server frontend's connection".into(),
        ));
    };
    let (query, planned, emitted) =
        crate::hydration_stage(request, catalog, env.grants, env.limits, env.trace)?;
    Ok(Compiled {
        planned,
        proved: true,
        emitted,
        request: Request::Hydration(Box::new(query)),
    })
}
