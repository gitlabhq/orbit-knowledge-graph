//! Compiles one stage of a DAG through the sub-pipeline registered for its frontend and backend,
//! once the runner has the inputs the stage reads before it plans.

use std::sync::Arc;

use orbit_authorization::Grants;
use orbit_server_config::QueryLimits;
use query_data_model::{Catalog, ClickHouse, DuckDb};

use super::{Remotes, Request, Stage, StageId, StageQuery, describe};
use crate::config::{self, CompilerCtx as _};
use crate::error::{QueryError, Result};
use crate::metrics::CountErr;
use crate::passes::plan::remote::Target;
use crate::passes::remote_security::Namespaces;
use crate::passes::result_contract::ResultContext;

/// The relational connection a deployment plans GQL against.
#[derive(Debug, Clone)]
pub enum Primary {
    ClickHouse(Arc<Catalog<ClickHouse>>),
    DuckDb(Arc<Catalog<DuckDb>>),
}

impl Primary {
    pub fn model(&self) -> &query_data_model::Model {
        match self {
            Self::ClickHouse(catalog) => catalog.model(),
            Self::DuckDb(catalog) => catalog.model(),
        }
    }
}

/// The catalogs and caller every stage of one query compiles against.
pub struct Env<'a> {
    pub primary: &'a Primary,
    pub remotes: &'a Remotes,
    pub grants: &'a Grants,
    pub limits: &'a QueryLimits,
    /// Keep each stage's planned and emitted plan for an observer.
    pub trace: bool,
}

/// What a stage reads before it plans, besides the key sets `bind` puts in its statement: the
/// scope keys its remote reads resolved, and the result contexts of the stages the engine joins.
#[derive(Debug, Default)]
pub struct Inputs {
    pub namespaces: Namespaces,
    pub contexts: Vec<(StageId, ResultContext)>,
    /// Key sets on composite keys, which only a code stage reads, as branch heads.
    pub composite: Vec<super::KeySet>,
}

/// Each composite key as the (repository, branch) head a code search runs at.
pub fn heads(sets: &[super::KeySet]) -> Result<Vec<(i64, String)>> {
    use orbit_authorization::Key;
    let mut heads = Vec::new();
    for key in sets.iter().flat_map(|set| &set.keys) {
        match key {
            Key::Composite(parts) => match &parts[..] {
                [Key::Int(project), Key::Text(branch)] => heads.push((*project, branch.clone())),
                _ => {
                    return Err(QueryError::Validation(format!(
                        "a structural search runs at (repository, branch) heads, not {key}"
                    )));
                }
            },
            _ => {
                return Err(QueryError::Validation(format!(
                    "a structural search runs at (repository, branch) heads, not {key}"
                )));
            }
        }
    }
    Ok(heads)
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

pub fn compile(stage: Stage, inputs: Inputs, env: &Env<'_>) -> Result<Compiled> {
    let (kind, relational, response) = (
        stage.query_kind(),
        stage.backend.relational(),
        stage.response,
    );
    if !inputs.composite.is_empty() && stage.backend != super::Backend::OrbitCode {
        return Err(QueryError::Validation(format!(
            "a {} stage reads single-column key sets; composite keys cross into {} stages",
            <&str>::from(stage.backend),
            <&str>::from(super::Backend::OrbitCode)
        )));
    }
    let connection = stage.connection.clone().unwrap_or_default();
    let (statement, engine) = match stage.query {
        StageQuery::Gql(statement) => (Some(statement), None),
        StageQuery::Engine(query) => (None, Some(query)),
        StageQuery::Hydration(_) => (None, None),
    };
    let (grants, limits, trace) = (env.grants.clone(), *env.limits, env.trace);
    match (kind, env.primary) {
        (Kind::Gql, Primary::ClickHouse(catalog)) if relational => {
            let mut ctx =
                config::GqlClickhouseCtx::new(grants, limits, trace, response, Arc::clone(catalog));
            ctx.set_statement(produced(statement)?);
            config::run_gql_clickhouse(&mut ctx).count_err()?;
            let emitted = trace_relational(trace, ctx.relational().as_ref());
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: ctx.take_proved().unwrap_or_default(),
                emitted,
                request: Request::Query(Box::new(produced(ctx.take_output())?)),
            })
        }
        (Kind::Gql, Primary::DuckDb(catalog)) if relational => {
            let mut ctx = config::GqlDuckdbCtx::new(limits, trace, Arc::clone(catalog));
            ctx.set_statement(produced(statement)?);
            config::run_gql_duckdb(&mut ctx).count_err()?;
            let emitted = trace_relational(trace, ctx.relational().as_ref());
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: false,
                emitted,
                request: Request::Query(Box::new(produced(ctx.take_output())?)),
            })
        }
        (Kind::Gql, _) => match env.remotes.target(&connection) {
            Some(Target::Code(code)) => {
                let mut ctx = config::GqlOrbitCodeCtx::new(grants, limits, trace, Arc::clone(code));
                ctx.set_statement(produced(statement)?);
                ctx.set_namespaces(inputs.namespaces);
                ctx.set_heads(heads(&inputs.composite)?);
                config::run_gql_orbit_code(&mut ctx).count_err()?;
                let emitted = trace.then(|| ctx.code().as_ref().map(describe)).flatten();
                Ok(Compiled {
                    planned: ctx.take_planned().unwrap_or_default(),
                    proved: ctx.take_proved().unwrap_or_default(),
                    emitted: emitted.unwrap_or_default(),
                    request: produced(ctx.take_request())?,
                })
            }
            Some(Target::Live(live)) => {
                let mut ctx = config::GqlLiveCtx::new(grants, limits, trace, Arc::clone(live));
                ctx.set_statement(produced(statement)?);
                ctx.set_namespaces(inputs.namespaces);
                config::run_gql_live(&mut ctx).count_err()?;
                let emitted = trace.then(|| ctx.live().as_ref().map(describe)).flatten();
                Ok(Compiled {
                    planned: ctx.take_planned().unwrap_or_default(),
                    proved: ctx.take_proved().unwrap_or_default(),
                    emitted: emitted.unwrap_or_default(),
                    request: produced(ctx.take_request())?,
                })
            }
            None => Err(QueryError::PipelineInvariant(format!(
                "no served connection {connection}"
            ))),
        },
        (Kind::Engine, _) => {
            let shared = match env.primary {
                Primary::ClickHouse(catalog) => Arc::clone(catalog.shared_model()),
                Primary::DuckDb(catalog) => Arc::clone(catalog.shared_model()),
            };
            let mut ctx = config::GqlEngineCtx::new(limits, trace, shared);
            ctx.set_engine_query(produced(engine)?);
            ctx.set_stage_contexts(inputs.contexts);
            config::run_gql_engine(&mut ctx).count_err()?;
            let plan = produced(ctx.take_engine())?;
            Ok(Compiled {
                planned: ctx.take_planned().unwrap_or_default(),
                proved: false,
                emitted: if trace { describe(&plan) } else { Vec::new() },
                request: Request::Engine {
                    plan,
                    result: produced(ctx.take_result_ctx())?,
                },
            })
        }
        (Kind::Hydration, _) => Err(QueryError::PipelineInvariant(
            "a hydration stage compiles through compile_hydration".into(),
        )),
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Gql,
    Engine,
    Hydration,
}

impl Stage {
    fn query_kind(&self) -> Kind {
        match self.query {
            StageQuery::Gql(_) => Kind::Gql,
            StageQuery::Engine(_) => Kind::Engine,
            StageQuery::Hydration(_) => Kind::Hydration,
        }
    }
}

/// What the top-level pipeline reads: query text, or a statement a surface already parsed.
pub enum Source<'a> {
    Text(&'a str),
    Parsed(query_data_model::surface::Statement),
}

/// The top-level pipeline over the deployment's relational connection and its served remotes:
/// parse, normalize, restrict, admit, split into stages.
pub fn split_query(source: Source<'_>, env: &Env<'_>) -> Result<super::Dag> {
    let remotes = env.remotes.clone();
    match env.primary {
        Primary::ClickHouse(catalog) => {
            let grants = env.grants.clone();
            let mut ctx = config::GqlQueryCtx::new(
                grants,
                *env.limits,
                remotes,
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
        Primary::DuckDb(catalog) => {
            let mut ctx =
                config::GqlLocalCtx::new(*env.limits, remotes, env.trace, Arc::clone(catalog));
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

/// The `(hydration, clickhouse)` stage: the rows a relational query's checks kept, read by key.
pub fn compile_hydration(
    request: crate::hydration::HydrationRequest,
    env: &Env<'_>,
) -> Result<Compiled> {
    let Primary::ClickHouse(catalog) = env.primary else {
        return Err(QueryError::PipelineInvariant(
            "hydration reads the ClickHouse connection".into(),
        ));
    };
    let (query, planned, emitted) =
        crate::hydration_stage(request, catalog, env.grants, env.limits, env.trace)?;
    Ok(Compiled {
        planned,
        proved: true,
        emitted,
        request: Request::Hydration(query),
    })
}

impl Primary {
    pub fn connection(&self) -> &str {
        match self {
            Self::ClickHouse(catalog) => catalog.connection(),
            Self::DuckDb(catalog) => catalog.connection(),
        }
    }
}
