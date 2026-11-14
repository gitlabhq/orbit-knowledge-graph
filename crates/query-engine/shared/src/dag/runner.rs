//! Runs a query's DAG: each stage in dependency order reads the key sets earlier stages
//! returned, compiles through its sub-pipeline, runs on its connection's executor, and puts its
//! rows through the stage's checks before later stages read them.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use compiler::dag::{
    Compiled, Dag, Env, Input, Inputs, KeySet, Request, Source, Stage, StageId, StageQuery, bind,
    compile, split_query,
};
use compiler::passes::result_contract::ResultContext;
use compiler::{CompiledQueryContext, HydrationRequest, ParameterizedQuery, QueryError};

use compiler::orbit_authorization::Checks;

use super::executor::{Executed, ExecutorError, Executors};
use super::observer::{Event, KEY_SAMPLE, Observer, StageInfo};
use super::{remote, rows};

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error(transparent)]
    Compile(#[from] QueryError),
    #[error(transparent)]
    Execute(#[from] ExecutorError),
}

impl From<DagError> for pipeline::PipelineError {
    fn from(error: DagError) -> Self {
        match error {
            DagError::Compile(error) => error.into(),
            DagError::Execute(error) => Self::Execution(error.0),
        }
    }
}

/// What a query returns once its stages ran: the compiled context of the stage whose rows are
/// the response, a single relational stage or the Combine stage, and those rows.
#[derive(Debug)]
pub struct Outcome {
    pub compiled: Arc<CompiledQueryContext>,
    pub executed: Executed,
}

pub struct Runner<'r> {
    pub env: Env<'r>,
    pub executors: &'r Executors,
    pub checks: &'r Checks,
    pub observer: &'r mut dyn Observer,
    /// The stage the next hydration runs as.
    next: StageId,
}

impl<'r> Runner<'r> {
    pub fn new(
        mut env: Env<'r>,
        executors: &'r Executors,
        checks: &'r Checks,
        observer: &'r mut dyn Observer,
    ) -> Self {
        env.trace = observer.enabled();
        Self {
            env,
            executors,
            checks,
            observer,
            next: 0,
        }
    }

    pub(super) fn emit(&mut self, event: impl FnOnce() -> Event) {
        if self.env.trace {
            self.observer.event(event());
        }
    }

    /// Splits the query into stages and runs them.
    pub async fn run(&mut self, source: Source<'_>) -> Result<Outcome, DagError> {
        let mut dag = split_query(source, &self.env)?;
        if self.env.trace {
            let plan = std::mem::take(&mut dag.logical);
            self.observer.event(Event::Logical { plan });
            let stages = dag
                .stages
                .iter()
                .map(|stage| StageInfo {
                    id: stage.id,
                    backend: stage.backend,
                    connection: stage.runs.connection().map(|connection| {
                        self.env
                            .frontend
                            .model()
                            .connection(connection)
                            .name()
                            .to_string()
                    }),
                    inputs: stage.inputs.clone(),
                    logical: match &stage.query {
                        StageQuery::Gql(statement) => compiler::dag::describe(&statement.plan),
                        StageQuery::Combine(..) | StageQuery::Hydration(_) => Vec::new(),
                    },
                })
                .collect();
            self.observer.event(Event::Split { stages });
        }
        self.stages(dag).await
    }

    async fn stages(&mut self, dag: Dag) -> Result<Outcome, DagError> {
        let single = dag.stages.len() == 1;
        self.next = dag.stages.len();
        let mut results: HashMap<StageId, Vec<RecordBatch>> = HashMap::new();
        let mut contexts: Vec<(StageId, ResultContext)> = Vec::new();
        let mut log = Vec::new();
        let mut last = None;
        for stage in dag.stages {
            let id = stage.id;
            self.emit(|| Event::Started { stage: id });
            let started = Instant::now();
            let ran = self.stage(stage, &results, &mut contexts).await;
            let (compiled, executed) = match ran {
                Ok(ran) => ran,
                Err(error) => {
                    self.emit(|| Event::Failed {
                        stage: id,
                        error: error.to_string(),
                    });
                    return Err(error);
                }
            };
            let count = executed.batches.iter().map(RecordBatch::num_rows).sum();
            self.emit(|| Event::Rows { stage: id, count });
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            self.emit(|| Event::Finished {
                stage: id,
                elapsed_ms,
            });
            log.extend(executed.log.iter().cloned());
            results.insert(id, executed.batches.clone());
            last = Some((compiled, executed));
        }
        let (request, executed) = last
            .ok_or_else(|| QueryError::PipelineInvariant("a DAG has at least one stage".into()))?;
        let compiled = match request {
            Some(Request::Query(compiled)) if single => compiled,
            Some(Request::Combine { compiled, .. }) => compiled,
            _ => {
                return Err(QueryError::PipelineInvariant(
                    "the last stage of a DAG is its response stage".into(),
                )
                .into());
            }
        };
        Ok(Outcome {
            compiled: Arc::from(compiled),
            executed: Executed {
                batches: executed.batches,
                log,
            },
        })
    }

    /// Compiles and runs one stage; a stage whose key set came back empty reads nothing.
    async fn stage(
        &mut self,
        mut stage: Stage,
        results: &HashMap<StageId, Vec<RecordBatch>>,
        contexts: &mut Vec<(StageId, ResultContext)>,
    ) -> Result<(Option<Request>, Executed), DagError> {
        let id = stage.id;
        let mut sets = Vec::new();
        let mut scopes = Vec::new();
        for input in &stage.inputs {
            let (input, into) = match input {
                Input::Keys(input) => (input, &mut sets),
                Input::Scope(input) => (input, &mut scopes),
                Input::Rows { .. } => continue,
            };
            let produced = results.get(&input.from).map_or(&[][..], Vec::as_slice);
            let keys = rows::keys(produced, &input.positions, &input.types)?;
            self.emit(|| Event::Keys {
                from: input.from,
                to: id,
                binding: input.binding.clone(),
                count: keys.len(),
                sample: keys.iter().take(KEY_SAMPLE).cloned().collect(),
            });
            into.push(KeySet {
                input: input.clone(),
                keys,
            });
        }
        if sets.iter().chain(&scopes).any(|set| set.keys.is_empty()) {
            return Ok((None, Executed::default()));
        }
        let mut inputs = Inputs {
            contexts: contexts.clone(),
            ..Inputs::default()
        };
        if let StageQuery::Gql(statement) = stage.query {
            let connections = self.env.connections.ids();
            let bound = bind(
                statement,
                &sets,
                self.env.frontend.model(),
                self.env.limits,
                connections,
            )?;
            if let Some(target) = stage
                .runs
                .connection()
                .and_then(|connection| self.env.connections.target(connection))
            {
                let admitted =
                    compiler::remote::admitted(target, &bound.plan, self.env.grants, &scopes)?;
                if admitted.is_empty() {
                    return Ok((None, Executed::default()));
                }
                inputs.admitted = Some(admitted);
            }
            stage.query = StageQuery::Gql(bound);
        }
        let (connection, backend) = (stage.runs.connection(), stage.backend);
        let contract = stage.contract.clone();
        let checks = std::mem::take(&mut stage.checks);
        let compiled = compile(stage, inputs, &self.env)?;
        self.trace(id, &compiled);
        if !compiled.proved && backend != compiler::dag::Backend::Combine {
            return Err(QueryError::Security(format!(
                "stage {id} carries no proof that each read has its policy"
            ))
            .into());
        }
        let executed = match (&compiled.request, &connection) {
            (Request::Combine { plan, .. }, _) => Executed {
                batches: vec![super::combine::run(plan, results)?],
                log: Vec::new(),
            },
            (request, Some(connection)) => {
                self.executors
                    .get(*connection)?
                    .execute(request, contract.as_ref())
                    .await?
            }
            (_, None) => {
                return Err(QueryError::PipelineInvariant(format!(
                    "a {backend:?} stage has no connection"
                ))
                .into());
            }
        };
        let rows: usize = executed.batches.iter().map(RecordBatch::num_rows).sum();
        let cap = self.env.limits.stage_keys;
        if contract.is_some() && rows > cap {
            return Err(QueryError::LimitExceeded(format!(
                "stage {id} returned more than {cap} rows; a stage hands at most {cap} rows to the next"
            ))
            .into());
        }
        let executed =
            remote::checked(self.checks, self.env.grants.caller(), &checks, executed).await?;
        if let Request::Query(query) = &compiled.request {
            contexts.push((id, query.base.result_context.clone()));
        }
        Ok((Some(compiled.request), executed))
    }

    fn trace(&mut self, stage: StageId, compiled: &Compiled) {
        self.emit(|| Event::Planned {
            stage,
            plan: compiled.planned.clone(),
        });
        self.emit(|| Event::Emitted {
            stage,
            plan: compiled.emitted.clone(),
        });
        self.emit(|| Event::Rendered {
            stage,
            requests: compiled.request.rendered(),
        });
    }

    /// Runs the hydration stage of a relational query on the rows its checks kept.
    pub async fn hydrate(
        &mut self,
        request: HydrationRequest,
    ) -> Result<(ParameterizedQuery, Executed), DagError> {
        let id = self.next;
        self.next += 1;
        self.emit(|| Event::Started { stage: id });
        let started = Instant::now();
        let ran = self.hydration(id, request).await;
        match &ran {
            Ok((_, executed)) => {
                let count = executed.batches.iter().map(RecordBatch::num_rows).sum();
                self.emit(|| Event::Rows { stage: id, count });
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                self.emit(|| Event::Finished {
                    stage: id,
                    elapsed_ms,
                });
            }
            Err(error) => self.emit(|| Event::Failed {
                stage: id,
                error: error.to_string(),
            }),
        }
        ran
    }

    async fn hydration(
        &mut self,
        id: StageId,
        request: HydrationRequest,
    ) -> Result<(ParameterizedQuery, Executed), DagError> {
        let compiled = compiler::dag::compile_hydration(request, &self.env)?;
        self.trace(id, &compiled);
        let compiler::dag::Frontend::Server(graph) = self.env.frontend else {
            return Err(QueryError::PipelineInvariant(
                "hydration runs on the server frontend's connection".into(),
            )
            .into());
        };
        let executor = self.executors.get(graph.connection())?;
        let executed = executor.execute(&compiled.request, None).await?;
        let Request::Hydration(query) = compiled.request else {
            return Err(QueryError::PipelineInvariant(
                "the hydration stage compiled no hydration query".into(),
            )
            .into());
        };
        Ok((*query, executed))
    }
}
