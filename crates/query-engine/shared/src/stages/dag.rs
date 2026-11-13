//! Runs every query through the DAG runner: the top-level pipeline splits it into stages, and
//! each stage compiles and runs on the executor the deployment registers for its connection.

use std::sync::Arc;
use std::time::{Duration, Instant};

use compiler::dag::{Connections, Env, Frontend, Source};
use compiler::gql::ParsedQuery;
use compiler::{CompiledQueryContext, QueryError};
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use query_data_model::{Catalog, ClickHouse};

use compiler::orbit_authorization::Checks;

use crate::dag::{Executed, Executors, Outcome, Runner, Silent};
use crate::types::{ExecutionOutput, QueryExecutionLog};

/// The executors a deployment registers, the connections it serves next to its graph
/// connection, and the providers that check each stage's rows.
pub struct DagStage {
    pub executors: Executors,
    pub peers: Vec<compiler::dag::Peer>,
    pub checks: Checks,
}

impl DagStage {
    /// A deployment that serves its graph connection alone, checking rows through the
    /// deployment's providers.
    pub fn relational(executors: Executors, checks: Checks) -> Self {
        Self {
            executors,
            peers: Vec::new(),
            checks,
        }
    }
}

fn data_model(ctx: &QueryPipelineContext) -> Result<Arc<Catalog<ClickHouse>>, PipelineError> {
    ctx.server_extensions
        .get::<Arc<Catalog<ClickHouse>>>()
        .cloned()
        .ok_or_else(|| QueryError::PipelineInvariant("no data model in the pipeline".into()).into())
}

fn record(
    obs: &mut dyn PipelineObserver,
    compiled: &CompiledQueryContext,
    executed: &Executed,
    start: Instant,
) {
    let executing: Duration = executed
        .log
        .iter()
        .map(|execution| Duration::from_secs_f64(execution.elapsed_ms / 1000.0))
        .sum();
    let query_type: &str = compiled.query_type.into();
    obs.set_query_type(query_type);
    obs.set_compiled(compiled);
    obs.compiled(start.elapsed().saturating_sub(executing));
    obs.executed(executing, executed.batches.len());
    for execution in &executed.log {
        obs.query_executed(
            &execution.label,
            execution.stats.read_rows,
            execution.stats.read_bytes,
            execution.stats.memory_usage,
        );
    }
}

impl PipelineStage for DagStage {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let start = Instant::now();
        let parsed = ctx.phases.remove::<ParsedQuery>();
        let data_model = data_model(ctx).inspect_err(|e| obs.record_error(e))?;
        let grants = ctx.grants().inspect_err(|e| obs.record_error(e))?;
        let limits = ctx.limits().inspect_err(|e| obs.record_error(e))?;
        let connections = Connections::new(
            std::iter::once(compiler::dag::Peer::ClickHouse(Arc::clone(&data_model)))
                .chain(self.peers.iter().cloned())
                .collect(),
        );
        let frontend = Frontend::Server(data_model);
        let env = Env {
            frontend: &frontend,
            connections: &connections,
            grants,
            limits,
            trace: false,
        };
        let source = match parsed {
            Some(query) => Source::Parsed(query.statement),
            None => Source::Text(&ctx.query_json),
        };
        let mut silent = Silent;
        let outcome = Runner::new(env, &self.executors, &self.checks, &mut silent)
            .run(source)
            .await
            .map_err(PipelineError::from)
            .inspect_err(|e| obs.record_error(e))?;
        let Outcome { compiled, executed } = outcome;
        record(obs, &compiled, &executed, start);
        let result_context = compiled.base.result_context.clone();
        ctx.compiled = Some(compiled);
        ctx.phases
            .get_or_insert_default::<QueryExecutionLog>()
            .0
            .extend(executed.log);
        Ok(ExecutionOutput {
            batches: executed.batches,
            result_context,
        })
    }
}
