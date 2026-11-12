//! The DuckDB executor a local deployment registers with the DAG runner, and a local run: a
//! query the DuckDB connection serves alone, as a one-stage DAG.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use compiler::dag::{Env, Primary, Remotes, Request, Source};
use compiler::orbit_authorization::{Caller, Grants};
use orbit_server_config::QueryLimits;
use query_data_model::{Catalog, DuckDb};
use shared::dag::{
    DagError, Executed, Executor, ExecutorError, Executors, NoRemoteChecks, Outcome, Runner, Silent,
};
use shared::{QueryExecution, QueryExecutionStats};

use crate::DuckDbClient;

/// Runs each relational stage's SQL on one local database.
pub struct DuckDbExecutor {
    pub client: Arc<Mutex<DuckDbClient>>,
}

#[async_trait]
impl Executor for DuckDbExecutor {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Query(compiled) = request else {
            return Err(ExecutorError(format!(
                "DuckDB runs queries, not {request:?}"
            )));
        };
        let start = std::time::Instant::now();
        let sql = compiled.base.render();
        let client = self
            .client
            .lock()
            .map_err(|_| ExecutorError("the local database lock is poisoned".into()))?;
        let batches = client
            .query_arrow(&sql)
            .map_err(|error| ExecutorError(error.to_string()))?;
        let elapsed = start.elapsed();
        let execution = QueryExecution {
            label: "base".into(),
            rendered_sql: sql,
            query_id: String::new(),
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats: QueryExecutionStats {
                result_rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
                elapsed_ns: elapsed.as_nanos() as u64,
                ..Default::default()
            },
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };
        Ok(Executed {
            batches,
            log: vec![execution],
        })
    }
}

/// Runs `query` through the DAG runner on the local database: local mode trusts its data, so
/// the caller holds no grants and no remote connection is served.
pub fn run_local(
    client: &Arc<Mutex<DuckDbClient>>,
    data_model: &Arc<Catalog<DuckDb>>,
    query: &str,
    limits: &QueryLimits,
) -> Result<Outcome, DagError> {
    let executor = DuckDbExecutor {
        client: Arc::clone(client),
    };
    let executors = Executors::default().with(data_model.connection(), Arc::new(executor));
    let primary = Primary::DuckDb(Arc::clone(data_model));
    let remotes = Remotes::default();
    let grants = Grants::new(Caller::new("local", true, ()));
    let env = Env {
        primary: &primary,
        remotes: &remotes,
        grants: &grants,
        limits,
        trace: false,
    };
    let mut silent = Silent;
    let mut runner = Runner::new(env, &executors, &NoRemoteChecks, &mut silent);
    futures::executor::block_on(runner.run(Source::Text(query)))
}
