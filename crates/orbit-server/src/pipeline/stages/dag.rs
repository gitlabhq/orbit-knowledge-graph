//! The executors the server registers with the DAG runner: ClickHouse on its graph connection.

use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use query_data_model::{Catalog, ClickHouse};
use query_engine::compiler::dag::{Env, Primary, Remotes};
use query_engine::compiler::{self, HydrationRequest};
use query_engine::pipeline::{PipelineError, QueryPipelineContext};
use query_engine::shared::dag::{DagError, Executed, Executors, NoRemoteChecks, Runner, Silent};

use super::execution::ClickHouseExecutor;
use crate::pipeline::correlation;

/// ClickHouse on the graph connection, under `label` and `log_comment`.
pub fn graph_executors(
    client: Arc<ArrowClickHouseClient>,
    data_model: &Catalog<ClickHouse>,
    label: &str,
    log_comment: String,
    settings: Vec<(String, String)>,
) -> Executors {
    let executor = ClickHouseExecutor {
        client,
        label: label.to_string(),
        log_comment,
        settings,
    };
    Executors::default().with(data_model.connection(), Arc::new(executor))
}

/// The hydration stage of the query in `ctx`, on the rows its checks kept.
pub(crate) async fn hydrate(
    ctx: &QueryPipelineContext,
    request: HydrationRequest,
    kind: &str,
) -> Result<(compiler::ParameterizedQuery, Executed), PipelineError> {
    let data_model = ctx
        .server_extensions
        .get::<Arc<Catalog<ClickHouse>>>()
        .ok_or_else(|| PipelineError::custom("query data model missing"))?;
    let client = ctx
        .server_extensions
        .get::<Arc<ArrowClickHouseClient>>()
        .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))?;
    let label = format!("hydration:{kind}");
    let log_comment = correlation::log_comment(Some(&label));
    let executors = graph_executors(
        Arc::clone(client),
        data_model,
        &label,
        log_comment,
        Vec::new(),
    );
    let primary = Primary::ClickHouse(Arc::clone(data_model));
    let remotes = Remotes::default();
    let env = Env {
        primary: &primary,
        remotes: &remotes,
        grants: ctx.grants()?,
        limits: ctx.limits()?,
        trace: false,
    };
    let mut silent = Silent;
    let mut runner = Runner::new(env, &executors, &NoRemoteChecks, &mut silent);
    runner.hydrate(request).await.map_err(|error| match error {
        DagError::Compile(error) => PipelineError::Compile {
            client_safe: false,
            reason: error.rejection_reason(),
            message: error.to_string(),
        },
        DagError::Execute(error) => PipelineError::Execution(error.0),
    })
}
