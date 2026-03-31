use std::sync::Arc;
use std::time::Duration;

use crate::auth::Claims;
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::{ArrowClickHouseClient, ProfilingConfig};
use ontology::Ontology;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{CompilationStage, ExtractionStage, OutputStage, PipelineOutput};

use super::metrics::OTelPipelineObserver;
use crate::config::QueryConfig;

use super::stages::{
    AuthorizationStage, ClickHouseExecutor, HydrationStage, RedactionStage, SecurityStage,
};

/// Default query timeout if not configured.
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct QueryPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    profiling: ProfilingConfig,
    query_timeout: Duration,
}

impl QueryPipelineService {
    pub fn new(
        ontology: Arc<Ontology>,
        client: Arc<ArrowClickHouseClient>,
        profiling: ProfilingConfig,
    ) -> Self {
        Self {
            ontology,
            client,
            profiling,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    pub async fn run_query(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        tokio::time::timeout(
            self.query_timeout,
            self.run_pipeline(claims, query_json, tx, stream),
        )
        .await
        .map_err(|_| {
            PipelineError::Execution(format!(
                "query timed out after {}s",
                self.query_timeout.as_secs()
            ))
        })?
    }

    async fn run_pipeline(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = OTelPipelineObserver::start();

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(self.profiling.clone());
        server_extensions.insert(QueryConfig {
            timeout_secs: Some(self.query_timeout.as_secs()),
        });
        server_extensions.insert(claims);
        server_extensions.insert(tx);
        server_extensions.insert(stream);

        let mut ctx = QueryPipelineContext {
            query_json: query_json.to_string(),
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: None,
            server_extensions,
            phases: TypeMap::default(),
        };

        let output = PipelineRunner::start(&mut ctx, &mut obs)
            .then(&SecurityStage)
            .await?
            .then(&CompilationStage)
            .await?
            .then(&ClickHouseExecutor)
            .await?
            .then(&ExtractionStage)
            .await?
            .then(&AuthorizationStage)
            .await?
            .then(&RedactionStage)
            .await?
            .then(&HydrationStage)
            .await?
            .then(&OutputStage)
            .await?
            .finish()
            .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))?;

        obs.finish(output.row_count, output.redacted_count);
        Ok(output)
    }
}
