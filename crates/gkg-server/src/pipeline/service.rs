use std::sync::Arc;

use crate::auth::Claims;
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};

use querying_pipeline::{
    PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use querying_shared_stages::{CompilationStage, ExtractionStage, OutputStage, PipelineOutput};

use super::metrics::OTelPipelineObserver;
use super::stages::{
    AuthorizationChannel, AuthorizationStage, ClickHouseExecutor, HydrationStage, RedactionStage,
    SecurityStage,
};

#[derive(Clone)]
pub struct QueryPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
}

impl QueryPipelineService {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>) -> Self {
        Self { ontology, client }
    }

    pub async fn run_query(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = OTelPipelineObserver::start();

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(claims);
        server_extensions.insert(AuthorizationChannel {
            tx,
            stream: Mutex::new(stream),
        });

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
            .expect("OutputStage should produce PipelineOutput");

        obs.finish(output.row_count, output.redacted_count);
        Ok(output)
    }
}
