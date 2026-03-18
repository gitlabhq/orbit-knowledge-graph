use std::sync::Arc;

use crate::auth::Claims;
use crate::redaction::RedactionMessage;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use querying_pipeline::{
    CompilationStage, ExtractionStage, FormattingStage, PipelineError, PipelineOutput,
    PipelineRunner, QueryPipelineContext, RedactionStage, ResultFormatter,
};

use super::metrics::OTelPipelineObserver;
use super::stages::{ClickHouseExecutor, GrpcAuthorizer, HydrationStage, SecurityStage};

#[derive(Clone)]
pub struct QueryPipelineService<F: ResultFormatter + Clone> {
    ontology: Arc<Ontology>,
    executor: ClickHouseExecutor,
    hydrator: HydrationStage,
    formatter: FormattingStage<F>,
}

impl<F: ResultFormatter + Clone> QueryPipelineService<F> {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>, formatter: F) -> Self {
        Self {
            ontology,
            executor: ClickHouseExecutor::new(client.clone()),
            hydrator: HydrationStage::new(client),
            formatter: FormattingStage::new(formatter),
        }
    }

    pub async fn run_query<M: RedactionMessage + 'static>(
        &self,
        claims: &Claims,
        query_json: &str,
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = OTelPipelineObserver::start();

        let mut ctx = QueryPipelineContext {
            query_json: query_json.to_string(),
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: None,
        };

        let security = SecurityStage::new(claims);
        let authorizer = GrpcAuthorizer::new(tx, stream);

        let output = PipelineRunner::start(&mut ctx, &mut obs)
            .then(&security)
            .await?
            .then(&CompilationStage)
            .await?
            .then(&self.executor)
            .await?
            .then(&ExtractionStage)
            .await?
            .then(&authorizer)
            .await?
            .then(&RedactionStage)
            .await?
            .then(&self.hydrator)
            .await?
            .then(&self.formatter)
            .await?
            .finish();

        obs.finish(&output);
        Ok(output)
    }
}
