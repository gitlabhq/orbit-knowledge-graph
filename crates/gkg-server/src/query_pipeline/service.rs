use std::sync::Arc;

use crate::auth::Claims;
use crate::redaction::RedactionMessage;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use super::error::PipelineError;
use super::formatters::ResultFormatter;
use super::metrics::PipelineObserver;
use super::stages::{
    AuthorizationStage, CompilationStage, ExecutionStage, ExtractionStage, FormattingStage,
    HydrationStage, PipelineRunner, RedactionStage, SecurityStage,
};
use super::types::{PipelineOutput, PipelineRequest, QueryPipelineContext};

#[derive(Clone)]
pub struct QueryPipelineService<F: ResultFormatter + Clone> {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    formatter: FormattingStage<F>,
}

impl<F: ResultFormatter + Clone> QueryPipelineService<F> {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>, formatter: F) -> Self {
        Self {
            ontology,
            client,
            formatter: FormattingStage::new(formatter),
        }
    }

    pub async fn run_query<M: RedactionMessage>(
        &self,
        claims: &Claims,
        query_json: &str,
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = PipelineObserver::start();

        let mut ctx = QueryPipelineContext {
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            client: Arc::clone(&self.client),
            security_context: None,
        };

        let req = PipelineRequest {
            claims,
            query_json,
            tx: Some(tx),
            stream: Some(stream),
        };

        let output = PipelineRunner::start(&mut ctx, req, &mut obs)
            .then(&SecurityStage)
            .await?
            .then(&CompilationStage)
            .await?
            .then(&ExecutionStage)
            .await?
            .then(&ExtractionStage)
            .await?
            .then(&AuthorizationStage)
            .await?
            .then(&RedactionStage)
            .await?
            .then(&HydrationStage)
            .await?
            .then(&self.formatter)
            .await?
            .finish();

        obs.finish(&output);
        Ok(output)
    }
}
