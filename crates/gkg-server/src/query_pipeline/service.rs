use std::sync::Arc;

use crate::auth::Claims;
use crate::redaction::RedactionMessage;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use querying_pipeline::{
    Authorizer, CompilationStage, ExecutionOutput, ExtractionStage, FormattingStage, Hydrator,
    PipelineError, PipelineOutput, QueryExecutor, QueryPipelineContext, RedactionStage,
    ResultFormatter,
};

use super::metrics::OTelPipelineObserver;
use super::stages::{ClickHouseExecutor, GrpcAuthorizer, HydrationStage, SecurityStage};

#[derive(Clone)]
pub struct QueryPipelineService<F: ResultFormatter + Clone> {
    ontology: Arc<Ontology>,
    executor: Arc<ClickHouseExecutor>,
    hydrator: Arc<HydrationStage>,
    formatter: FormattingStage<F>,
}

impl<F: ResultFormatter + Clone> QueryPipelineService<F> {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>, formatter: F) -> Self {
        Self {
            ontology: ontology.clone(),
            executor: Arc::new(ClickHouseExecutor::new(client.clone())),
            hydrator: Arc::new(HydrationStage::new(client)),
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

        // Security: build context from JWT claims (server-specific)
        let security_context = obs.check_result(
            SecurityStage::build_context(claims)
                .map_err(|e| PipelineError::Security(e.to_string())),
        )?;

        let mut ctx = QueryPipelineContext {
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: Some(security_context),
        };

        // Compilation (pure)
        CompilationStage.execute(query_json, &mut ctx, &mut obs)?;

        // Execution (ClickHouse)
        let batches = self.executor.execute(&ctx, &mut obs).await?;
        let execution_output = ExecutionOutput {
            batches,
            result_context: ctx.compiled()?.base.result_context.clone(),
        };

        // Extraction (pure)
        let extraction_output = ExtractionStage.execute(execution_output);

        // Authorization (gRPC)
        let mut authorizer = GrpcAuthorizer::new(tx, stream);
        let authorization_output = authorizer.authorize(extraction_output, &mut obs).await?;

        // Redaction (pure)
        let redaction_output = RedactionStage.execute(authorization_output);

        // Hydration (ClickHouse)
        let hydration_output = self
            .hydrator
            .hydrate(redaction_output, &ctx, &mut obs)
            .await?;

        // Formatting (pure)
        let output = self.formatter.execute(hydration_output, &ctx)?;

        obs.finish(&output);
        Ok(output)
    }
}
