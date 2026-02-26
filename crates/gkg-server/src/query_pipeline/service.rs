use std::sync::Arc;

use crate::auth::Claims;
use crate::redaction::RedactionMessage;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use super::error::PipelineError;
use super::formatter::ResultFormatter;
use super::metrics::PipelineObserver;
use super::stages::{
    AuthorizationStage, CompilationStage, ExecutionStage, ExtractionStage, FormattingStage,
    HydrationStage, RedactionStage, SecurityStage,
};
use super::types::PipelineOutput;

#[derive(Clone)]
pub struct QueryPipelineService<F: ResultFormatter + Clone> {
    ontology: Arc<Ontology>,
    execution: Arc<ExecutionStage>,
    hydration: Arc<HydrationStage>,
    formatter: F,
}

impl<F: ResultFormatter + Clone> QueryPipelineService<F> {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>, formatter: F) -> Self {
        let hydration = Arc::new(HydrationStage::new(
            Arc::clone(&ontology),
            Arc::clone(&client),
        ));
        let execution = Arc::new(ExecutionStage::new(client));
        Self {
            ontology,
            execution,
            hydration,
            formatter,
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

        let security_context = SecurityStage::execute(claims, &obs)?;

        let compiled =
            CompilationStage::execute(query_json, &self.ontology, &security_context, &mut obs)?;

        let execution_output = self.execution.execute(&compiled, &mut obs).await?;
        let query_result = ExtractionStage::execute(execution_output, &obs);

        let authorized = AuthorizationStage::execute(query_result, tx, stream, &mut obs).await?;

        let redacted = RedactionStage::execute(authorized, &obs);

        let hydrated = self
            .hydration
            .execute(redacted, &security_context, &mut obs)
            .await?;

        let formatting_stage =
            FormattingStage::new(self.formatter.clone(), Arc::clone(&self.ontology));
        let output = formatting_stage.execute(hydrated, &compiled, &obs);
        obs.finish(&output);

        Ok(output)
    }
}
