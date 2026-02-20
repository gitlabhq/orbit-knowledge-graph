use std::sync::Arc;

use crate::auth::Claims;
use crate::redaction::RedactionMessage;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::compile;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use super::error::PipelineError;
use super::formatter::ResultFormatter;
use super::stages::{
    AuthorizationStage, ExtractionStage, FormattingStage, HydrationStage, RedactionStage,
    SecurityStage,
};
use super::types::{ExecutionOutput, PipelineOutput};

#[derive(Clone)]
pub struct QueryPipelineService<F: ResultFormatter + Clone> {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    hydration: Arc<HydrationStage>,
    formatter: F,
}

impl<F: ResultFormatter + Clone> QueryPipelineService<F> {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>, formatter: F) -> Self {
        let hydration = Arc::new(HydrationStage::new(
            Arc::clone(&ontology),
            Arc::clone(&client),
        ));
        Self {
            ontology,
            client,
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
        let security_context = SecurityStage::execute(claims)?;

        let compiled = compile(query_json, &self.ontology, &security_context)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;
        let batches = self.execute_query(&compiled).await?;

        let execution_output = ExecutionOutput {
            batches,
            result_context: compiled.result_context,
        };

        let query_result = ExtractionStage::execute(execution_output);
        let authorized = AuthorizationStage::execute(query_result, tx, stream).await?;
        let redacted = RedactionStage::execute(authorized);
        let redacted_count = redacted.redacted_count;
        let query_result = redacted.query_result;

        let result_context = query_result.ctx().clone();
        let hydrated = self
            .hydration
            .execute(query_result, &result_context, &security_context)
            .await?;

        let formatting_stage =
            FormattingStage::new(self.formatter.clone(), Arc::clone(&self.ontology));
        Ok(formatting_stage.execute(hydrated, result_context, redacted_count, compiled.sql))
    }

    async fn execute_query(
        &self,
        compiled: &query_engine::ParameterizedQuery,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, PipelineError> {
        let mut query = self.client.query(&compiled.sql);
        for (key, value) in &compiled.params {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
    }
}
