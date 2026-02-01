use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::compile;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::auth::Claims;
use crate::redaction::RedactionMessage;

use super::error::PipelineError;
use super::formatter::ResultFormatter;
use super::stages::{
    AuthorizationStage, ExtractionStage, FormattingStage, RedactionStage, SecurityStage,
};
use super::types::{ExecutionOutput, PipelineOutput};

#[derive(Clone)]
pub struct QueryPipelineService<F: ResultFormatter + Clone> {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    extraction: Arc<ExtractionStage>,
    formatter: F,
}

impl<F: ResultFormatter + Clone> QueryPipelineService<F> {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>, formatter: F) -> Self {
        let extraction = Arc::new(ExtractionStage::new(Arc::clone(&ontology)));
        Self {
            ontology,
            client,
            extraction,
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
            generated_sql: compiled.sql,
        };

        let extracted = self.extraction.execute(execution_output);

        if extracted.redaction_plan.resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
        }

        let authorized = AuthorizationStage::execute(extracted, tx, stream).await?;
        let redacted = RedactionStage::execute(authorized);
        let formatting_stage =
            FormattingStage::new(self.formatter.clone(), Arc::clone(&self.ontology));
        Ok(formatting_stage.execute(redacted))
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
