use std::sync::Arc;

use crate::auth::Claims;
use crate::redaction::{QueryResult, RedactionMessage};
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::{CompiledQuery, ParameterizedQuery, compile};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use super::error::PipelineError;
use super::formatter::ResultFormatter;
use super::stages::{
    AuthorizationStage, FormattingStage, HydrationStage, RedactionStage, SecurityStage,
};
use super::types::PipelineOutput;

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

        let compiled: CompiledQuery = compile(query_json, &self.ontology, &security_context)
            .map_err(|e| PipelineError::Compile(e.to_string()))?;
        let structural_sql = compiled.structural.sql.clone();
        let batches = self.execute_query(&compiled.structural).await?;

        let mut query_result =
            QueryResult::from_batches(&batches, &compiled.structural.result_context);

        // Pre-auth hydration: for dynamic queries with indirect-auth entities,
        // resolve auth_id_column values before authorization so resource_checks()
        // and apply_authorizations() can use the correct auth IDs.
        if matches!(compiled.hydration, query_engine::HydrationPlan::Dynamic) {
            let result_ctx = query_result.ctx().clone();
            self.hydration
                .resolve_auth_ids(&mut query_result, &result_ctx, &security_context)
                .await?;
        }

        let authorized = AuthorizationStage::execute(query_result, tx, stream).await?;
        let (query_result, redacted_count) = RedactionStage::execute(authorized);

        let result_context = query_result.ctx().clone();
        let hydrated = self
            .hydration
            .execute(
                query_result,
                &result_context,
                &security_context,
                &compiled.hydration,
            )
            .await?;

        let formatting_stage =
            FormattingStage::new(self.formatter.clone(), Arc::clone(&self.ontology));
        Ok(formatting_stage.execute(hydrated, result_context, redacted_count, structural_sql))
    }

    async fn execute_query(
        &self,
        compiled: &ParameterizedQuery,
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
