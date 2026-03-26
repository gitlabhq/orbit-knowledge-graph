use std::sync::Arc;

use crate::auth::Claims;
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::{ArrowClickHouseClient, ProfilingConfig};
use ontology::Ontology;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{
    CompilationStage, ExtractionStage, OutputStage, PaginationMeta, PipelineOutput,
};

use super::cache::QueryResultCache;
use super::metrics::OTelPipelineObserver;
use super::stages::{
    AuthorizationStage, ClickHouseExecutor, HydrationStage, RedactionStage, SecurityStage,
};

#[derive(Clone)]
pub struct QueryPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    profiling: ProfilingConfig,
    cache: Arc<QueryResultCache>,
}

impl QueryPipelineService {
    pub fn new(
        ontology: Arc<Ontology>,
        client: Arc<ArrowClickHouseClient>,
        profiling: ProfilingConfig,
        cache: Arc<QueryResultCache>,
    ) -> Self {
        Self {
            ontology,
            client,
            profiling,
            cache,
        }
    }

    pub async fn run_query(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = OTelPipelineObserver::start();

        // Check cache for a previous execution of this query by this user.
        let mut output = if let Some(cached) = self.cache.get(claims.user_id, query_json) {
            cached
        } else {
            let mut server_extensions = TypeMap::default();
            server_extensions.insert(Arc::clone(&self.client));
            server_extensions.insert(self.profiling.clone());
            server_extensions.insert(claims.clone());
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
                .ok_or_else(|| {
                    PipelineError::custom("OutputStage did not produce PipelineOutput")
                })?;

            // Cache the full (pre-cursor) result for subsequent pages.
            self.cache.put(claims.user_id, query_json, output.clone());
            output
        };

        // Apply cursor slicing after the pipeline (or cache hit).
        if let Some(cursor) = output.compiled.input.cursor {
            let total_rows = output.query_result.authorized_count();
            let has_more = output
                .query_result
                .apply_cursor(cursor.offset, cursor.page_size);
            output.row_count = output.query_result.authorized_count();
            output.pagination = Some(PaginationMeta {
                has_more,
                total_rows,
            });
        }

        obs.finish(output.row_count, output.redacted_count);
        Ok(output)
    }
}
