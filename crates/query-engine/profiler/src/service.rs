use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use compiler::SecurityContext;
use ontology::Ontology;
use pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use shared::{
    AuthorizationOutput, CompilationStage, ExtractionOutput, ExtractionStage, OutputStage,
    PipelineOutput,
};
use types::ResourceAuthorization;

use gkg_server::pipeline::{ClickHouseExecutor, HydrationStage, RedactionStage};

pub struct ProfilerPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
}

impl ProfilerPipelineService {
    pub fn new(ontology: Arc<Ontology>, client: Arc<ArrowClickHouseClient>) -> Self {
        Self { ontology, client }
    }

    pub async fn run_query(
        &self,
        security_ctx: SecurityContext,
        query_json: &str,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = NoOpObserver;

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));

        let mut ctx = QueryPipelineContext {
            query_json: query_json.to_string(),
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: Some(security_ctx),
            server_extensions,
            phases: TypeMap::default(),
        };

        let output = PipelineRunner::start(&mut ctx, &mut obs)
            .then(&CompilationStage)
            .await?
            .then(&ClickHouseExecutor)
            .await?
            .then(&ExtractionStage)
            .await?
            .then(&MockAuthorizationStage)
            .await?
            .then(&RedactionStage)
            .await?
            .then(&HydrationStage)
            .await?
            .then(&OutputStage)
            .await?
            .finish()
            .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))?;

        Ok(output)
    }
}

struct MockAuthorizationStage;

impl PipelineStage for MockAuthorizationStage {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx
            .phases
            .get::<ExtractionOutput>()
            .ok_or_else(|| PipelineError::Authorization("ExtractionOutput not found".into()))?;

        let checks = input.query_result.resource_checks();
        let authorizations: Vec<ResourceAuthorization> = checks
            .iter()
            .map(|check| ResourceAuthorization {
                resource_type: check.resource_type.clone(),
                authorized: check.ids.iter().map(|id| (*id, true)).collect(),
            })
            .collect();

        Ok(AuthorizationOutput {
            query_result: input.query_result.clone(),
            authorizations,
        })
    }
}
