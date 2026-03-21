use std::sync::Arc;

use anyhow::Result;
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

pub struct ProfilerOptions {
    pub explain: bool,
    pub profile: bool,
    pub processors: bool,
}

pub async fn run_profiler_pipeline(
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
    security_ctx: SecurityContext,
    query_json: &str,
) -> Result<PipelineOutput> {
    let mut obs = NoOpObserver;

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);

    let mut ctx = QueryPipelineContext {
        query_json: query_json.to_string(),
        compiled: None,
        ontology,
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
        .ok_or_else(|| anyhow::anyhow!("OutputStage did not produce PipelineOutput"))?;

    Ok(output)
}

pub async fn enrich_output(
    client: &ArrowClickHouseClient,
    output: &mut PipelineOutput,
    opts: &ProfilerOptions,
) {
    for exec in &mut output.execution_log {
        let rendered = &exec.rendered_sql;
        if opts.explain {
            exec.explain_plan = client.profiler().explain_plan(rendered).await.ok();
            exec.explain_pipeline = client.profiler().explain_pipeline(rendered).await.ok();
        }
        if opts.profile
            && let Ok(Some(entry)) = client.profiler().fetch_query_log(&exec.query_id).await
        {
            exec.query_log = Some(serde_json::to_value(&entry).unwrap_or_default());
        }
        if opts.processors
            && let Ok(profiles) = client
                .profiler()
                .fetch_processors_profile(&exec.query_id)
                .await
            && !profiles.is_empty()
        {
            exec.processors = Some(serde_json::to_value(&profiles).unwrap_or_default());
        }
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
