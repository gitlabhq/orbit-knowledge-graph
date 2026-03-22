use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use compiler::SecurityContext;
use ontology::Ontology;
use pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use shared::{
    AuthorizationOutput, CompilationStage, ExecutionOutput, ExtractionOutput, ExtractionStage,
    OutputStage, PipelineOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};
use types::ResourceAuthorization;

use gkg_server::pipeline::{HydrationStage, RedactionStage};

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
            .then(&ProfilerExecutor)
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

struct ProfilerExecutor;

impl PipelineStage for ProfilerExecutor {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = std::time::Instant::now();
        let client = ctx
            .server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))?;
        let compiled = ctx.compiled()?;
        let result_context = compiled.base.result_context.clone();
        let rendered_sql = compiled.base.render();
        let http_params: Vec<(String, String)> = compiled
            .base
            .params
            .iter()
            .map(|(k, v)| (k.clone(), v.render_http_param()))
            .collect();
        let extra_settings: Vec<(&str, &str)> = compiled
            .base
            .settings
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (batches, query_stats) = client
            .profiler()
            .execute_with_stats(&compiled.base.sql, &http_params, &extra_settings)
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        let elapsed = t.elapsed();
        obs.executed(elapsed, batches.len());

        let execution = QueryExecution {
            label: "base".into(),
            rendered_sql,
            query_id: query_stats.query_id.clone(),
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats: QueryExecutionStats {
                read_rows: query_stats.read_rows,
                read_bytes: query_stats.read_bytes,
                result_rows: query_stats.result_rows,
                result_bytes: query_stats.result_bytes,
                elapsed_ns: query_stats.elapsed_ns,
                memory_usage: query_stats.memory_usage,
            },
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };

        ctx.phases
            .get_or_insert_default::<QueryExecutionLog>()
            .0
            .push(execution);

        Ok(ExecutionOutput {
            batches,
            result_context,
        })
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
