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
            table_prefix: String::new(),
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
        let start = std::time::Instant::now();
        let client = ctx
            .server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))?;
        let compiled = ctx.compiled()?;
        let result_context = compiled.base.result_context.clone();
        let rendered_sql = compiled.base.render();

        let profiling_id = uuid::Uuid::new_v4().to_string();
        let log_comment = format!("gkg;profiler;profiling_id={profiling_id}");

        let mut query = client
            .query(&compiled.base.sql)
            .with_setting("log_comment", &log_comment);
        for (key, param) in &compiled.base.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        let elapsed = start.elapsed();
        obs.executed(elapsed, batches.len());
        let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

        let mut execution = QueryExecution {
            label: "base".into(),
            rendered_sql,
            query_id: String::new(),
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats: QueryExecutionStats {
                result_rows,
                elapsed_ns: elapsed.as_nanos() as u64,
                ..Default::default()
            },
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };

        // Backfill stats from system.query_log using the profiling_id
        if let Ok(Some(entry)) = client.fetch_query_log(&profiling_id).await {
            execution.query_id = entry.query_id.clone();
            execution.stats.read_rows = entry.read_rows;
            execution.stats.read_bytes = entry.read_bytes;
            execution.stats.result_rows = entry.result_rows;
            execution.stats.result_bytes = entry.result_bytes;
            execution.stats.memory_usage = entry.memory_usage as i64;
        }

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
