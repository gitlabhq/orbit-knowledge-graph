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

use gkg_server::pipeline::{HydrationStage, PathResolutionStage, PathResolver, RedactionStage};

pub struct ProfilerPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    resolver: Option<Arc<PathResolver>>,
}

impl ProfilerPipelineService {
    pub fn new(
        ontology: Arc<Ontology>,
        client: Arc<ArrowClickHouseClient>,
        resolver: Option<Arc<PathResolver>>,
    ) -> Self {
        Self {
            ontology,
            client,
            resolver,
        }
    }

    pub async fn run_query(
        &self,
        security_ctx: SecurityContext,
        query_json: &str,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = NoOpObserver;

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        if let Some(resolver) = &self.resolver {
            server_extensions.insert(Arc::clone(resolver));
        }

        let mut ctx = QueryPipelineContext {
            query_json: query_json.to_string(),
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: Some(security_ctx),
            server_extensions,
            phases: TypeMap::default(),
        };

        let output = PipelineRunner::start(&mut ctx, &mut obs)
            .then(&PathResolutionStage)
            .await?
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

        let correlation_id = labkit::correlation::current();
        let log_comment = match &correlation_id {
            Some(id) => format!("gkg;profiler;correlation_id={id}"),
            None => "gkg;profiler".to_string(),
        };

        let mut query = client
            .query(&compiled.base.sql)
            .with_setting("log_comment", &log_comment);
        for (key, param) in &compiled.base.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let (batches, summary) = query
            .fetch_arrow_with_summary()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        let elapsed = start.elapsed();
        obs.executed(elapsed, batches.len());
        let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

        let summary = summary.ok_or_else(|| {
            PipelineError::Execution("missing X-ClickHouse-Summary header".into())
        })?;

        let execution = QueryExecution {
            label: "base".into(),
            rendered_sql,
            query_id: String::new(),
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats: QueryExecutionStats {
                read_rows: summary.read_rows().unwrap_or(0),
                read_bytes: summary.read_bytes().unwrap_or(0),
                result_rows: summary.result_rows().unwrap_or(result_rows),
                result_bytes: summary.result_bytes().unwrap_or(0),
                elapsed_ns: summary.elapsed_ns().unwrap_or(elapsed.as_nanos() as u64),
                memory_usage: summary.memory_usage().map(|v| v as i64).unwrap_or(0),
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

#[cfg(test)]
mod tests {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{EnvFilter, Layer};

    // The env filter must stay per-layer; as a global registry filter it would
    // disable the span below its level and current() would return None.
    #[test]
    fn correlation_id_resolves_within_profile_span() {
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(EnvFilter::new("error")),
            )
            .with(labkit::correlation::CorrelationCaptureLayer::new());
        let _guard = tracing::subscriber::set_default(subscriber);

        let id = labkit::correlation::generate_id();
        let span = labkit::context::span_with_id("profile", &id);
        let resolved = span.in_scope(labkit::correlation::current);

        assert_eq!(resolved.as_deref(), Some(id.as_str()));
    }
}
