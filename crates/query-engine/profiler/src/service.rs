use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use compiler::dag::{Connections, Request};
use pipeline::SecurityContext;
use pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use shared::dag::{Executed, Executor, ExecutorError, Executors};
use shared::{
    AuthorizationOutput, DagStage, ExtractionOutput, ExtractionStage, OutputStage, PipelineOutput,
    QueryExecution, QueryExecutionStats,
};
use types::ResourceAuthorization;

use orbit_server::pipeline::{HydrationStage, RedactionStage};

/// The scope answers the default deployment's `gitlab.session` gives the profiler's session.
pub fn grants(
    data_model: &query_data_model::Catalog<query_data_model::ClickHouse>,
    session: &SecurityContext,
) -> anyhow::Result<compiler::orbit_authorization::Grants> {
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(
        &config,
        data_model.model(),
        session.clone(),
    )
    .map_err(|error| anyhow::anyhow!("{error}"))
}

pub struct ProfilerPipelineService {
    data_model: Arc<query_data_model::Catalog<query_data_model::ClickHouse>>,
    connections: Connections,
    client: Arc<ArrowClickHouseClient>,
    limits: compiler::QueryLimits,
}

impl ProfilerPipelineService {
    pub fn new(
        data_model: Arc<query_data_model::Catalog<query_data_model::ClickHouse>>,
        client: Arc<ArrowClickHouseClient>,
        limits: compiler::QueryLimits,
    ) -> Result<Self, query_data_model::DataModelError> {
        Ok(Self {
            connections: Connections::of(data_model.shared_model())?,
            data_model,
            client,
            limits,
        })
    }

    pub async fn run_query(
        &self,
        security_ctx: SecurityContext,
        query_json: &str,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = NoOpObserver;

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(Arc::clone(&self.data_model));
        server_extensions.insert(self.limits);

        let executor = ProfilerExecutor {
            client: Arc::clone(&self.client),
        };
        let executors = Executors::default().with(self.data_model.connection(), Arc::new(executor));
        let dag = DagStage {
            executors,
            connections: self.connections.clone(),
            checks: Default::default(),
        };
        let mut ctx = QueryPipelineContext {
            query_json: query_json.to_string(),
            compiled: None,
            grants: Some(
                grants(&self.data_model, &security_ctx)
                    .map_err(|e| PipelineError::Security(e.to_string()))?,
            ),
            security_context: Some(security_ctx),
            server_extensions,
            phases: TypeMap::default(),
        };

        let output = PipelineRunner::start(&mut ctx, &mut obs)
            .then(&dag)
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

/// Runs each stage's SQL with the profiler's log comment and keeps ClickHouse's summary.
struct ProfilerExecutor {
    client: Arc<ArrowClickHouseClient>,
}

#[async_trait::async_trait]
impl Executor for ProfilerExecutor {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Query(compiled) = request else {
            return Err(ExecutorError(format!(
                "the profiler runs queries, not {request:?}"
            )));
        };
        let start = std::time::Instant::now();
        let correlation_id = labkit::correlation::current();
        let log_comment = match &correlation_id {
            Some(id) => format!("gkg;profiler;correlation_id={id}"),
            None => "gkg;profiler".to_string(),
        };
        let mut query = self
            .client
            .query(&compiled.base.sql)
            .with_setting("log_comment", &log_comment);
        for (key, param) in &compiled.base.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.data_type);
        }
        let (batches, summary) = query
            .fetch_arrow_with_summary()
            .await
            .map_err(|e| ExecutorError(e.to_string()))?;
        let elapsed = start.elapsed();
        let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;
        let summary =
            summary.ok_or_else(|| ExecutorError("missing X-ClickHouse-Summary header".into()))?;
        let execution = QueryExecution {
            label: "base".into(),
            rendered_sql: compiled.base.render(),
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
        Ok(Executed {
            batches,
            log: vec![execution],
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
        let mut input = ctx
            .phases
            .remove::<ExtractionOutput>()
            .ok_or_else(|| PipelineError::Authorization("ExtractionOutput not found".into()))?;

        HydrationStage::resolve_keys(ctx, &mut input.query_result).await?;
        let checks = input.query_result.resource_checks();
        let authorizations: Vec<ResourceAuthorization> = checks
            .iter()
            .map(|check| ResourceAuthorization {
                provider: check.provider.clone(),
                resource_type: check.resource_type.clone(),
                authorized: check.ids.iter().map(|id| (id.clone(), true)).collect(),
            })
            .collect();

        Ok(AuthorizationOutput {
            query_result: input.query_result,
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
