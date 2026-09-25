use std::sync::Arc;

use crate::active_schema::SchemaSnapshot;
use crate::analytics::{AnalyticsObserver, AnalyticsTracker};
use crate::auth::RequestContext;
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::ArrowClickHouseClient;
use nats_client::NatsClient;
use orbit_billing::{BillingObserver, BillingTracker};
use orbit_server_config::AnalyticsConfig;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use ontology::introspection::SchemaResponse;
use query_engine::compiler::Frontend;
use query_engine::pipeline::{
    MultiObserver, PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{CompilationStage, ExtractionStage, OutputStage, PipelineOutput};

use super::metrics::OTelPipelineObserver;
use super::stages::{
    AuthorizationStage, ClickHouseExecutor, HydrationStage, RedactionStage, RoutingOutput,
    RoutingStage, SecurityStage,
};

pub struct RawQuery {
    pub text: String,
    pub frontend: Frontend,
}

pub enum QueryServiceOutput {
    Graph(Box<PipelineOutput>),
    Schema(SchemaResponse),
}

#[derive(Clone)]
pub struct QueryPipelineService {
    client: Arc<ArrowClickHouseClient>,
    resolver_registry: Option<Arc<ColumnResolverRegistry>>,
    cache_broker: Option<Arc<NatsClient>>,
    billing_tracker: Option<Arc<dyn BillingTracker>>,
    analytics_tracker: Option<Arc<dyn AnalyticsTracker>>,
    analytics_config: Arc<AnalyticsConfig>,
}

impl QueryPipelineService {
    pub fn new(client: Arc<ArrowClickHouseClient>, analytics_config: Arc<AnalyticsConfig>) -> Self {
        Self {
            client,
            resolver_registry: None,
            cache_broker: None,
            billing_tracker: None,
            analytics_tracker: None,
            analytics_config,
        }
    }

    pub fn with_resolver_registry(mut self, registry: Arc<ColumnResolverRegistry>) -> Self {
        self.resolver_registry = Some(registry);
        self
    }

    pub fn with_cache_broker(mut self, broker: Arc<NatsClient>) -> Self {
        self.cache_broker = Some(broker);
        self
    }

    pub fn with_billing(mut self, tracker: Arc<dyn BillingTracker>) -> Self {
        self.billing_tracker = Some(tracker);
        self
    }

    pub fn with_analytics(mut self, tracker: Arc<dyn AnalyticsTracker>) -> Self {
        self.analytics_tracker = Some(tracker);
        self
    }

    pub(crate) async fn run_query(
        &self,
        schema: &SchemaSnapshot,
        request_context: RequestContext,
        query: RawQuery,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
        timeout: std::time::Duration,
    ) -> Result<QueryServiceOutput, PipelineError> {
        let coding_agent = request_context.coding_agent().map(String::from);
        let claims = request_context.claims;
        let schema_obs = OTelPipelineObserver::start();
        let mut obs = MultiObserver::new(vec![
            Box::new(schema_obs.clone()),
            Box::new(BillingObserver::new(
                self.billing_tracker.clone(),
                crate::billing_adapter::billing_inputs(&claims, coding_agent.clone()),
            )),
            Box::new(AnalyticsObserver::new(
                self.analytics_tracker.clone(),
                Arc::clone(&self.analytics_config),
                claims.clone(),
                "query_graph",
                coding_agent,
                schema.migration_version.to_string(),
            )),
        ]);

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(Arc::clone(&schema.data_model));
        server_extensions.insert(claims);
        server_extensions.insert(tx);
        server_extensions.insert(stream);
        if let Some(registry) = &self.resolver_registry {
            server_extensions.insert(ColumnResolverRegistry::clone(registry));
        }
        if let Some(broker) = &self.cache_broker {
            server_extensions.insert(Arc::clone(broker));
        }

        let mut ctx = QueryPipelineContext {
            frontend: query.frontend,
            query_json: query.text,
            compiled: None,
            ontology: Arc::clone(&schema.ontology),
            security_context: None,
            server_extensions,
            phases: TypeMap::default(),
        };

        // The timeout lives inside run_query so the observer is still alive
        // when it fires. Dropping the future from outside (the prior shape)
        // tore down the observer before record_error could run, leaving
        // timed-out queries invisible to every metric.
        let pipeline = async {
            let route = PipelineRunner::start(&mut ctx, &mut obs)
                .then(&RoutingStage)
                .await?
                .finish()
                .ok_or_else(|| PipelineError::custom("RoutingStage produced no output"))?;
            if let RoutingOutput::Schema(response) = route {
                return Ok(QueryServiceOutput::Schema(response));
            }

            let output = PipelineRunner::start(&mut ctx, &mut obs)
                .then(&SecurityStage)
                .await?
                .then(&CompilationStage)
                .await?
                .then(&ClickHouseExecutor {
                    migration_version: schema.migration_version,
                })
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
            Ok(QueryServiceOutput::Graph(Box::new(output)))
        };

        let output = match tokio::time::timeout(timeout, pipeline).await {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                let e = PipelineError::Timeout;
                obs.record_error(&e);
                return Err(e);
            }
        };

        match &output {
            QueryServiceOutput::Graph(output) => {
                obs.finish(output.row_count, output.redacted_count)
            }
            QueryServiceOutput::Schema(_) => schema_obs.finish_schema(),
        }
        Ok(output)
    }
}
