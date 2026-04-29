use std::sync::Arc;

use crate::analytics::{AnalyticsObserver, AnalyticsTracker};
use crate::auth::Claims;
use crate::billing::{BillingObserver, BillingTracker};
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::{AnalyticsConfig, ProfilingConfig};
use nats_client::NatsClient;
use ontology::Ontology;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use query_engine::pipeline::{
    MultiObserver, PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{CompilationStage, ExtractionStage, OutputStage, PipelineOutput};

use super::metrics::OTelPipelineObserver;
use super::stages::{
    AuthorizationStage, ClickHouseExecutor, HydrationStage, RedactionStage, SecurityStage,
};

#[derive(Clone)]
pub struct QueryPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    profiling: ProfilingConfig,
    resolver_registry: Option<Arc<ColumnResolverRegistry>>,
    cache_broker: Option<Arc<NatsClient>>,
    billing_tracker: Option<Arc<dyn BillingTracker>>,
    analytics_tracker: Option<Arc<dyn AnalyticsTracker>>,
    analytics_config: Arc<AnalyticsConfig>,
}

impl QueryPipelineService {
    pub fn new(
        ontology: Arc<Ontology>,
        client: Arc<ArrowClickHouseClient>,
        profiling: ProfilingConfig,
        analytics_config: Arc<AnalyticsConfig>,
    ) -> Self {
        Self {
            ontology,
            client,
            profiling,
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

    pub async fn run_query(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = MultiObserver::new(vec![
            Box::new(OTelPipelineObserver::start()),
            Box::new(BillingObserver::new(
                self.billing_tracker.clone(),
                claims.clone(),
            )),
            Box::new(AnalyticsObserver::new(
                self.analytics_tracker.clone(),
                Arc::clone(&self.analytics_config),
                claims.clone(),
            )),
        ]);

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(self.profiling.clone());
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
            .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))?;

        obs.finish(output.row_count, output.redacted_count);
        Ok(output)
    }
}
