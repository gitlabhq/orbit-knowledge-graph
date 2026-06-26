use std::sync::Arc;

use crate::analytics::{AnalyticsObserver, AnalyticsTracker};
use crate::auth::Claims;
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::ArrowClickHouseClient;
use gkg_billing::{BillingInputs, BillingObserver, BillingTracker};
use gkg_server_config::AnalyticsConfig;
use indexer::schema::version::SCHEMA_VERSION;
use nats_client::NatsClient;
use ontology::Ontology;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use query_engine::compiler::QueryInput;
use query_engine::pipeline::{
    MultiObserver, PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{CompilationStage, ExtractionStage, OutputStage, PipelineOutput};

use super::metrics::OTelPipelineObserver;
use super::path_resolver::PathResolver;
use super::stages::{
    AuthorizationStage, ClickHouseExecutor, HydrationStage, PathResolutionStage, RedactionStage,
    SecurityStage,
};

#[derive(Clone)]
pub struct QueryPipelineService {
    ontology: Arc<Ontology>,
    client: Arc<ArrowClickHouseClient>,
    resolver_registry: Option<Arc<ColumnResolverRegistry>>,
    cache_broker: Option<Arc<NatsClient>>,
    path_resolver: Option<Arc<PathResolver>>,
    billing_tracker: Option<Arc<dyn BillingTracker>>,
    analytics_tracker: Option<Arc<dyn AnalyticsTracker>>,
    analytics_config: Arc<AnalyticsConfig>,
}

impl QueryPipelineService {
    pub fn new(
        ontology: Arc<Ontology>,
        client: Arc<ArrowClickHouseClient>,
        analytics_config: Arc<AnalyticsConfig>,
    ) -> Self {
        Self {
            ontology,
            client,
            resolver_registry: None,
            cache_broker: None,
            path_resolver: None,
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

    pub fn with_path_resolver(mut self, resolver: Arc<PathResolver>) -> Self {
        self.path_resolver = Some(resolver);
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
        coding_agent: Option<String>,
        query: QueryInput,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
        timeout: std::time::Duration,
    ) -> Result<PipelineOutput, PipelineError> {
        let mut obs = MultiObserver::new(vec![
            Box::new(OTelPipelineObserver::start()),
            Box::new(BillingObserver::new(
                self.billing_tracker.clone(),
                BillingInputs::from(&claims),
            )),
            Box::new(AnalyticsObserver::new(
                self.analytics_tracker.clone(),
                Arc::clone(&self.analytics_config),
                claims.clone(),
                "query_graph",
                coding_agent,
                SCHEMA_VERSION.to_string(),
            )),
        ]);

        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(claims);
        server_extensions.insert(tx);
        server_extensions.insert(stream);
        if let Some(registry) = &self.resolver_registry {
            server_extensions.insert(ColumnResolverRegistry::clone(registry));
        }
        if let Some(broker) = &self.cache_broker {
            server_extensions.insert(Arc::clone(broker));
        }
        if let Some(resolver) = &self.path_resolver {
            server_extensions.insert(Arc::clone(resolver));
        }

        let mut ctx = QueryPipelineContext {
            query,
            compiled: None,
            ontology: Arc::clone(&self.ontology),
            security_context: None,
            server_extensions,
            phases: TypeMap::default(),
        };

        // The timeout lives inside run_query so the observer is still alive
        // when it fires. Dropping the future from outside (the prior shape)
        // tore down the observer before record_error could run, leaving
        // timed-out queries invisible to every metric.
        let pipeline = async {
            PipelineRunner::start(&mut ctx, &mut obs)
                .then(&SecurityStage)
                .await?
                .then(&PathResolutionStage)
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
                .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))
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

        obs.finish(output.row_count, output.redacted_count);
        Ok(output)
    }
}
