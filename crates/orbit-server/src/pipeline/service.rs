use std::sync::Arc;
use std::time::Duration;

use crate::active_schema::SchemaSnapshot;
use crate::analytics::{AnalyticsObserver, AnalyticsTracker};
use crate::auth::RequestContext;
use crate::authorization::gitlab::RailsExchange;
use crate::proto::ExecuteQueryMessage;
use crate::proto::execute_query_message::Content;
use clickhouse_client::ArrowClickHouseClient;
use orbit_billing::{BillingObserver, BillingTracker};
use orbit_server_config::AnalyticsConfig;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use query_data_model::introspection::SchemaResponse;
use query_engine::pipeline::{
    MultiObserver, PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::{DagStage, ExtractionStage, OutputStage, PipelineOutput};

use super::correlation;
use super::helpers::send_query_result;
use super::metrics::OTelPipelineObserver;
use super::stages::{
    AuthorizationStage, HydrationStage, PeerExecutors, RedactionStage, RoutingOutput, RoutingStage,
    SecurityStage, graph_executors,
};

pub struct RawQuery {
    pub text: String,
    pub settings: Vec<(String, String)>,
}

pub enum QueryServiceOutput {
    Graph(Box<PipelineOutput>),
    Schema(SchemaResponse),
}

#[derive(Clone)]
pub struct QueryPipelineService {
    client: Arc<ArrowClickHouseClient>,
    peers: PeerExecutors,
    resolver_registry: Option<Arc<ColumnResolverRegistry>>,
    billing_tracker: Option<Arc<dyn BillingTracker>>,
    analytics_tracker: Option<Arc<dyn AnalyticsTracker>>,
    analytics_config: Arc<AnalyticsConfig>,
    stream_timeout: Duration,
}

impl QueryPipelineService {
    pub fn new(
        client: Arc<ArrowClickHouseClient>,
        analytics_config: Arc<AnalyticsConfig>,
        stream_timeout: Duration,
    ) -> Self {
        Self {
            client,
            peers: PeerExecutors::default(),
            resolver_registry: None,
            billing_tracker: None,
            analytics_tracker: None,
            analytics_config,
            stream_timeout,
        }
    }

    pub fn with_peers(mut self, peers: PeerExecutors) -> Self {
        self.peers = peers;
        self
    }

    pub fn with_resolver_registry(mut self, registry: Arc<ColumnResolverRegistry>) -> Self {
        self.resolver_registry = Some(registry);
        self
    }

    /// The routing and DAG stages of one query, over every connection the snapshot serves.
    fn stages(
        &self,
        schema: &SchemaSnapshot,
        log_comment: String,
        settings: Vec<(String, String)>,
    ) -> (RoutingStage, DagStage) {
        let graph = graph_executors(
            Arc::clone(&self.client),
            &schema.data_model,
            "base",
            log_comment,
            settings,
        );
        let routing = RoutingStage {
            connections: schema.connections.clone(),
        };
        let dag = DagStage {
            executors: self.peers.with_graph(graph),
            connections: schema.connections.clone(),
            checks: schema.authorization.checks().clone(),
        };
        (routing, dag)
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
        tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
        render: impl FnOnce(QueryServiceOutput) -> Result<Vec<Content>, PipelineError>,
    ) -> Result<(), PipelineError> {
        let RequestContext { claims, user_agent } = request_context;
        let schema_obs = OTelPipelineObserver::start();
        let mut obs = MultiObserver::new(vec![
            Box::new(schema_obs.clone()),
            Box::new(BillingObserver::new(
                self.billing_tracker.clone(),
                crate::billing_adapter::billing_inputs(&claims, user_agent.coding_agent.clone()),
            )),
            Box::new(AnalyticsObserver::new(
                self.analytics_tracker.clone(),
                Arc::clone(&self.analytics_config),
                claims.clone(),
                "query_graph",
                user_agent,
                schema.migration_version.to_string(),
            )),
        ]);

        let (routing, dag) = self.stages(
            schema,
            correlation::log_comment_base(claims.user_id, &query.text, schema.migration_version),
            query.settings,
        );
        let mut server_extensions = TypeMap::default();
        server_extensions.insert(Arc::clone(&self.client));
        server_extensions.insert(Arc::clone(&schema.data_model));
        server_extensions.insert(schema.limits);
        server_extensions.insert(claims);
        server_extensions.insert(tx.clone());
        server_extensions.insert(Arc::new(RailsExchange::new(tx.clone(), stream)));
        server_extensions.insert(Arc::clone(&schema.authorization));
        if let Some(registry) = &self.resolver_registry {
            server_extensions.insert(ColumnResolverRegistry::clone(registry));
        }

        let mut ctx = QueryPipelineContext {
            query_json: query.text,
            compiled: None,
            security_context: None,
            grants: None,
            server_extensions,
            phases: TypeMap::default(),
        };

        // The timeout lives inside run_query so the observer is still alive
        // when it fires. Dropping the future from outside (the prior shape)
        // tore down the observer before record_error could run, leaving
        // timed-out queries invisible to every metric.
        let pipeline = async {
            let route = PipelineRunner::start(&mut ctx, &mut obs)
                .then(&routing)
                .await?
                .finish()
                .ok_or_else(|| PipelineError::custom("RoutingStage produced no output"))
                .inspect_err(|e| obs.record_error(e))?;
            if let RoutingOutput::Schema(response) = route {
                return Ok(QueryServiceOutput::Schema(response));
            }

            let output = PipelineRunner::start(&mut ctx, &mut obs)
                .then(&SecurityStage)
                .await?
                .then(&dag)
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
                .inspect_err(|e| obs.record_error(e))?;
            Ok::<_, PipelineError>(QueryServiceOutput::Graph(Box::new(output)))
        };

        let output = tokio::select! {
            biased;
            () = tx.closed() => Err(PipelineError::client_closed()),
            result = tokio::time::timeout(self.stream_timeout, pipeline) => {
                result.map_err(|_| PipelineError::Timeout)
            }
        };
        let counts = match output {
            Ok(stages) => deliver(tx, stages?, render).await,
            Err(e) => Err(e),
        }
        .inspect_err(|e| obs.record_error(e))?;

        match counts {
            Some((row_count, redacted_count)) => obs.finish(row_count, redacted_count),
            None => schema_obs.finish_schema(),
        }
        Ok(())
    }
}

async fn deliver(
    responses: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    output: QueryServiceOutput,
    render: impl FnOnce(QueryServiceOutput) -> Result<Vec<Content>, PipelineError>,
) -> Result<Option<(usize, usize)>, PipelineError> {
    let counts = match &output {
        QueryServiceOutput::Graph(output) => Some((output.row_count, output.redacted_count)),
        QueryServiceOutput::Schema(_) => None,
    };
    send_query_result(responses, render(output)?).await?;
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use clickhouse_client::ClickHouseConfigurationExt;
    use orbit_server_config::AppConfig;
    use query_data_model::{Catalog, ClickHouse, Packages};
    use query_engine::compiler::dag::Connections;

    use super::*;
    use crate::active_schema::ActiveSchema;

    #[test]
    fn routing_and_the_dag_plan_over_the_connections_of_the_model_in_its_order() {
        let model = Packages::embedded().deployment("demo").unwrap();
        let graph = Arc::new(Catalog::<ClickHouse>::from_model(&model).unwrap());
        let schema = ActiveSchema::pinned(Arc::clone(&graph)).snapshot().unwrap();
        let config = AppConfig::embedded_defaults();
        let service = QueryPipelineService::new(
            Arc::new(config.graph.build_client("graph")),
            Arc::new(config.analytics),
            Duration::from_secs(1),
        );
        let (routing, dag) = service.stages(&schema, String::new(), Vec::new());
        let expected = Connections::of(&model).unwrap();
        assert_ne!(expected.ids()[0], graph.connection());
        assert_eq!(routing.connections.ids(), expected.ids());
        assert_eq!(dag.connections.ids(), expected.ids());
    }
}
