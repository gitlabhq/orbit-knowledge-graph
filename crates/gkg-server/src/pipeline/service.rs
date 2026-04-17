use std::sync::Arc;
use std::time::Instant;

use crate::auth::Claims;
use crate::proto::ExecuteQueryMessage;
use clickhouse_client::ArrowClickHouseClient;
use gkg_analytics::{
    Analytics, DeploymentType, OrbitCommon, QueryContext, SourceType, events::QueryExecuted,
};
use gkg_server_config::ProfilingConfig;
use indexer::nats::NatsBroker;
use ontology::Ontology;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineRunner, QueryPipelineContext, TypeMap,
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
    analytics: Analytics,
    resolver_registry: Option<Arc<ColumnResolverRegistry>>,
    cache_broker: Option<Arc<NatsBroker>>,
}

impl QueryPipelineService {
    pub fn new(
        ontology: Arc<Ontology>,
        client: Arc<ArrowClickHouseClient>,
        profiling: ProfilingConfig,
        analytics: Analytics,
    ) -> Self {
        Self {
            ontology,
            client,
            profiling,
            analytics,
            resolver_registry: None,
            cache_broker: None,
        }
    }

    pub fn with_resolver_registry(mut self, registry: Arc<ColumnResolverRegistry>) -> Self {
        self.resolver_registry = Some(registry);
        self
    }

    pub fn with_cache_broker(mut self, broker: Arc<NatsBroker>) -> Self {
        self.cache_broker = Some(broker);
        self
    }

    pub async fn run_query(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        let common = build_common_context();
        let query_ctx = build_query_context(&claims);

        common
            .scope(query_ctx.scope(self.execute(claims, query_json, tx, stream)))
            .await
    }

    async fn execute(
        &self,
        claims: Claims,
        query_json: &str,
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Result<PipelineOutput, PipelineError> {
        let start = Instant::now();
        let mut obs = OTelPipelineObserver::start();

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
        self.analytics.track(
            QueryExecuted::builder()
                .query_type(output.query_type.clone())
                .duration_ms(start.elapsed().as_millis() as u64)
                .build(),
        );
        Ok(output)
    }
}

/// Build the `orbit_common` context from request-scoped observability.
///
/// `correlation_id` comes from the current labkit span (populated by the
/// gRPC/HTTP correlation middleware). `deployment_type` is a placeholder
/// until `AppConfig.analytics.deployment_type` lands.
fn build_common_context() -> OrbitCommon {
    OrbitCommon::builder()
        .deployment_type(DeploymentType::Com)
        .maybe_correlation_id(labkit::correlation::current())
        .build()
}

/// Build the `orbit_query` context from the JWT [`Claims`]. Fields not
/// carried on the token today (`user_type`, `tier`, `is_gitlab_team_member`)
/// are deferred to follow-up MRs that add them as claims.
fn build_query_context(claims: &Claims) -> QueryContext {
    QueryContext::builder()
        .source_type(parse_source_type(&claims.source_type))
        .maybe_global_user_id(Some(claims.user_id.to_string()))
        .maybe_session_id(claims.ai_session_id.clone())
        .maybe_namespace_id(claims.group_traversal_ids.first().cloned())
        .maybe_root_namespace_id(claims.organization_id.map(|id| id.to_string()))
        .build()
}

fn parse_source_type(raw: &str) -> SourceType {
    match raw {
        "dap" => SourceType::Dap,
        "mcp" => SourceType::Mcp,
        "cli" => SourceType::Cli,
        _ => SourceType::RestApi,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_claims() -> Claims {
        Claims {
            sub: "u-1".into(),
            iss: "gitlab".into(),
            aud: "gkg".into(),
            iat: 0,
            exp: 0,
            user_id: 42,
            username: "alice".into(),
            admin: false,
            organization_id: Some(9970),
            min_access_level: None,
            group_traversal_ids: vec!["9970/42/".into()],
            source_type: "mcp".into(),
            ai_session_id: Some("sess-abc".into()),
        }
    }

    #[test]
    fn build_query_context_maps_claims_fields() {
        let ctx = build_query_context(&test_claims());
        assert_eq!(ctx.source_type, SourceType::Mcp);
        assert_eq!(ctx.global_user_id.as_deref(), Some("42"));
        assert_eq!(ctx.session_id.as_deref(), Some("sess-abc"));
        assert_eq!(ctx.namespace_id.as_deref(), Some("9970/42/"));
        assert_eq!(ctx.root_namespace_id.as_deref(), Some("9970"));
    }

    #[test]
    fn parse_source_type_maps_known_variants() {
        assert_eq!(parse_source_type("dap"), SourceType::Dap);
        assert_eq!(parse_source_type("mcp"), SourceType::Mcp);
        assert_eq!(parse_source_type("cli"), SourceType::Cli);
        assert_eq!(parse_source_type("rest"), SourceType::RestApi);
        assert_eq!(parse_source_type("whatever"), SourceType::RestApi);
    }
}
