mod arrow_converter;
mod checkpoint;
pub mod config;
mod handler;
mod incremental;
pub mod metrics;
pub(crate) mod observer;
mod pipeline;
pub mod repository;
mod stale_data_cleaner;
#[cfg(test)]
pub(crate) mod test_helpers;

use std::sync::Arc;

use crate::IndexerConfig;
use crate::analytics::IndexingAnalytics;
use crate::clickhouse::{ArrowClickHouseClient, ClickHouseClients};

use crate::handler::{HandlerInitError, HandlerRegistry};
use crate::topic::{CODE_INDEXING_TASK_TOPIC, CodeIndexingTaskRequest};
use crate::types::Event;
use config::CodeTableNames;
use gitlab_client::GitlabClient;
use metrics::CodeMetrics;
use orbit_server_config::{CodeIndexingPipelineConfig, IndexerModule, SubscriptionConfig};
use repository::RepositoryResolver;

pub use checkpoint::{ClickHouseCodeCheckpointStore, CodeCheckpoint, CodeCheckpointStore};
pub use handler::{CodeIndexingTaskHandler, project_lock_key};
pub use pipeline::{CodeIndexer, IndexError, IndexingRequest};
pub use repository::{
    CachingRepositoryService, LocalRepositoryCache, RailsRepositoryService, RepositoryCache,
    RepositoryService, RepositoryServiceError,
};
pub use stale_data_cleaner::{ClickHouseStaleDataCleaner, StaleDataCleaner};

const CODE_CONCURRENCY_GROUP: &str = IndexerModule::Code.concurrency_group();

pub fn code_indexing_task_topic_policy() -> SubscriptionConfig {
    SubscriptionConfig {
        concurrency_group: Some(CODE_CONCURRENCY_GROUP.to_string()),
        max_attempts: Some(5),
        retry_interval_secs: Some(60),
        dead_letter_on_exhaustion: Some(true),
        max_ack_pending: None,
    }
}

pub fn code_graph_client(
    graph: ArrowClickHouseClient,
    pipeline: &CodeIndexingPipelineConfig,
) -> ArrowClickHouseClient {
    graph.with_async_insert_settings(&pipeline.async_insert_settings())
}

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    clients: &ClickHouseClients,
    ontology: &ontology::Ontology,
    writer: Arc<crate::clickhouse::ClickHouseWriter>,
    analytics: IndexingAnalytics,
    resources: &orbit_server_config::ContainerResources,
) -> Result<(), HandlerInitError> {
    let Some(gitlab_config) = &config.gitlab else {
        tracing::info!("Code handlers disabled (GitLab client not configured)");
        return Ok(());
    };

    let code_indexing_task_config = config.engine.handlers.code_indexing_task.clone();

    let table_names =
        Arc::new(CodeTableNames::from_ontology(ontology).map_err(HandlerInitError::new)?);

    let gitlab_client =
        Arc::new(GitlabClient::new(gitlab_config.clone()).map_err(HandlerInitError::new)?);
    let client = code_graph_client(clients.graph.clone(), &code_indexing_task_config.pipeline);
    let writer = Arc::new(writer.with_client(client.clone()));
    let client = Arc::new(client);

    let repository_service: Arc<dyn RepositoryService> =
        CachingRepositoryService::create(RailsRepositoryService::create(gitlab_client));
    let checkpoint_store: Arc<dyn checkpoint::CodeCheckpointStore> =
        Arc::new(ClickHouseCodeCheckpointStore::new(Arc::clone(&client)));
    let stale_data_cleaner: Arc<dyn stale_data_cleaner::StaleDataCleaner> = Arc::new(
        stale_data_cleaner::ClickHouseStaleDataCleaner::new(client, &table_names),
    );
    let metrics = CodeMetrics::new();

    let local_cache = LocalRepositoryCache::new(
        LocalRepositoryCache::default_dir(),
        code_indexing_task_config.pipeline.max_file_size_bytes,
        code_indexing_task_config.pipeline.max_total_bytes,
        metrics.clone(),
    );
    if let Err(error) = local_cache.purge_all().await {
        tracing::warn!(%error, "failed to purge repository cache on startup");
    }
    let cache: Arc<dyn repository::RepositoryCache> = Arc::new(local_cache);

    let resolver = RepositoryResolver::new(Arc::clone(&repository_service), cache);

    let mut pipeline_config = code_indexing_task_config.pipeline.clone();
    pipeline_config.resolve_runtime_defaults(resources);
    let pipeline = Arc::new(pipeline::CodeIndexer::new(
        resolver,
        writer,
        Arc::clone(&checkpoint_store),
        stale_data_cleaner,
        metrics.clone(),
        table_names,
        Arc::new(ontology.clone()),
        pipeline_config,
    ));

    let policy = code_indexing_task_topic_policy()
        .with_optional_override(config.engine.topics.get(CODE_INDEXING_TASK_TOPIC));
    let mut subscription = CodeIndexingTaskRequest::subscription().with_config(&policy);
    if let Some(max_inflight) = pipeline.max_inflight() {
        subscription = subscription.with_max_inflight(max_inflight);
    }

    registry.register_handler(Box::new(CodeIndexingTaskHandler::new(
        Arc::clone(&pipeline),
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        metrics,
        config.nats.ack_wait(),
        subscription,
        analytics,
    )));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ArrowClickHouseClient, code_graph_client, code_indexing_task_topic_policy};
    use crate::clickhouse::insert_overrides;
    use crate::durability::WriteDurability;
    use clickhouse_client::ClickHouseConfigurationExt;
    use orbit_server_config::{AppConfig, SubscriptionConfig};

    const ARGO_CODE_POOL_INSERT_SETTINGS: [(&str, &str); 3] = [
        ("async_insert", "1"),
        ("wait_for_async_insert", "0"),
        ("async_insert_busy_timeout_min_ms", "1000"),
    ];

    fn graph_client(overlay: &[(&str, &str)], replicated: bool) -> ArrowClickHouseClient {
        let mut graph = AppConfig::embedded_defaults().graph;
        graph
            .insert_settings
            .extend(overlay.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        graph.replicated = replicated;
        graph.build_client("graph")
    }

    fn durable_insert_sql(client: &ArrowClickHouseClient) -> String {
        client.build_insert_sql_with_overrides("t", insert_overrides(WriteDurability::Durable))
    }

    fn code_client(graph: ArrowClickHouseClient) -> ArrowClickHouseClient {
        code_graph_client(
            graph,
            &AppConfig::embedded_defaults()
                .engine
                .handlers
                .code_indexing_task
                .pipeline,
        )
    }

    #[test]
    fn code_inserts_send_the_flush_timeout_bounds_with_or_without_the_argo_overlay() {
        for overlay in [&[][..], &ARGO_CODE_POOL_INSERT_SETTINGS[..]] {
            assert_eq!(
                durable_insert_sql(&code_client(graph_client(overlay, false))),
                "INSERT INTO t SETTINGS async_insert=1, async_insert_busy_timeout_max_ms=1000, \
                 async_insert_busy_timeout_min_ms=1000, optimize_on_insert=0, \
                 wait_for_async_insert=1 FORMAT ArrowStream",
                "{overlay:?}"
            );
        }
    }

    #[test]
    fn quorum_code_inserts_send_only_the_shared_settings() {
        let graph = graph_client(&[], true);
        assert_eq!(
            durable_insert_sql(&code_client(graph.clone())),
            durable_insert_sql(&graph)
        );
        assert_eq!(
            durable_insert_sql(&graph),
            "INSERT INTO t SETTINGS optimize_on_insert=0 FORMAT ArrowStream"
        );
    }

    #[test]
    fn sdlc_inserts_keep_the_shared_client_settings() {
        let graph = graph_client(&[], false);
        let _code = code_client(graph.clone());
        assert_eq!(
            durable_insert_sql(&graph),
            "INSERT INTO t SETTINGS async_insert=1, optimize_on_insert=0, \
             wait_for_async_insert=1 FORMAT ArrowStream"
        );
    }

    #[test]
    fn declared_policy_retries_and_dead_letters() {
        let policy = code_indexing_task_topic_policy();
        assert_eq!(policy.max_attempts, Some(5));
        assert_eq!(policy.retry_interval_secs, Some(60));
        assert_eq!(policy.dead_letter_on_exhaustion, Some(true));
        assert_eq!(policy.concurrency_group.as_deref(), Some("code"));
    }

    #[test]
    fn sparse_override_changes_only_named_field() {
        let resolved =
            code_indexing_task_topic_policy().with_optional_override(Some(&SubscriptionConfig {
                max_attempts: Some(2),
                ..Default::default()
            }));
        assert_eq!(resolved.max_attempts, Some(2));
        assert_eq!(resolved.retry_interval_secs, Some(60));
        assert_eq!(resolved.dead_letter_on_exhaustion, Some(true));
        assert_eq!(resolved.concurrency_group.as_deref(), Some("code"));
    }

    #[test]
    fn absent_override_keeps_declared_policy() {
        let resolved = code_indexing_task_topic_policy().with_optional_override(None);
        assert_eq!(resolved.max_attempts, Some(5));
        assert_eq!(resolved.dead_letter_on_exhaustion, Some(true));
    }
}
