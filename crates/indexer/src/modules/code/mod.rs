mod arrow_converter;
mod checkpoint;
pub mod config;
mod handler;
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
use crate::clickhouse::ClickHouseConfigurationExt;

use crate::handler::{HandlerInitError, HandlerRegistry};
use crate::topic::{CODE_INDEXING_TASK_TOPIC, CodeIndexingTaskRequest};
use crate::types::Event;
use config::CodeTableNames;
use gitlab_client::GitlabClient;
use gkg_server_config::SubscriptionConfig;
use metrics::CodeMetrics;
use repository::RepositoryResolver;

pub use checkpoint::ClickHouseCodeCheckpointStore;
pub use handler::CodeIndexingTaskHandler;
pub use pipeline::{CodeIndexingPipeline, IndexingRequest};
pub use repository::{
    CachingRepositoryService, LocalRepositoryCache, RailsRepositoryService, RepositoryCache,
    RepositoryService, RepositoryServiceError,
};
pub use stale_data_cleaner::{ClickHouseStaleDataCleaner, StaleDataCleaner};

const CODE_CONCURRENCY_GROUP: &str = "code";

/// Default subscription policy for [`CODE_INDEXING_TASK_TOPIC`]. Code indexing is
/// event-driven (one Siphon push per repository) and never re-dispatched, so a
/// transient failure must retry and dead-letter on exhaustion; acking on failure
/// would silently drop that repository's index update.
pub fn code_indexing_task_topic_policy() -> SubscriptionConfig {
    SubscriptionConfig {
        concurrency_group: Some(CODE_CONCURRENCY_GROUP.to_string()),
        max_attempts: Some(5),
        retry_interval_secs: Some(60),
        dead_letter_on_exhaustion: Some(true),
        max_ack_pending: None,
    }
}

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
    writer: Arc<crate::clickhouse::ClickHouseWriter>,
    analytics: IndexingAnalytics,
) -> Result<(), HandlerInitError> {
    let Some(gitlab_config) = &config.gitlab else {
        tracing::info!("Code handlers disabled (GitLab client not configured)");
        return Ok(());
    };

    let code_indexing_task_config = config.engine.handlers.code_indexing_task.clone();

    let job_timeout_secs = code_indexing_task_config.pipeline.job_timeout_secs;
    let ack_wait_secs = config.nats.ack_wait_secs;
    if job_timeout_outlives_ack_wait(job_timeout_secs, ack_wait_secs) {
        tracing::warn!(
            job_timeout_secs,
            ack_wait_secs,
            "code indexing job_timeout_secs is not below nats ack_wait_secs; a long job will be redelivered (and its lock can lapse) before the timeout fires — lower job_timeout_secs below ack_wait_secs"
        );
    }

    let table_names =
        Arc::new(CodeTableNames::from_ontology(ontology).map_err(HandlerInitError::new)?);

    let gitlab_client =
        Arc::new(GitlabClient::new(gitlab_config.clone()).map_err(HandlerInitError::new)?);
    let client = Arc::new(config.graph.build_client());

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

    let pipeline_config = code_indexing_task_config.pipeline.clone();
    let pipeline = Arc::new(pipeline::CodeIndexingPipeline::new(
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

/// True when the job timeout isn't safely below the NATS ack window, so a job is redelivered (and its lock can lapse) before the timeout fires.
fn job_timeout_outlives_ack_wait(job_timeout_secs: u64, ack_wait_secs: u64) -> bool {
    job_timeout_secs > 0 && job_timeout_secs >= ack_wait_secs
}

#[cfg(test)]
mod tests {
    use super::{code_indexing_task_topic_policy, job_timeout_outlives_ack_wait};
    use gkg_server_config::SubscriptionConfig;

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

    #[test]
    fn job_timeout_must_sit_below_ack_wait() {
        assert!(!job_timeout_outlives_ack_wait(250, 300), "default is safe");
        assert!(
            !job_timeout_outlives_ack_wait(0, 300),
            "0 disables the timeout"
        );
        assert!(
            job_timeout_outlives_ack_wait(300, 300),
            "equal leaves no margin"
        );
        assert!(
            job_timeout_outlives_ack_wait(400, 300),
            "exceeds the ack window"
        );
    }
}
