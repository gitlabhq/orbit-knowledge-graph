//! Code Indexing Module
//!
//! This module processes code indexing tasks from the Siphon CDC stream,
//! fetches repository code from Gitaly, runs the code-graph, and
//! writes the resulting graph data to ClickHouse.

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
    let aggregator = crate::clickhouse::CodeWriteAggregator::start(
        writer,
        pipeline_config.write_channel_capacity,
        pipeline_config.write_slice_rows,
        pipeline_config.write_max_concurrent_writes,
        pipeline_config.aggregator_max_buffer_age(),
    );

    let pipeline = Arc::new(pipeline::CodeIndexingPipeline::new(
        resolver,
        aggregator,
        Arc::clone(&checkpoint_store),
        stale_data_cleaner,
        metrics.clone(),
        table_names,
        Arc::new(ontology.clone()),
        pipeline_config,
    ));

    let mut subscription = CodeIndexingTaskRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(CODE_INDEXING_TASK_TOPIC) {
        subscription = subscription.with_config(topic_config);
    }
    if let Some(max_inflight) = pipeline.max_inflight() {
        subscription = subscription.with_max_inflight(max_inflight);
    }

    registry.register_handler(Box::new(CodeIndexingTaskHandler::new(
        Arc::clone(&pipeline),
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        metrics,
        config.nats.ack_wait(),
        code_indexing_task_config.pipeline.aggregator_heartbeat(),
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
    use super::job_timeout_outlives_ack_wait;

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
