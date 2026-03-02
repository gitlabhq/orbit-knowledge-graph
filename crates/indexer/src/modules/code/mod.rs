//! Code Indexing Module
//!
//! This module processes git push events from the Siphon CDC stream,
//! fetches repository code from Gitaly, runs the code-graph, and
//! writes the resulting graph data to ClickHouse.

mod arrow_converter;
pub mod config;
pub mod dispatch;
pub mod indexing_pipeline;
pub mod metrics;
mod project_code_indexing_handler;
mod project_store;
mod push_event_handler;
mod push_event_store;
mod repository_service;
mod siphon_decoder;
mod stale_data_cleaner;
#[cfg(test)]
mod test_helpers;
mod watermark_store;

use std::collections::HashMap;
use std::sync::Arc;

use crate::clickhouse::ClickHouseConfiguration;
use crate::handler::{Handler, HandlerInitError, deserialize_handler_config};
use gitlab_client::GitlabClient;
use metrics::CodeMetrics;
pub use project_code_indexing_handler::ProjectCodeIndexingHandlerConfig;
pub use push_event_handler::PushEventHandlerConfig;

pub use indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
pub use project_code_indexing_handler::ProjectCodeIndexingHandler;
pub use project_store::ClickHouseProjectStore;
pub use push_event_handler::PushEventHandler;
pub use push_event_store::ClickHousePushEventStore;
pub use repository_service::{
    CachingRepositoryService, GitLabRepositoryService, RepositoryService,
};
pub use stale_data_cleaner::ClickHouseStaleDataCleaner;
pub use watermark_store::ClickHouseCodeWatermarkStore;

pub fn create_code_handlers(
    graph_config: &ClickHouseConfiguration,
    datalake_config: &ClickHouseConfiguration,
    gitlab_client: Arc<GitlabClient>,
    handler_configs: &HashMap<String, serde_json::Value>,
) -> Result<Vec<Box<dyn Handler>>, HandlerInitError> {
    let push_event_config: PushEventHandlerConfig =
        deserialize_handler_config(handler_configs, "code-push-event")?;

    let project_reconciliation_config: ProjectCodeIndexingHandlerConfig =
        deserialize_handler_config(handler_configs, "code-project-reconciliation")?;

    let client = Arc::new(graph_config.build_client());

    let repository_service: Arc<dyn RepositoryService> =
        CachingRepositoryService::create(GitLabRepositoryService::create(gitlab_client));
    let watermark_store: Arc<dyn watermark_store::CodeWatermarkStore> =
        Arc::new(ClickHouseCodeWatermarkStore::new(Arc::clone(&client)));
    let project_store: Arc<dyn project_store::ProjectStore> =
        Arc::new(ClickHouseProjectStore::new(Arc::clone(&client)));
    let stale_data_cleaner: Arc<dyn stale_data_cleaner::StaleDataCleaner> =
        Arc::new(stale_data_cleaner::ClickHouseStaleDataCleaner::new(client));
    let push_event_store: Arc<dyn push_event_store::PushEventStore> = Arc::new(
        push_event_store::ClickHousePushEventStore::new(datalake_config.build_client()),
    );
    let metrics = CodeMetrics::new();

    let pipeline = Arc::new(indexing_pipeline::CodeIndexingPipeline::new(
        Arc::clone(&repository_service),
        Arc::clone(&watermark_store),
        stale_data_cleaner,
        metrics.clone(),
    ));

    Ok(vec![
        Box::new(PushEventHandler::new(
            Arc::clone(&pipeline),
            Arc::clone(&repository_service),
            Arc::clone(&watermark_store),
            Arc::clone(&project_store),
            metrics.clone(),
            push_event_config,
        )),
        Box::new(
            project_code_indexing_handler::ProjectCodeIndexingHandler::new(
                Arc::clone(&pipeline),
                Arc::clone(&repository_service),
                Arc::clone(&watermark_store),
                Arc::clone(&project_store),
                Arc::clone(&push_event_store),
                metrics.clone(),
                project_reconciliation_config,
            ),
        ),
    ])
}
