//! Code Indexing Module
//!
//! This module processes git push events from the Siphon CDC stream,
//! fetches repository code from Gitaly, runs the code-graph, and
//! writes the resulting graph data to ClickHouse.

mod archive;
mod arrow_converter;
mod checkpoint_store;
pub mod config;
pub mod dispatch;
pub mod indexing_pipeline;
pub mod locking;
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

use std::sync::Arc;

use crate::IndexerConfig;
use crate::handler::{HandlerInitError, HandlerRegistry};
use config::CodeTableNames;
use gitlab_client::GitlabClient;
use metrics::CodeMetrics;
pub use project_code_indexing_handler::ProjectCodeIndexingHandlerConfig;
pub use push_event_handler::PushEventHandlerConfig;

pub use checkpoint_store::ClickHouseCodeCheckpointStore;
pub use indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
pub use project_code_indexing_handler::ProjectCodeIndexingHandler;
pub use project_store::ClickHouseProjectStore;
pub use push_event_handler::PushEventHandler;
pub use push_event_store::ClickHousePushEventStore;
pub use repository_service::{
    CachingRepositoryService, GitLabRepositoryService, RepositoryService,
};
pub use stale_data_cleaner::ClickHouseStaleDataCleaner;

pub fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
) -> Result<(), HandlerInitError> {
    let Some(gitlab_config) = &config.gitlab else {
        tracing::info!("Code handlers disabled (GitLab client not configured)");
        return Ok(());
    };

    let push_event_config = config.engine.handlers.code_push_event.clone();
    let project_reconciliation_config = config.engine.handlers.code_project_reconciliation.clone();

    let table_names =
        Arc::new(CodeTableNames::from_ontology(ontology).map_err(HandlerInitError::new)?);

    let gitlab_client =
        Arc::new(GitlabClient::new(gitlab_config.clone()).map_err(HandlerInitError::new)?);
    let client = Arc::new(config.graph.build_client());

    let repository_service: Arc<dyn RepositoryService> =
        CachingRepositoryService::create(GitLabRepositoryService::create(gitlab_client));
    let checkpoint_store: Arc<dyn checkpoint_store::CodeCheckpointStore> =
        Arc::new(ClickHouseCodeCheckpointStore::new(Arc::clone(&client)));
    let project_store: Arc<dyn project_store::ProjectStore> =
        Arc::new(ClickHouseProjectStore::new(Arc::clone(&client)));
    let stale_data_cleaner: Arc<dyn stale_data_cleaner::StaleDataCleaner> = Arc::new(
        stale_data_cleaner::ClickHouseStaleDataCleaner::new(client, &table_names),
    );
    let push_event_store: Arc<dyn push_event_store::PushEventStore> = Arc::new(
        push_event_store::ClickHousePushEventStore::new(config.datalake.build_client()),
    );
    let metrics = CodeMetrics::new();

    let pipeline = Arc::new(indexing_pipeline::CodeIndexingPipeline::new(
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        stale_data_cleaner,
        metrics.clone(),
        table_names,
    ));

    registry.register_handler(Box::new(PushEventHandler::new(
        Arc::clone(&pipeline),
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        Arc::clone(&project_store),
        metrics.clone(),
        push_event_config,
    )));

    registry.register_handler(Box::new(
        project_code_indexing_handler::ProjectCodeIndexingHandler::new(
            Arc::clone(&pipeline),
            Arc::clone(&repository_service),
            Arc::clone(&checkpoint_store),
            Arc::clone(&project_store),
            Arc::clone(&push_event_store),
            metrics.clone(),
            project_reconciliation_config,
        ),
    ));

    Ok(())
}
