//! Code Indexing Module
//!
//! This module processes code indexing tasks from the Siphon CDC stream,
//! fetches repository code from Gitaly, runs the code-graph, and
//! writes the resulting graph data to ClickHouse.

mod archive;
mod arrow_converter;
mod checkpoint_store;
mod code_backfill_dispatch_handler;
mod code_backfill_handler;
mod code_indexing_task_handler;
pub mod config;
pub mod indexing_pipeline;
pub mod locking;
pub mod metrics;
mod namespace_backfill_dispatcher;
mod repository_service;
mod siphon_code_indexing_task_dispatcher;
mod siphon_decoder;
mod stale_data_cleaner;
#[cfg(test)]
mod test_helpers;

use std::sync::Arc;

use crate::IndexerConfig;
use crate::handler::{HandlerInitError, HandlerRegistry};
pub use code_backfill_dispatch_handler::CodeBackfillDispatchHandler;
pub use code_backfill_dispatch_handler::CodeBackfillDispatchHandlerConfig;
pub use code_backfill_handler::CodeBackfillHandler;
pub use code_backfill_handler::CodeBackfillHandlerConfig;
pub use code_indexing_task_handler::CodeIndexingTaskHandler;
pub use code_indexing_task_handler::CodeIndexingTaskHandlerConfig;
use config::CodeTableNames;
use gitlab_client::GitlabClient;
use metrics::CodeMetrics;
pub use namespace_backfill_dispatcher::{
    NamespaceCodeBackfillDispatcher, NamespaceCodeBackfillDispatcherConfig,
};
pub use siphon_code_indexing_task_dispatcher::{
    SiphonCodeIndexingTaskDispatcher, SiphonCodeIndexingTaskDispatcherConfig,
};

pub use checkpoint_store::ClickHouseCodeCheckpointStore;
pub use indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
pub use repository_service::{
    CachingRepositoryService, RailsRepositoryService, RepositoryService, RepositoryServiceError,
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

    let code_indexing_task_config = config.engine.handlers.code_indexing_task.clone();
    let backfill_config = config.engine.handlers.code_backfill.clone();
    let backfill_dispatch_config = config.engine.handlers.code_backfill_dispatch.clone();

    let table_names =
        Arc::new(CodeTableNames::from_ontology(ontology).map_err(HandlerInitError::new)?);

    let gitlab_client =
        Arc::new(GitlabClient::new(gitlab_config.clone()).map_err(HandlerInitError::new)?);
    let client = Arc::new(config.graph.build_client());

    let repository_service: Arc<dyn RepositoryService> =
        CachingRepositoryService::create(RailsRepositoryService::create(gitlab_client));
    let checkpoint_store: Arc<dyn checkpoint_store::CodeCheckpointStore> =
        Arc::new(ClickHouseCodeCheckpointStore::new(Arc::clone(&client)));
    let stale_data_cleaner: Arc<dyn stale_data_cleaner::StaleDataCleaner> = Arc::new(
        stale_data_cleaner::ClickHouseStaleDataCleaner::new(client, &table_names),
    );
    let metrics = CodeMetrics::new();

    let pipeline = Arc::new(indexing_pipeline::CodeIndexingPipeline::new(
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        stale_data_cleaner,
        metrics.clone(),
        table_names,
    ));

    registry.register_handler(Box::new(CodeIndexingTaskHandler::new(
        Arc::clone(&pipeline),
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        metrics.clone(),
        code_indexing_task_config,
    )));

    registry.register_handler(Box::new(CodeBackfillHandler::new(
        Arc::clone(&pipeline),
        Arc::clone(&repository_service),
        Arc::clone(&checkpoint_store),
        metrics,
        backfill_config,
    )));

    registry.register_handler(Box::new(CodeBackfillDispatchHandler::new(
        config.datalake.build_client(),
        backfill_dispatch_config,
    )));

    Ok(())
}
