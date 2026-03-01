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

use std::sync::Arc;

use crate::clickhouse::ClickHouseConfiguration;
use crate::module::{Handler, Module, ModuleInitError};
use gitlab_client::GitlabClient;
use metrics::CodeMetrics;

pub use config::CodeIndexingConfig;
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

pub struct CodeModule {
    pipeline: Arc<indexing_pipeline::CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    watermark_store: Arc<dyn watermark_store::CodeWatermarkStore>,
    project_store: Arc<dyn project_store::ProjectStore>,
    push_event_store: Arc<dyn push_event_store::PushEventStore>,
    metrics: CodeMetrics,
    config: CodeIndexingConfig,
}

impl CodeModule {
    pub fn new(
        graph_config: &ClickHouseConfiguration,
        datalake_config: &ClickHouseConfiguration,
        gitlab_client: Arc<GitlabClient>,
        config: CodeIndexingConfig,
    ) -> Result<Self, ModuleInitError> {
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

        Ok(Self {
            pipeline,
            repository_service,
            watermark_store,
            project_store,
            push_event_store,
            metrics,
            config,
        })
    }
}

impl Module for CodeModule {
    fn name(&self) -> &str {
        "code"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![
            Box::new(PushEventHandler::new(
                Arc::clone(&self.pipeline),
                Arc::clone(&self.repository_service),
                Arc::clone(&self.watermark_store),
                Arc::clone(&self.project_store),
                self.metrics.clone(),
                self.config.clone(),
            )),
            Box::new(
                project_code_indexing_handler::ProjectCodeIndexingHandler::new(
                    Arc::clone(&self.pipeline),
                    Arc::clone(&self.repository_service),
                    Arc::clone(&self.watermark_store),
                    Arc::clone(&self.project_store),
                    Arc::clone(&self.push_event_store),
                    self.metrics.clone(),
                ),
            ),
        ]
    }

    fn entities(&self) -> Vec<crate::entities::Entity> {
        Vec::new()
    }
}
