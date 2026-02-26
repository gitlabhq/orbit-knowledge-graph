//! Code Indexing Module
//!
//! This module processes git push events from the Siphon CDC stream,
//! fetches repository code from Gitaly, runs the code-graph, and
//! writes the resulting graph data to ClickHouse.

mod arrow_converter;
pub mod config;
mod project_store;
mod push_event_handler;
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

pub use config::CodeIndexingConfig;
pub use project_store::ClickHouseProjectStore;
pub use push_event_handler::PushEventHandler;
pub use repository_service::{
    CachingRepositoryService, GitLabRepositoryService, RepositoryService,
};
pub use stale_data_cleaner::ClickHouseStaleDataCleaner;
pub use watermark_store::ClickHouseCodeWatermarkStore;

pub struct CodeModule {
    repository_service: Arc<dyn RepositoryService>,
    watermark_store: Arc<dyn watermark_store::CodeWatermarkStore>,
    project_store: Arc<dyn project_store::ProjectStore>,
    stale_data_cleaner: Arc<dyn stale_data_cleaner::StaleDataCleaner>,
    config: CodeIndexingConfig,
}

impl CodeModule {
    pub fn new(
        clickhouse_config: &ClickHouseConfiguration,
        gitlab_client: Arc<GitlabClient>,
        config: CodeIndexingConfig,
    ) -> Result<Self, ModuleInitError> {
        let client = Arc::new(clickhouse_config.build_client());

        Ok(Self {
            repository_service: CachingRepositoryService::create(GitLabRepositoryService::create(
                gitlab_client,
            )),
            watermark_store: Arc::new(ClickHouseCodeWatermarkStore::new(Arc::clone(&client))),
            project_store: Arc::new(ClickHouseProjectStore::new(Arc::clone(&client))),
            stale_data_cleaner: Arc::new(stale_data_cleaner::ClickHouseStaleDataCleaner::new(
                client,
            )),
            config,
        })
    }
}

impl Module for CodeModule {
    fn name(&self) -> &str {
        "code"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![Box::new(PushEventHandler::new(
            Arc::clone(&self.repository_service),
            Arc::clone(&self.watermark_store),
            Arc::clone(&self.project_store),
            Arc::clone(&self.stale_data_cleaner),
            self.config.clone(),
        ))]
    }

    fn entities(&self) -> Vec<crate::entities::Entity> {
        Vec::new()
    }
}
