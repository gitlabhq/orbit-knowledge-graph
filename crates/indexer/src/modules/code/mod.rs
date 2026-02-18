//! Code Indexing Module
//!
//! This module processes git push events from the Siphon CDC stream,
//! fetches repository code from Gitaly, runs the code-graph, and
//! writes the resulting graph data to ClickHouse.

mod arrow_converter;
pub mod config;
mod event_cache_handler;
mod gitaly;
mod project_store;
mod push_event_handler;
mod siphon_decoder;
#[cfg(test)]
mod test_helpers;
mod watermark_store;

use std::sync::Arc;

use crate::clickhouse::ClickHouseConfiguration;
use crate::module::{Handler, Module, ModuleInitError};

pub use config::CodeIndexingConfig;
pub use gitaly::{GitalyConfiguration, GitalyRepositoryService, RepositoryService};
pub use project_store::ClickHouseProjectStore;
pub use push_event_handler::PushEventHandler;
pub use watermark_store::ClickHouseCodeWatermarkStore;

use event_cache_handler::EventCacheHandler;

pub struct CodeModule {
    repository_service: Arc<dyn RepositoryService>,
    watermark_store: Arc<dyn watermark_store::CodeWatermarkStore>,
    project_store: Arc<dyn project_store::ProjectStore>,
    config: CodeIndexingConfig,
}

impl CodeModule {
    pub fn new(
        clickhouse_config: &ClickHouseConfiguration,
        gitaly_config: &GitalyConfiguration,
        config: CodeIndexingConfig,
    ) -> Result<Self, ModuleInitError> {
        let client = Arc::new(clickhouse_config.build_client());

        Ok(Self {
            repository_service: GitalyRepositoryService::create(gitaly_config.clone()),
            watermark_store: Arc::new(ClickHouseCodeWatermarkStore::new(Arc::clone(&client))),
            project_store: Arc::new(ClickHouseProjectStore::new(client)),
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
            Box::new(EventCacheHandler::new(self.config.clone())),
            Box::new(PushEventHandler::new(
                Arc::clone(&self.repository_service),
                Arc::clone(&self.watermark_store),
                Arc::clone(&self.project_store),
                self.config.clone(),
            )),
        ]
    }

    fn entities(&self) -> Vec<crate::entities::Entity> {
        Vec::new()
    }
}
