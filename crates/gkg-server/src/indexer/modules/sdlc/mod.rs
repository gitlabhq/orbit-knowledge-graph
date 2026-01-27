mod datalake;
mod group_handler;
mod namespace_handler;
mod project_handler;
mod user_handler;
mod watermark_store;

use std::sync::Arc;

use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::module::{Handler, Module, ModuleInitError};
use namespace_handler::NamespaceHandler;
use user_handler::UserHandler;
use watermark_store::WatermarkClient;

use crate::indexer::modules::sdlc::datalake::DatalakeClient;

pub struct SdlcModule {
    datalake_client: DatalakeClient,
    watermark_client: WatermarkClient,
}

impl SdlcModule {
    pub async fn new(configuration: &ClickHouseConfiguration) -> Result<Self, ModuleInitError> {
        let client = configuration
            .build_client()
            .await
            .map_err(ModuleInitError::new)?;
        
        let shared_client = Arc::new(client);

        Ok(Self {
            datalake_client: Arc::clone(&shared_client),
            watermark_client: shared_client,
        })

    }
}

impl Module for SdlcModule {
    fn name(&self) -> &str {
        "sdlc"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![
            Box::new(UserHandler::new(
                Arc::clone(&self.datalake_client),
                Arc::clone(&self.watermark_client),
            )),
            Box::new(NamespaceHandler::new(
                Arc::clone(&self.datalake_client),
                Arc::clone(&self.watermark_client),
            )),
        ]
    }

    fn entities(&self) -> Vec<etl_engine::entities::Entity> {
        Vec::new()
    }
}
