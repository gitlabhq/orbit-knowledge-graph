mod datalake;
mod group_handler;
mod namespace_handler;
mod project_handler;
mod user_handler;
mod watermark_store;

use std::sync::Arc;

use datalake::{Datalake, DatalakeQuery};
use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::module::{Handler, Module, ModuleInitError};
use namespace_handler::NamespaceHandler;
use user_handler::UserHandler;
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub struct SdlcModule {
    datalake: Arc<dyn DatalakeQuery>,
    watermark_store: Arc<dyn WatermarkStore>,
}

impl SdlcModule {
    pub async fn new(configuration: &ClickHouseConfiguration) -> Result<Self, ModuleInitError> {
        let client = Arc::new(configuration.build_client());

        Ok(Self {
            datalake: Arc::new(Datalake::new(Arc::clone(&client))),
            watermark_store: Arc::new(ClickHouseWatermarkStore::new(client)),
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
                Arc::clone(&self.datalake),
                Arc::clone(&self.watermark_store),
            )),
            Box::new(NamespaceHandler::new(
                Arc::clone(&self.datalake),
                Arc::clone(&self.watermark_store),
            )),
        ]
    }

    fn entities(&self) -> Vec<etl_engine::entities::Entity> {
        Vec::new()
    }
}
