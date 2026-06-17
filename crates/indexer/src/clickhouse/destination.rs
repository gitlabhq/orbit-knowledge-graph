use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;

use super::batch_writer::ClickHouseBatchWriter;
use crate::destination::{BatchWriter, Destination, DestinationError};
use crate::durability::WriteDurability;
use crate::metrics::EngineMetrics;

pub struct ClickHouseDestination {
    client: ArrowClickHouseClient,
    metrics: Arc<EngineMetrics>,
}

impl ClickHouseDestination {
    pub fn new(
        configuration: ClickHouseConfiguration,
        metrics: Arc<EngineMetrics>,
    ) -> Result<Self, DestinationError> {
        configuration
            .validate()
            .map_err(|e| DestinationError::InvalidConfiguration(e.to_string()))?;
        let client = configuration.build_client();
        Ok(Self { client, metrics })
    }
}

#[async_trait]
impl Destination for ClickHouseDestination {
    async fn new_batch_writer_with_durability(
        &self,
        table: &str,
        durability: WriteDurability,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        Ok(Box::new(ClickHouseBatchWriter::new(
            self.client.clone(),
            table.to_string(),
            durability,
            self.metrics.clone(),
        )))
    }
}
