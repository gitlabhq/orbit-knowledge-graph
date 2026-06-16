use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;

use super::batch_writer::ClickHouseBatchWriter;
use crate::checkpoint::WriteDurability;
use crate::destination::{BatchWriter, Destination, DestinationError};
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
    async fn new_batch_writer(
        &self,
        table: &str,
        durability: WriteDurability,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        let insert_sql = self
            .client
            .build_insert_sql_with_overrides(table, durability_overrides(durability));
        Ok(Box::new(ClickHouseBatchWriter::new(
            self.client.clone(),
            table.to_string(),
            insert_sql,
            self.metrics.clone(),
        )))
    }
}

/// `Durable` pins async-batching plus the flush wait over any config tuning; `FireAndForget`
/// defers to the configured `insert_settings`.
fn durability_overrides(durability: WriteDurability) -> &'static [(&'static str, &'static str)] {
    match durability {
        WriteDurability::Durable => &[("async_insert", "1"), ("wait_for_async_insert", "1")],
        WriteDurability::FireAndForget => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_pins_async_insert_and_wait() {
        assert_eq!(
            durability_overrides(WriteDurability::Durable),
            &[("async_insert", "1"), ("wait_for_async_insert", "1")]
        );
    }

    #[test]
    fn fire_and_forget_defers_to_config() {
        assert!(durability_overrides(WriteDurability::FireAndForget).is_empty());
    }
}
