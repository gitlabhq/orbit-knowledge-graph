//! ClickHouse destination.

use async_trait::async_trait;

use super::arrow_client::ArrowClickHouseClient;
use super::batch_writer::ClickHouseBatchWriter;
use super::configuration::ClickHouseConfiguration;
use crate::destination::{BatchWriter, Destination, DestinationError};

pub struct ClickHouseDestination {
    configuration: ClickHouseConfiguration,
}

impl ClickHouseDestination {
    pub fn new(configuration: ClickHouseConfiguration) -> Result<Self, DestinationError> {
        configuration.validate()?;
        Ok(Self { configuration })
    }

    fn create_client(&self) -> ArrowClickHouseClient {
        self.configuration.build_client()
    }
}

#[async_trait]
impl Destination for ClickHouseDestination {
    async fn new_batch_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        let client = self.create_client();

        Ok(Box::new(ClickHouseBatchWriter::new(
            client,
            table.to_string(),
        )))
    }
}
