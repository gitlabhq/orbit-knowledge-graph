//! ClickHouse destination.

use async_trait::async_trait;
use clickhouse_arrow::{ArrowFormat, Client, ClientBuilder};

use crate::destination::{BatchWriter, Destination, DestinationError};

use super::batch_writer::ClickHouseBatchWriter;
use super::configuration::ClickHouseConfiguration;
use super::error::ClickHouseError;

pub struct ClickHouseDestination {
    configuration: ClickHouseConfiguration,
}

impl ClickHouseDestination {
    pub fn new(configuration: ClickHouseConfiguration) -> Result<Self, DestinationError> {
        configuration.validate()?;
        Ok(Self { configuration })
    }

    async fn create_client(&self) -> Result<Client<ArrowFormat>, ClickHouseError> {
        let mut builder = ClientBuilder::new()
            .with_endpoint(&self.configuration.url)
            .with_database(&self.configuration.database)
            .with_username(&self.configuration.username);

        if let Some(ref password) = self.configuration.password {
            builder = builder.with_password(password);
        }

        builder
            .build_arrow()
            .await
            .map_err(ClickHouseError::Connection)
    }
}

#[async_trait]
impl Destination for ClickHouseDestination {
    async fn new_batch_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        let client = self.create_client().await?;

        Ok(Box::new(ClickHouseBatchWriter::new(
            client,
            table.to_string(),
        )))
    }
}
