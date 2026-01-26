//! ClickHouse destination.

use async_trait::async_trait;
use clickhouse_arrow::{ArrowFormat, Client, ClientBuilder};

use crate::constants::EDGE_TABLE_NAME;
use crate::destination::{BatchWriter, Destination, DestinationError};
use crate::entities::Entity;

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

    fn extract_table_name(entity: &Entity) -> &str {
        match entity {
            Entity::Node { name, .. } => name,
            Entity::Edge { .. } => EDGE_TABLE_NAME,
        }
    }
}

#[async_trait]
impl Destination for ClickHouseDestination {
    async fn new_batch_writer(
        &self,
        entity: &Entity,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        let table = Self::extract_table_name(entity);
        let client = self.create_client().await?;

        Ok(Box::new(ClickHouseBatchWriter::new(
            client,
            table.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name_from_node() {
        let entity = Entity::Node {
            name: "users".to_string(),
            fields: vec![],
            primary_keys: vec![],
        };

        assert_eq!(ClickHouseDestination::extract_table_name(&entity), "users");
    }

    #[test]
    fn test_extract_table_name_from_edge() {
        let entity = Entity::Edge {
            source: "user_id".to_string(),
            source_type: "User".to_string(),
            relationship_type: "user_follows".to_string(),
            target: "follows_id".to_string(),
            target_type: "User".to_string(),
        };

        assert_eq!(ClickHouseDestination::extract_table_name(&entity), "edges");
    }
}
