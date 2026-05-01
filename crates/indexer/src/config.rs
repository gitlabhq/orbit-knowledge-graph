use std::net::SocketAddr;

use gkg_server_config::{
    ClickHouseConfiguration, EngineConfigError, EngineConfiguration, GitlabClientConfiguration,
    NatsConfiguration, ScheduleConfig, SchemaConfig,
};
use thiserror::Error;

use crate::engine::handler::HandlerInitError;

fn default_health_bind_address() -> SocketAddr {
    "0.0.0.0:4202".parse().unwrap()
}

fn default_dispatcher_health_bind_address() -> SocketAddr {
    "0.0.0.0:4203".parse().unwrap()
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct IndexerConfig {
    #[serde(default)]
    pub nats: NatsConfiguration,
    #[serde(default)]
    pub graph: ClickHouseConfiguration,
    #[serde(default)]
    pub datalake: ClickHouseConfiguration,
    #[serde(default)]
    pub engine: EngineConfiguration,
    #[serde(default)]
    pub gitlab: Option<GitlabClientConfiguration>,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    #[serde(default = "default_health_bind_address")]
    pub health_bind_address: SocketAddr,
    #[serde(default)]
    pub schema: SchemaConfig,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            nats: NatsConfiguration::default(),
            graph: ClickHouseConfiguration::default(),
            datalake: ClickHouseConfiguration::default(),
            engine: EngineConfiguration::default(),
            gitlab: None,
            schedule: ScheduleConfig::default(),
            health_bind_address: default_health_bind_address(),
            schema: SchemaConfig::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] crate::nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] crate::engine::destination::DestinationError),

    #[error("Engine error: {0}")]
    Engine(#[from] crate::engine::EngineError),

    #[error("Handler initialization failed: {0}")]
    HandlerInit(#[from] HandlerInitError),

    #[error("Health server failed: {0}")]
    Health(#[from] std::io::Error),

    #[error("Schema version error: {0}")]
    SchemaVersion(#[from] crate::schema::version::SchemaVersionError),

    #[error("Schema migration error: {0}")]
    SchemaMigration(#[from] crate::schema::migration::MigrationError),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(#[from] gkg_server_config::SchemaConfigError),

    #[error("Invalid engine configuration: {0}")]
    InvalidEngineConfig(#[from] EngineConfigError),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DispatcherConfig {
    #[serde(default)]
    pub nats: NatsConfiguration,
    #[serde(default)]
    pub graph: ClickHouseConfiguration,
    #[serde(default)]
    pub datalake: ClickHouseConfiguration,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    #[serde(default)]
    pub schema: SchemaConfig,
    #[serde(default = "default_dispatcher_health_bind_address")]
    pub health_bind_address: SocketAddr,
}

#[derive(Debug, Error)]
pub enum DispatcherError {
    #[error("scheduler error: {0}")]
    Scheduler(#[from] crate::scheduler::SchedulerError),

    #[error("health server failed: {0}")]
    Health(#[from] std::io::Error),
}
