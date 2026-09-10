use std::net::SocketAddr;

use orbit_server_config::{
    AnalyticsConfig, AppConfig, ClickHouseConfiguration, EngineConfigError, EngineConfiguration,
    GitlabClientConfiguration, NatsConfiguration, ScheduleConfig, SchemaConfig,
};
use thiserror::Error;

use crate::engine::handler::HandlerInitError;

#[derive(Clone, Debug)]
pub struct IndexerConfig {
    pub nats: NatsConfiguration,
    pub graph: ClickHouseConfiguration,
    pub datalake: ClickHouseConfiguration,
    pub engine: EngineConfiguration,
    pub gitlab: Option<GitlabClientConfiguration>,
    pub schedule: ScheduleConfig,
    pub health_bind_address: SocketAddr,
    pub schema: SchemaConfig,
    pub analytics: AnalyticsConfig,
}

impl From<&AppConfig> for IndexerConfig {
    fn from(config: &AppConfig) -> Self {
        Self {
            nats: config.nats.clone(),
            graph: config.graph.clone(),
            datalake: config.datalake.clone(),
            engine: config.engine.clone(),
            gitlab: config.gitlab_client_config(),
            schedule: config.schedule.clone(),
            health_bind_address: config.indexer_health_bind_address,
            schema: config.schema.clone(),
            analytics: config.analytics.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] crate::nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] crate::clickhouse::WriteError),

    #[error("Engine error: {0}")]
    Engine(#[from] crate::engine::EngineError),

    #[error("Handler initialization failed: {0}")]
    HandlerInit(#[from] HandlerInitError),

    #[error("Health server failed: {0}")]
    Health(#[from] std::io::Error),

    #[error("Schema version error: {0}")]
    SchemaVersion(#[from] orbit_migrations::version::SchemaVersionError),

    #[error("Schema readiness wait failed: {0}")]
    SchemaWait(#[from] crate::schema::version::SchemaWaitError),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(#[from] orbit_server_config::SchemaConfigError),

    #[error("Invalid engine configuration: {0}")]
    InvalidEngineConfig(#[from] EngineConfigError),

    #[error("Analytics tracker initialization failed: {0}")]
    Analytics(#[from] labkit_events::Error),
}

#[derive(Clone, Debug)]
pub struct DispatcherConfig {
    pub nats: NatsConfiguration,
    pub graph: ClickHouseConfiguration,
    pub datalake: ClickHouseConfiguration,
    pub schedule: ScheduleConfig,
    pub schema: SchemaConfig,
    pub health_bind_address: SocketAddr,
}

impl From<&AppConfig> for DispatcherConfig {
    fn from(config: &AppConfig) -> Self {
        Self {
            nats: config.nats.clone(),
            graph: config.graph.clone(),
            datalake: config.datalake.clone(),
            schedule: config.schedule.clone(),
            schema: config.schema.clone(),
            health_bind_address: config.dispatcher_health_bind_address,
        }
    }
}

#[derive(Debug, Error)]
pub enum DispatcherError {
    #[error("ontology archive error: {0}")]
    Archive(#[from] orbit_migrations::catalog::CatalogError),

    #[error("schema version error: {0}")]
    SchemaVersion(#[from] orbit_migrations::version::SchemaVersionError),

    #[error("scheduler error: {0}")]
    Scheduler(#[from] crate::orchestrator::scheduled::SchedulerError),

    #[error("trigger error: {0}")]
    Trigger(#[from] crate::orchestrator::TriggerError),

    #[error("schema migration error: {0}")]
    Migration(#[from] crate::schema::migration::DispatcherMigrationError),

    #[error("health server failed: {0}")]
    Health(#[from] std::io::Error),
}
