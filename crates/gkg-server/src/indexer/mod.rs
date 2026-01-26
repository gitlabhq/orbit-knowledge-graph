pub mod modules;

use std::sync::Arc;

use etl_engine::clickhouse::ClickHouseDestination;
use etl_engine::engine::EngineBuilder;
use etl_engine::module::ModuleRegistry;
use etl_engine::nats::NatsBroker;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::AppConfig;

use self::modules::PlaceholderModule;

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] etl_engine::nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] etl_engine::destination::DestinationError),

    #[error("Engine error: {0}")]
    Engine(#[from] etl_engine::engine::EngineError),
}

/// Runs the indexer until completion or until the token is cancelled.
pub async fn run(config: &AppConfig, shutdown: CancellationToken) -> Result<(), IndexerError> {
    info!(url = %config.nats.url, "connecting to NATS");
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);

    info!(url = %config.clickhouse.url, "connecting to ClickHouse");
    let destination = Arc::new(ClickHouseDestination::new(config.clickhouse.clone())?);

    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(&PlaceholderModule);
    info!(topics = registry.topics().len(), "registered modules");

    let engine = Arc::new(EngineBuilder::new(broker, registry, destination).build());

    let engine_handle = engine.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown.cancelled().await;
        info!("received shutdown signal");
        engine_handle.stop();
    });

    info!("indexer started");
    let result = engine.run(&config.engine).await;

    shutdown_task.abort();

    info!("indexer stopped");
    result.map_err(Into::into)
}
