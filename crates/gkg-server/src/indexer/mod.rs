pub mod modules;
pub mod topic;

use std::sync::Arc;

use etl_engine::clickhouse::ClickHouseDestination;
use etl_engine::engine::EngineBuilder;
use etl_engine::module::{ModuleInitError, ModuleRegistry};
use etl_engine::nats::{KvBucketConfig, NatsBroker};
use gitaly_client::GitalyError;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::AppConfig;

use self::modules::code::config::buckets::EVENTS_CACHE;
use self::modules::sdlc::locking::INDEXING_LOCKS_BUCKET;
use self::modules::{CodeModule, SdlcModule};

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] etl_engine::nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] etl_engine::destination::DestinationError),

    #[error("Gitaly configuration failed: {0}")]
    GitalyConfiguration(#[from] GitalyError),

    #[error("Engine error: {0}")]
    Engine(#[from] etl_engine::engine::EngineError),

    #[error("Module initialization failed: {0}")]
    ModuleInit(#[from] ModuleInitError),
}

/// Runs the indexer until completion or until the token is cancelled.
pub async fn run(config: &AppConfig, shutdown: CancellationToken) -> Result<(), IndexerError> {
    info!(url = %config.nats.url, "connecting to NATS");
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);

    let per_message_ttl = KvBucketConfig::with_per_message_ttl();
    broker
        .ensure_kv_bucket_exists(INDEXING_LOCKS_BUCKET, per_message_ttl.clone())
        .await?;
    broker
        .ensure_kv_bucket_exists(EVENTS_CACHE, per_message_ttl)
        .await?;

    info!(url = %config.graph.url, "connecting to graph ClickHouse");
    let destination = Arc::new(ClickHouseDestination::new(config.graph.clone())?);

    info!("initializing SDLC module");
    let sdlc_module = SdlcModule::new(&config.datalake, &config.graph).await?;

    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(&sdlc_module);

    if let Some(gitaly_config) = &config.gitaly {
        info!("initializing Code module");
        let code_module =
            CodeModule::new(&config.graph, gitaly_config, config.code_indexing.clone())
                .map_err(IndexerError::ModuleInit)?;
        registry.register_module(&code_module);
    } else {
        info!("Code module disabled (GITALY_ADDRESS not set)");
    }

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
