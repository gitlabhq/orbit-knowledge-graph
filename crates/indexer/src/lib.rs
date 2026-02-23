//! # Indexer
//!
//! Message processing framework and domain modules for the GitLab Knowledge Graph.
//!
//! This crate contains both the engine (message routing, concurrency, destinations)
//! and the domain modules (SDLC, Code) that implement indexing logic.
//!
//! ## Engine
//!
//! You provide:
//! - A [`NatsBroker`](nats::NatsBroker) for message streaming
//! - A [`Destination`](destination::Destination) (database, data lake, etc.)
//! - One or more [`Module`](module::Module)s containing [`Handler`](module::Handler)s
//!
//! ```text
//! NatsBroker ──▶ Engine ──▶ Destination
//!                  │
//!                  ▼
//!            ModuleRegistry
//!              └─ Module
//!                  └─ Handler
//!                  └─ Handler
//! ```
//!
//! ## Domain modules
//!
//! - [`modules::sdlc`] - SDLC entities (users, projects, MRs, CI, etc.)
//! - [`modules::code`] - Code indexing (call graphs, definitions, references)
//!
pub mod clickhouse;
pub mod configuration;
pub mod constants;
pub mod destination;
pub mod dispatcher;
pub mod engine;
pub mod entities;
pub(crate) mod env;
pub mod locking;
pub mod metrics;
pub mod module;
pub mod modules;
pub mod nats;
pub mod topic;
pub mod types;
pub mod worker_pool;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

use std::sync::Arc;

use clickhouse::ClickHouseConfiguration;
use clickhouse::ClickHouseDestination;
use configuration::EngineConfiguration;
use engine::EngineBuilder;
use gitaly_client::GitalyError;
use module::{ModuleInitError, ModuleRegistry};
use modules::code::GitalyConfiguration;
use modules::code::config::CodeIndexingConfig;
use modules::sdlc::config::SdlcIndexingConfig;
use modules::sdlc::locking::INDEXING_LOCKS_BUCKET;
use modules::{CodeModule, SdlcModule};
use nats::{KvBucketConfig, NatsBroker, NatsConfiguration};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    pub gitaly: Option<GitalyConfiguration>,
    #[serde(default)]
    pub code_indexing: CodeIndexingConfig,
    #[serde(default)]
    pub sdlc_indexing: SdlcIndexingConfig,
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] destination::DestinationError),

    #[error("Gitaly configuration failed: {0}")]
    GitalyConfiguration(#[from] GitalyError),

    #[error("Engine error: {0}")]
    Engine(#[from] engine::EngineError),

    #[error("Module initialization failed: {0}")]
    ModuleInit(#[from] ModuleInitError),
}

/// Runs the indexer until completion or until the token is cancelled.
pub async fn run(config: &IndexerConfig, shutdown: CancellationToken) -> Result<(), IndexerError> {
    info!(url = %config.nats.url, "connecting to NATS");
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);

    let per_message_ttl = KvBucketConfig::with_per_message_ttl();
    broker
        .ensure_kv_bucket_exists(INDEXING_LOCKS_BUCKET, per_message_ttl)
        .await?;

    info!(url = %config.graph.url, "connecting to graph ClickHouse");
    let destination = Arc::new(ClickHouseDestination::new(config.graph.clone())?);

    info!("initializing SDLC module");
    let sdlc_module = SdlcModule::new(
        &config.datalake,
        &config.graph,
        config.sdlc_indexing.datalake_batch_size,
    )
    .await?;

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
