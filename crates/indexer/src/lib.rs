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
pub mod health;
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

use std::net::SocketAddr;
use std::sync::Arc;

use clickhouse::ClickHouseConfiguration;
use clickhouse::ClickHouseDestination;
use configuration::EngineConfiguration;
use dispatcher::DispatchConfig;
use engine::EngineBuilder;
use gitlab_client::{GitlabClient, GitlabClientConfiguration};
use health::{HealthState, run_health_server};
use module::{ModuleInitError, ModuleRegistry};
use modules::code::config::CodeIndexingConfig;
use modules::sdlc::config::SdlcIndexingConfig;
use modules::sdlc::locking::INDEXING_LOCKS_BUCKET;
use modules::{CodeModule, SdlcModule};
use nats::{KvBucketConfig, NatsBroker, NatsConfiguration};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

fn default_health_bind_address() -> SocketAddr {
    "0.0.0.0:4202".parse().unwrap()
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
    pub modules: ModulesConfig,
    #[serde(default = "default_health_bind_address")]
    pub health_bind_address: SocketAddr,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct ModulesConfig {
    #[serde(default)]
    pub sdlc: SdlcIndexingConfig,
    #[serde(default)]
    pub code: CodeIndexingConfig,
    #[serde(default)]
    pub dispatch: DispatchConfig,
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] destination::DestinationError),

    #[error("Engine error: {0}")]
    Engine(#[from] engine::EngineError),

    #[error("Module initialization failed: {0}")]
    ModuleInit(#[from] ModuleInitError),

    #[error("Health server failed: {0}")]
    Health(#[from] std::io::Error),
}

/// Runs the indexer until completion or until the token is cancelled.
pub async fn run(config: &IndexerConfig, shutdown: CancellationToken) -> Result<(), IndexerError> {
    info!(url = %config.nats.url, "connecting to NATS");
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);

    let per_message_ttl = KvBucketConfig::with_per_message_ttl();
    broker
        .ensure_kv_bucket_exists(INDEXING_LOCKS_BUCKET, per_message_ttl)
        .await?;

    let metrics = Arc::new(metrics::EngineMetrics::new());

    info!(url = %config.graph.url, "connecting to graph ClickHouse");
    let destination = Arc::new(ClickHouseDestination::new(
        config.graph.clone(),
        metrics.clone(),
    )?);

    info!("initializing SDLC module");
    let sdlc_module =
        SdlcModule::new(&config.datalake, &config.graph, &config.modules.sdlc).await?;

    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(&sdlc_module);

    if let Some(gitlab_config) = &config.gitlab {
        info!("initializing Code module");
        let gitlab_client =
            Arc::new(GitlabClient::new(gitlab_config.clone()).map_err(ModuleInitError::new)?);
        let code_module = CodeModule::new(
            &config.graph,
            &config.datalake,
            gitlab_client,
            config.modules.code.clone(),
        )
        .map_err(IndexerError::ModuleInit)?;
        registry.register_module(&code_module);
    } else {
        info!("Code module disabled (GitLab client not configured)");
    }

    info!(topics = registry.topics().len(), "registered modules");

    let health_state = HealthState {
        nats_client: broker.nats_client().clone(),
        graph_client: config.graph.build_client(),
        datalake_client: config.datalake.build_client(),
    };

    let engine = Arc::new(
        EngineBuilder::new(broker, registry, destination)
            .metrics(metrics)
            .build(),
    );

    let engine_handle = engine.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown.cancelled().await;
        info!("received shutdown signal");
        engine_handle.stop();
    });

    let mut engine_config = config.engine.clone();
    engine_config
        .modules
        .insert("sdlc".into(), config.modules.sdlc.engine.clone());
    if config.gitlab.is_some() {
        engine_config
            .modules
            .insert("code".into(), config.modules.code.engine.clone());
    }

    info!("indexer started");
    let result = tokio::select! {
        result = engine.run(&engine_config) => result.map_err(IndexerError::from),
        result = run_health_server(config.health_bind_address, health_state) => {
            let error = result.err().unwrap_or_else(|| std::io::Error::other(
                "health server exited unexpectedly",
            ));
            Err(IndexerError::Health(error))
        }
    };

    shutdown_task.abort();

    info!("indexer stopped");
    result
}
