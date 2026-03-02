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
//! - One or more [`Handler`](handler::Handler)s registered in a [`HandlerRegistry`](handler::HandlerRegistry)
//!
//! ```text
//! NatsBroker ──▶ Engine ──▶ Destination
//!                  │
//!                  ▼
//!            HandlerRegistry
//!              └─ Handler
//!              └─ Handler
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
pub(crate) mod env;
pub mod handler;
pub mod health;
pub mod locking;
pub mod metrics;
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
use handler::{HandlerInitError, HandlerRegistry};
use health::{HealthState, run_health_server};
use modules::sdlc::locking::INDEXING_LOCKS_BUCKET;
use modules::{create_code_handlers, create_sdlc_handlers};
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
    pub dispatch: DispatchConfig,
    #[serde(default = "default_health_bind_address")]
    pub health_bind_address: SocketAddr,
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] destination::DestinationError),

    #[error("Engine error: {0}")]
    Engine(#[from] engine::EngineError),

    #[error("Handler initialization failed: {0}")]
    HandlerInit(#[from] HandlerInitError),

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

    let registry = Arc::new(HandlerRegistry::default());

    info!("initializing SDLC handlers");
    for handler in
        create_sdlc_handlers(&config.datalake, &config.graph, &config.engine.handlers).await?
    {
        registry.register_handler(handler);
    }

    if let Some(gitlab_config) = &config.gitlab {
        info!("initializing Code handlers");
        let gitlab_client =
            Arc::new(GitlabClient::new(gitlab_config.clone()).map_err(HandlerInitError::new)?);
        for handler in create_code_handlers(
            &config.graph,
            &config.datalake,
            gitlab_client,
            &config.engine.handlers,
        )? {
            registry.register_handler(handler);
        }
    } else {
        info!("Code handlers disabled (GitLab client not configured)");
    }

    info!(topics = registry.topics().len(), "registered handlers");

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

    info!("indexer started");
    let result = tokio::select! {
        result = engine.run(&config.engine) => result.map_err(IndexerError::from),
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
