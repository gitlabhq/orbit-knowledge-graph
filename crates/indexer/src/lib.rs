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
pub mod checkpoint;
pub mod clickhouse;
pub mod dead_letter;
pub mod destination;
pub mod engine;
pub mod handler;
pub mod health;
pub mod llqm_v1;
pub mod locking;
pub mod metrics;
pub mod modules;
pub mod nats;
pub mod scheduler;
pub mod schema_version;
pub mod topic;
pub mod types;
pub mod worker_pool;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

use std::net::SocketAddr;
use std::sync::Arc;

use clickhouse::ClickHouseConfigurationExt;
use clickhouse::ClickHouseDestination;
use engine::EngineBuilder;
use gitlab_client::GitlabClient;
use gkg_server_config::{
    ClickHouseConfiguration, EngineConfiguration, GitlabClientConfiguration, NatsConfiguration,
    ScheduleConfig, SchemaVersionCheckConfig,
};
use handler::{HandlerInitError, HandlerRegistry};
use health::{HealthState, run_health_server};
use locking::INDEXING_LOCKS_BUCKET;
use nats::{KvBucketConfig, NatsBroker};
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
    pub schedule: ScheduleConfig,
    #[serde(default = "default_health_bind_address")]
    pub health_bind_address: SocketAddr,
    #[serde(default)]
    pub schema_version_check: SchemaVersionCheckConfig,
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
            schema_version_check: SchemaVersionCheckConfig::default(),
        }
    }
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
pub async fn run(
    config: &IndexerConfig,
    ontology: Arc<ontology::Ontology>,
    shutdown: CancellationToken,
) -> Result<(), IndexerError> {
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
    modules::sdlc::register_handlers(&registry, config, &ontology).await?;

    info!("initializing Code handlers");
    modules::code::register_handlers(&registry, config, &ontology)?;

    info!("initializing Namespace Deletion handler");
    modules::namespace_deletion::register_handlers(&registry, config, &ontology)?;

    info!(
        subscriptions = registry.subscriptions().len(),
        "registered handlers"
    );

    let gitlab_client = config
        .gitlab
        .as_ref()
        .map(|cfg| GitlabClient::new(cfg.clone()))
        .transpose()
        .map_err(HandlerInitError::new)?
        .map(Arc::new);

    let health_state = HealthState {
        nats_client: broker.nats_client().clone(),
        graph_client: config.graph.build_client(),
        datalake_client: config.datalake.build_client(),
        gitlab_client,
    };

    let engine = Arc::new(
        EngineBuilder::new(broker, registry, destination)
            .metrics(metrics)
            .build(),
    );

    let schema_check_shutdown = shutdown.clone();

    let engine_handle = engine.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown.cancelled().await;
        info!("received shutdown signal");
        engine_handle.stop();
    });
    let schema_check_graph = config.graph.build_client();
    let schema_check_datalake = config.datalake.build_client();
    let schema_check_interval = config.schema_version_check.interval();
    let schema_check_handle = tokio::spawn(schema_version::run_check_loop(
        schema_check_graph,
        schema_check_datalake,
        schema_check_interval,
        schema_check_shutdown,
    ));

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

    schema_check_handle.abort();
    shutdown_task.abort();

    info!("indexer stopped");
    result
}
