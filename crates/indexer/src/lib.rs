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
pub mod indexing_status;
pub mod llqm_v1;
pub mod locking;
pub mod metrics;
pub mod modules;
pub mod nats;
pub mod scheduler;
pub mod schema;
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
    ClickHouseConfiguration, EngineConfigError, EngineConfiguration, GitlabClientConfiguration,
    IndexerModule, NatsConfiguration, ScheduleConfig, SchemaConfig,
};
use handler::{HandlerInitError, HandlerRegistry};
use health::{HealthState, run_health_server};
use indexing_status::{INDEXING_PROGRESS_BUCKET, IndexingStatusStore};
use locking::INDEXING_LOCKS_BUCKET;
use modules::code::{NamespaceCodeBackfillDispatcher, SiphonCodeIndexingTaskDispatcher};
use modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionScheduler, NamespaceDeletionStore,
};
use modules::sdlc::dispatch::{GlobalDispatcher, NamespaceDispatcher};
use nats::{KvBucketConfig, NatsBroker};
use scheduler::{ScheduledTask, ScheduledTaskMetrics, TableCleanup};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    NatsConnection(#[from] nats::NatsError),

    #[error("ClickHouse connection failed: {0}")]
    ClickHouseConnection(#[from] destination::DestinationError),

    #[error("Engine error: {0}")]
    Engine(#[from] engine::EngineError),

    #[error("Handler initialization failed: {0}")]
    HandlerInit(#[from] HandlerInitError),

    #[error("Health server failed: {0}")]
    Health(#[from] std::io::Error),

    #[error("Schema version error: {0}")]
    SchemaVersion(#[from] schema::version::SchemaVersionError),

    #[error("Schema migration error: {0}")]
    SchemaMigration(#[from] schema::migration::MigrationError),

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
    Scheduler(#[from] scheduler::SchedulerError),

    #[error("health server failed: {0}")]
    Health(#[from] std::io::Error),
}

/// Runs the indexer until completion or until the token is cancelled.
pub async fn run(
    config: &IndexerConfig,
    ontology: Arc<ontology::Ontology>,
    shutdown: CancellationToken,
) -> Result<(), IndexerError> {
    config.schema.validate()?;
    config.engine.validate()?;

    info!(modules = ?config.engine.modules, "indexer module selection");

    let graph_client = config.graph.build_client();
    info!(url = %config.graph.url, "initializing schema version table");
    schema::version::init(&graph_client).await?;

    info!(url = %config.nats.url, "connecting to NATS");
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);

    let per_message_ttl = KvBucketConfig::with_per_message_ttl();
    broker
        .ensure_kv_bucket_exists(INDEXING_LOCKS_BUCKET, per_message_ttl)
        .await?;
    broker
        .ensure_kv_bucket_exists(INDEXING_PROGRESS_BUCKET, KvBucketConfig::default())
        .await?;

    broker
        .ensure_managed_streams(&topic::all_managed_subscriptions())
        .await?;

    // Run the migration orchestrator before the engine starts consuming messages.
    // This ensures no in-flight NATS messages exist during the drain phase.
    let migration_metrics = metrics::MigrationMetrics::new();
    let nats_services: Arc<dyn nats::NatsServices> =
        Arc::new(nats::NatsServicesImpl::new(broker.clone()));
    let lock_service: Arc<dyn locking::LockService> =
        Arc::new(locking::NatsLockService::new(nats_services.clone()));
    let indexing_status = Arc::new(IndexingStatusStore::new(Arc::new(
        nats_client::KvServicesImpl::new(broker.client().clone()),
    )));
    info!("running schema migration check");
    schema::migration::run_if_needed(&graph_client, &lock_service, &ontology, &migration_metrics)
        .await?;

    let metrics = Arc::new(metrics::EngineMetrics::new());

    info!(url = %config.graph.url, "connecting to graph ClickHouse");
    let destination = Arc::new(ClickHouseDestination::new(
        config.graph.clone(),
        metrics.clone(),
    )?);

    let registry = Arc::new(HandlerRegistry::default());

    if config.engine.is_module_enabled(IndexerModule::Sdlc) {
        info!("initializing SDLC handlers");
        modules::sdlc::register_handlers(&registry, config, &ontology).await?;
    } else {
        info!("SDLC handlers disabled by engine.modules");
    }

    if config.engine.is_module_enabled(IndexerModule::Code) {
        info!("initializing Code handlers");
        modules::code::register_handlers(&registry, config, &ontology)?;
    } else {
        info!("Code handlers disabled by engine.modules");
    }

    if config
        .engine
        .is_module_enabled(IndexerModule::NamespaceDeletion)
    {
        info!("initializing Namespace Deletion handler");
        modules::namespace_deletion::register_handlers(&registry, config, &ontology)?;
    } else {
        info!("Namespace Deletion handler disabled by engine.modules");
    }

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
        EngineBuilder::new(broker, registry, destination, indexing_status)
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

/// Runs the dispatcher (scheduled task loops + health server) until shutdown.
pub async fn run_dispatcher(
    config: &DispatcherConfig,
    ontology: &ontology::Ontology,
    shutdown: CancellationToken,
) -> Result<(), DispatcherError> {
    let services = scheduler::connect(&config.nats).await?;
    let graph = config.graph.build_client();
    let datalake = config.datalake.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let lock_service = services.lock_service.clone();

    let deletion_graph = Arc::new(config.graph.build_client());
    let deletion_datalake = Arc::new(config.datalake.build_client());
    let deletion_store: Arc<dyn NamespaceDeletionStore> =
        Arc::new(ClickHouseNamespaceDeletionStore::new(
            deletion_datalake,
            Arc::clone(&deletion_graph),
            ontology,
        ));
    let checkpoint_store = Arc::new(checkpoint::ClickHouseCheckpointStore::new(deletion_graph));

    let health_state = HealthState {
        nats_client: services.nats_client.clone(),
        graph_client: config.graph.build_client(),
        datalake_client: config.datalake.build_client(),
        gitlab_client: None,
    };

    let tasks: Vec<Box<dyn ScheduledTask>> = vec![
        Box::new(GlobalDispatcher::new(
            services.nats.clone(),
            metrics.clone(),
            config.schedule.tasks.global.clone(),
        )),
        Box::new(NamespaceDispatcher::new(
            services.nats.clone(),
            datalake,
            metrics.clone(),
            config.schedule.tasks.namespace.clone(),
        )),
        Box::new(SiphonCodeIndexingTaskDispatcher::new(
            services.nats.clone(),
            metrics.clone(),
            config.schedule.tasks.code_indexing_task.clone(),
        )),
        Box::new(NamespaceCodeBackfillDispatcher::new(
            services.nats.clone(),
            config.graph.build_client(),
            config.datalake.build_client(),
            metrics.clone(),
            config.schedule.tasks.namespace_code_backfill.clone(),
        )),
        Box::new(TableCleanup::new(
            graph,
            metrics.clone(),
            config.schedule.tasks.table_cleanup.clone(),
        )),
        Box::new(NamespaceDeletionScheduler::new(
            deletion_store,
            checkpoint_store,
            services.nats.clone(),
            metrics.clone(),
            config.schedule.tasks.namespace_deletion.clone(),
        )),
        Box::new(schema::completion::MigrationCompletionChecker::new(
            config.graph.build_client(),
            config.datalake.build_client(),
            lock_service.clone(),
            Arc::new(ontology.clone()),
            config.schema.clone(),
            config.schedule.tasks.migration_completion.clone(),
            metrics,
        )),
    ];

    tokio::select! {
        result = scheduler::run_loop(tasks, lock_service, shutdown.clone()) => {
            result.map_err(DispatcherError::from)
        }
        result = run_health_server(config.health_bind_address, health_state) => {
            // Health server died — cancel shutdown token so the scheduler drains gracefully
            shutdown.cancel();
            let error = result.err().unwrap_or_else(|| std::io::Error::other(
                "dispatcher health server exited unexpectedly",
            ));
            Err(DispatcherError::Health(error))
        }
    }
}
