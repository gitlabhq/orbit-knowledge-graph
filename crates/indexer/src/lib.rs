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
//! - A [`Destination`](engine::destination::Destination) (database, data lake, etc.)
//! - One or more [`Handler`](engine::handler::Handler)s registered in a [`HandlerRegistry`](engine::handler::HandlerRegistry)
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
pub mod analytics;
pub mod campaign;
pub mod checkpoint;
pub mod clickhouse;
pub mod config;
pub mod engine;
pub mod health;
pub mod indexing_status;
pub mod locking;
pub mod modules;
pub mod nats;
pub mod observer;
pub mod scheduler;
pub mod schema;
pub mod topic;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

// Re-export engine submodules at crate root for external API stability.
pub use config::*;
pub use engine::{dead_letter, destination, durability, handler, types, worker_pool};

/// Re-export metrics from their canonical locations for external API stability.
pub mod metrics {
    pub use crate::engine::metrics::*;
    pub use crate::schema::metrics::*;
}

use std::sync::Arc;
use std::time::Duration;

use clickhouse::ClickHouseConfigurationExt;
use clickhouse::ClickHouseDestination;
use engine::EngineBuilder;
use engine::handler::{HandlerInitError, HandlerRegistry};
use gitlab_client::GitlabClient;
use gkg_server_config::IndexerModule;
use health::{HealthState, run_health_server};
use indexing_status::{INDEXING_PROGRESS_BUCKET, IndexingStatusStore};
use locking::INDEXING_LOCKS_BUCKET;
use modules::code::{NamespaceCodeBackfillDispatcher, SiphonCodeIndexingTaskDispatcher};
use modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionScheduler, NamespaceDeletionStore,
};
use modules::sdlc::dispatch::{GlobalDispatcher, NamespaceDispatcher};
use nats::{KvBucketConfig, NatsBroker};
use scheduler::{ScheduledTask, ScheduledTaskMetrics, StaleEdgeReconciliation, TableCleanup};
use tokio_util::sync::CancellationToken;
use tracing::info;

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

    broker
        .ensure_kv_bucket_exists(INDEXING_LOCKS_BUCKET, KvBucketConfig::default())
        .await?;
    broker
        .ensure_kv_bucket_exists(INDEXING_PROGRESS_BUCKET, KvBucketConfig::default())
        .await?;

    broker
        .ensure_managed_streams(&topic::all_managed_subscriptions())
        .await?;

    let indexing_status = Arc::new(IndexingStatusStore::new(broker.clone()));

    let gitlab_client = config
        .gitlab
        .as_ref()
        .map(|cfg| GitlabClient::new(cfg.clone()))
        .transpose()
        .map_err(HandlerInitError::new)?
        .map(Arc::new);

    // Start the health server before waiting for schema readiness so that the
    // Kubernetes liveness probe is answered during the (potentially long) schema
    // wait phase. Readiness stays `503` until the gate clears (`serving`).
    let serving = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health_state = HealthState {
        nats_client: broker.nats_client().clone(),
        graph_client: config.graph.build_client(),
        datalake_client: config.datalake.build_client(),
        gitlab_client,
        serving: serving.clone(),
    };
    let health_shutdown = shutdown.clone();
    let health_bind_address = config.health_bind_address;
    let health_task = tokio::spawn(async move {
        tokio::select! {
            result = run_health_server(health_bind_address, health_state) => result,
            _ = health_shutdown.cancelled() => Ok(()),
        }
    });

    let schema_version = *schema::version::SCHEMA_VERSION;
    info!(
        schema_version,
        "waiting for dispatcher to prepare schema version before processing"
    );
    schema::version::wait_until_ready(
        &graph_client,
        schema_version,
        Duration::from_secs(config.schema.indexer_schema_wait_timeout_secs),
        Duration::from_secs(config.schema.version_poll_interval_secs),
    )
    .await?;

    let metrics = Arc::new(engine::metrics::EngineMetrics::new());

    info!(url = %config.graph.url, "connecting to graph ClickHouse");
    let destination = Arc::new(ClickHouseDestination::new(
        config.graph.clone(),
        metrics.clone(),
    )?);

    let registry = Arc::new(HandlerRegistry::default());

    let analytics = analytics::IndexingAnalytics::from_config(&config.analytics)?;
    if analytics.is_enabled() {
        info!(
            collector_url = %config.analytics.collector_url,
            "indexing analytics enabled"
        );
    }

    if config.engine.is_module_enabled(IndexerModule::Sdlc) {
        info!("initializing SDLC handlers");
        modules::sdlc::register_handlers(&registry, config, &ontology, analytics.clone()).await?;
    } else {
        info!("SDLC handlers disabled by engine.modules");
    }

    if config.engine.is_module_enabled(IndexerModule::Code) {
        info!("initializing Code handlers");
        modules::code::register_handlers(&registry, config, &ontology, analytics.clone()).await?;
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

    serving.store(true, std::sync::atomic::Ordering::Relaxed);
    info!("indexer started");
    let health_abort = health_task.abort_handle();
    let result = tokio::select! {
        result = engine.run(&config.engine) => {
            health_abort.abort();
            result.map_err(IndexerError::from)
        }
        result = health_task => {
            let error = result
                .unwrap_or_else(|e| Err(std::io::Error::other(e)))
                .err()
                .unwrap_or_else(|| std::io::Error::other(
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

    // Start the health server before migration so that the Kubernetes liveness
    // probe is answered during the (potentially long) DDL phase. Readiness stays
    // `503` until migration completes (`serving`).
    let serving = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let health_state = HealthState {
        nats_client: services.nats_client.clone(),
        graph_client: config.graph.build_client(),
        datalake_client: config.datalake.build_client(),
        gitlab_client: None,
        serving: serving.clone(),
    };
    let health_shutdown = shutdown.clone();
    let health_bind_address = config.health_bind_address;
    let health_task = tokio::spawn(async move {
        tokio::select! {
            result = run_health_server(health_bind_address, health_state) => result,
            _ = health_shutdown.cancelled() => Ok(()),
        }
    });

    let campaign = Arc::new(campaign::CampaignState::new());

    let migration_metrics = schema::metrics::MigrationMetrics::new();
    info!("running schema migration check");
    let dictionary_source = query_engine::compiler::DictionarySource {
        database: &config.graph.database,
        user: &config.graph.username,
        password: config.graph.password.as_deref(),
    };
    schema::migration::run_if_needed(
        &graph,
        &dictionary_source,
        &lock_service,
        ontology,
        &migration_metrics,
        &campaign,
    )
    .await?;
    serving.store(true, std::sync::atomic::Ordering::Relaxed);

    let deletion_graph = Arc::new(config.graph.build_client());
    let deletion_datalake = Arc::new(config.datalake.build_client());
    let deletion_store: Arc<dyn NamespaceDeletionStore> =
        Arc::new(ClickHouseNamespaceDeletionStore::new(
            deletion_datalake,
            Arc::clone(&deletion_graph),
            ontology,
        ));
    let checkpoint_store = Arc::new(checkpoint::ClickHouseCheckpointStore::new(deletion_graph));

    let tasks: Vec<Box<dyn ScheduledTask>> = vec![
        Box::new(GlobalDispatcher::new(
            services.nats.clone(),
            metrics.clone(),
            config.schedule.tasks.global.clone(),
            campaign.clone(),
        )),
        Box::new(NamespaceDispatcher::new(
            services.nats.clone(),
            datalake,
            metrics.clone(),
            config.schedule.tasks.namespace.clone(),
            campaign.clone(),
        )),
        Box::new(SiphonCodeIndexingTaskDispatcher::new(
            services.nats.clone(),
            metrics.clone(),
            config.schedule.tasks.code_indexing_task.clone(),
            campaign.clone(),
        )),
        Box::new(NamespaceCodeBackfillDispatcher::new(
            services.nats.clone(),
            config.graph.build_client(),
            config.datalake.build_client(),
            metrics.clone(),
            config.schedule.tasks.namespace_code_backfill.clone(),
            campaign.clone(),
        )),
        Box::new(TableCleanup::new(
            graph,
            ontology,
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
        Box::new(StaleEdgeReconciliation::new(
            config.graph.build_client(),
            ontology,
            Arc::new(checkpoint::ClickHouseCheckpointStore::new(Arc::new(
                config.graph.build_client(),
            ))),
            metrics.clone(),
            config.schedule.tasks.stale_edge_reconciliation.clone(),
        )),
        Box::new(schema::completion::MigrationCompletionChecker::new(
            config.graph.build_client(),
            config.datalake.build_client(),
            lock_service.clone(),
            Arc::new(ontology.clone()),
            config.schema.clone(),
            config.schedule.tasks.migration_completion.clone(),
            metrics,
            campaign.clone(),
            services.nats_client.clone(),
        )),
    ];

    let health_abort = health_task.abort_handle();
    tokio::select! {
        result = scheduler::run_loop(tasks, lock_service, shutdown.clone()) => {
            health_abort.abort();
            result.map_err(DispatcherError::from)
        }
        result = health_task => {
            shutdown.cancel();
            let error = result
                .unwrap_or_else(|e| Err(std::io::Error::other(e)))
                .err()
                .unwrap_or_else(|| std::io::Error::other(
                    "dispatcher health server exited unexpectedly",
                ));
            Err(DispatcherError::Health(error))
        }
    }
}
