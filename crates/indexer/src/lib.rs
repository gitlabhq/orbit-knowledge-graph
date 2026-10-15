//! # Indexer
//!
//! Message processing framework and domain modules for GitLab Orbit.
//!
//! This crate contains both the engine (message routing, concurrency)
//! and the domain modules (SDLC, Code) that implement indexing logic.
//!
//! ## Engine
//!
//! You provide:
//! - A [`NatsBroker`](nats::NatsBroker) for message streaming
//! - A [`ClickHouseWriter`](clickhouse::ClickHouseWriter) for writing record batches
//! - One or more [`Handler`](engine::handler::Handler)s registered in a [`HandlerRegistry`](engine::handler::HandlerRegistry)
//!
//! ```text
//! NatsBroker ──▶ Engine ──▶ HandlerRegistry
//!                              └─ Handler (owns Destination)
//!                              └─ Handler (owns Destination)
//! ```
//!
//! ## Domain modules
//!
//! - [`modules::sdlc`] - SDLC entities (users, projects, MRs, CI, etc.)
//! - [`modules::code`] - Code indexing (call graphs, definitions, references)
//!
pub mod analytics;
pub use orbit_migrations::campaign;
pub mod checkpoint;
pub mod clickhouse;
pub mod config;
pub mod engine;
pub mod locking;
pub mod modules;
pub mod nats;
pub mod observer;
pub mod orchestrator;
pub mod schema;
pub mod topic;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

// Re-export engine submodules at crate root for external API stability.
pub use config::*;
pub use engine::{dead_letter, durability, handler, retry, types, worker_pool};

/// Re-export metrics from their canonical locations for external API stability.
pub mod metrics {
    pub use crate::engine::metrics::*;
    pub use crate::schema::metrics::*;
}

use std::sync::Arc;
use std::time::Duration;

use clickhouse::{ClickHouseClients, ClickHouseWriter};
use engine::EngineBuilder;
use engine::handler::HandlerRegistry;
use locking::INDEXING_LOCKS_BUCKET;
use modules::namespace_deletion::{ClickHouseNamespaceDeletionStore, NamespaceDeletionStore};
use nats::{KvBucketConfig, NatsBroker};
use orbit_migrations::catalog::OntologyCatalog;
use orbit_server_config::IndexerModule;
use orchestrator::Trigger;
use orchestrator::dispatch::{CodeBackfill, NamespaceIndexingDispatch};
use orchestrator::max_deliveries::MaxDeliveriesReconciler;
use orchestrator::scheduled::{
    CodeBackfillSweep, CodeStaleSweep, GlobalDispatcher, MigrationCompletionChecker,
    NamespaceDeletionScheduler, NamespaceDispatcher, Scheduled, StaleEdgeReconciliation,
    TableCleanup,
};
use orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use orchestrator::siphon::{CodeIndexingTaskRoute, EnabledNamespacesRoute, Route, Siphon};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub async fn run(
    config: &IndexerConfig,
    ontology: Arc<ontology::Ontology>,
    serving: Arc<std::sync::atomic::AtomicBool>,
    shutdown: CancellationToken,
    sql_transform: modules::sdlc::SqlTransformFactory,
) -> Result<(), IndexerError> {
    let resources = orbit_server_config::ContainerResources::detect();
    let mut config = config.clone();
    config.engine.resolve_runtime_defaults(&resources);
    let config = &config;

    config.schema.validate()?;
    config.engine.validate()?;
    config.graph.validate()?;

    info!(modules = ?config.engine.modules, "indexer module selection");

    let clients = ClickHouseClients::new(&config.graph, &config.datalake);
    info!(url = %config.graph.url, "initializing schema version table");
    schema::version::init(&clients.graph).await?;

    info!(url = %config.nats.url, "connecting to NATS");
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);

    broker
        .ensure_kv_bucket_exists(INDEXING_LOCKS_BUCKET, KvBucketConfig::default())
        .await?;

    broker
        .ensure_managed_streams(&topic::all_managed_subscriptions())
        .await?;

    let schema_version = *schema::version::SCHEMA_VERSION;
    info!(
        schema_version,
        "waiting for dispatcher to prepare schema version before processing"
    );
    schema::version::wait_until_ready(
        &clients.graph,
        schema_version,
        Duration::from_secs(config.schema.indexer_schema_wait_timeout_secs),
        Duration::from_secs(config.schema.version_poll_interval_secs),
    )
    .await?;

    let metrics = Arc::new(engine::metrics::EngineMetrics::new());

    info!(url = %config.graph.url, "connecting to graph ClickHouse");
    let writer = Arc::new(ClickHouseWriter::new(
        clients.graph.clone(),
        metrics.clone(),
    ));

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
        modules::sdlc::register_handlers(
            &registry,
            config,
            &clients,
            &ontology,
            writer.clone(),
            analytics.clone(),
            modules::sdlc::PARTITION_MIN_ROWS,
            sql_transform,
        )
        .await?;
    } else {
        info!("SDLC handlers disabled by engine.modules");
    }

    if config.engine.is_module_enabled(IndexerModule::Code) {
        info!("initializing Code handlers");
        modules::code::register_handlers(
            &registry,
            config,
            &clients,
            &ontology,
            writer.clone(),
            analytics.clone(),
            &resources,
        )
        .await?;
    } else {
        info!("Code handlers disabled by engine.modules");
    }

    if config
        .engine
        .is_module_enabled(IndexerModule::NamespaceDeletion)
    {
        info!("initializing Namespace Deletion handler");
        modules::namespace_deletion::register_handlers(&registry, config, &clients, &ontology)?;
    } else {
        info!("Namespace Deletion handler disabled by engine.modules");
    }

    info!(
        subscriptions = registry.subscriptions().len(),
        "registered handlers"
    );

    let engine = Arc::new(
        EngineBuilder::new(broker, registry)
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
    let result = engine.run(&config.engine).await.map_err(IndexerError::from);

    shutdown_task.abort();
    analytics.shutdown().await;

    info!("indexer stopped");
    result
}

pub async fn run_dispatcher(
    config: &DispatcherConfig,
    archive: &ontology::archive::OntologyArchive,
    serving: Arc<std::sync::atomic::AtomicBool>,
    shutdown: CancellationToken,
) -> Result<(), DispatcherError> {
    let candidate = archive
        .load_ontology()
        .map_err(orbit_migrations::catalog::CatalogError::from)?;
    let code_tables = modules::code::config::CodeTableNames::from_ontology(&candidate)?;
    let clients = ClickHouseClients::new(&config.graph, &config.datalake);

    let services = orchestrator::scheduled::connect(&config.nats).await?;

    let catalog = OntologyCatalog::open(services.nats_client.clone()).await?;
    let ontology = catalog.publish(archive).await?;

    let graph = clients.graph.clone();
    if let Some(active_version) = orbit_migrations::version::read_active_version(&graph).await?
        && active_version != archive.schema_version()
    {
        catalog.ensure_archive(active_version).await?;
    }

    if let Err(error) = nats::versioning::gc_idle_release_streams(
        &services.nats_connection,
        config.nats.release_gc_idle_threshold(),
    )
    .await
    {
        warn!(%error, "release GC failed, will retry next startup");
    }

    let datalake = clients.datalake.clone();
    let metrics = ScheduledTaskMetrics::new();
    let lock_service = services.lock_service.clone();

    let campaign = Arc::new(campaign::CampaignState::new());

    let migration_metrics = schema::metrics::MigrationMetrics::new();
    info!("running schema migration check");
    let dictionary_credentials = orbit_migrations::schema::DictionaryCredentials {
        database: config.graph.database.clone(),
        user: config.graph.username.clone(),
        password: config.graph.password.clone(),
    };
    schema::migration::run_if_needed(
        &graph,
        &dictionary_credentials,
        &lock_service,
        &ontology,
        &migration_metrics,
        &campaign,
    )
    .await?;
    if let Err(error) = catalog.sync_active_version(&graph).await {
        warn!(%error, "active version not mirrored at startup; retried every completion tick");
    }
    serving.store(true, std::sync::atomic::Ordering::Relaxed);

    match schema::version::read_active_version(&graph).await {
        Ok(Some(active_version)) if active_version == *schema::version::SCHEMA_VERSION => {
            {
                let graph_schema = orbit_migrations::schema::GraphSchema::from_ontology_replicated(
                    &ontology,
                    graph.is_replicated(),
                )?;
                if let Err(error) =
                    orbit_migrations::execute::create_unversioned_definitions(&graph, &graph_schema)
                        .await
                {
                    warn!(%error, "failed to create unversioned tables at startup");
                }
            }
            if let Err(error) = orbit_migrations::execute::replace_refreshable_views(
                &graph,
                &ontology,
                active_version,
            )
            .await
            {
                warn!(%error, "failed to replace refreshable views at startup");
            }
        }
        Ok(_) => {}
        Err(error) => {
            warn!(%error, "failed to read active version for auxiliary tables and refreshable views")
        }
    }

    let deletion_graph = Arc::new(clients.graph.clone());
    let deletion_datalake = Arc::new(clients.datalake.clone());
    let deletion_store: Arc<dyn NamespaceDeletionStore> =
        Arc::new(ClickHouseNamespaceDeletionStore::new(
            deletion_datalake,
            Arc::clone(&deletion_graph),
            &ontology,
        ));
    let checkpoint_store = Arc::new(checkpoint::ClickHouseCheckpointStore::new(deletion_graph));

    let backfill = Arc::new(CodeBackfill::new(
        services.nats.clone(),
        clients.graph.clone(),
        clients.datalake.clone(),
        metrics.clone(),
        campaign.clone(),
        config.schedule.tasks.code_backfill.publish_window,
    ));

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
            Arc::new(checkpoint::ClickHouseCheckpointStore::new(Arc::new(
                clients.graph.clone(),
            ))),
            metrics.clone(),
            config.schedule.tasks.namespace.clone(),
            campaign.clone(),
            &ontology,
        )),
        Box::new(CodeBackfillSweep::new(
            backfill.clone(),
            CodeStaleSweep::new(
                clients.graph.clone(),
                &code_tables,
                checkpoint_store.clone(),
                config.schedule.tasks.code_backfill.stale_sweeps_per_tick,
            ),
            config.schedule.tasks.code_backfill.clone(),
        )),
        Box::new(TableCleanup::new(
            graph,
            &ontology,
            metrics.clone(),
            config.schedule.tasks.table_cleanup.clone(),
        )?),
        Box::new(NamespaceDeletionScheduler::new(
            deletion_store,
            checkpoint_store,
            services.nats.clone(),
            metrics.clone(),
            config.schedule.tasks.namespace_deletion.clone(),
        )),
        Box::new(StaleEdgeReconciliation::new(
            clients.graph.clone(),
            &ontology,
            Arc::new(checkpoint::ClickHouseCheckpointStore::new(Arc::new(
                clients.graph.clone(),
            ))),
            metrics.clone(),
            config.schedule.tasks.stale_edge_reconciliation.clone(),
        )),
        Box::new(MigrationCompletionChecker::new(
            clients.graph.clone(),
            clients.datalake.clone(),
            lock_service.clone(),
            Arc::new(ontology),
            config.schema.clone(),
            config.schedule.tasks.migration_completion.clone(),
            metrics.clone(),
            campaign.clone(),
            services.nats_connection.clone(),
            catalog,
        )),
    ];

    let routes: Vec<Arc<dyn Route>> = vec![
        Arc::new(CodeIndexingTaskRoute::new(
            services.nats.clone(),
            metrics.clone(),
        )),
        Arc::new(EnabledNamespacesRoute::new(
            NamespaceIndexingDispatch::new(services.nats.clone()),
            backfill.clone(),
            clients.datalake.clone(),
        )),
    ];

    let scheduled = Scheduled::new(tasks, lock_service);
    let siphon = Siphon::new(
        services.nats.clone(),
        metrics,
        config.schedule.tasks.siphon.clone(),
        campaign.clone(),
        routes,
    );
    let max_deliveries_reconciler = MaxDeliveriesReconciler::new(services.nats_connection.clone());
    let triggers: Vec<Box<dyn Trigger>> = vec![
        Box::new(scheduled),
        Box::new(siphon),
        Box::new(max_deliveries_reconciler),
    ];

    orchestrator::launch(triggers, shutdown)
        .await
        .map_err(DispatcherError::from)
}
