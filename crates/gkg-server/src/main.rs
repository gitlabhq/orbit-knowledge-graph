use std::sync::Arc;

use clap::Parser;
use gkg_server::auth::JwtValidator;
use gkg_server::cli::{Args, Mode};
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::config::AppConfig;
use gkg_server::content;
use gkg_server::grpc::GrpcServer;
use gkg_server::health_check as health_check_mode;
use gkg_server::shutdown;
use gkg_server::webserver::Server as HttpServer;
use indexer::IndexerConfig;
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::modules::code::{NamespaceCodeBackfillDispatcher, SiphonCodeIndexingTaskDispatcher};
use indexer::modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionScheduler, NamespaceDeletionStore,
};
use indexer::modules::sdlc::dispatch::{GlobalDispatcher, NamespaceDispatcher};
use indexer::scheduler::{ScheduledTask, ScheduledTaskMetrics, TableCleanup};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls CryptoProvider");

    let args = Args::parse();
    let config = AppConfig::load()?;

    let mut builder = labkit::Builder::new(args.mode.service_name())
        .propagate_correlation(true)
        .echo_response_header(true);
    if let Some(level) = config
        .metrics
        .log_level
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        builder = builder.log_level(level);
    }
    if config.metrics.otel.enabled && !config.metrics.otel.endpoint.is_empty() {
        builder = builder.otel_grpc_endpoint(&config.metrics.otel.endpoint);
    }
    if config.metrics.prometheus.enabled {
        builder = builder.prometheus_metrics_port(config.metrics.prometheus.port);
    }
    let _guard = builder.init().expect("labkit init");

    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    ontology::constants::validate_ontology_constants(&ontology);

    info!(mode = ?args.mode, "starting");

    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));

    let result = match args.mode {
        Mode::DispatchIndexing => {
            let services = indexer::scheduler::connect(&config.nats).await?;
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
                    &ontology,
                ));
            let checkpoint_store = Arc::new(ClickHouseCheckpointStore::new(deletion_graph));

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
                    metrics,
                    config.schedule.tasks.namespace_deletion.clone(),
                )),
            ];
            indexer::scheduler::run(&tasks, &*lock_service)
                .await
                .map_err(Into::into)
        }
        Mode::HealthCheck => health_check_mode::run(&config).await.map_err(Into::into),
        Mode::Indexer => {
            let indexer_config = IndexerConfig {
                nats: config.nats.clone(),
                graph: config.graph.clone(),
                datalake: config.datalake.clone(),
                engine: config.engine.clone(),
                gitlab: config.gitlab_client_config(),
                schedule: config.schedule.clone(),
                health_bind_address: config.indexer_health_bind_address,
            };
            indexer::run(&indexer_config, ontology, shutdown)
                .await
                .map_err(Into::into)
        }
        Mode::Webserver => run_webserver(&config, ontology).await,
    };

    signal_task.abort();

    result
}

async fn run_webserver(
    config: &AppConfig,
    ontology: Arc<ontology::Ontology>,
) -> anyhow::Result<()> {
    let validator = Arc::new(JwtValidator::new(
        config.jwt_secret()?,
        config.jwt_clock_skew_secs,
    )?);

    let cluster_health = ClusterHealthChecker::new(config.health_check_url.clone()).into_arc();

    let resolver_registry = config
        .gitlab_client_config()
        .map(|cfg| {
            let client = gitlab_client::GitlabClient::new(cfg)
                .map(Arc::new)
                .map_err(|e| anyhow::anyhow!("failed to create GitlabClient: {e}"))?;
            let mut registry = content::ColumnResolverRegistry::new();
            registry.register(
                "gitaly",
                Arc::new(content::gitaly::GitalyContentService::new(client)),
            );
            Ok::<_, anyhow::Error>(Arc::new(registry))
        })
        .transpose()?;

    if resolver_registry.is_some() {
        info!("Content resolution enabled (GitlabClient configured)");
    } else {
        info!("Content resolution disabled (no GitLab client config)");
    }

    let graph_client = config.graph.build_client();
    let http_server = HttpServer::bind(config.bind_address, graph_client).await?;
    info!(addr = %config.bind_address, "HTTP server bound");

    let tls_config = config.tls.load_tls_config().await?;

    let grpc_server = GrpcServer::new(
        config.grpc_bind_address,
        validator,
        ontology,
        &config.graph,
        cluster_health,
        tls_config,
        resolver_registry,
    );
    info!(addr = %config.grpc_bind_address, "gRPC server starting");

    tokio::select! {
        res = http_server.run() => res.map_err(Into::into),
        res = grpc_server.run() => res.map_err(Into::into),
    }
}
