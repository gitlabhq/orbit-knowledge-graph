use std::sync::Arc;

use clap::Parser;
use gkg_server::auth::JwtValidator;
use gkg_server::cli::{Args, Mode};
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::config::AppConfig;
use gkg_server::grpc::GrpcServer;
use gkg_server::health_check as health_check_mode;
use gkg_server::shutdown;
use gkg_server::webserver::Server as HttpServer;
use indexer::IndexerConfig;
use indexer::dispatcher::ScheduledTask;
use indexer::dispatcher::ScheduledTaskMetrics;
use indexer::modules::code::dispatch::ProjectCodeDispatcher;
use indexer::modules::sdlc::dispatch::{GlobalDispatcher, NamespaceDispatcher};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls CryptoProvider");

    labkit_rs::logging::init();

    let args = Args::parse();
    let config = AppConfig::load()?;
    ontology::constants::validate_ontology_constants();

    let _metrics = labkit_rs::metrics::try_init_with_config(config.metrics.clone()).ok();

    info!(mode = ?args.mode, "starting");

    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));

    let result = match args.mode {
        Mode::DispatchIndexing => {
            let services = indexer::dispatcher::connect(&config.nats).await?;
            let graph = config.graph.build_client();
            let datalake = config.datalake.build_client();
            let metrics = ScheduledTaskMetrics::new();
            let lock_service = services.lock_service.clone();
            let tasks: Vec<Box<dyn ScheduledTask>> = vec![
                Box::new(GlobalDispatcher::new(
                    services.nats.clone(),
                    metrics.clone(),
                    config.schedule.tasks.global.clone(),
                )),
                Box::new(NamespaceDispatcher::new(
                    services.nats.clone(),
                    datalake.clone(),
                    metrics.clone(),
                    config.schedule.tasks.namespace.clone(),
                )),
                Box::new(ProjectCodeDispatcher::new(
                    services.nats,
                    graph,
                    metrics,
                    config.schedule.tasks.project_code.clone(),
                )),
            ];
            indexer::dispatcher::run(&tasks, &*lock_service)
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
            indexer::run(&indexer_config, shutdown)
                .await
                .map_err(Into::into)
        }
        Mode::Webserver => run_webserver(&config).await,
    };

    signal_task.abort();

    result
}

async fn run_webserver(config: &AppConfig) -> anyhow::Result<()> {
    let validator = Arc::new(JwtValidator::new(
        config.jwt_secret()?,
        config.jwt_clock_skew_secs,
    )?);

    let cluster_health = ClusterHealthChecker::new(config.health_check_url.clone()).into_arc();

    let http_server = HttpServer::bind(
        config.bind_address,
        (*validator).clone(),
        Arc::clone(&cluster_health),
    )
    .await?;
    info!(addr = %config.bind_address, "HTTP server bound");

    let grpc_server = GrpcServer::new(
        config.grpc_bind_address,
        validator,
        &config.graph,
        Arc::clone(&cluster_health),
    );
    info!(addr = %config.grpc_bind_address, "gRPC server starting");

    tokio::select! {
        res = http_server.run() => res.map_err(Into::into),
        res = grpc_server.run() => res.map_err(Into::into),
    }
}
