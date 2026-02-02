use std::sync::Arc;

use clap::Parser;
use gkg_server::auth::JwtValidator;
use gkg_server::cli::{Args, Mode};
use gkg_server::config::AppConfig;
use gkg_server::dispatcher;
use gkg_server::grpc::GrpcServer;
use gkg_server::health_check as health_check_mode;
use gkg_server::indexer;
use gkg_server::shutdown;
use gkg_server::webserver::Server as HttpServer;
use mailbox::storage::PluginStore;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    labkit_rs::logging::init();
    let _metrics = labkit_rs::metrics::try_init().ok();

    let args = Args::parse();
    let config = AppConfig::from_env()?;

    info!(mode = ?args.mode, "starting");

    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));

    let result = match args.mode {
        Mode::DispatchIndexing => dispatcher::run(&config).await.map_err(Into::into),
        Mode::HealthCheck => health_check_mode::run(&config).await.map_err(Into::into),
        Mode::Indexer => indexer::run(&config, shutdown).await.map_err(Into::into),
        Mode::TrelloSync => trello_sync::run(&args.trello_config).await.map_err(Into::into),
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

    let clickhouse_client = Arc::new(config.graph.build_client());
    let plugin_store = Arc::new(PluginStore::new(clickhouse_client));

    let http_server = HttpServer::bind(
        config.bind_address,
        (*validator).clone(),
        config.health_check_url.clone(),
        &config.nats,
        &config.graph,
    )
    .await?;
    info!(addr = %config.bind_address, "HTTP server bound");

    let grpc_server = GrpcServer::new(
        config.grpc_bind_address,
        validator,
        &config.graph,
        config.health_check_url.clone(),
        plugin_store,
    )
    .await;
    info!(addr = %config.grpc_bind_address, "gRPC server starting");

    tokio::select! {
        res = http_server.run() => res.map_err(Into::into),
        res = grpc_server.run() => res.map_err(Into::into),
    }
}
