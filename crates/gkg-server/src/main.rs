use clap::Parser;
use gkg_server::auth::JwtValidator;
use gkg_server::cli::{Args, Mode};
use gkg_server::config::AppConfig;
use gkg_server::indexer;
use gkg_server::shutdown;
use gkg_server::webserver::Server;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    labkit_rs::logging::init();

    let args = Args::parse();
    let config = AppConfig::from_env()?;

    info!(mode = ?args.mode, "starting");

    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));

    let result = match args.mode {
        Mode::Indexer => indexer::run(&config, shutdown).await.map_err(Into::into),
        Mode::Webserver => {
            let validator = JwtValidator::new(&config.jwt_secret, config.jwt_clock_skew_secs)?;
            let server = Server::bind(config.bind_address, args.mode, validator).await?;
            server.run().await.map_err(Into::into)
        }
    };

    signal_task.abort();

    result
}
