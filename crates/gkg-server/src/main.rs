use clap::Parser;
use gkg_server::auth::JwtValidator;
use gkg_server::cli::Args;
use gkg_server::config::AppConfig;
use gkg_server::webserver::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let config = AppConfig::from_env()?;

    info!(mode = ?args.mode, "starting server");

    let validator = JwtValidator::new(&config.jwt_secret, config.jwt_clock_skew_secs)?;
    let server = Server::bind(config.bind_address, args.mode, validator).await?;
    server.run().await?;

    Ok(())
}
