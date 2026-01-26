use std::env;

use clap::{Parser, ValueEnum};
use gkg_server::{ServerConfig, ToolRegistry, indexer, webserver};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Knowledge Graph server")]
struct Args {
    #[arg(short, long, value_enum, default_value_t = Mode::Webserver)]
    mode: Mode,
}

#[derive(Debug, Clone, ValueEnum)]
enum Mode {
    Indexer,
    Webserver,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    let config = ServerConfig {
        bind_address: env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:8080".into()),
        jwt_secret: env::var("JWT_SECRET").expect("JWT_SECRET required"),
        jwt_issuer: env::var("JWT_ISSUER").unwrap_or_else(|_| "gitlab".into()),
        jwt_audience: env::var("JWT_AUDIENCE").unwrap_or_else(|_| "gitlab-knowledge-graph".into()),
        jwt_clock_skew_secs: 60,
    };

    let registry = ToolRegistry::new();

    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install signal handler");
        tracing::info!("Shutdown signal received");
    };

    match args.mode {
        Mode::Webserver => {
            tracing::info!("Starting in webserver mode");
            let server = webserver::WebServer::new(&config, registry).await?;
            server.run_until_stopped(shutdown).await?;
        }
        Mode::Indexer => {
            tracing::info!("Starting in indexer mode");
            let server = indexer::IndexerServer::new(&config, registry).await?;
            server.run_until_stopped(shutdown).await?;
        }
    }

    Ok(())
}
