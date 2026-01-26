use std::env;

use gkg_webserver::{ServerBuilder, ToolRegistry, WebserverConfig};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET environment variable must be set");

    let bind_address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    let jwt_issuer = env::var("JWT_ISSUER").unwrap_or_else(|_| "gitlab".to_string());

    let jwt_audience =
        env::var("JWT_AUDIENCE").unwrap_or_else(|_| "gitlab-knowledge-graph".to_string());

    let config = WebserverConfig {
        bind_address,
        jwt_secret,
        jwt_issuer,
        jwt_audience,
        jwt_clock_skew_secs: 60,
    };

    let registry = ToolRegistry::new();

    let server = ServerBuilder::new(config)
        .with_registry(registry)
        .build()
        .await?;

    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        tracing::info!("Received shutdown signal");
    };

    server.run_until_stopped(shutdown_signal).await?;

    Ok(())
}
