mod actions;
mod agent;
mod api_client;
mod config;
mod data_generator;
mod metrics;
mod orchestrator;
mod shared_state;

use anyhow::Result;
use clap::Parser;
use config::Config;
use orchestrator::Orchestrator;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,gitlab_load_testing=debug")),
        )
        .init();

    // Parse configuration
    let config = Config::parse();

    println!("============================================================");
    println!("GitLab Load Testing Framework (Rust)");
    println!("============================================================");
    println!("Target:    {}", config.base_url);
    println!("Agents:    {}", config.agent_count);
    println!("Duration:  {} minutes", config.duration_minutes);
    if config.dry_run {
        println!("Mode:      DRY-RUN (no API calls will be made)");
    }
    println!("============================================================");

    // Run orchestrator
    let mut orchestrator = Orchestrator::new(config)?;
    orchestrator.run().await?;

    Ok(())
}
