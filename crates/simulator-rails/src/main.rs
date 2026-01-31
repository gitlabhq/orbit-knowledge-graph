mod actions;
mod agent;
mod api_client;
mod config;
mod data_generator;
mod metrics;
mod orchestrator;
mod shared_state;

use anyhow::Result;
use config::Config;
use orchestrator::Orchestrator;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                if config.verbose {
                    EnvFilter::new("debug,simulator_rails=trace")
                } else {
                    EnvFilter::new("info,simulator_rails=debug")
                }
            }),
        )
        .init();

    println!("============================================================");
    println!("GitLab Load Testing Framework (Rust)");
    println!("============================================================");
    println!("Target:      {}", config.base_url);
    println!("Agents:      {}", config.agent_count);
    println!("Duration:    {} minutes", config.duration_minutes);
    println!("Namespaces:  {}", config.namespace_count);
    if config.dry_run {
        println!("Mode:        DRY-RUN (no API calls will be made)");
    }
    println!("============================================================");

    let mut orchestrator = Orchestrator::new(config)?;
    orchestrator.run().await?;

    Ok(())
}
