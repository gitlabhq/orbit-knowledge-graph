use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "datalake-generate", about = "Datalake generator harness")]
struct Cli {
    /// YAML config path
    #[arg(short, long, default_value = "datalake-generator.yaml")]
    config: String,

    /// Skip the initial seed and run from saved state
    #[arg(long)]
    skip_seeding: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = datalake_generator::config::SimulatorConfig::load(&cli.config)?;

    datalake_generator::run(&config, cli.skip_seeding).await
}
