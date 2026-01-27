//! CLI for generating fake data and importing to ClickHouse.

use anyhow::Result;
use clap::Parser;
use ontology::Ontology;
use simulator::{Config, Generator};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "simulate")]
#[command(about = "Generate fake GitLab Knowledge Graph data and import to ClickHouse")]
struct Args {
    /// Path to YAML configuration file
    #[arg(short, long, default_value = "simulator.yaml")]
    config: PathBuf,

    /// Just print the generation plan without executing
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("GitLab Knowledge Graph Simulator");
    println!("================================\n");

    println!("Loading config from {:?}...", args.config);
    let config = Config::load(&args.config)?;

    println!("Loading ontology from {:?}...", config.generation.ontology_path);
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;
    println!(
        "Loaded {} node types and {} edge types\n",
        ontology.node_count(),
        ontology.edge_count()
    );

    let generator = Generator::new(ontology, config.clone());
    generator.print_plan();

    if args.dry_run {
        println!("Dry run - not executing.");
        return Ok(());
    }

    if config.generation.parallel {
        println!("Running in parallel mode...\n");
        generator.run_parallel().await?;
    } else {
        println!("Running in sequential mode...\n");
        generator.run().await?;
    }

    println!("\nDone!");
    Ok(())
}
