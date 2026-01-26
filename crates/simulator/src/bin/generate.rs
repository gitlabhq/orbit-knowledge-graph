//! CLI for generating fake data and importing to ClickHouse.
//!
//! All data generation is driven by the ontology - no hardcoded entity types.

use anyhow::Result;
use clap::Parser;
use ontology::Ontology;
use simulator::{Config, Generator};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "simulate")]
#[command(
    about = "Generate fake GitLab Knowledge Graph data from ontology and import to ClickHouse"
)]
struct Args {
    /// Path to ontology fixtures directory
    #[arg(long, default_value = "fixtures/ontology")]
    ontology_path: PathBuf,

    /// ClickHouse URL
    #[arg(long, default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// Number of tenants to generate
    #[arg(long, default_value = "2")]
    tenants: u32,

    /// Default number of nodes per type
    #[arg(long, default_value = "100")]
    nodes_per_type: usize,

    /// Override count for specific node types (can be repeated)
    /// Format: NodeType=count (e.g., --node-count User=500 --node-count Project=200)
    #[arg(long = "node-count", value_parser = parse_node_count)]
    node_counts: Vec<(String, usize)>,

    /// Number of edges to generate per source node
    #[arg(long, default_value = "3")]
    edges_per_source: usize,

    /// Batch size for ClickHouse inserts
    #[arg(long, default_value = "10000")]
    batch_size: usize,

    /// Just print the generation plan without executing
    #[arg(long)]
    dry_run: bool,
}

/// Parse "NodeType=count" format
fn parse_node_count(s: &str) -> Result<(String, usize), String> {
    let parts: Vec<&str> = s.split('=').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid format '{}', expected NodeType=count", s));
    }
    let count = parts[1]
        .parse::<usize>()
        .map_err(|_| format!("Invalid count '{}' in '{}'", parts[1], s))?;
    Ok((parts[0].to_string(), count))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("GitLab Knowledge Graph Simulator");
    println!("================================");
    println!("(Fully ontology-driven - no hardcoded entities)");
    println!();

    // Load ontology
    println!("Loading ontology from {:?}...", args.ontology_path);
    let ontology = Ontology::load_from_dir(&args.ontology_path)?;
    println!(
        "Loaded {} node types and {} edge types",
        ontology.node_count(),
        ontology.edge_count()
    );
    println!();

    // Build config
    let node_counts: HashMap<String, usize> = args.node_counts.into_iter().collect();
    let config = Config {
        clickhouse_url: args.clickhouse_url,
        num_tenants: args.tenants,
        default_nodes_per_type: args.nodes_per_type,
        node_counts,
        edges_per_source: args.edges_per_source,
        batch_size: args.batch_size,
    };

    // Create generator and print plan
    let generator = Generator::new(ontology, config);
    generator.print_plan();

    if args.dry_run {
        println!("Dry run - not executing.");
        return Ok(());
    }

    // Run generator
    generator.run().await?;

    println!("\nDone!");
    Ok(())
}
