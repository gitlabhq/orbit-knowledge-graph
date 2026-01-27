//! CLI for generating fake data and importing to ClickHouse.
//!
//! All data generation is driven by the ontology - no hardcoded entity types.

use anyhow::{Result, bail};
use clap::Parser;
use ontology::Ontology;
use simulator::{Config, Generator};
use std::collections::{HashMap, HashSet};
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

    /// Number of organizations to generate
    #[arg(long, default_value = "2")]
    organizations: u32,

    /// Number of traversal IDs per organization
    #[arg(long, default_value = "1000")]
    traversal_ids: usize,

    /// Maximum depth of traversal ID hierarchy
    #[arg(long, default_value = "5")]
    max_traversal_depth: usize,

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

    /// Generate organizations in parallel (faster but uses more CPU)
    #[arg(long)]
    parallel: bool,
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

/// Validate that all node types in node_counts exist in the ontology.
fn validate_node_counts(ontology: &Ontology, node_counts: &HashMap<String, usize>) -> Result<()> {
    let valid_types: HashSet<&str> = ontology.nodes().map(|n| n.name.as_str()).collect();

    for node_type in node_counts.keys() {
        if !valid_types.contains(node_type.as_str()) {
            let suggestions: Vec<&str> = valid_types
                .iter()
                .filter(|t| t.to_lowercase().contains(&node_type.to_lowercase()))
                .copied()
                .take(3)
                .collect();

            let hint = if suggestions.is_empty() {
                format!(
                    "Available types: {}",
                    valid_types
                        .iter()
                        .take(10)
                        .copied()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!("Did you mean: {}?", suggestions.join(", "))
            };

            bail!(
                "Unknown node type '{}' in --node-count. {}",
                node_type,
                hint
            );
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("GitLab Knowledge Graph Simulator");
    println!("================================");
    println!("(Fully ontology-driven - no hardcoded entities)");
    println!();

    println!("Loading ontology from {:?}...", args.ontology_path);
    let ontology = Ontology::load_from_dir(&args.ontology_path)?;
    println!(
        "Loaded {} node types and {} edge types",
        ontology.node_count(),
        ontology.edge_count()
    );
    println!();

    let node_counts: HashMap<String, usize> = args.node_counts.into_iter().collect();
    validate_node_counts(&ontology, &node_counts)?;

    let config = Config {
        clickhouse_url: args.clickhouse_url,
        num_organizations: args.organizations,
        traversal_ids_per_org: args.traversal_ids,
        max_traversal_depth: args.max_traversal_depth,
        default_nodes_per_type: args.nodes_per_type,
        node_counts,
        edges_per_source: args.edges_per_source,
        batch_size: args.batch_size,
    };

    let generator = Generator::new(ontology, config);
    generator.print_plan();

    if args.dry_run {
        println!("Dry run - not executing.");
        return Ok(());
    }

    if args.parallel {
        println!("Running in parallel mode...\n");
        generator.run_parallel().await?;
    } else {
        println!("Running in sequential mode...\n");
        generator.run().await?;
    }

    println!("\nDone!");
    Ok(())
}
