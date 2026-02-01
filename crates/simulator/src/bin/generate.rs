//! CLI for generating fake data to Parquet files.

use anyhow::Result;
use clap::Parser;
use ontology::Ontology;
use simulator::parquet::ParquetWriter;
use simulator::{Config, Generator};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "generate")]
#[command(about = "Generate fake GitLab Knowledge Graph data to Parquet files")]
struct Args {
    /// Path to YAML configuration file
    #[arg(short, long, default_value = "simulator.yaml")]
    config: PathBuf,

    /// Just print the generation plan without executing
    #[arg(long)]
    dry_run: bool,

    /// Force regeneration even if data exists
    #[arg(long)]
    force: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("GitLab Knowledge Graph Generator");
    println!("=================================\n");

    println!("Loading config from {:?}...", args.config);
    let config = Config::load(&args.config)?;

    let writer = ParquetWriter::new(&config.generation.output_dir);

    if !args.force && config.generation.skip_if_present && writer.data_exists() {
        println!(
            "Data already exists in {:?}, skipping generation.",
            config.generation.output_dir
        );
        println!("Use --force to regenerate.");
        return Ok(());
    }

    println!(
        "Loading ontology from {:?}...",
        config.generation.ontology_path
    );
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;
    println!(
        "Loaded {} node types and {} edge types\n",
        ontology.node_count(),
        ontology.edge_count()
    );

    let generator = Generator::new(ontology.clone(), config.clone())?;
    generator.print_plan();

    if args.dry_run {
        println!("Dry run - not executing.");
        return Ok(());
    }

    println!("Output directory: {:?}\n", config.generation.output_dir);

    if std::path::Path::new(&config.generation.output_dir).exists() {
        println!("Removing existing data directory...");
        std::fs::remove_dir_all(&config.generation.output_dir)?;
    }

    std::fs::create_dir_all(&config.generation.output_dir)?;

    let overall_start = std::time::Instant::now();

    for org_id in 1..=config.generation.organizations {
        println!(
            "=== Organization {}/{} ===",
            org_id, config.generation.organizations
        );

        // Create streaming edge writer to avoid accumulating all edges in memory
        let mut edge_writer = writer.create_edge_writer(org_id)?;

        let gen_start = std::time::Instant::now();
        let org_nodes = generator.generate_organization_streaming(org_id, &mut edge_writer)?;
        let gen_elapsed = gen_start.elapsed().as_secs_f64();

        let node_count: usize = org_nodes
            .nodes
            .values()
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
            .sum();

        let edge_count = edge_writer.count();

        println!(
            "  Generated {} nodes + {} edges ({:.1}s)",
            node_count, edge_count, gen_elapsed
        );

        let write_start = std::time::Instant::now();

        // Write nodes to Parquet
        writer.write_organization_nodes(org_id, &org_nodes)?;

        // Close edge writer (flushes remaining edges)
        edge_writer.close()?;

        let write_elapsed = write_start.elapsed().as_secs_f64();

        println!("  Written to Parquet ({:.1}s)\n", write_elapsed);
    }

    writer.write_manifest(&ontology, config.generation.organizations)?;

    println!(
        "Done! Total time: {:.1}s",
        overall_start.elapsed().as_secs_f64()
    );
    println!("Data written to: {:?}", config.generation.output_dir);

    Ok(())
}
