//! CLI for loading Parquet data into ClickHouse.

use anyhow::Result;
use clap::Parser;
use ontology::Ontology;
use simulator::clickhouse::ClickHouseWriter;
use simulator::parquet::ParquetReader;
use simulator::Config;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "load")]
#[command(about = "Load generated Parquet data into ClickHouse")]
struct Args {
    /// Path to YAML configuration file
    #[arg(short, long, default_value = "simulator.yaml")]
    config: PathBuf,

    /// Skip creating/dropping tables (useful for reloading)
    #[arg(long)]
    no_schema: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("GitLab Knowledge Graph Loader");
    println!("=============================\n");

    println!("Loading config from {:?}...", args.config);
    let config = Config::load(&args.config)?;

    let reader = ParquetReader::new(&config.generation.output_dir);
    let orgs = reader.list_organizations()?;

    if orgs.is_empty() {
        println!("No data found in {:?}", config.generation.output_dir);
        println!("Run 'generate' first to create Parquet files.");
        return Ok(());
    }

    println!("Found {} organization(s) in {:?}", orgs.len(), config.generation.output_dir);

    println!("Loading ontology from {:?}...", config.generation.ontology_path);
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;

    let writer = ClickHouseWriter::new(&config.clickhouse.url);

    if !args.no_schema {
        println!("\nCreating ClickHouse schemas...");
        writer.create_schemas(&ontology).await?;
    }

    println!("\nLoading data into ClickHouse...");
    let overall_start = std::time::Instant::now();

    for org_id in &orgs {
        println!("\n=== Organization {} ===", org_id);

        // Load node tables
        for node in ontology.nodes() {
            let batches = reader.read_batches(*org_id, &node.name)?;
            if batches.is_empty() {
                continue;
            }

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let tbl_name = ontology.table_name(&node.name)?;

            print!("  {} ({} rows)... ", node.name, total_rows);
            std::io::Write::flush(&mut std::io::stdout()).ok();

            let start = std::time::Instant::now();
            writer.write_batches(&tbl_name, &batches).await?;
            let elapsed = start.elapsed().as_secs_f64();

            println!(
                "✓ {:.1}s ({:.0} rows/s)",
                elapsed,
                total_rows as f64 / elapsed.max(0.001)
            );
        }

        // Load edges
        let edge_batches = reader.read_edges(*org_id)?;
        if !edge_batches.is_empty() {
            let total_rows: usize = edge_batches.iter().map(|b| b.num_rows()).sum();

            print!("  edges ({} rows)... ", total_rows);
            std::io::Write::flush(&mut std::io::stdout()).ok();

            let start = std::time::Instant::now();
            writer.write_batches("kg_edges", &edge_batches).await?;
            let elapsed = start.elapsed().as_secs_f64();

            println!(
                "✓ {:.1}s ({:.0} rows/s)",
                elapsed,
                total_rows as f64 / elapsed.max(0.001)
            );
        }
    }

    println!(
        "\nDone! Total time: {:.1}s",
        overall_start.elapsed().as_secs_f64()
    );

    writer.print_statistics(&ontology).await?;

    Ok(())
}
