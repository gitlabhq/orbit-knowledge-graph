//! CLI for loading Parquet data into ClickHouse.

use anyhow::{Result, bail};
use clap::Parser;
use clickhouse::Client;
use ontology::Ontology;
use simulator::Config;
use simulator::clickhouse::ClickHouseWriter;
use simulator::parquet::ParquetReader;
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

    /// Skip loading data (useful for just adding indexes/projections)
    #[arg(long)]
    no_data: bool,

    /// Skip adding indexes
    #[arg(long)]
    no_indexes: bool,

    /// Skip adding projections
    #[arg(long)]
    no_projections: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("GitLab Knowledge Graph Loader");
    println!("=============================\n");

    println!("Loading config from {:?}...", args.config);
    let config = Config::load(&args.config)?;

    println!(
        "Loading ontology from {:?}...",
        config.generation.ontology_path
    );
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;

    // Check ClickHouse connectivity before proceeding
    println!("Checking ClickHouse connection at {}...", config.clickhouse.url);
    check_clickhouse_health(&config.clickhouse.url).await?;
    println!("ClickHouse is healthy");

    let writer = ClickHouseWriter::with_config(&config.clickhouse);

    // Create schemas
    if !args.no_schema {
        println!("\n=== Schema Setup ===");
        writer.create_schemas(&ontology, &config.clickhouse).await?;
    }

    // Load data
    if !args.no_data {
        println!("\n=== Loading Data ===");

        let reader = ParquetReader::new(&config.generation.output_dir);
        let orgs = reader.list_organizations()?;

        if orgs.is_empty() {
            println!("No data found in {:?}", config.generation.output_dir);
            println!("Run 'generate' first to create Parquet files.");
            return Ok(());
        }

        println!(
            "Found {} organization(s) in {:?}",
            orgs.len(),
            config.generation.output_dir
        );

        let overall_start = std::time::Instant::now();

        for org_id in &orgs {
            println!("\nOrganization {}:", org_id);

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
                    "{:.1}s ({:.0} rows/s)",
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
                    "{:.1}s ({:.0} rows/s)",
                    elapsed,
                    total_rows as f64 / elapsed.max(0.001)
                );
            }
        }

        println!(
            "\nData loaded in {:.1}s",
            overall_start.elapsed().as_secs_f64()
        );
    }

    // Add indexes (after data load for efficiency)
    if !args.no_indexes && !config.clickhouse.schema.indexes.is_empty() {
        println!("\n=== Indexes ===");
        writer.add_indexes(&ontology, &config.clickhouse).await?;
    }

    // Add projections (after data load for efficiency)
    if !args.no_projections && !config.clickhouse.schema.projections.is_empty() {
        println!("\n=== Projections ===");
        writer
            .add_projections(&ontology, &config.clickhouse)
            .await?;
    }

    // Print statistics
    println!();
    writer.print_statistics(&ontology).await?;

    Ok(())
}

/// Check that ClickHouse is running and healthy.
async fn check_clickhouse_health(url: &str) -> Result<()> {
    let client = Client::default().with_url(url);

    // Try a simple query to verify connectivity
    let result: Result<String, _> = client.query("SELECT version()").fetch_one().await;

    match result {
        Ok(version) => {
            println!("ClickHouse version: {}", version);
            Ok(())
        }
        Err(e) => {
            let error_msg = e.to_string();

            if error_msg.contains("Connect") || error_msg.contains("connection") {
                bail!(
                    "Cannot connect to ClickHouse at {}\n\n\
                     Make sure ClickHouse is running:\n\
                     - Docker: docker run -d -p 8123:8123 clickhouse/clickhouse-server\n\
                     - Local: clickhouse-server\n\n\
                     Error: {}",
                    url,
                    error_msg
                );
            } else {
                bail!("ClickHouse health check failed: {}", error_msg)
            }
        }
    }
}
