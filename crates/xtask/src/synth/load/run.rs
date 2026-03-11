//! Load generated Parquet data into ClickHouse.

use super::ParquetReader;
use crate::synth::clickhouse::{ClickHouseWriter, check_clickhouse_health};
use crate::synth::config::Config;
use anyhow::Result;
use ontology::Ontology;
use ontology::constants::EDGE_TABLE;
use std::path::Path;

pub async fn run(
    config_path: &Path,
    no_schema: bool,
    no_data: bool,
    no_indexes: bool,
    no_projections: bool,
    use_cli: bool,
) -> Result<()> {
    println!("GitLab Knowledge Graph Loader");
    println!("=============================\n");

    println!("Loading config from {:?}...", config_path);
    let config = Config::load(config_path)?;

    println!(
        "Loading ontology from {:?}...",
        config.generation.ontology_path
    );
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;

    // Check ClickHouse connectivity before proceeding
    println!(
        "Checking ClickHouse connection at {}...",
        config.clickhouse.url
    );
    let writer = ClickHouseWriter::with_config(&config.clickhouse);
    check_clickhouse_health(&writer.client).await?;
    println!("ClickHouse is healthy");

    // Create schemas
    if !no_schema {
        println!("\n=== Schema Setup ===");
        writer.create_schemas(&ontology, &config.clickhouse).await?;
    }

    // Load data
    if !no_data {
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

        if use_cli {
            ClickHouseWriter::check_cli_available()?;
            println!("Using clickhouse client for loading (faster)");
        }

        for org_id in &orgs {
            println!("\nOrganization {}:", org_id);

            // Load node tables
            for node in ontology.nodes() {
                let file_path = reader.file_path(*org_id, &node.name);
                if !file_path.exists() {
                    continue;
                }

                let tbl_name = ontology.table_name(&node.name)?;
                print!("  {}... ", node.name);
                std::io::Write::flush(&mut std::io::stdout()).ok();

                let start = std::time::Instant::now();

                if use_cli {
                    writer.load_parquet_file(tbl_name, &file_path)?;
                } else {
                    let batches = reader.read_batches(*org_id, &node.name)?;
                    writer.write_batches(tbl_name, &batches).await?;
                }

                println!("{:.1}s", start.elapsed().as_secs_f64());
            }

            // Load edges
            let edges_path = reader.file_path(*org_id, "edges");
            if edges_path.exists() {
                print!("  edges... ");
                std::io::Write::flush(&mut std::io::stdout()).ok();

                let start = std::time::Instant::now();

                if use_cli {
                    writer.load_parquet_file(EDGE_TABLE, &edges_path)?;
                } else {
                    let edge_batches = reader.read_edges(*org_id)?;
                    writer.write_batches(EDGE_TABLE, &edge_batches).await?;
                }

                println!("{:.1}s", start.elapsed().as_secs_f64());
            }
        }

        println!(
            "\nData loaded in {:.1}s",
            overall_start.elapsed().as_secs_f64()
        );
    }

    // Add indexes (after data load for efficiency)
    if !no_indexes && !config.clickhouse.schema.indexes.is_empty() {
        println!("\n=== Indexes ===");
        writer.add_indexes(&ontology, &config.clickhouse).await?;
    }

    // Add projections (after data load for efficiency)
    if !no_projections && !config.clickhouse.schema.projections.is_empty() {
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
