//! ClickHouse data writer with streaming batch inserts.

use super::schema::SchemaGenerator;
use crate::config::ClickHouseConfig;
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use ontology::{EDGE_TABLE, Ontology};
use std::path::Path;
use std::process::Command;

/// Writes data to ClickHouse with batched inserts.
pub struct ClickHouseWriter {
    pub client: ArrowClickHouseClient,
    url: String,
}

impl ClickHouseWriter {
    pub fn with_config(config: &ClickHouseConfig) -> Self {
        Self {
            client: config.build_client(),
            url: config.url.clone(),
        }
    }

    /// Check that clickhouse CLI is available in PATH.
    pub fn check_cli_available() -> Result<()> {
        let output = Command::new("clickhouse").arg("--version").output();

        match output {
            Ok(o) if o.status.success() => Ok(()),
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                anyhow::bail!("clickhouse CLI error: {}", stderr)
            }
            Err(_) => anyhow::bail!(
                "clickhouse CLI not found in PATH.\n\
                 Install via: mise install\n\
                 Or: brew install clickhouse"
            ),
        }
    }

    /// Load a Parquet file directly into a table using clickhouse client.
    /// Much faster and more reliable than HTTP streaming for large files.
    pub fn load_parquet_file(&self, table_name: &str, parquet_path: &Path) -> Result<()> {
        let path_str = parquet_path.to_str().context("Invalid path encoding")?;

        let output = Command::new("clickhouse")
            .arg("client")
            .arg("--query")
            .arg(format!("INSERT INTO {} FORMAT Parquet", table_name))
            .arg("--host")
            .arg(self.parse_host())
            .arg("--port")
            .arg(self.parse_port())
            .stdin(std::fs::File::open(parquet_path)?)
            .output()
            .context("Failed to run clickhouse client")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("clickhouse client failed for {}: {}", path_str, stderr);
        }

        Ok(())
    }

    fn parse_host(&self) -> String {
        // Extract host from URL like "http://localhost:8123"
        self.url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    }

    fn parse_port(&self) -> String {
        // Default to native port 9000 for clickhouse-client
        "9000".to_string()
    }

    pub async fn write_batches(&self, table_name: &str, batches: &[RecordBatch]) -> Result<()> {
        self.client.insert_arrow(table_name, batches).await?;
        Ok(())
    }

    pub async fn create_schemas(
        &self,
        ontology: &Ontology,
        config: &ClickHouseConfig,
    ) -> Result<()> {
        let generator = SchemaGenerator::new(ontology, &config.schema);

        println!("Dropping existing tables...");
        for drop_sql in generator.generate_drop_tables() {
            self.client.execute(&drop_sql).await?;
        }

        println!("Creating tables...");
        for (table_name, ddl) in generator.generate_create_tables() {
            println!("  Creating {}...", table_name);
            self.client.execute(&ddl).await?;
        }

        Ok(())
    }

    pub async fn add_indexes(&self, ontology: &Ontology, config: &ClickHouseConfig) -> Result<()> {
        let generator = SchemaGenerator::new(ontology, &config.schema);

        let add_statements = generator.generate_add_indexes();
        if add_statements.is_empty() {
            return Ok(());
        }

        println!("Adding indexes...");
        for sql in add_statements {
            println!(
                "  {}",
                sql.split_whitespace().take(8).collect::<Vec<_>>().join(" ")
            );
            self.client.execute(&sql).await?;
        }

        println!("Materializing indexes...");
        for sql in generator.generate_materialize_indexes() {
            self.client.execute(&sql).await?;
        }

        Ok(())
    }

    pub async fn add_projections(
        &self,
        ontology: &Ontology,
        config: &ClickHouseConfig,
    ) -> Result<()> {
        let generator = SchemaGenerator::new(ontology, &config.schema);

        let add_statements = generator.generate_add_projections();
        if add_statements.is_empty() {
            return Ok(());
        }

        println!("Adding projections...");
        for sql in add_statements {
            println!(
                "  {}",
                sql.split_whitespace().take(8).collect::<Vec<_>>().join(" ")
            );
            self.client.execute(&sql).await?;
        }

        println!("Materializing projections (this may take a while)...");
        for sql in generator.generate_materialize_projections() {
            let start = std::time::Instant::now();
            let table = sql.split_whitespace().nth(2).unwrap_or("?");
            print!("  {}... ", table);
            std::io::Write::flush(&mut std::io::stdout()).ok();

            self.client.execute(&sql).await?;

            println!("done ({:.1}s)", start.elapsed().as_secs_f64());
        }

        Ok(())
    }

    pub async fn print_statistics(&self, ontology: &Ontology) -> Result<()> {
        println!("\n=== Database Statistics ===");

        for node in ontology.nodes() {
            let tbl_name = ontology.table_name(&node.name)?;
            let count: u64 = self
                .client
                .inner()
                .query(&format!("SELECT count() FROM {}", tbl_name))
                .fetch_one()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Warning: Failed to query table {}: {}", tbl_name, e);
                    0
                });
            println!("{:30} {:>12} rows", tbl_name, count);
        }

        let edge_count: u64 = self
            .client
            .inner()
            .query(&format!("SELECT count() FROM {}", EDGE_TABLE))
            .fetch_one()
            .await
            .unwrap_or_else(|e| {
                eprintln!("Warning: Failed to query table {}: {}", EDGE_TABLE, e);
                0
            });
        println!("{:30} {:>12} rows", EDGE_TABLE, edge_count);

        println!("\n=== Edge Types ===");
        let edge_types: Vec<(String, u64)> = self
            .client
            .inner()
            .query(&format!(
                "SELECT relationship_kind, count() FROM {} GROUP BY relationship_kind ORDER BY count() DESC LIMIT 10",
                EDGE_TABLE
            ))
            .fetch_all()
            .await
            .unwrap_or_else(|e| {
                eprintln!("Warning: Failed to query edge types: {}", e);
                Vec::new()
            });

        for (rel_type, count) in edge_types {
            println!("  {:28} {:>12} edges", rel_type, count);
        }

        Ok(())
    }
}
