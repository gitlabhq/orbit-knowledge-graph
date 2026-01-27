//! ClickHouse data writer with streaming batch inserts.

use super::schema::SchemaGenerator;
use crate::generator::{EdgeRecord, OrganizationData};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use clickhouse::{Client, Row};
use ontology::{EDGE_TABLE, Ontology};
use serde::Serialize;

/// ClickHouse row for edges (matches EdgeEntity).
#[derive(Debug, Clone, Serialize, Row)]
pub struct EdgeRow {
    pub relationship_kind: String,
    pub source: i64,
    pub source_kind: String,
    pub target: i64,
    pub target_kind: String,
}

impl From<&EdgeRecord> for EdgeRow {
    fn from(record: &EdgeRecord) -> Self {
        Self {
            relationship_kind: record.relationship_kind.clone(),
            source: record.source,
            source_kind: record.source_kind.clone(),
            target: record.target,
            target_kind: record.target_kind.clone(),
        }
    }
}

/// Writes data to ClickHouse with batched inserts.
pub struct ClickHouseWriter {
    client: Client,
}

impl ClickHouseWriter {
    /// Create a new writer connected to ClickHouse.
    pub fn new(url: &str) -> Self {
        let client = Client::default().with_url(url);
        Self { client }
    }

    /// Create all schemas from ontology.
    pub async fn create_schemas(&self, ontology: &Ontology) -> Result<()> {
        let generator = SchemaGenerator::new(ontology);

        // Drop existing tables
        println!("Dropping existing tables...");
        for drop_sql in generator.generate_drop_all() {
            self.client.query(&drop_sql).execute().await?;
        }

        // Create tables
        println!("Creating tables...");
        for (table_name, ddl) in generator.generate_all_ddl() {
            println!("  Creating {}...", table_name);
            self.client.query(&ddl).execute().await?;
        }

        Ok(())
    }

    /// Write all data for an organization.
    pub async fn write_organization_data(
        &self,
        ontology: &Ontology,
        data: &OrganizationData,
    ) -> Result<()> {
        // Write node batches with progress
        for (node_name, batches) in &data.nodes {
            if !batches.is_empty() {
                let tbl_name = ontology.table_name(node_name)?;
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                
                print!("    {} ({} rows)... ", node_name, total_rows);
                std::io::Write::flush(&mut std::io::stdout()).ok();
                
                let start = std::time::Instant::now();
                self.write_batches(&tbl_name, batches).await?;
                let elapsed = start.elapsed().as_secs_f64();
                
                println!("✓ {:.1}s ({:.0} rows/s)", elapsed, total_rows as f64 / elapsed.max(0.001));
            }
        }

        // Write edges
        if !data.edges.is_empty() {
            print!("    edges ({} rows)... ", data.edges.len());
            std::io::Write::flush(&mut std::io::stdout()).ok();
            
            let start = std::time::Instant::now();
            self.write_edges(&data.edges).await?;
            let elapsed = start.elapsed().as_secs_f64();
            
            println!("✓ {:.1}s ({:.0} edges/s)", elapsed, data.edges.len() as f64 / elapsed.max(0.001));
        }

        Ok(())
    }

    /// Write Arrow RecordBatches to a table.
    pub async fn write_batches(&self, table_name: &str, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.write_batch_as_rows(table_name, batch).await?;
        }
        Ok(())
    }

    /// Write a RecordBatch by converting to row-based inserts.
    ///
    /// This is less efficient than native Arrow/Parquet but works with
    /// the clickhouse-rs driver which doesn't support direct Arrow inserts.
    async fn write_batch_as_rows(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        if num_rows == 0 {
            return Ok(());
        }

        // Build INSERT statement with column names from schema
        let schema = batch.schema();
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let columns_str = column_names.join(", ");

        // Stream chunks directly instead of building all values first
        let chunk_size = 5000; // Larger chunks for better throughput
        let mut chunk_values: Vec<String> = Vec::with_capacity(chunk_size);

        for row_idx in 0..num_rows {
            let mut row_values: Vec<String> = Vec::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                let value = column_value_to_sql(col, row_idx);
                row_values.push(value);
            }

            chunk_values.push(format!("({})", row_values.join(", ")));

            // Flush chunk when full
            if chunk_values.len() >= chunk_size {
                let insert_sql = format!(
                    "INSERT INTO {} ({}) SETTINGS max_memory_usage=8000000000 VALUES {}",
                    table_name,
                    columns_str,
                    chunk_values.join(", ")
                );
                self.client.query(&insert_sql).execute().await?;
                chunk_values.clear();
            }
        }

        // Flush remaining rows
        if !chunk_values.is_empty() {
            let insert_sql = format!(
                "INSERT INTO {} ({}) SETTINGS max_memory_usage=8000000000 VALUES {}",
                table_name,
                columns_str,
                chunk_values.join(", ")
            );
            self.client.query(&insert_sql).execute().await?;
        }

        Ok(())
    }

    /// Write edges using the typed Row interface.
    pub async fn write_edges(&self, edges: &[EdgeRecord]) -> Result<()> {
        if edges.is_empty() {
            return Ok(());
        }

        let mut inserter = self.client.insert::<EdgeRow>(EDGE_TABLE).await?;

        for edge in edges {
            inserter.write(&EdgeRow::from(edge)).await?;
        }

        inserter.end().await?;
        Ok(())
    }

    /// Print statistics about the imported data.
    pub async fn print_statistics(&self, ontology: &Ontology) -> Result<()> {
        println!("\n=== Database Statistics ===");

        // Node counts
        for node in ontology.nodes() {
            let tbl_name = ontology.table_name(&node.name)?;
            let count: u64 = self
                .client
                .query(&format!("SELECT count() FROM {}", tbl_name))
                .fetch_one()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Warning: Failed to query table {}: {}", tbl_name, e);
                    0
                });
            println!("{:30} {:>12} rows", tbl_name, count);
        }

        // Edge count
        let edge_count: u64 = self
            .client
            .query(&format!("SELECT count() FROM {}", EDGE_TABLE))
            .fetch_one()
            .await
            .unwrap_or_else(|e| {
                eprintln!("Warning: Failed to query table {}: {}", EDGE_TABLE, e);
                0
            });
        println!("{:30} {:>12} rows", EDGE_TABLE, edge_count);

        // Edge breakdown by type
        println!("\n=== Edge Types ===");
        let edge_types: Vec<(String, u64)> = self
            .client
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

/// Convert an Arrow array value at a given index to SQL literal.
fn column_value_to_sql(col: &arrow::array::ArrayRef, row_idx: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(row_idx) {
        return "NULL".to_string();
    }

    match col.data_type() {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(row_idx) { "1" } else { "0" }.to_string()
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            let val = arr.value(row_idx);
            // Escape single quotes for SQL
            format!("'{}'", val.replace('\'', "''"))
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let val = arr.value(row_idx);
            format!("'{}'", val.replace('\'', "''"))
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            // Arrow Date32 stores days since Unix epoch (1970-01-01)
            let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = unix_epoch + chrono::Duration::days(days as i64);
            format!("'{}'", date.format("%Y-%m-%d"))
        }
        _ => "NULL".to_string(),
    }
}
