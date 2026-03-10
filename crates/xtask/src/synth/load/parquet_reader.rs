//! Parquet file reader for loading generated data into ClickHouse.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Reads Parquet data for loading into ClickHouse.
pub struct ParquetReader {
    input_dir: PathBuf,
}

impl ParquetReader {
    pub fn new(input_dir: impl AsRef<Path>) -> Self {
        Self {
            input_dir: input_dir.as_ref().to_path_buf(),
        }
    }

    /// List all organization directories.
    pub fn list_organizations(&self) -> Result<Vec<u32>> {
        let mut orgs = Vec::new();

        for entry in fs::read_dir(&self.input_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.starts_with("org_")
                && let Ok(org_id) = name_str[4..].parse::<u32>()
            {
                orgs.push(org_id);
            }
        }

        orgs.sort();
        Ok(orgs)
    }

    /// Get the path to a Parquet file.
    pub fn file_path(&self, org_id: u32, table_name: &str) -> PathBuf {
        self.input_dir
            .join(format!("org_{}", org_id))
            .join(format!("{}.parquet", table_name.to_lowercase()))
    }

    /// Read all batches from a Parquet file.
    pub fn read_batches(&self, org_id: u32, table_name: &str) -> Result<Vec<RecordBatch>> {
        let file_path = self.file_path(org_id, table_name);

        if !file_path.exists() {
            return Ok(vec![]);
        }

        let file = fs::File::open(&file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let batches: Result<Vec<_>, _> = reader.collect();
        Ok(batches?)
    }

    /// Read edges from a Parquet file.
    pub fn read_edges(&self, org_id: u32) -> Result<Vec<RecordBatch>> {
        self.read_batches(org_id, "edges")
    }

    /// Get the schema for a table from the first org's Parquet file.
    pub fn get_schema(&self, table_name: &str) -> Result<Option<Arc<Schema>>> {
        let orgs = self.list_organizations()?;
        if orgs.is_empty() {
            return Ok(None);
        }

        let file_path = self
            .input_dir
            .join(format!("org_{}", orgs[0]))
            .join(format!("{}.parquet", table_name.to_lowercase()));

        if !file_path.exists() {
            return Ok(None);
        }

        let file = fs::File::open(&file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(Some(builder.schema().clone()))
    }
}
