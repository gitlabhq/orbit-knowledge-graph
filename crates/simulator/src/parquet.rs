//! Parquet file I/O for generated data.

use crate::arrow_schema::edge_schema;
use crate::generator::{EdgeRecord, OrganizationData};
use anyhow::{Context, Result};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Schema;
use ontology::Ontology;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Writes generated data to Parquet files.
pub struct ParquetWriter {
    output_dir: PathBuf,
}

impl ParquetWriter {
    pub fn new(output_dir: impl AsRef<Path>) -> Self {
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
        }
    }

    /// Check if data already exists for the given configuration.
    pub fn data_exists(&self) -> bool {
        self.output_dir.exists() && self.output_dir.join("edges.parquet").exists()
    }

    /// Write organization data to Parquet files.
    pub fn write_organization_data(
        &self,
        _ontology: &Ontology,
        org_id: u32,
        data: &OrganizationData,
    ) -> Result<()> {
        let org_dir = self.output_dir.join(format!("org_{}", org_id));
        fs::create_dir_all(&org_dir)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        // Write node tables
        for (node_name, batches) in &data.nodes {
            if batches.is_empty() {
                continue;
            }

            let file_path = org_dir.join(format!("{}.parquet", node_name.to_lowercase()));
            let file = File::create(&file_path)
                .with_context(|| format!("Failed to create {}", file_path.display()))?;

            let schema = batches[0].schema();
            let mut writer = ArrowWriter::try_new(file, schema, Some(props.clone()))?;

            for batch in batches {
                writer.write(batch)?;
            }

            writer.close()?;
        }

        // Write edges
        if !data.edges.is_empty() {
            let file_path = org_dir.join("edges.parquet");
            self.write_edges(&file_path, &data.edges)?;
        }

        Ok(())
    }

    /// Write edges to a Parquet file.
    fn write_edges(&self, path: &Path, edges: &[EdgeRecord]) -> Result<()> {
        let schema = Arc::new(edge_schema());

        let relationship_kind: StringArray = edges
            .iter()
            .map(|e| Some(e.relationship_kind.as_str()))
            .collect();
        let source: Int64Array = edges.iter().map(|e| Some(e.source)).collect();
        let source_kind: StringArray = edges.iter().map(|e| Some(e.source_kind.as_str())).collect();
        let target: Int64Array = edges.iter().map(|e| Some(e.target)).collect();
        let target_kind: StringArray = edges.iter().map(|e| Some(e.target_kind.as_str())).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(relationship_kind),
                Arc::new(source),
                Arc::new(source_kind),
                Arc::new(target),
                Arc::new(target_kind),
            ],
        )?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    /// Finalize by writing a manifest file.
    pub fn write_manifest(&self, ontology: &Ontology, num_orgs: u32) -> Result<()> {
        let manifest = Manifest {
            node_types: ontology.nodes().map(|n| n.name.clone()).collect(),
            organizations: num_orgs,
        };

        let manifest_path = self.output_dir.join("manifest.json");
        let file = File::create(&manifest_path)?;
        serde_json::to_writer_pretty(file, &manifest)?;

        Ok(())
    }
}

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

    /// Read all batches from a Parquet file.
    pub fn read_batches(&self, org_id: u32, table_name: &str) -> Result<Vec<RecordBatch>> {
        let file_path = self
            .input_dir
            .join(format!("org_{}", org_id))
            .join(format!("{}.parquet", table_name.to_lowercase()));

        if !file_path.exists() {
            return Ok(vec![]);
        }

        let file = File::open(&file_path)?;
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

        let file = File::open(&file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(Some(builder.schema().clone()))
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Manifest {
    node_types: Vec<String>,
    organizations: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_writer_data_exists() {
        let writer = ParquetWriter::new("/nonexistent/path");
        assert!(!writer.data_exists());
    }
}
