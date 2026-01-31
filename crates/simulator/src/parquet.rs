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
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Default flush threshold: 1 million edges per batch write.
const DEFAULT_EDGE_FLUSH_THRESHOLD: usize = 1_000_000;

/// Streaming edge writer that flushes to Parquet incrementally.
/// Keeps the Parquet writer open and writes row groups as edges accumulate.
pub struct StreamingEdgeWriter {
    writer: Option<ArrowWriter<BufWriter<File>>>,
    buffer: Vec<EdgeRecord>,
    flush_threshold: usize,
    schema: Arc<Schema>,
    total_written: usize,
}

impl StreamingEdgeWriter {
    /// Create a new streaming edge writer for the given path.
    pub fn new(path: &Path, flush_threshold: Option<usize>) -> Result<Self> {
        let schema = Arc::new(edge_schema());
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let file = File::create(path)
            .with_context(|| format!("Failed to create edge file: {}", path.display()))?;
        let buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file); // 8MB buffer
        let writer = ArrowWriter::try_new(buf_writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: Some(writer),
            buffer: Vec::with_capacity(flush_threshold.unwrap_or(DEFAULT_EDGE_FLUSH_THRESHOLD)),
            flush_threshold: flush_threshold.unwrap_or(DEFAULT_EDGE_FLUSH_THRESHOLD),
            schema,
            total_written: 0,
        })
    }

    /// Add an edge to the buffer. Flushes automatically when threshold is reached.
    #[inline]
    pub fn push(&mut self, edge: EdgeRecord) -> Result<()> {
        self.buffer.push(edge);
        if self.buffer.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    /// Add multiple edges. Flushes as needed.
    pub fn extend(&mut self, edges: impl IntoIterator<Item = EdgeRecord>) -> Result<()> {
        for edge in edges {
            self.push(edge)?;
        }
        Ok(())
    }

    /// Flush buffered edges to the Parquet file.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = self.edges_to_batch(&self.buffer)?;
        if let Some(writer) = &mut self.writer {
            writer.write(&batch)?;
        }

        self.total_written += self.buffer.len();
        self.buffer.clear();
        Ok(())
    }

    /// Close the writer and finalize the Parquet file.
    pub fn close(mut self) -> Result<usize> {
        self.flush()?;
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(self.total_written)
    }

    /// Get the number of edges written so far (including buffered).
    pub fn count(&self) -> usize {
        self.total_written + self.buffer.len()
    }

    /// Convert edge buffer to Arrow RecordBatch.
    fn edges_to_batch(&self, edges: &[EdgeRecord]) -> Result<RecordBatch> {
        let relationship_kind: StringArray = edges
            .iter()
            .map(|e| Some(&*e.relationship_kind))
            .collect();
        let source: Int64Array = edges.iter().map(|e| Some(e.source)).collect();
        let source_kind: StringArray = edges.iter().map(|e| Some(&*e.source_kind)).collect();
        let target: Int64Array = edges.iter().map(|e| Some(e.target)).collect();
        let target_kind: StringArray = edges.iter().map(|e| Some(&*e.target_kind)).collect();

        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(relationship_kind),
                Arc::new(source),
                Arc::new(source_kind),
                Arc::new(target),
                Arc::new(target_kind),
            ],
        )?)
    }
}

impl Drop for StreamingEdgeWriter {
    fn drop(&mut self) {
        // Best effort flush on drop
        if !self.buffer.is_empty() {
            let buffer = std::mem::take(&mut self.buffer);
            if let Ok(batch) = self.edges_to_batch(&buffer) {
                if let Some(writer) = &mut self.writer {
                    let _ = writer.write(&batch);
                }
            }
        }
        if let Some(writer) = self.writer.take() {
            let _ = writer.close();
        }
    }
}

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

    /// Create a streaming edge writer for an organization.
    pub fn create_edge_writer(&self, org_id: u32) -> Result<StreamingEdgeWriter> {
        let org_dir = self.output_dir.join(format!("org_{}", org_id));
        fs::create_dir_all(&org_dir)?;
        let edge_path = org_dir.join("edges.parquet");
        StreamingEdgeWriter::new(&edge_path, None)
    }

    /// Write only node data to Parquet files (edges written separately via streaming).
    pub fn write_organization_nodes(
        &self,
        org_id: u32,
        nodes: &crate::generator::OrganizationNodes,
    ) -> Result<()> {
        let org_dir = self.output_dir.join(format!("org_{}", org_id));
        fs::create_dir_all(&org_dir)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        for (node_name, batches) in &nodes.nodes {
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

        Ok(())
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
            .map(|e| Some(&*e.relationship_kind))
            .collect();
        let source: Int64Array = edges.iter().map(|e| Some(e.source)).collect();
        let source_kind: StringArray = edges.iter().map(|e| Some(&*e.source_kind)).collect();
        let target: Int64Array = edges.iter().map(|e| Some(e.target)).collect();
        let target_kind: StringArray = edges.iter().map(|e| Some(&*e.target_kind)).collect();

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

        let manifest_path = self.output_dir.join("gkg_simulator_manifest.json");
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
