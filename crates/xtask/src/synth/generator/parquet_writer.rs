use super::run::{EdgeRecord, OrganizationNodes};
use crate::synth::arrow_schema::edge_schema;
use crate::synth::constants::DEFAULT_EDGE_FLUSH_THRESHOLD;
use anyhow::{Context, Result};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Schema;
use ontology::Ontology;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct StreamingEdgeWriter {
    writer: Option<ArrowWriter<BufWriter<File>>>,
    buffer: Vec<EdgeRecord>,
    flush_threshold: usize,
    schema: Arc<Schema>,
    total_written: usize,
}

impl StreamingEdgeWriter {
    pub fn new(
        path: &Path,
        flush_threshold: Option<usize>,
        ontology: &ontology::Ontology,
    ) -> Result<Self> {
        let schema = Arc::new(edge_schema(ontology));
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let file = File::create(path)
            .with_context(|| format!("Failed to create edge file: {}", path.display()))?;
        let buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        let writer = ArrowWriter::try_new(buf_writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: Some(writer),
            buffer: Vec::with_capacity(flush_threshold.unwrap_or(DEFAULT_EDGE_FLUSH_THRESHOLD)),
            flush_threshold: flush_threshold.unwrap_or(DEFAULT_EDGE_FLUSH_THRESHOLD),
            schema,
            total_written: 0,
        })
    }

    /// Flushes automatically when the buffer reaches the threshold.
    #[inline]
    pub fn push(&mut self, edge: EdgeRecord) -> Result<()> {
        self.buffer.push(edge);
        if self.buffer.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn extend(&mut self, edges: impl IntoIterator<Item = EdgeRecord>) -> Result<()> {
        for edge in edges {
            self.push(edge)?;
        }
        Ok(())
    }

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

    pub fn close(mut self) -> Result<usize> {
        self.flush()?;
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(self.total_written)
    }

    /// Includes edges still buffered and not yet flushed.
    pub fn count(&self) -> usize {
        self.total_written + self.buffer.len()
    }

    fn edges_to_batch(&self, edges: &[EdgeRecord]) -> Result<RecordBatch> {
        let traversal_path: StringArray = edges.iter().map(|e| Some(&*e.traversal_path)).collect();
        let relationship_kind: StringArray =
            edges.iter().map(|e| Some(&*e.relationship_kind)).collect();
        let source: Int64Array = edges.iter().map(|e| Some(e.source)).collect();
        let source_kind: StringArray = edges.iter().map(|e| Some(&*e.source_kind)).collect();
        let target: Int64Array = edges.iter().map(|e| Some(e.target)).collect();
        let target_kind: StringArray = edges.iter().map(|e| Some(&*e.target_kind)).collect();

        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(traversal_path),
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
        if !self.buffer.is_empty() {
            let buffer = std::mem::take(&mut self.buffer);
            if let Ok(batch) = self.edges_to_batch(&buffer)
                && let Some(writer) = &mut self.writer
            {
                let _ = writer.write(&batch);
            }
        }
        if let Some(writer) = self.writer.take() {
            let _ = writer.close();
        }
    }
}

pub struct ParquetWriter {
    output_dir: PathBuf,
}

impl ParquetWriter {
    pub fn new(output_dir: impl AsRef<Path>) -> Self {
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
        }
    }

    pub fn data_exists(&self) -> bool {
        self.output_dir.exists() && self.output_dir.join("edges.parquet").exists()
    }

    pub fn create_edge_writer(
        &self,
        org_id: u32,
        ontology: &ontology::Ontology,
    ) -> Result<StreamingEdgeWriter> {
        let org_dir = self.output_dir.join(format!("org_{}", org_id));
        fs::create_dir_all(&org_dir)?;
        let edge_path = org_dir.join("edges.parquet");
        StreamingEdgeWriter::new(&edge_path, None, ontology)
    }

    /// Writes nodes only; edges are written separately via the streaming writer.
    pub fn write_organization_nodes(&self, org_id: u32, nodes: &OrganizationNodes) -> Result<()> {
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
