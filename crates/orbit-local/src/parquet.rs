use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub(crate) struct ParquetSink {
    dir: PathBuf,
    writers: Mutex<HashMap<String, ArrowWriter<File>>>,
}

impl ParquetSink {
    pub(crate) fn create(dir: PathBuf) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        Ok(Arc::new(Self {
            dir,
            writers: Mutex::new(HashMap::new()),
        }))
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn on_batch(self: &Arc<Self>) -> Arc<code_graph::v2::OnBatch> {
        let sink = Arc::clone(self);
        Arc::new(move |table: &str, batch: RecordBatch| {
            sink.write(table, &batch)
                .map_err(|e| code_graph::v2::SinkError(format!("Parquet write to {table}: {e:#}")))
        })
    }

    fn write(&self, table: &str, batch: &RecordBatch) -> Result<()> {
        let mut writers = self.writers.lock().unwrap_or_else(PoisonError::into_inner);
        let writer = match writers.entry(table.to_string()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let path = self.dir.join(format!("{table}.parquet"));
                let file = File::create(&path)
                    .with_context(|| format!("failed to create {}", path.display()))?;
                let props = WriterProperties::builder()
                    .set_compression(Compression::ZSTD(Default::default()))
                    .build();
                entry.insert(ArrowWriter::try_new(file, batch.schema(), Some(props))?)
            }
        };
        writer.write(batch)?;
        Ok(())
    }

    /// Finishes every file even when one fails, since a Parquet file without a
    /// footer is unreadable.
    pub(crate) fn close(self: Arc<Self>) -> Result<()> {
        let sink = Arc::into_inner(self).context("parquet sink is still shared")?;
        let writers = sink
            .writers
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        let mut first_error = None;
        for (table, writer) in writers {
            if let Err(e) = writer.close() {
                first_error.get_or_insert_with(|| {
                    anyhow::Error::from(e).context(format!("failed to finish {table}.parquet"))
                });
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
