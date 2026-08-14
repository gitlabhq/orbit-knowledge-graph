//! Dev-only memory harness for the ClickHouse indexing path.
//!
//! Runs the shared code-graph pipeline with the real `IndexerConverter` (the
//! server's ClickHouse Arrow path) over a local repo and drops every batch, so
//! peak RSS reflects parse + graph build + CH conversion without needing NATS
//! or a live ClickHouse. Pair with an external `ps` poller for ground-truth
//! peak RSS. Gated behind the `mem-harness` feature; not compiled in normal
//! builds.

use crate::modules::code::arrow_converter::{IndexerConverter, IndexerEnvelope};
use crate::modules::code::config::CodeTableNames;
use ontology::Ontology;
use std::path::Path;
use std::sync::Arc;

const MAX_INDEXED_FILE_BYTES: u64 = 5_000_000;

pub fn run(repo: &Path, threads: usize) {
    let ontology = Ontology::load_embedded().expect("load embedded ontology");
    let table_names =
        Arc::new(CodeTableNames::from_ontology(&ontology).expect("resolve code table names"));

    let mut filter = code_graph::v2::config::CodeFilter::new(
        MAX_INDEXED_FILE_BYTES,
        0,
        code_graph::v2::config::detect_language_from_path,
    );
    let inventory: Arc<[code_graph::v2::FileInventoryEntry]> =
        Arc::from(orbit_utils::walk::walk_dir(repo, &mut filter).expect("walk repo"));

    let envelope = IndexerEnvelope::new(
        "0/".to_string(),
        1,
        "main".to_string(),
        "0".repeat(40),
        chrono::Utc::now(),
    );
    let converter: Arc<dyn code_graph::v2::GraphConverter> =
        Arc::new(IndexerConverter::new(envelope, &ontology, table_names));

    let on_batch: Arc<code_graph::v2::OnBatch> =
        Arc::new(|_table: &str, batch: arrow::record_batch::RecordBatch| {
            std::hint::black_box(batch.num_rows());
            Ok::<(), code_graph::v2::SinkError>(())
        });

    let config = code_graph::v2::PipelineConfig {
        worker_threads: threads,
        per_file_timeout: Some(std::time::Duration::from_secs(2)),
        per_file_parse_timeout: Some(std::time::Duration::from_millis(100)),
        per_file_walk_timeout: Some(std::time::Duration::from_millis(100)),
        per_file_ssa_timeout: Some(std::time::Duration::from_millis(100)),
        cross_file_resolve_timeout: Some(std::time::Duration::from_secs(180)),
        ..Default::default()
    };

    let tracer = code_graph::v2::trace::Tracer::new(false);
    let result = code_graph::v2::Pipeline::run_with_tracer(
        repo,
        inventory,
        config,
        filter.file_reasons(),
        tracer,
        converter,
        on_batch,
    );
    let s = &result.stats;
    eprintln!(
        "CH-HARNESS done: files={} defs={} imports={} edges={}",
        s.files_indexed, s.definitions_count, s.imports_count, s.edges_count
    );
}
