use std::collections::HashMap;
use std::fmt::Write;
use std::sync::atomic::AtomicUsize;

use arrow_56::compute::concat_batches;
use std::sync::Arc;

use code_graph::v2::dispatch_by_tag;
use code_graph::v2::linker::graph::RowContext;
use code_graph::v2::trace::Tracer;
use code_graph::v2::{
    BatchTx, GraphConverter, NullSink, Pipeline, PipelineConfig, PipelineContext,
};

use super::assertions::{Severity, TestSuite};
use super::config::make_graph_config;
use super::datasets::{LanceDatasets, to_lance_datasets};
use super::validator::run_suite;

/// Convert an arrow 58 RecordBatch to arrow 56 via IPC roundtrip.
///
/// Arrow IPC format is stable across versions — serialize with arrow 58,
/// deserialize with arrow 56. Zero semantic loss.
fn arrow58_to_arrow56(
    batch: &arrow::record_batch::RecordBatch,
) -> arrow_56::record_batch::RecordBatch {
    use arrow::ipc::writer::StreamWriter;
    use arrow_56::ipc::reader::StreamReader;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(buf), None).unwrap();
    reader.into_iter().next().unwrap().unwrap()
}

/// Converter that builds lance datasets directly from CodeGraphs.
/// Stores them in a side channel — returns nothing to the sink.
struct LanceConverter {
    datasets: std::sync::Mutex<LanceDatasets>,
}

impl LanceConverter {
    fn new() -> Self {
        Self {
            datasets: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn take(&self) -> LanceDatasets {
        std::mem::take(&mut *self.datasets.lock().unwrap())
    }
}

impl GraphConverter for LanceConverter {
    fn convert(
        &self,
        graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, code_graph::v2::SinkError> {
        let row_ctx = RowContext::empty();
        let ds = to_lance_datasets(&graph, &row_ctx)
            .map_err(|e| code_graph::v2::SinkError(format!("Lance conversion: {e}")))?;
        let mut datasets = self.datasets.lock().unwrap();
        extend_datasets(&mut datasets, ds);
        Ok(Vec::new())
    }
}

/// Resolve the workspace root (where `Cargo.toml` with `[workspace]` lives).
fn workspace_root() -> std::path::PathBuf {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version=1", "--no-deps"])
        .output()
        .expect("Failed to run cargo metadata");
    let meta: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Failed to parse cargo metadata");
    std::path::PathBuf::from(meta["workspace_root"].as_str().unwrap())
}

/// Copy all files from `src_dir` into `dst_dir`, preserving relative paths.
fn copy_dir_recursive(src_dir: &std::path::Path, dst_dir: &std::path::Path) {
    for entry in walkdir::WalkDir::new(src_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let rel = entry.path().strip_prefix(src_dir).unwrap();
        let dst = dst_dir.join(rel);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst).ok();
        } else {
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::copy(entry.path(), &dst)
                .unwrap_or_else(|e| panic!("Failed to copy {}: {e}", entry.path().display()));
        }
    }
}

fn extend_datasets(into: &mut LanceDatasets, incoming: LanceDatasets) {
    for (table, batch) in incoming {
        if let Some(existing) = into.get_mut(&table) {
            let merged = concat_batches(&existing.schema(), &[existing.clone(), batch])
                .unwrap_or_else(|error| panic!("Failed to merge {table} batches: {error}"));
            *existing = merged;
        } else {
            into.insert(table, batch);
        }
    }
}

/// Run a YAML test suite from a string. Panics on any error-severity failure.
pub async fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = serde_yaml::from_str(yaml).expect("Failed to parse YAML suite");

    // Skip pipeline entirely if all tests are skipped
    if suite.tests.iter().all(|t| t.skip) {
        eprintln!(
            "[PASS] Suite: {} ({} tests, all skipped)",
            suite.name,
            suite.tests.len()
        );
        return;
    }

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");

    // Copy fixture_dir contents first (if set)
    if let Some(dir) = &suite.fixture_dir {
        let root = workspace_root();
        let src = root.join(dir);
        assert!(src.is_dir(), "fixture_dir not found: {}", src.display());
        copy_dir_recursive(&src, tmp.path());
    }

    // Write inline fixtures (override anything from fixture_dir)
    for fixture in &suite.fixtures {
        let path = tmp.path().join(&fixture.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("Failed to create dir {}: {e}", parent.display()));
        }
        std::fs::write(&path, &fixture.content)
            .unwrap_or_else(|e| panic!("Failed to write {}: {e}", path.display()));
    }

    let root = tmp.path().to_string_lossy().to_string();

    let trace_any = suite.trace || suite.tests.iter().any(|t| t.debug);
    let tracer = Tracer::new(trace_any);

    // Single-thread rayon when tracing so trace output isn't interleaved
    let pool = if trace_any {
        Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        )
    } else {
        None
    };

    let (datasets, pipeline_ctx) = match suite.pipeline.as_deref() {
        None | Some("generic") => {
            let config = PipelineConfig::default();
            let converter = Arc::new(LanceConverter::new());
            let sink: Arc<dyn code_graph::v2::BatchSink> = Arc::new(NullSink);
            let result = if let Some(pool) = &pool {
                let c = converter.clone() as Arc<dyn GraphConverter>;
                let s = sink.clone();
                pool.install(|| Pipeline::run_with_tracer(tmp.path(), config, tracer, c, s))
            } else {
                Pipeline::run_with_tracer(
                    tmp.path(),
                    config,
                    tracer,
                    converter.clone() as Arc<dyn GraphConverter>,
                    sink,
                )
            };
            assert!(
                result.errors.is_empty(),
                "Pipeline errors: {:?}",
                result.errors
            );

            (converter.take(), result.ctx.clone())
        }
        Some(tag) => {
            let files: Vec<String> = suite
                .fixtures
                .iter()
                .map(|f| format!("{root}/{}", f.path))
                .collect();
            let ctx = Arc::new(PipelineContext {
                config: PipelineConfig::default(),
                tracer,
                root_path: root.clone(),
                skipped: std::sync::Mutex::new(Vec::new()),
                faults: std::sync::Mutex::new(Vec::new()),
            });
            let converter = LanceConverter::new();
            let (tx, rx) = crossbeam_channel::unbounded();
            let defs = AtomicUsize::new(0);
            let imps = AtomicUsize::new(0);
            let edgs = AtomicUsize::new(0);
            {
                let errors = std::sync::Mutex::new(Vec::new());
                let btx = BatchTx::new(&tx, &converter, &errors, &defs, &imps, &edgs);
                dispatch_by_tag(tag, &files, &ctx, &btx)
                    .unwrap_or_else(|| panic!("unknown pipeline tag: {tag}"))
                    .unwrap_or_else(|e| panic!("pipeline {tag} failed: {e:?}"));
            }
            drop(tx);
            // Collect any raw batches sent via send_raw (e.g. Ruby/Prism)
            let mut datasets = converter.take();
            for (table, batch) in rx.try_iter() {
                extend_datasets(
                    &mut datasets,
                    HashMap::from([(table, arrow58_to_arrow56(&batch))]),
                );
            }
            (datasets, ctx)
        }
    };

    // Dump trace once, after all execution is complete
    pipeline_ctx.tracer.dump(&suite.name);

    let config = make_graph_config().expect("Failed to build graph config");

    let failures = run_suite(&suite, &datasets, &config).await;
    if failures.is_empty() {
        eprintln!("[PASS] Suite: {} ({} tests)", suite.name, suite.tests.len());
        return;
    }

    let mut msg = format!(
        "\n[FAIL] Suite: {} ({} failures)\n",
        suite.name,
        failures.len()
    );
    for f in &failures {
        writeln!(msg, "  [{}] \"{}\" — {}", f.severity, f.test, f.message).unwrap();
    }

    if failures.iter().any(|f| f.severity == Severity::Error) {
        panic!("{msg}");
    } else {
        eprintln!("{msg}");
    }
}
