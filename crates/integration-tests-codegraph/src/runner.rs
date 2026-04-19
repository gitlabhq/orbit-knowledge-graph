use std::collections::HashMap;
use std::fmt::Write;

use code_graph::v2::dispatch_by_tag;
use code_graph::v2::linker::graph::RowContext;
use code_graph::v2::{Pipeline, PipelineConfig, PipelineOutput};

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

fn output_to_datasets(output: PipelineOutput) -> LanceDatasets {
    match output {
        PipelineOutput::Batches(batches) => batches
            .iter()
            .map(|(table, batch)| (table.clone(), arrow58_to_arrow56(batch)))
            .collect(),
        PipelineOutput::Graph(graph) => {
            let ctx = RowContext::empty();
            to_lance_datasets(&graph, &ctx).expect("Failed to convert graph to datasets")
        }
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

    let datasets = match suite.pipeline.as_deref() {
        None | Some("generic") => {
            let mut config = PipelineConfig::default();
            config.trace = trace_any;
            let pipeline = Pipeline::new(config);
            let result = if let Some(pool) = &pool {
                pool.install(|| pipeline.run(tmp.path()))
            } else {
                pipeline.run(tmp.path())
            };
            assert!(
                result.errors.is_empty(),
                "Pipeline errors: {:?}",
                result.errors
            );

            let ctx = RowContext::empty();
            let mut datasets = HashMap::new();
            for graph in &result.graphs {
                let graph_datasets =
                    to_lance_datasets(graph, &ctx).expect("Failed to convert graph to datasets");
                datasets.extend(graph_datasets);
            }
            for (table, batch) in &result.batches {
                datasets.insert(table.clone(), arrow58_to_arrow56(batch));
            }
            datasets
        }
        Some(tag) => {
            let files: Vec<String> = suite
                .fixtures
                .iter()
                .map(|f| format!("{root}/{}", f.path))
                .collect();
            let run = || {
                dispatch_by_tag(tag, &files, &root)
                    .unwrap_or_else(|| panic!("unknown pipeline tag: {tag}"))
                    .unwrap_or_else(|e| panic!("pipeline {tag} failed: {e:?}"))
            };
            let output = if let Some(pool) = &pool {
                pool.install(run)
            } else {
                run()
            };
            output_to_datasets(output)
        }
    };

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
