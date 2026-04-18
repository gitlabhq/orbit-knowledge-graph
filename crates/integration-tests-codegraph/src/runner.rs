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

/// Run a YAML test suite from a string. Panics on any error-severity failure.
pub async fn run_yaml_suite(yaml: &str) {
    let suite: TestSuite = serde_yaml::from_str(yaml).expect("Failed to parse YAML suite");

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
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

    let datasets = match suite.pipeline.as_deref() {
        None | Some("generic") => {
            let pipeline = Pipeline::new(PipelineConfig::default());
            let result = pipeline.run(tmp.path());
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
            let output = dispatch_by_tag(tag, &files, &root)
                .unwrap_or_else(|| panic!("unknown pipeline tag: {tag}"))
                .unwrap_or_else(|e| panic!("pipeline {tag} failed: {e:?}"));
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
