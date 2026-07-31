use std::path::Path;
use std::sync::Arc;

use code_graph::v2::{
    Decision, FileInventoryEntry, GraphConverter, OnBatch, Pipeline, PipelineConfig, PipelineResult,
};

struct NoopConverter;

impl GraphConverter for NoopConverter {
    fn convert(
        &self,
        _graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, code_graph::v2::SinkError> {
        Ok(Vec::new())
    }
}

fn run_pipeline(root: &Path, inventory: Vec<FileInventoryEntry>) -> PipelineResult {
    let on_batch: Arc<OnBatch> = Arc::new(|_: &str, _: arrow::record_batch::RecordBatch| Ok(()));
    Pipeline::run(
        root,
        Arc::from(inventory),
        PipelineConfig::default(),
        &Default::default(),
        Arc::new(NoopConverter),
        on_batch,
    )
}

fn js_entry(path: &str) -> FileInventoryEntry {
    FileInventoryEntry {
        path: path.to_string(),
        size: 20,
        decision: Decision::Parse,
    }
}

#[test]
fn vanished_repository_tree_does_not_fault_remaining_js_files() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let root = tmp.path().join("repo");
    std::fs::create_dir(&root).expect("create repo dir");
    let mut inventory = Vec::new();
    for i in 0..8 {
        let name = format!("mod{i}.js");
        std::fs::write(root.join(&name), "export const x = 1;\n").expect("write fixture");
        inventory.push(js_entry(&name));
    }
    std::fs::remove_dir_all(&root).expect("remove repo dir");

    let result = run_pipeline(&root, inventory);

    assert!(
        result.faults.is_empty(),
        "a vanished tree must abort JS analysis, not fault every file: {:?}",
        result.faults
    );
}

#[test]
fn missing_js_file_still_faults_while_tree_exists() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let root = tmp.path().join("repo");
    std::fs::create_dir(&root).expect("create repo dir");
    std::fs::write(root.join("present.js"), "export const x = 1;\n").expect("write fixture");

    let result = run_pipeline(&root, vec![js_entry("present.js"), js_entry("absent.js")]);

    assert_eq!(
        result.faults.len(),
        1,
        "a single missing file in a live tree must keep faulting: {:?}",
        result.faults
    );
    assert_eq!(result.faults[0].path, "absent.js");
}
