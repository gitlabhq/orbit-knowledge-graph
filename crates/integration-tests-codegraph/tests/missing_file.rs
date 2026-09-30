//! A file the inventory lists but the repository filesystem cannot produce
//! is a fault for that file alone; every other file still parses.

use std::sync::Arc;

use code_graph::v2::{
    Decision, File, GraphConverter, Inventory, OnBatch, Pipeline, PipelineConfig, Vfs,
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

fn js_entry(path: &str) -> File {
    File {
        path: path.to_string(),
        size: 20,
        decision: Decision::Parse,
        label: Default::default(),
        symlink: false,
        checked: true,
    }
}

#[test]
fn a_missing_file_faults_alone() {
    let repo = Vfs::default();
    repo.write("present.js", b"export const x = 1;\n".to_vec())
        .unwrap();
    let inventory = Inventory::new(vec![js_entry("present.js"), js_entry("absent.js")]);
    let on_batch: Arc<OnBatch> = Arc::new(|_: &str, _: arrow::record_batch::RecordBatch| Ok(()));

    let result = Pipeline::run(
        Arc::new(repo),
        Arc::new(inventory),
        PipelineConfig::default(),
        Arc::new(NoopConverter),
        on_batch,
    );

    assert_eq!(
        result.faults.len(),
        1,
        "only the missing file faults: {:?}",
        result.faults
    );
    assert_eq!(result.faults[0].path, "absent.js");
    assert!(result.errors.is_empty());
}
