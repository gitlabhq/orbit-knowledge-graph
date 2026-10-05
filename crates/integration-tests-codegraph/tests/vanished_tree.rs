use std::sync::Arc;

use code_graph::v2::config::{CodeFilter, Role, detect_language_from_path};
use code_graph::v2::{GraphConverter, OnBatch, Pipeline, PipelineConfig, PipelineResult};
use orbit_utils::files::{
    Vfs,
    sources::{Checkout, Memory},
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

fn run_pipeline(vfs: Vfs<Role>) -> PipelineResult {
    let on_batch: Arc<OnBatch> = Arc::new(|_: &str, _: arrow::record_batch::RecordBatch| Ok(()));
    Pipeline::run(
        Arc::new(vfs),
        PipelineConfig::default(),
        Arc::new(NoopConverter),
        on_batch,
    )
}

#[test]
fn stored_repository_does_not_depend_on_the_original_directory() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let root = tmp.path().join("repo");
    std::fs::create_dir(&root).expect("create repo dir");
    let mut inventory = Vec::new();
    for i in 0..8 {
        let name = format!("mod{i}.js");
        std::fs::write(root.join(&name), "export const x = 1;\n").expect("write fixture");
        inventory.push((name, b"export const x = 1;\n".to_vec()));
    }
    std::fs::remove_dir_all(&root).expect("remove repo dir");

    let vfs = Vfs::load(
        Memory(inventory),
        CodeFilter::new(detect_language_from_path),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let result = run_pipeline(vfs);

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
    std::fs::write(root.join("absent.js"), "export const y = 2;\n").expect("write fixture");
    let vfs = Vfs::load(
        Checkout(&root),
        CodeFilter::new(detect_language_from_path),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    std::fs::remove_file(root.join("absent.js")).unwrap();

    let result = run_pipeline(vfs);

    assert_eq!(
        result.faults.len(),
        1,
        "a single missing file in a live tree must keep faulting: {:?}",
        result.faults
    );
    assert_eq!(result.faults[0].path, "absent.js");
}

#[test]
fn content_rejected_on_first_read_is_a_skip_not_a_fault() {
    let root = tempfile::tempdir().unwrap();
    for name in ["binary.py", "binary.js", "binary.rs"] {
        std::fs::write(root.path().join(name), b"\0not source").unwrap();
    }
    let vfs = Vfs::load(
        Checkout(root.path()),
        CodeFilter::new(detect_language_from_path),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let result = run_pipeline(vfs);
    assert!(result.faults.is_empty(), "{:?}", result.faults);
    assert_eq!(result.skipped.len(), 3);
    assert!(
        result
            .skipped
            .iter()
            .all(|file| file.kind.as_metric_label() == "binary")
    );
}
