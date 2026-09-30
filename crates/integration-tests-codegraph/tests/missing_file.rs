//! A linked file that vanishes from disk before its parser reads it is a
//! fault for that file alone; every other file still parses.

use std::sync::Arc;

use code_graph::v2::{GraphConverter, OnBatch, Pipeline, PipelineConfig, Vfs};

struct NoopConverter;

impl GraphConverter for NoopConverter {
    fn convert(
        &self,
        _graph: code_graph::v2::linker::CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, code_graph::v2::SinkError> {
        Ok(Vec::new())
    }
}

#[test]
fn a_vanished_file_faults_alone() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Vfs::default();
    for name in ["present.js", "absent.js"] {
        let on_disk = dir.path().join(name);
        std::fs::write(&on_disk, b"export const x = 1;\n").unwrap();
        repo.link(name, on_disk, 20).unwrap();
    }
    std::fs::remove_file(dir.path().join("absent.js")).unwrap();
    let on_batch: Arc<OnBatch> = Arc::new(|_: &str, _: arrow::record_batch::RecordBatch| Ok(()));

    let result = Pipeline::run(
        Arc::new(repo),
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
