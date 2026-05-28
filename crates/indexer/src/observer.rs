use std::fmt;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineType {
    Sdlc,
    Code,
}

impl fmt::Display for PipelineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sdlc => f.write_str("sdlc"),
            Self::Code => f.write_str("code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexingMode {
    Full,
    Incremental,
}

impl fmt::Display for IndexingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("full"),
            Self::Incremental => f.write_str("incremental"),
        }
    }
}

pub trait IndexingObserver: Send {
    fn set_dispatch_id(&mut self, _dispatch_id: Uuid) {}

    fn set_pipeline_type(&mut self, _pipeline_type: PipelineType) {}

    fn set_traversal_path(&mut self, traversal_path: &str) {
        if let Some(namespace_id) =
            gkg_utils::traversal_path::top_level_namespace_id(traversal_path)
        {
            self.set_namespace(namespace_id);
        }
    }

    fn set_namespace(&mut self, _namespace_id: i64) {}

    fn set_entity_type(&mut self, _entity_type: &str) {}

    fn set_project(&mut self, _project_id: i64, _branch: &str) {}

    fn set_indexing_mode(&mut self, _mode: IndexingMode) {}

    fn extracted(&mut self, _rows: u64, _bytes: u64) {}

    fn files_processed(&mut self, _discovered: u64, _parsed: u64, _skipped: u64) {}

    fn nodes_indexed(&mut self, _kind: &str, _count: u64) {}

    fn record_error(&mut self, _error: &str) {}

    fn finish(&mut self) {}
}

pub struct NoOpObserver;

impl IndexingObserver for NoOpObserver {}

pub type MultiObserver = gkg_utils::observability::MultiObserver<dyn IndexingObserver>;

impl IndexingObserver for MultiObserver {
    fn set_dispatch_id(&mut self, dispatch_id: Uuid) {
        for o in self.iter_mut() {
            o.set_dispatch_id(dispatch_id);
        }
    }

    fn set_pipeline_type(&mut self, pipeline_type: PipelineType) {
        for o in self.iter_mut() {
            o.set_pipeline_type(pipeline_type);
        }
    }

    fn set_traversal_path(&mut self, traversal_path: &str) {
        for o in self.iter_mut() {
            o.set_traversal_path(traversal_path);
        }
    }

    fn set_namespace(&mut self, namespace_id: i64) {
        for o in self.iter_mut() {
            o.set_namespace(namespace_id);
        }
    }

    fn set_entity_type(&mut self, entity_type: &str) {
        for o in self.iter_mut() {
            o.set_entity_type(entity_type);
        }
    }

    fn set_project(&mut self, project_id: i64, branch: &str) {
        for o in self.iter_mut() {
            o.set_project(project_id, branch);
        }
    }

    fn set_indexing_mode(&mut self, mode: IndexingMode) {
        for o in self.iter_mut() {
            o.set_indexing_mode(mode);
        }
    }

    fn extracted(&mut self, rows: u64, bytes: u64) {
        for o in self.iter_mut() {
            o.extracted(rows, bytes);
        }
    }

    fn files_processed(&mut self, discovered: u64, parsed: u64, skipped: u64) {
        for o in self.iter_mut() {
            o.files_processed(discovered, parsed, skipped);
        }
    }

    fn nodes_indexed(&mut self, kind: &str, count: u64) {
        for o in self.iter_mut() {
            o.nodes_indexed(kind, count);
        }
    }

    fn record_error(&mut self, error: &str) {
        for o in self.iter_mut() {
            o.record_error(error);
        }
    }

    fn finish(&mut self) {
        for o in self.iter_mut() {
            o.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    type Log = Arc<Mutex<Vec<&'static str>>>;

    struct RecordingObserver(Log);

    impl RecordingObserver {
        fn new(log: &Log) -> Self {
            Self(Arc::clone(log))
        }

        fn push(&self, method: &'static str) {
            self.0.lock().unwrap().push(method);
        }
    }

    impl IndexingObserver for RecordingObserver {
        fn set_dispatch_id(&mut self, _: Uuid) {
            self.push("set_dispatch_id");
        }
        fn set_pipeline_type(&mut self, _: PipelineType) {
            self.push("set_pipeline_type");
        }
        fn set_traversal_path(&mut self, _: &str) {
            self.push("set_traversal_path");
        }
        fn set_entity_type(&mut self, _: &str) {
            self.push("set_entity_type");
        }
        fn set_project(&mut self, _: i64, _: &str) {
            self.push("set_project");
        }
        fn set_indexing_mode(&mut self, _: IndexingMode) {
            self.push("set_indexing_mode");
        }
        fn extracted(&mut self, _: u64, _: u64) {
            self.push("extracted");
        }
        fn files_processed(&mut self, _: u64, _: u64, _: u64) {
            self.push("files_processed");
        }
        fn nodes_indexed(&mut self, _: &str, _: u64) {
            self.push("nodes_indexed");
        }
        fn record_error(&mut self, _: &str) {
            self.push("record_error");
        }
        fn finish(&mut self) {
            self.push("finish");
        }
    }

    #[test]
    fn multi_observer_forwards_to_all_children() {
        let log_a: Log = Default::default();
        let log_b: Log = Default::default();
        let mut obs = MultiObserver::new(vec![
            Box::new(RecordingObserver::new(&log_a)),
            Box::new(RecordingObserver::new(&log_b)),
        ]);

        obs.set_dispatch_id(Uuid::new_v4());
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_traversal_path("42/100/");
        obs.set_entity_type("MergeRequest");
        obs.set_indexing_mode(IndexingMode::Incremental);
        obs.extracted(1000, 50_000);
        obs.finish();

        let expected = vec![
            "set_dispatch_id",
            "set_pipeline_type",
            "set_traversal_path",
            "set_entity_type",
            "set_indexing_mode",
            "extracted",
            "finish",
        ];
        assert_eq!(*log_a.lock().unwrap(), expected);
        assert_eq!(*log_b.lock().unwrap(), expected);
    }

    #[test]
    fn multi_observer_forwards_code_path() {
        let log: Log = Default::default();
        let mut obs = MultiObserver::new(vec![Box::new(RecordingObserver::new(&log))]);

        obs.set_pipeline_type(PipelineType::Code);
        obs.set_project(99, "main");
        obs.set_indexing_mode(IndexingMode::Full);
        obs.files_processed(500, 480, 20);
        obs.nodes_indexed("definition", 3000);
        obs.nodes_indexed("file", 480);
        obs.finish();

        let log = log.lock().unwrap();
        assert_eq!(log.iter().filter(|&&m| m == "nodes_indexed").count(), 2);
        assert!(log.contains(&"files_processed"));
        assert!(log.ends_with(&["finish"]));
    }

    #[test]
    fn multi_observer_forwards_error_path() {
        let log: Log = Default::default();
        let mut obs = MultiObserver::new(vec![Box::new(RecordingObserver::new(&log))]);

        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.record_error("datalake query timeout");
        obs.finish();

        let log = log.lock().unwrap();
        assert!(log.contains(&"record_error"));
        assert!(log.ends_with(&["finish"]));
    }

    #[test]
    fn multi_observer_empty_does_not_panic() {
        let mut obs: MultiObserver = MultiObserver::new(vec![]);
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.extracted(100, 5000);
        obs.finish();
    }

    #[test]
    fn noop_observer_compiles() {
        let mut obs = NoOpObserver;
        obs.set_pipeline_type(PipelineType::Code);
        obs.extracted(0, 0);
        obs.finish();
    }

    #[test]
    fn pipeline_type_display() {
        assert_eq!(PipelineType::Sdlc.to_string(), "sdlc");
        assert_eq!(PipelineType::Code.to_string(), "code");
    }

    #[test]
    fn indexing_mode_display() {
        assert_eq!(IndexingMode::Full.to_string(), "full");
        assert_eq!(IndexingMode::Incremental.to_string(), "incremental");
    }
}
