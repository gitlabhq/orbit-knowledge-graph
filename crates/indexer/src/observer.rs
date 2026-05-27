use std::fmt;

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

/// Observer for indexing pipeline stages.
///
/// Mirrors the query path's `PipelineObserver` pattern: handlers call
/// methods at natural boundaries, `MultiObserver` fans out to OTel,
/// analytics, and future observers without polluting production code.
///
/// All methods default to no-ops so implementations only override the
/// signals they care about.
pub trait IndexingObserver: Send {
    // -- Identity (set once from the NATS message) --

    fn set_pipeline_type(&mut self, _pipeline_type: PipelineType) {}

    fn set_namespace(&mut self, _namespace_id: i64) {}

    /// SDLC entity type, e.g. "MergeRequest", "Issue".
    fn set_entity_type(&mut self, _entity_type: &str) {}

    /// Code pipeline project identity.
    fn set_project(&mut self, _project_id: i64, _branch: &str) {}

    // -- Discovered mid-run --

    /// Set after checkpoint load reveals whether this is a first-time or delta run.
    fn set_indexing_mode(&mut self, _mode: IndexingMode) {}

    // -- Accumulated during pipeline execution --

    /// Datalake rows extracted (SDLC: per-batch).
    fn extracted(&mut self, _rows: u64, _bytes: u64) {}

    /// Rows written to a graph table.
    fn written(&mut self, _table: &str, _rows: u64, _bytes: u64) {}

    /// File processing stats after code indexing (Code pipeline).
    fn files_processed(&mut self, _discovered: u64, _parsed: u64, _skipped: u64) {}

    /// Node counts by kind after code indexing (Code pipeline).
    fn nodes_indexed(&mut self, _kind: &str, _count: u64) {}

    // -- Terminal --

    /// Signals an error occurred. Implementations may gate `finish()` on this.
    fn record_error(&self, _error: &str) {}

    /// Emit analytics events or finalize metrics. Called exactly once per run.
    fn finish(&self) {}
}

pub struct NoOpObserver;

impl IndexingObserver for NoOpObserver {}

pub type MultiObserver = gkg_utils::observability::MultiObserver<dyn IndexingObserver>;

impl IndexingObserver for MultiObserver {
    fn set_pipeline_type(&mut self, pipeline_type: PipelineType) {
        for o in self.iter_mut() {
            o.set_pipeline_type(pipeline_type);
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

    fn written(&mut self, table: &str, rows: u64, bytes: u64) {
        for o in self.iter_mut() {
            o.written(table, rows, bytes);
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

    fn record_error(&self, error: &str) {
        for o in self.iter() {
            o.record_error(error);
        }
    }

    fn finish(&self) {
        for o in self.iter() {
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
        fn set_pipeline_type(&mut self, _: PipelineType) {
            self.push("set_pipeline_type");
        }
        fn set_namespace(&mut self, _: i64) {
            self.push("set_namespace");
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
        fn written(&mut self, _: &str, _: u64, _: u64) {
            self.push("written");
        }
        fn files_processed(&mut self, _: u64, _: u64, _: u64) {
            self.push("files_processed");
        }
        fn nodes_indexed(&mut self, _: &str, _: u64) {
            self.push("nodes_indexed");
        }
        fn record_error(&self, _: &str) {
            self.push("record_error");
        }
        fn finish(&self) {
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

        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_namespace(42);
        obs.set_entity_type("MergeRequest");
        obs.set_indexing_mode(IndexingMode::Incremental);
        obs.extracted(1000, 50_000);
        obs.written("gl_node", 200, 10_000);
        obs.finish();

        let expected = vec![
            "set_pipeline_type",
            "set_namespace",
            "set_entity_type",
            "set_indexing_mode",
            "extracted",
            "written",
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
