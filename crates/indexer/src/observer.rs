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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingObserver {
        set_pipeline_type_calls: Arc<AtomicUsize>,
        set_namespace_calls: Arc<AtomicUsize>,
        set_entity_type_calls: Arc<AtomicUsize>,
        set_project_calls: Arc<AtomicUsize>,
        set_indexing_mode_calls: Arc<AtomicUsize>,
        extracted_calls: Arc<AtomicUsize>,
        written_calls: Arc<AtomicUsize>,
        files_processed_calls: Arc<AtomicUsize>,
        nodes_indexed_calls: Arc<AtomicUsize>,
        record_error_calls: Arc<AtomicUsize>,
        finish_calls: Arc<AtomicUsize>,
        errored: Arc<AtomicBool>,
    }

    impl CountingObserver {
        fn handle(&self) -> CountingHandle {
            CountingHandle {
                set_pipeline_type_calls: Arc::clone(&self.set_pipeline_type_calls),
                set_namespace_calls: Arc::clone(&self.set_namespace_calls),
                set_entity_type_calls: Arc::clone(&self.set_entity_type_calls),
                set_project_calls: Arc::clone(&self.set_project_calls),
                set_indexing_mode_calls: Arc::clone(&self.set_indexing_mode_calls),
                extracted_calls: Arc::clone(&self.extracted_calls),
                written_calls: Arc::clone(&self.written_calls),
                files_processed_calls: Arc::clone(&self.files_processed_calls),
                nodes_indexed_calls: Arc::clone(&self.nodes_indexed_calls),
                record_error_calls: Arc::clone(&self.record_error_calls),
                finish_calls: Arc::clone(&self.finish_calls),
                errored: Arc::clone(&self.errored),
            }
        }
    }

    struct CountingHandle {
        set_pipeline_type_calls: Arc<AtomicUsize>,
        set_namespace_calls: Arc<AtomicUsize>,
        set_entity_type_calls: Arc<AtomicUsize>,
        set_project_calls: Arc<AtomicUsize>,
        set_indexing_mode_calls: Arc<AtomicUsize>,
        extracted_calls: Arc<AtomicUsize>,
        written_calls: Arc<AtomicUsize>,
        files_processed_calls: Arc<AtomicUsize>,
        nodes_indexed_calls: Arc<AtomicUsize>,
        record_error_calls: Arc<AtomicUsize>,
        finish_calls: Arc<AtomicUsize>,
        errored: Arc<AtomicBool>,
    }

    impl IndexingObserver for CountingObserver {
        fn set_pipeline_type(&mut self, _pipeline_type: PipelineType) {
            self.set_pipeline_type_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn set_namespace(&mut self, _namespace_id: i64) {
            self.set_namespace_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn set_entity_type(&mut self, _entity_type: &str) {
            self.set_entity_type_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn set_project(&mut self, _project_id: i64, _branch: &str) {
            self.set_project_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn set_indexing_mode(&mut self, _mode: IndexingMode) {
            self.set_indexing_mode_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn extracted(&mut self, _rows: u64, _bytes: u64) {
            self.extracted_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn written(&mut self, _table: &str, _rows: u64, _bytes: u64) {
            self.written_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn files_processed(&mut self, _discovered: u64, _parsed: u64, _skipped: u64) {
            self.files_processed_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn nodes_indexed(&mut self, _kind: &str, _count: u64) {
            self.nodes_indexed_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn record_error(&self, _error: &str) {
            self.record_error_calls.fetch_add(1, Ordering::Relaxed);
            self.errored.store(true, Ordering::Relaxed);
        }
        fn finish(&self) {
            self.finish_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn multi_observer_forwards_sdlc_success_path() {
        let a = CountingObserver::default();
        let b = CountingObserver::default();
        let a_handle = a.handle();
        let b_handle = b.handle();

        let mut obs = MultiObserver::new(vec![Box::new(a), Box::new(b)]);

        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_namespace(42);
        obs.set_entity_type("MergeRequest");
        obs.set_indexing_mode(IndexingMode::Incremental);
        obs.extracted(1000, 50_000);
        obs.written("gl_node", 200, 10_000);
        obs.finish();

        for handle in [&a_handle, &b_handle] {
            assert_eq!(handle.set_pipeline_type_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.set_namespace_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.set_entity_type_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.set_indexing_mode_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.extracted_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.written_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.finish_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.record_error_calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn multi_observer_forwards_code_success_path() {
        let a = CountingObserver::default();
        let handle = a.handle();

        let mut obs: MultiObserver = MultiObserver::new(vec![Box::new(a)]);

        obs.set_pipeline_type(PipelineType::Code);
        obs.set_project(99, "main");
        obs.set_indexing_mode(IndexingMode::Full);
        obs.files_processed(500, 480, 20);
        obs.nodes_indexed("definition", 3000);
        obs.nodes_indexed("file", 480);
        obs.written("gl_node", 3480, 200_000);
        obs.finish();

        assert_eq!(handle.set_pipeline_type_calls.load(Ordering::Relaxed), 1);
        assert_eq!(handle.set_project_calls.load(Ordering::Relaxed), 1);
        assert_eq!(handle.files_processed_calls.load(Ordering::Relaxed), 1);
        assert_eq!(handle.nodes_indexed_calls.load(Ordering::Relaxed), 2);
        assert_eq!(handle.written_calls.load(Ordering::Relaxed), 1);
        assert_eq!(handle.finish_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn multi_observer_forwards_error_path() {
        let a = CountingObserver::default();
        let b = CountingObserver::default();
        let a_handle = a.handle();
        let b_handle = b.handle();

        let mut obs = MultiObserver::new(vec![Box::new(a), Box::new(b)]);

        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.set_namespace(42);
        obs.record_error("datalake query timeout");
        obs.finish();

        for handle in [&a_handle, &b_handle] {
            assert_eq!(handle.record_error_calls.load(Ordering::Relaxed), 1);
            assert!(handle.errored.load(Ordering::Relaxed));
            assert_eq!(handle.finish_calls.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn multi_observer_empty_is_valid() {
        let mut obs: MultiObserver = MultiObserver::new(vec![]);
        obs.set_pipeline_type(PipelineType::Sdlc);
        obs.extracted(100, 5000);
        obs.finish();
    }

    #[test]
    fn noop_observer_compiles() {
        let mut obs = NoOpObserver;
        obs.set_pipeline_type(PipelineType::Code);
        obs.set_namespace(1);
        obs.set_entity_type("Issue");
        obs.set_project(1, "main");
        obs.set_indexing_mode(IndexingMode::Full);
        obs.extracted(0, 0);
        obs.written("gl_node", 0, 0);
        obs.files_processed(0, 0, 0);
        obs.nodes_indexed("definition", 0);
        obs.record_error("test");
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
