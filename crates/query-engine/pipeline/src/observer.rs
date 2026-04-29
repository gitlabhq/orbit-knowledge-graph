use std::time::Duration;

use crate::error::PipelineError;

/// Trait for observing pipeline stage timings and outcomes.
pub trait PipelineObserver: Send {
    fn set_query_type(&mut self, query_type: &'static str);
    fn compiled(&mut self, elapsed: Duration);
    fn executed(&mut self, elapsed: Duration, batch_count: usize);
    fn authorized(&mut self, elapsed: Duration);
    fn hydrated(&mut self, elapsed: Duration);

    /// Called for each ClickHouse query execution (base and hydration queries).
    fn query_executed(&mut self, _label: &str, _read_rows: u64, _read_bytes: u64, _memory: i64) {}

    /// Record an error that occurred during pipeline execution.
    fn record_error(&self, error: &PipelineError);

    /// Record all metrics for a successful pipeline run.
    fn finish(&self, row_count: usize, redacted_count: usize);
}

/// No-op observer for local/CLI usage that doesn't need metrics.
pub struct NoOpObserver;

impl PipelineObserver for NoOpObserver {
    fn set_query_type(&mut self, _query_type: &'static str) {}
    fn compiled(&mut self, _elapsed: Duration) {}
    fn executed(&mut self, _elapsed: Duration, _batch_count: usize) {}
    fn authorized(&mut self, _elapsed: Duration) {}
    fn hydrated(&mut self, _elapsed: Duration) {}
    fn record_error(&self, _error: &PipelineError) {}
    fn finish(&self, _row_count: usize, _redacted_count: usize) {}
}

/// Observer that forwards every callback to a list of inner observers.
///
/// Use this to run multiple independent observers (e.g. OTel metrics +
/// billing events) against the same pipeline without coupling them.
pub type MultiObserver = gkg_utils::observability::MultiObserver<dyn PipelineObserver>;

impl PipelineObserver for MultiObserver {
    fn set_query_type(&mut self, query_type: &'static str) {
        for o in self.iter_mut() {
            o.set_query_type(query_type);
        }
    }

    fn compiled(&mut self, elapsed: Duration) {
        for o in self.iter_mut() {
            o.compiled(elapsed);
        }
    }

    fn executed(&mut self, elapsed: Duration, batch_count: usize) {
        for o in self.iter_mut() {
            o.executed(elapsed, batch_count);
        }
    }

    fn authorized(&mut self, elapsed: Duration) {
        for o in self.iter_mut() {
            o.authorized(elapsed);
        }
    }

    fn hydrated(&mut self, elapsed: Duration) {
        for o in self.iter_mut() {
            o.hydrated(elapsed);
        }
    }

    fn query_executed(&mut self, label: &str, read_rows: u64, read_bytes: u64, memory: i64) {
        for o in self.iter_mut() {
            o.query_executed(label, read_rows, read_bytes, memory);
        }
    }

    fn record_error(&self, error: &PipelineError) {
        for o in self.iter() {
            o.record_error(error);
        }
    }

    fn finish(&self, row_count: usize, redacted_count: usize) {
        for o in self.iter() {
            o.finish(row_count, redacted_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingObserver {
        set_query_type_calls: Arc<AtomicUsize>,
        compiled_calls: Arc<AtomicUsize>,
        executed_calls: Arc<AtomicUsize>,
        authorized_calls: Arc<AtomicUsize>,
        hydrated_calls: Arc<AtomicUsize>,
        query_executed_calls: Arc<AtomicUsize>,
        record_error_calls: Arc<AtomicUsize>,
        finish_calls: Arc<AtomicUsize>,
    }

    impl CountingObserver {
        fn handle(&self) -> CountingObserverHandle {
            CountingObserverHandle {
                set_query_type_calls: Arc::clone(&self.set_query_type_calls),
                compiled_calls: Arc::clone(&self.compiled_calls),
                executed_calls: Arc::clone(&self.executed_calls),
                authorized_calls: Arc::clone(&self.authorized_calls),
                hydrated_calls: Arc::clone(&self.hydrated_calls),
                query_executed_calls: Arc::clone(&self.query_executed_calls),
                record_error_calls: Arc::clone(&self.record_error_calls),
                finish_calls: Arc::clone(&self.finish_calls),
            }
        }
    }

    struct CountingObserverHandle {
        set_query_type_calls: Arc<AtomicUsize>,
        compiled_calls: Arc<AtomicUsize>,
        executed_calls: Arc<AtomicUsize>,
        authorized_calls: Arc<AtomicUsize>,
        hydrated_calls: Arc<AtomicUsize>,
        query_executed_calls: Arc<AtomicUsize>,
        record_error_calls: Arc<AtomicUsize>,
        finish_calls: Arc<AtomicUsize>,
    }

    impl PipelineObserver for CountingObserver {
        fn set_query_type(&mut self, _qt: &'static str) {
            self.set_query_type_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn compiled(&mut self, _elapsed: Duration) {
            self.compiled_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn executed(&mut self, _elapsed: Duration, _batch_count: usize) {
            self.executed_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn authorized(&mut self, _elapsed: Duration) {
            self.authorized_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn hydrated(&mut self, _elapsed: Duration) {
            self.hydrated_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn query_executed(
            &mut self,
            _label: &str,
            _read_rows: u64,
            _read_bytes: u64,
            _memory: i64,
        ) {
            self.query_executed_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn record_error(&self, _error: &PipelineError) {
            self.record_error_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn finish(&self, _row_count: usize, _redacted_count: usize) {
            self.finish_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn multi_observer_forwards_success_path_to_all() {
        let a = CountingObserver::default();
        let b = CountingObserver::default();
        let a_handle = a.handle();
        let b_handle = b.handle();

        let mut obs = MultiObserver::new(vec![Box::new(a), Box::new(b)]);

        obs.set_query_type("traversal");
        obs.compiled(Duration::from_millis(1));
        obs.executed(Duration::from_millis(10), 3);
        obs.authorized(Duration::from_millis(2));
        obs.hydrated(Duration::from_millis(1));
        obs.query_executed("base", 100, 1000, 50_000);
        obs.finish(42, 3);

        for handle in [&a_handle, &b_handle] {
            assert_eq!(handle.set_query_type_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.compiled_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.executed_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.authorized_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.hydrated_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.query_executed_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.finish_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.record_error_calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn multi_observer_forwards_error_path_to_all() {
        let a = CountingObserver::default();
        let b = CountingObserver::default();
        let a_handle = a.handle();
        let b_handle = b.handle();

        let mut obs = MultiObserver::new(vec![Box::new(a), Box::new(b)]);

        obs.set_query_type("traversal");
        obs.record_error(&PipelineError::Execution("fail".into()));

        for handle in [&a_handle, &b_handle] {
            assert_eq!(handle.set_query_type_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.record_error_calls.load(Ordering::Relaxed), 1);
            assert_eq!(handle.finish_calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn multi_observer_empty_is_valid_noop() {
        // Passes if forwarded methods don't panic on an empty observer list.
        let mut obs = MultiObserver::new(vec![]);
        obs.set_query_type("traversal");
        obs.compiled(Duration::from_millis(1));
        obs.finish(1, 0);
    }
}
