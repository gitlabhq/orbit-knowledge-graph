//! Per-namespace debouncer shared by `ProgressWriter` and `CodeProgressWriter`.
//! Both writers coalesce rapid successive writes per namespace by skipping any
//! call that lands within `debounce_secs` of the last recorded update.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;

pub(crate) struct Debouncer {
    last_update: Mutex<HashMap<i64, Instant>>,
    debounce_secs: u64,
}

impl Debouncer {
    pub(crate) fn new(debounce_secs: u64) -> Self {
        Self {
            last_update: Mutex::new(HashMap::new()),
            debounce_secs,
        }
    }

    pub(crate) fn is_debounced(&self, id: i64) -> bool {
        match self.last_update.lock().get(&id) {
            Some(last) => last.elapsed().as_secs() < self.debounce_secs,
            None => false,
        }
    }

    pub(crate) fn record(&self, id: i64) {
        self.last_update.lock().insert(id, Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_id_is_not_debounced() {
        let d = Debouncer::new(60);
        assert!(!d.is_debounced(1));
    }

    #[test]
    fn record_then_immediate_check_is_debounced() {
        let d = Debouncer::new(60);
        d.record(1);
        assert!(d.is_debounced(1));
    }

    #[test]
    fn zero_window_never_debounces() {
        let d = Debouncer::new(0);
        d.record(1);
        assert!(!d.is_debounced(1));
    }
}
