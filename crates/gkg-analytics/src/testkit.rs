use std::sync::Arc;

use parking_lot::Mutex;

use crate::analytics::Recorded;

/// Access events captured by [`crate::Analytics::recording`].
#[derive(Clone)]
pub struct RecordingHandle {
    sink: Arc<Mutex<Vec<Recorded>>>,
}

impl RecordingHandle {
    pub(crate) fn new(sink: Arc<Mutex<Vec<Recorded>>>) -> Self {
        Self { sink }
    }

    pub fn events(&self) -> Vec<Recorded> {
        self.sink.lock().clone()
    }

    pub fn clear(&self) {
        self.sink.lock().clear();
    }
}
