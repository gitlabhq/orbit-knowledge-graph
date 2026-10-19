use std::sync::Arc;

use parking_lot::Mutex;
use tracing::Dispatch;
use tracing::subscriber::{DefaultGuard, NoSubscriber};

#[derive(Clone, Default)]
pub struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct LogCaptureGuard {
    _default: DefaultGuard,
    _second_dispatch: Dispatch,
}

/// Captures the log lines of the current thread while the guard lives.
pub fn capture_logs() -> (CapturedLogs, LogCaptureGuard) {
    // With one live dispatcher, tracing-core caches callsite interest from whichever thread
    // registers it first, so a concurrent test without a subscriber could disable our lines.
    let second_dispatch = Dispatch::new(NoSubscriber::default());
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let guard = LogCaptureGuard {
        _default: tracing::subscriber::set_default(subscriber),
        _second_dispatch: second_dispatch,
    };
    (logs, guard)
}
