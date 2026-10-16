use std::io;
use std::sync::{Arc, Mutex};

use orbit_log_fields::{info, info_span, warn};

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn capture(emit: impl FnOnce()) -> String {
    let buffer = Buffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_target(true)
        .finish();
    tracing::subscriber::with_default(subscriber, emit);
    String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap()
}

#[test]
fn fields_get_their_registry_names() {
    let logs = capture(|| {
        info!(
            safe(GL_PROJECT_ID) = 7,
            expanded(FILE_PATH) = %"app/models/user.rb",
            "parsed file"
        );
    });

    assert!(logs.contains("INFO"), "{logs}");
    assert!(logs.contains("gl_project_id=7"), "{logs}");
    assert!(logs.contains("file_path=app/models/user.rb"), "{logs}");
    assert!(logs.contains("parsed file"), "{logs}");
}

#[test]
fn values_keep_their_own_parentheses() {
    let items = [1_u64, 0, 3];
    let logs = capture(|| {
        warn!(
            safe(COUNT) = items.iter().filter(|item| **item > 0).count(),
            "counted items"
        );
    });

    assert!(logs.contains("WARN"), "{logs}");
    assert!(logs.contains("count=2"), "{logs}");
}

#[test]
fn target_is_passed_through() {
    let logs = capture(|| {
        info!(target: "code_graph::breadcrumb", safe(SIZE_BYTES) = 9, "large file");
    });

    assert!(logs.contains("code_graph::breadcrumb"), "{logs}");
    assert!(logs.contains("size_bytes=9"), "{logs}");
}

#[test]
fn span_fields_get_their_registry_names() {
    let logs = capture(|| {
        let span = info_span!("work", safe(GL_ROOT_NAMESPACE_ID) = 9970_u64);
        span.in_scope(|| info!("inside"));
    });

    assert!(logs.contains("work{gl_root_namespace_id=9970}"), "{logs}");
}
