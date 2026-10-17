use std::io;
use std::sync::{Arc, Mutex};

use orbit_log_fields::{debug, info, info_span, safe, warn};
use tracing::field::Empty;

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
        .with_max_level(tracing::Level::DEBUG)
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

#[test]
fn many_fields_fit_in_one_call() {
    let logs = capture(|| {
        info!(
            safe(GL_PROJECT_ID) = 1,
            safe(GL_USER_ID) = 2,
            safe(GL_NAMESPACE_ID) = 3,
            safe(GL_ORGANIZATION_ID) = 4,
            safe(IID) = 5,
            safe(DIFF_ID) = 6,
            safe(TASK_ID) = 7,
            safe(COUNT) = 8,
            safe(ROW_COUNT) = 9,
            safe(BATCH_COUNT) = 10,
            safe(FILE_COUNT) = 11,
            safe(ERROR_COUNT) = 12,
            safe(SKIPPED_COUNT) = 13,
            safe(ENTITY_COUNT) = 14,
            safe(PROJECT_COUNT) = 15,
            safe(PARTITION_COUNT) = 16,
            safe(CUT_COUNT) = 17,
            safe(HANDLER_COUNT) = 18,
            safe(THREAD_COUNT) = 19,
            safe(CPU_COUNT) = 20,
            safe(EDGE_COUNT) = 21,
            safe(NODE_COUNT) = 22,
            safe(PHI_COUNT) = 23,
            safe(STRING_COUNT) = 24,
            expanded(FILE_PATH) = %"app/models/user.rb",
            "indexed file"
        );
    });

    assert!(logs.contains("gl_project_id=1"), "{logs}");
    assert!(logs.contains("string_count=24"), "{logs}");
    assert!(logs.contains("file_path=app/models/user.rb"), "{logs}");
}

#[test]
fn debug_values_are_formatted_with_debug() {
    let logs = capture(|| {
        debug!(safe(PHASE) = ?Some("extract"),);
    });

    assert!(logs.contains("DEBUG"), "{logs}");
    assert!(logs.contains("phase=Some(\"extract\")"), "{logs}");
}

#[test]
fn span_target_parent_and_expanded_fields_are_passed_through() {
    let mut target = None;
    let logs = capture(|| {
        let outer = info_span!("outer");
        let inner = info_span!(
            target: "code_graph::breadcrumb",
            parent: &outer,
            "inner",
            expanded(FILE_PATH) = "app/models/user.rb",
        );
        target = inner.metadata().map(|metadata| metadata.target());
        inner.in_scope(|| info!("inside"));
    });

    assert!(
        logs.contains("outer:inner{file_path=\"app/models/user.rb\"}"),
        "{logs}"
    );
    assert_eq!(target, Some("code_graph::breadcrumb"));
}

#[test]
fn empty_span_fields_can_be_recorded_later() {
    let logs = capture(|| {
        let span = info_span!("work", safe(GL_PROJECT_ID) = Empty);
        span.record(safe::GL_PROJECT_ID, 7);
        span.in_scope(|| info!("inside"));
    });

    assert!(logs.contains("work{gl_project_id=7}"), "{logs}");
}
