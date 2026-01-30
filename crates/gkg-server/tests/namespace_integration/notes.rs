//! Integration tests for note processing in the namespace handler.

use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, create_namespace_payload, default_test_watermark, get_namespace_handler,
    get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_notes_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, system, internal, traversal_path, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (1, 'MR comment', 'MergeRequest', 100, 1, false, false, '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
            (2, 'Work item note', 'WorkItem', 200, 2, false, false, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
            (3, 'Vuln comment', 'Vulnerability', 300, 1, false, true, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00')",
        )
        .await;

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_notes ORDER BY id").await;
    assert!(!result.is_empty(), "notes result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let author_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'AUTHORED' AND source_kind = 'User' AND target_kind = 'Note' ORDER BY target_id")
        .await;

    assert!(!author_edges.is_empty(), "author edges should exist");
    assert_eq!(author_edges[0].num_rows(), 3, "should have 3 author edges");

    let has_note_edges = context
        .query("SELECT source_id, source_kind, target_id FROM gl_edges WHERE relationship_kind = 'HAS_NOTE' ORDER BY target_id")
        .await;

    assert!(!has_note_edges.is_empty(), "has_note edges should exist");
    let batch = &has_note_edges[0];
    assert_eq!(batch.num_rows(), 3, "should have 3 has_note edges");

    let source_kind = get_string_column(batch, "source_kind");

    assert_eq!(source_kind.value(0), "MergeRequest");
    assert_eq!(source_kind.value(1), "WorkItem");
    assert_eq!(source_kind.value(2), "Vulnerability");
}
