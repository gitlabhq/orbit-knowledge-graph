use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, get_string_column,
    handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_notes_with_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, system, internal, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (1, 'MR comment', 'MergeRequest', 100, 1, false, false, '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
        (2, 'Work item note', 'WorkItem', 200, 2, false, false, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
        (3, 'Vuln comment', 'Vulnerability', 300, 1, false, true, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_note", 3).await;

    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "Note", "1/100/", 3).await;

    let has_note_edges = ctx
        .query(
            "SELECT source_kind FROM gl_edge FINAL \
             WHERE relationship_kind = 'HAS_NOTE' ORDER BY target_id",
        )
        .await;
    assert!(!has_note_edges.is_empty(), "HAS_NOTE edges should exist");
    assert_eq!(has_note_edges[0].num_rows(), 3);

    let source_kind = get_string_column(&has_note_edges[0], "source_kind");
    assert_eq!(source_kind.value(0), "MergeRequest");
    assert_eq!(source_kind.value(1), "WorkItem");
    assert_eq!(source_kind.value(2), "Vulnerability");
}

pub async fn filters_out_system_notes(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, system, internal, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (1, 'User comment', 'MergeRequest', 100, 1, false, false, '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
        (2, 'added 1 commit', 'MergeRequest', 100, 2, true, false, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
        (3, 'mentioned in issue #123', 'MergeRequest', 100, 2, true, false, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00'),
        (4, 'Another user comment', 'WorkItem', 200, 1, false, false, '1/100/', '2024-01-18', '2024-01-18', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_note", 2).await;

    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "Note", "1/100/", 2).await;

    let has_note_edges = ctx
        .query("SELECT 1 FROM gl_edge FINAL WHERE relationship_kind = 'HAS_NOTE'")
        .await;
    assert_eq!(has_note_edges[0].num_rows(), 2);
}
