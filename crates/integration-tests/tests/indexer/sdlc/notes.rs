use arrow::array::{Array, StringArray};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_notes_with_edges(ctx: &TestContext) {
    // Seed raw Rails `noteable_type` values as Siphon replicates them
    // (post `Note#noteable_type=` normalization to `base_class.to_s`).
    //   - Issue  → WorkItem (all Issue subclasses: Task, Incident, ...)
    //   - Epic   → WorkItem (legacy Groups::Epics::NotesController path)
    //   - Commit → has no ontology node, must be filtered out
    ctx.execute(
        "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, system, internal, st_diff, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (1, 'MR diff note', 'MergeRequest', 100, 1, false, false, '@@ -1 +1 @@\\n-old\\n+new', '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
        (2, 'Issue note', 'Issue', 200, 2, false, false, NULL, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
        (3, 'Vuln comment', 'Vulnerability', 300, 1, false, true, NULL, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00'),
        (4, 'Legacy epic note', 'Epic', 400, 1, false, false, NULL, '1/100/', '2024-01-18', '2024-01-18', '2024-01-20 12:00:00'),
        (5, 'Commit comment', 'Commit', 500, 1, false, false, NULL, '1/100/', '2024-01-19', '2024-01-19', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_note", 5).await;

    // Only note 1 carries a st_diff payload; the rest are NULL.
    let diff_rows = ctx
        .query(&format!(
            "SELECT id, st_diff FROM {} FINAL ORDER BY id",
            t("gl_note")
        ))
        .await;
    let st_diffs = ArrowUtils::get_column_by_name::<StringArray>(&diff_rows[0], "st_diff")
        .expect("st_diff column");
    assert!(st_diffs.value(0).contains("@@"));
    for i in 1..st_diffs.len() {
        assert!(st_diffs.is_null(i), "note {} expected NULL st_diff", i + 1);
    }

    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "Note", "1/100/", 5).await;

    let has_note_edges = ctx
        .query(&format!(
            "SELECT source_kind FROM {} FINAL \
             WHERE relationship_kind = 'HAS_NOTE' ORDER BY target_id",
            t("gl_edge")
        ))
        .await;
    assert!(!has_note_edges.is_empty(), "HAS_NOTE edges should exist");
    assert_eq!(
        has_note_edges[0].num_rows(),
        4,
        "expect 4 HAS_NOTE edges (MR, Issue→WorkItem, Vuln, Epic→WorkItem); Commit has no ontology target"
    );

    let source_kind =
        ArrowUtils::get_column_by_name::<StringArray>(&has_note_edges[0], "source_kind")
            .expect("source_kind column");
    assert_eq!(source_kind.value(0), "MergeRequest");
    assert_eq!(
        source_kind.value(1),
        "WorkItem",
        "Issue collapses to WorkItem"
    );
    assert_eq!(source_kind.value(2), "Vulnerability");
    assert_eq!(
        source_kind.value(3),
        "WorkItem",
        "Epic collapses to WorkItem"
    );
}

pub async fn filters_out_system_notes(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, system, internal, traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (1, 'User comment', 'MergeRequest', 100, 1, false, false, '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
        (2, 'added 1 commit', 'MergeRequest', 100, 2, true, false, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
        (3, 'mentioned in issue #123', 'MergeRequest', 100, 2, true, false, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00'),
        (4, 'Another user comment', 'Issue', 200, 1, false, false, '1/100/', '2024-01-18', '2024-01-18', '2024-01-20 12:00:00')",
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
        .query(&format!(
            "SELECT 1 FROM {} FINAL WHERE relationship_kind = 'HAS_NOTE'",
            t("gl_edge")
        ))
        .await;
    assert_eq!(has_note_edges[0].num_rows(), 2);
}

/// System notes with `merged`, `closed`, or `reopened` actions produce lifecycle
/// edges (MERGED, CLOSED, REOPENED) without generating Note nodes or HAS_NOTE edges.
/// Source: `siphon_system_note_metadata` joined to `siphon_notes` via note_id.
pub async fn materialises_lifecycle_edges_from_system_notes(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    // Three system notes, one per lifecycle action, each on a different noteable.
    // Note IDs: 10=merged (MR 500), 11=closed (MR 501), 12=reopened (Issue 600).
    // Author IDs: user 1 merged, user 2 closed, user 3 reopened.
    ctx.execute(
        "INSERT INTO siphon_notes
            (id, note, noteable_type, noteable_id, author_id, system, internal,
             traversal_path, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (10, 'merged',          'MergeRequest', 500, 1, true, false, '1/100/', '2024-01-15', '2024-01-15', '2024-01-20 12:00:00'),
        (11, 'closed',          'MergeRequest', 501, 2, true, false, '1/100/', '2024-01-16', '2024-01-16', '2024-01-20 12:00:00'),
        (12, 'closed',          'Issue',        600, 3, true, false, '1/100/', '2024-01-17', '2024-01-17', '2024-01-20 12:00:00'),
        (13, 'reopened',        'MergeRequest', 501, 2, true, false, '1/100/', '2024-01-18', '2024-01-18', '2024-01-20 12:00:00'),
        (14, 'reopened',        'Issue',        600, 3, true, false, '1/100/', '2024-01-19', '2024-01-19', '2024-01-20 12:00:00'),
        (15, 'mentioned in !999', 'MergeRequest', 500, 1, true, false, '1/100/', '2024-01-20', '2024-01-20', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_system_note_metadata
            (id, note_id, action, traversal_path, _siphon_replicated_at)
        VALUES
        (1, 10, 'merged',   '1/100/', '2024-01-20 12:00:00'),
        (2, 11, 'closed',   '1/100/', '2024-01-20 12:00:00'),
        (3, 12, 'closed',   '1/100/', '2024-01-20 12:00:00'),
        (4, 13, 'reopened', '1/100/', '2024-01-20 12:00:00'),
        (5, 14, 'reopened', '1/100/', '2024-01-20 12:00:00'),
        (6, 15, 'cross_reference', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    // System notes never produce Note nodes.
    assert_node_count(ctx, "gl_note", 0).await;

    // MERGED: user 1 → MergeRequest 500
    assert_edges_have_traversal_path(ctx, "MERGED", "User", "MergeRequest", "1/100/", 1).await;

    // CLOSED: user 2 → MergeRequest 501, user 3 → WorkItem 600 (Issue→WorkItem)
    let closed_edges = ctx
        .query(&format!(
            "SELECT target_kind FROM {} FINAL \
             WHERE relationship_kind = 'CLOSED' \
             ORDER BY target_id",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        closed_edges[0].num_rows(),
        2,
        "expect 2 CLOSED edges (MR + WorkItem)"
    );
    let target_kind =
        ArrowUtils::get_column_by_name::<StringArray>(&closed_edges[0], "target_kind")
            .expect("target_kind column");
    assert_eq!(target_kind.value(0), "MergeRequest");
    assert_eq!(
        target_kind.value(1),
        "WorkItem",
        "Issue collapses to WorkItem"
    );

    // REOPENED: user 2 → MergeRequest 501, user 3 → WorkItem 600
    let reopened_edges = ctx
        .query(&format!(
            "SELECT target_kind FROM {} FINAL \
             WHERE relationship_kind = 'REOPENED' \
             ORDER BY target_id",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        reopened_edges[0].num_rows(),
        2,
        "expect 2 REOPENED edges (MR + WorkItem)"
    );
    let reopened_target_kind =
        ArrowUtils::get_column_by_name::<StringArray>(&reopened_edges[0], "target_kind")
            .expect("target_kind column");
    assert_eq!(reopened_target_kind.value(0), "MergeRequest");
    assert_eq!(
        reopened_target_kind.value(1),
        "WorkItem",
        "Issue collapses to WorkItem"
    );
}
