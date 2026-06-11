//! Checkpoint and keyset-paging coverage for the system-notes edge handler
//! (ADR 013). The edge-materialization behaviour itself is covered by the
//! YAML scenarios under `tests/indexer/scenarios/sdlc/system_notes/`; these
//! two tests assert checkpoint mechanics the scenario format does not model.

use arrow::array::{Int64Array, StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_route, handler_context, namespace_envelope, system_notes_handler,
};

/// Seed a work item reachable by `(project_id, iid)`.
async fn seed_work_item(
    ctx: &TestContext,
    id: i64,
    iid: i64,
    project_id: i64,
    namespace_id: i64,
    traversal_path: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO work_items (id, iid, title, description, project_id, namespace_id, \
         work_item_type_id, created_at, updated_at, traversal_path, _siphon_replicated_at) \
         VALUES ({id}, {iid}, 'WI {iid}', '', {project_id}, {namespace_id}, 1, \
         '2024-01-15', '2024-01-15', '{traversal_path}', '2024-01-20 12:00:00')"
    ))
    .await;
}

/// Insert a single system note plus its `system_note_metadata` sidecar row.
#[allow(clippy::too_many_arguments)]
async fn seed_system_note(
    ctx: &TestContext,
    id: i64,
    body: &str,
    action: &str,
    noteable_type: &str,
    noteable_id: i64,
    author_id: i64,
    project_id: i64,
    traversal_path: &str,
    created_at: &str,
) {
    // The extract watermarks on `_siphon_replicated_at` (not `created_at`), so
    // the seed's replicated-at must carry the per-note timestamp for the
    // incremental/watermark assertions to discriminate.
    ctx.execute(&format!(
        "INSERT INTO siphon_notes (id, note, noteable_type, noteable_id, author_id, project_id, \
         system, internal, traversal_path, created_at, updated_at, _siphon_replicated_at) \
         VALUES ({id}, '{body}', '{noteable_type}', {noteable_id}, {author_id}, {project_id}, \
         true, false, '{traversal_path}', '{created_at}', '{created_at}', '{created_at}')"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO siphon_system_note_metadata (id, note_id, action, namespace_id, \
         traversal_path, created_at, updated_at, _siphon_replicated_at) \
         VALUES ({id}, {id}, '{action}', 100, '{traversal_path}', '{created_at}', '{created_at}', \
         '{created_at}')"
    ))
    .await;
}

fn edge_count(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.first().map_or(0, |b| b.num_rows())
}

/// Checkpoint + keyset paging: with `datalake_batch_size = 1` the handler
/// pages one note at a time across the window. After a full drain the
/// `ns.{id}.SystemNote` checkpoint advances to the run's watermark, and a
/// second run with an earlier-seeded checkpoint only processes new notes.
pub async fn checkpoint_advances_after_draining_paged_window(ctx: &TestContext) {
    create_route(
        ctx,
        1,
        200,
        "Project",
        "my-group/my-proj",
        100,
        "1/100/200/",
    )
    .await;
    seed_work_item(ctx, 10, 1, 200, 100, "1/100/200/").await;
    seed_work_item(ctx, 11, 2, 200, 100, "1/100/200/").await;
    seed_work_item(ctx, 12, 3, 200, 100, "1/100/200/").await;

    // Three cross-reference notes at distinct timestamps; with batch_limit=1
    // they span three full pages plus a final empty page.
    seed_system_note(
        ctx,
        1,
        "mentioned in #1",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;
    seed_system_note(
        ctx,
        2,
        "mentioned in #2",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 10:00:00",
    )
    .await;
    seed_system_note(
        ctx,
        3,
        "mentioned in #3",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 11:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("first run should succeed");

    // All three pages drained -> three MENTIONS edges, no row skipped or
    // double-counted by the keyset cursor.
    let mentions = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL WHERE relationship_kind = 'MENTIONS'",
            t("gl_edge")
        ))
        .await;
    let cnt = ArrowUtils::get_column_by_name::<UInt64Array>(&mentions[0], "cnt").unwrap();
    assert_eq!(cnt.value(0), 3, "all three paged notes produce edges");

    // Checkpoint advanced to the run watermark (2024-01-21).
    let cp = ctx
        .query(&format!(
            "SELECT toString(watermark) AS w FROM {} FINAL WHERE key = 'ns.100.SystemNote'",
            t("checkpoint")
        ))
        .await;
    assert_eq!(
        edge_count(&cp),
        1,
        "checkpoint row written for ns.100.SystemNote"
    );
    let w = ArrowUtils::get_column_by_name::<StringArray>(&cp[0], "w").unwrap();
    assert!(
        w.value(0).starts_with("2024-01-21"),
        "checkpoint advanced to the run watermark, got {}",
        w.value(0)
    );
}

/// A pre-existing checkpoint makes the next run incremental: only notes
/// created after the saved watermark are processed.
pub async fn incremental_run_skips_already_processed_notes(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('ns.100.SystemNote', '2024-01-15 10:30:00.000000', 'null')",
        t("checkpoint")
    ))
    .await;

    create_route(
        ctx,
        1,
        200,
        "Project",
        "my-group/my-proj",
        100,
        "1/100/200/",
    )
    .await;
    seed_work_item(ctx, 10, 1, 200, 100, "1/100/200/").await;
    seed_work_item(ctx, 11, 2, 200, 100, "1/100/200/").await;

    // Note 1 predates the checkpoint (09:00 <= 10:30) -> skipped.
    seed_system_note(
        ctx,
        1,
        "mentioned in #1",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;
    // Note 2 is after the checkpoint -> processed.
    seed_system_note(
        ctx,
        2,
        "mentioned in #2",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 11:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("incremental run should succeed");

    let mentions = ctx
        .query(&format!(
            "SELECT target_id FROM {} FINAL WHERE relationship_kind = 'MENTIONS'",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        edge_count(&mentions),
        1,
        "only the post-checkpoint note is processed"
    );
    let tid = ArrowUtils::get_column_by_name::<Int64Array>(&mentions[0], "target_id").unwrap();
    assert_eq!(tid.value(0), 11, "edge points at #2's work item, not #1's");
}
