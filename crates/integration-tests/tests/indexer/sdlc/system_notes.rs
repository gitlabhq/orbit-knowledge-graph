//! End-to-end coverage for the system-notes edge handler (ADR 013): seed
//! `siphon_notes` + `siphon_system_note_metadata` (plus the routes / MR /
//! work-item rows the resolver reads), run the handler in isolation, and
//! assert on the `gl_edge` rows it writes.
//!
//! These exercise the full `extract → resolve → emit → write` path against a
//! real ClickHouse container, complementing the pure-function unit tests in
//! `crates/indexer/src/modules/sdlc/transform/system_notes/`.

use arrow::array::{Int64Array, StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_route, handler_context, namespace_envelope, system_notes_handler,
};

/// Seed a merge request reachable by `(target_project_id, iid)`.
async fn seed_merge_request(
    ctx: &TestContext,
    id: i64,
    iid: i64,
    project_id: i64,
    traversal_path: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO merge_requests (id, iid, title, target_project_id, source_project_id, \
         author_id, traversal_path, _siphon_replicated_at) \
         VALUES ({id}, {iid}, 'MR {iid}', {project_id}, {project_id}, 1, '{traversal_path}', \
         '2024-01-20 12:00:00')"
    ))
    .await;
}

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

/// Happy path: a realistic mix of same-project MENTIONS, a cross-project
/// (but same-top-level-namespace) MENTIONS, and a REOPENED lifecycle action
/// all materialize the expected `gl_edge` rows - and nothing else.
///
/// The cross-project target lives in a *different project* under the *same
/// top-level namespace* as the source (`1/100/200/` -> `1/100/400/`). v1
/// bounds resolution to the source top-level namespace, so this still
/// resolves; a *cross-top-level* reference is covered separately by
/// [`cross_top_level_reference_is_not_resolved`].
pub async fn materializes_mentions_and_lifecycle_edges(ctx: &TestContext) {
    // Two projects, both under top-level namespace 100: the source project
    // (1/100/200/) and a cross-project target in a sibling project
    // (1/100/400/).
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
    create_route(
        ctx,
        2,
        400,
        "Project",
        "my-group/other-proj",
        100,
        "1/100/400/",
    )
    .await;

    // Reference targets.
    seed_merge_request(ctx, 10, 5, 200, "1/100/200/").await; // !5 same-project
    seed_work_item(ctx, 20, 9, 200, 100, "1/100/200/").await; // #9 same-project
    seed_merge_request(ctx, 30, 42, 400, "1/100/400/").await; // cross-project !42 (same top-level)

    // Source MR (id 1000, iid 1) lives in my-proj.
    // - cross_reference to !5 (same project) -> MENTIONS MR->MR
    seed_system_note(
        ctx,
        1,
        "mentioned in !5",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;
    // - cross_reference to #9 (same project) -> MENTIONS MR->WorkItem
    seed_system_note(
        ctx,
        2,
        "mentioned in #9",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 10:00:00",
    )
    .await;
    // - cross_reference to my-group/other-proj!42 -> MENTIONS MR->MR
    //   (cross-project, same top-level namespace)
    seed_system_note(
        ctx,
        3,
        "mentioned in my-group/other-proj!42",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 11:00:00",
    )
    .await;
    // - reopened on a work item -> User REOPENED WorkItem
    seed_system_note(
        ctx,
        4,
        "reopened",
        "reopened",
        "Issue",
        777,
        2,
        200,
        "1/100/200/",
        "2024-01-15 12:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("system-notes handler should succeed");

    // Three MENTIONS edges, sourced from MR 1000.
    let mentions = ctx
        .query(&format!(
            "SELECT source_id, target_id, target_kind, traversal_path FROM {} FINAL \
             WHERE relationship_kind = 'MENTIONS' ORDER BY target_id",
            t("gl_edge")
        ))
        .await;
    assert_eq!(edge_count(&mentions), 3, "expect exactly 3 MENTIONS edges");

    let target_id =
        ArrowUtils::get_column_by_name::<Int64Array>(&mentions[0], "target_id").unwrap();
    let target_kind =
        ArrowUtils::get_column_by_name::<StringArray>(&mentions[0], "target_kind").unwrap();
    let tp = ArrowUtils::get_column_by_name::<StringArray>(&mentions[0], "traversal_path").unwrap();
    // Targets sorted by id: MR 10 (!5), WI 20 (#9), MR 30 (!42).
    assert_eq!(target_id.value(0), 10);
    assert_eq!(target_kind.value(0), "MergeRequest");
    assert_eq!(target_id.value(1), 20);
    assert_eq!(target_kind.value(1), "WorkItem");
    assert_eq!(target_id.value(2), 30);
    assert_eq!(target_kind.value(2), "MergeRequest");
    // The cross-project edge lands in the target's namespace partition (the
    // target project is a sibling under the same top-level namespace).
    assert_eq!(
        tp.value(2),
        "1/100/400/",
        "cross-project MENTIONS uses the target's traversal_path"
    );

    // One REOPENED edge: User 2 -> WorkItem 777.
    let reopened = ctx
        .query(&format!(
            "SELECT source_id, target_id, source_kind, target_kind FROM {} FINAL \
             WHERE relationship_kind = 'REOPENED'",
            t("gl_edge")
        ))
        .await;
    assert_eq!(edge_count(&reopened), 1, "expect exactly 1 REOPENED edge");
    let src = ArrowUtils::get_column_by_name::<Int64Array>(&reopened[0], "source_id").unwrap();
    let tgt = ArrowUtils::get_column_by_name::<Int64Array>(&reopened[0], "target_id").unwrap();
    let src_kind =
        ArrowUtils::get_column_by_name::<StringArray>(&reopened[0], "source_kind").unwrap();
    let tgt_kind =
        ArrowUtils::get_column_by_name::<StringArray>(&reopened[0], "target_kind").unwrap();
    assert_eq!(src.value(0), 2, "REOPENED source is the note author");
    assert_eq!(tgt.value(0), 777);
    assert_eq!(src_kind.value(0), "User");
    assert_eq!(tgt_kind.value(0), "WorkItem");

    // And nothing else: only MENTIONS + REOPENED were produced.
    let total = ctx
        .query(&format!("SELECT 1 FROM {} FINAL", t("gl_edge")))
        .await;
    assert_eq!(
        edge_count(&total),
        4,
        "exactly 4 edges total (3 MENTIONS + 1 REOPENED)"
    );
}

/// P1 fix, end-to-end: a lifecycle action (`closed`) on a `Commit` noteable
/// must NOT materialize an edge - there is no `Commit` node and the edge
/// variant is undeclared.
pub async fn commit_noteable_lifecycle_produces_no_edge(ctx: &TestContext) {
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

    // `closed` on a Commit noteable - must be dropped.
    seed_system_note(
        ctx,
        1,
        "closed",
        "closed",
        "Commit",
        555,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;
    // A valid `closed` on a work item, to prove the handler ran and writes
    // the declared variant.
    seed_system_note(
        ctx,
        2,
        "closed",
        "closed",
        "Issue",
        777,
        1,
        200,
        "1/100/200/",
        "2024-01-15 10:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("system-notes handler should succeed");

    let closed = ctx
        .query(&format!(
            "SELECT target_kind FROM {} FINAL WHERE relationship_kind = 'CLOSED'",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        edge_count(&closed),
        1,
        "only the WorkItem CLOSED edge survives"
    );
    let tk = ArrowUtils::get_column_by_name::<StringArray>(&closed[0], "target_kind").unwrap();
    assert_eq!(tk.value(0), "WorkItem");

    // No Commit-targeted edge of any kind.
    let commit_edges = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL WHERE target_kind = 'Commit'",
            t("gl_edge")
        ))
        .await;
    let cnt = ArrowUtils::get_column_by_name::<UInt64Array>(&commit_edges[0], "cnt").unwrap();
    assert_eq!(cnt.value(0), 0, "no Commit-targeted edge may be emitted");
}

/// P1 fix, end-to-end: same-project shorthand (`#9` with no project prefix)
/// resolves against the source note's owning project (`default_project`),
/// not against an empty path.
pub async fn same_project_reference_resolves_via_default_project(ctx: &TestContext) {
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
    seed_work_item(ctx, 20, 9, 200, 100, "1/100/200/").await;

    // `#9` carries no project prefix; the note's project_id (200) must supply
    // the default project so the work item resolves.
    seed_system_note(
        ctx,
        1,
        "mentioned in #9",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("system-notes handler should succeed");

    let mentions = ctx
        .query(&format!(
            "SELECT target_id, target_kind FROM {} FINAL WHERE relationship_kind = 'MENTIONS'",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        edge_count(&mentions),
        1,
        "same-project #9 must resolve to one edge"
    );
    let tid = ArrowUtils::get_column_by_name::<Int64Array>(&mentions[0], "target_id").unwrap();
    let tk = ArrowUtils::get_column_by_name::<StringArray>(&mentions[0], "target_kind").unwrap();
    assert_eq!(tid.value(0), 20);
    assert_eq!(tk.value(0), "WorkItem");
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

/// Negative cases: an unknown action and an unsupported noteable type are
/// both dropped without producing edges (and without failing the handler).
/// The `unknown_action` is pre-filtered out by the extract query's
/// `action IN (...)` IN-list, so it never reaches the parser; the
/// unsupported noteable type is dropped in `process_batch`. (The
/// `unknown_action` counter increment is asserted in the handler unit
/// tests, where the meter is observable.)
pub async fn drops_unknown_action_and_unsupported_noteable_type(ctx: &TestContext) {
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
    seed_work_item(ctx, 10, 9, 200, 100, "1/100/200/").await;

    // Unknown action (`description` is in ICON_TYPES but not handled): the
    // extract IN-list never selects it, so no edge.
    seed_system_note(
        ctx,
        1,
        "changed the description",
        "description",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;
    // Unsupported noteable type for a lifecycle action: dropped in process_batch.
    seed_system_note(
        ctx,
        2,
        "closed",
        "closed",
        "Snippet",
        2000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 10:00:00",
    )
    .await;
    // A valid note so we can prove the handler ran and writes the good edge.
    seed_system_note(
        ctx,
        3,
        "mentioned in #9",
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
        .expect("handler should succeed even with dropped rows");

    let total = ctx
        .query(&format!("SELECT 1 FROM {} FINAL", t("gl_edge")))
        .await;
    assert_eq!(
        edge_count(&total),
        1,
        "only the valid cross_reference note produces an edge"
    );
    let mentions = ctx
        .query(&format!(
            "SELECT target_id FROM {} FINAL WHERE relationship_kind = 'MENTIONS'",
            t("gl_edge")
        ))
        .await;
    let tid = ArrowUtils::get_column_by_name::<Int64Array>(&mentions[0], "target_id").unwrap();
    assert_eq!(tid.value(0), 10);
}

/// Insert a `siphon_routes` row with explicit `id` and replication time, so
/// a test can stage a stale + reconciled pair for the same PG route.
#[allow(clippy::too_many_arguments)]
async fn insert_route_version(
    ctx: &TestContext,
    id: i64,
    source_id: i64,
    source_type: &str,
    path: &str,
    namespace_id: i64,
    traversal_path: &str,
    replicated_at: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO siphon_routes \
         (id, source_id, source_type, path, namespace_id, traversal_path, created_at, updated_at, _siphon_replicated_at) \
         VALUES ({id}, {source_id}, '{source_type}', '{path}', {namespace_id}, '{traversal_path}', \
                 '2023-01-01', '2024-01-15', '{replicated_at}')"
    ))
    .await;
}

/// Regression for the cross-project `0/` bug: `siphon_routes` is a
/// ReplacingMergeTree whose sort key includes `traversal_path`, so the
/// traversal-path reconciler's stale (`0/`) and reconciled (`1/100/94/`) rows
/// for the same project coexist and never collapse under `FINAL`. The
/// resolver must deduplicate by PG primary key + `argMax(_siphon_replicated_at)`
/// and pick the reconciled row, so a cross-project MENTIONS lands in the
/// target's namespace partition rather than `0/`.
///
/// The target project is a sibling under the *same top-level namespace* as
/// the source (`1/100/200/` -> `1/100/94/`): v1 bounds resolution to the
/// source top-level namespace, and this test isolates the `argMax` dedup, not
/// the cross-top-level behaviour. Note the stale `0/` row also falls outside
/// the `1/100/` prefix, so picking it (the bug) would additionally drop the
/// edge — making this a doubly-strong guard under the bounded resolver.
pub async fn cross_project_mentions_uses_reconciled_route_not_stale_zero(ctx: &TestContext) {
    // Source project (where the note lives) and its route.
    create_route(
        ctx,
        1,
        200,
        "Project",
        "src-group/src-proj",
        100,
        "1/100/200/",
    )
    .await;

    // Target project route, present as BOTH a stale 0/ row and a later
    // reconciled 1/100/94/ row for the same PG id (id = 2). Insert the stale
    // row second by wall-clock-of-insertion to make ordering adversarial:
    // the reconciled row carries the larger _siphon_replicated_at, so only
    // argMax (not row order, not FINAL) resolves it correctly.
    insert_route_version(
        ctx,
        2,
        400,
        "Project",
        "src-group/proj-a",
        94,
        "1/100/94/",
        "2024-01-20 12:05:00",
    )
    .await;
    insert_route_version(
        ctx,
        2,
        400,
        "Project",
        "src-group/proj-a",
        94,
        "0/",
        "2024-01-20 12:00:00",
    )
    .await;

    // The cross-project target work item lives in the reconciled namespace.
    seed_work_item(ctx, 30, 7, 400, 94, "1/100/94/").await;

    // A note in the source project cross-references the target by full path.
    seed_system_note(
        ctx,
        1,
        "mentioned in src-group/proj-a#7",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("system-notes handler should succeed");

    let mentions = ctx
        .query(&format!(
            "SELECT target_id, traversal_path FROM {} FINAL WHERE relationship_kind = 'MENTIONS'",
            t("gl_edge")
        ))
        .await;
    assert_eq!(
        edge_count(&mentions),
        1,
        "cross-project reference resolves to one edge"
    );
    let tid = ArrowUtils::get_column_by_name::<Int64Array>(&mentions[0], "target_id").unwrap();
    let tp = ArrowUtils::get_column_by_name::<StringArray>(&mentions[0], "traversal_path").unwrap();
    assert_eq!(tid.value(0), 30);
    assert_eq!(
        tp.value(0),
        "1/100/94/",
        "edge must land in the reconciled namespace partition, not the stale 0/ route"
    );
}

/// v1 limitation guard: a cross-reference whose target lives in a *different
/// top-level namespace* is intentionally NOT resolved. The resolver bounds
/// every datalake scan to the source note's top-level namespace prefix
/// (`startsWith(traversal_path, "{org}/{top_level_ns}/")`) so it never
/// full-scans the shared Siphon datalake; the cost is that cross-top-level
/// references are deferred (see ADR 013 "Coverage and known limitations").
/// This asserts the deliberate drop so the limitation can't silently regress
/// into either a wrong edge or an unbounded scan.
pub async fn cross_top_level_reference_is_not_resolved(ctx: &TestContext) {
    // Source project under top-level namespace 100.
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
    // Target project under a DIFFERENT top-level namespace (300). Its route
    // and work item are fully present and correct — the only reason the edge
    // must not materialize is the top-level-namespace bound.
    create_route(
        ctx,
        2,
        400,
        "Project",
        "other-group/other-proj",
        300,
        "1/300/400/",
    )
    .await;
    seed_work_item(ctx, 30, 7, 400, 300, "1/300/400/").await;

    // A same-top-level target, to prove the handler ran and resolves what it
    // should within the bound.
    seed_work_item(ctx, 40, 9, 200, 100, "1/100/200/").await;

    // Cross-top-level reference by full path: must be dropped.
    seed_system_note(
        ctx,
        1,
        "mentioned in other-group/other-proj#7",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 09:00:00",
    )
    .await;
    // Same-top-level reference: must resolve.
    seed_system_note(
        ctx,
        2,
        "mentioned in #9",
        "cross_reference",
        "MergeRequest",
        1000,
        1,
        200,
        "1/100/200/",
        "2024-01-15 10:00:00",
    )
    .await;

    system_notes_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("system-notes handler should succeed");

    let mentions = ctx
        .query(&format!(
            "SELECT target_id, traversal_path FROM {} FINAL WHERE relationship_kind = 'MENTIONS'",
            t("gl_edge")
        ))
        .await;
    // Only the same-top-level #9 edge resolves; the cross-top-level #7 drops.
    assert_eq!(
        edge_count(&mentions),
        1,
        "only the same-top-level reference resolves; cross-top-level is deferred"
    );
    let tid = ArrowUtils::get_column_by_name::<Int64Array>(&mentions[0], "target_id").unwrap();
    assert_eq!(
        tid.value(0),
        40,
        "the resolved edge targets the same-top-level work item, not the cross-top-level one"
    );
}
