use arrow::array::{BooleanArray, Int64Array, StringArray};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edge_tags_by_target, assert_edges_have_traversal_path, assert_node_count,
    create_namespace, handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_work_items_with_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
        VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, description, author_id, state_id, work_item_type_id, confidential,
             milestone_id, namespace_id, assignees, label_ids,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'Fix login bug', 'Users cannot log in', 1, 1, 1, false, 10, 100, [2, 3], [(5, '2024-01-20 12:00:00'), (6, '2024-01-20 12:00:00'), (7, '2024-01-20 12:00:00')], '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'Add feature Y', 'New feature request', 2, 2, 5, true, NULL, 100, [], [(8, '2024-01-20 12:00:00')], '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_work_item", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT title, state, work_item_type FROM {} FINAL ORDER BY id",
            t("gl_work_item")
        ))
        .await;
    let batch = &result[0];

    let titles =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "title").expect("title column");
    assert_eq!(titles.value(0), "Fix login bug");
    assert_eq!(titles.value(1), "Add feature Y");

    let states =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "state").expect("state column");
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "closed");

    let work_item_types = ArrowUtils::get_column_by_name::<StringArray>(batch, "work_item_type")
        .expect("work_item_type column");
    assert_eq!(work_item_types.value(0), "issue");
    assert_eq!(work_item_types.value(1), "task");
}

pub async fn processes_work_item_single_value_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
        VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO namespace_traversal_paths (id, traversal_path, version)
        VALUES (100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, description, author_id, state_id, work_item_type_id, confidential,
             milestone_id, namespace_id, project_id, closed_by_id, traversal_path, _siphon_replicated_at)
        VALUES (1, 1, 'Test issue', 'Test description', 1, 2, 1, false, 10, 100, 1000, 2, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "WorkItem", "1/100/", 1).await;
    assert_edges_have_traversal_path(ctx, "IN_MILESTONE", "WorkItem", "Milestone", "1/100/", 1)
        .await;
    assert_edges_have_traversal_path(ctx, "IN_GROUP", "WorkItem", "Group", "1/100/", 1).await;
    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "WorkItem", "Project", "1/100/", 1).await;
    assert_edges_have_traversal_path(ctx, "CLOSED", "User", "WorkItem", "1/100/", 1).await;
}

pub async fn processes_standalone_assigned_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'Fix login bug', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'Add feature Y', 2, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_issue_assignees
            (user_id, issue_id, namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 1, 100, '1/100/', '2024-01-20 12:00:00'),
            (20, 1, 100, '1/100/', '2024-01-20 12:00:00'),
            (10, 2, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "ASSIGNED", "User", "WorkItem", "1/100/", 3).await;
    assert_edge_tags_by_target(
        ctx,
        "ASSIGNED",
        "User",
        "WorkItem",
        "target_tags",
        &[
            (1, &["state:opened", "wi_type:issue"]),
            (2, &["state:opened", "wi_type:task"]),
        ],
    )
    .await;
}

pub async fn processes_standalone_has_label_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'Fix login bug', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'Add feature Y', 2, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_label_links
            (id, label_id, target_id, target_type, created_at, updated_at,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 5, 1, 'Issue', '2024-01-15 10:00:00', '2024-01-15 10:00:00', 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 6, 1, 'Issue', '2024-01-15 10:00:00', '2024-01-15 10:00:00', 100, '1/100/', '2024-01-20 12:00:00'),
            (3, 7, 2, 'Issue', '2024-01-15 10:00:00', '2024-01-15 10:00:00', 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "HAS_LABEL", "WorkItem", "Label", "1/100/", 3).await;
}

pub async fn processes_work_item_parent_links(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'Epic: Q1 Goals', 1, 1, 8, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'Task: Design review', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (3, 3, 'Task: Implementation', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (4, 4, 'Sub-task: Frontend', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_work_item_parent_links
            (id, work_item_id, work_item_parent_id, namespace_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 2, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 3, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (3, 4, 3, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "CONTAINS", "WorkItem", "WorkItem", "1/100/", 3).await;
}

pub async fn processes_issue_links(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'Issue A', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'Issue B', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (3, 3, 'Issue C', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_issue_links
            (id, source_id, target_id, link_type, namespace_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 1, 2, 0, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 2, 3, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "RELATED_TO", "WorkItem", "WorkItem", "1/100/", 2).await;
}

/// Postgres `date` is wider than ClickHouse `Date32` (1900-01-01..2299-12-31), so a
/// single out-of-range row replicated by Siphon used to abort the whole SDLC pipeline.
/// The destination INSERT goes through ClickHouse's `ArrowBlockInputFormat`, which
/// validates Date32 values and rejects the entire Arrow batch with `Code: 321
/// VALUE_IS_OUT_OF_RANGE` if any row carries an out-of-range day count. The fix at
/// `crates/indexer/src/modules/sdlc/plan/lower.rs` clamps these values to NULL at the
/// extract projection. This test plants raw out-of-range Int32 day-counts directly
/// in the source `Date32` column (textual `INSERT … VALUES` would saturate), then
/// asserts the pipeline succeeds and the rows are NULLed in the destination.
pub async fn clamps_out_of_range_due_date_to_null(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    // Land the rows via JSONEachRow with `date_time_overflow_behavior='ignore'`.
    // That combination is the one input path that preserves the raw Int32 bytes
    // for an out-of-range `Date32`; textual VALUES, `CAST(int AS Date32)`, and
    // `toDate32(int)` all saturate to the boundary. Selecting those rows back via
    // ArrowStream (which is what `INSERT INTO gl_work_item` does) would fail with
    // `Code: 321 VALUE_IS_OUT_OF_RANGE` when the destination table validates them
    // unless the extract projection clamps the value first.
    //   id=1: -715643 days -> well below 1900-01-01 (production-observed value)
    //   id=2:  200000 days -> above 2299-12-31
    //   id=3:       0 days -> 1970-01-01 (in range, must survive unchanged)
    let body = "\
{\"id\":1,\"iid\":1,\"title\":\"Way past\",\"description\":\"\",\"author_id\":1,\"state_id\":1,\"work_item_type_id\":1,\"confidential\":false,\"namespace_id\":100,\"due_date\":-715643,\"traversal_path\":\"1/100/\",\"_siphon_replicated_at\":\"2024-01-20 12:00:00\"}
{\"id\":2,\"iid\":2,\"title\":\"Way future\",\"description\":\"\",\"author_id\":1,\"state_id\":1,\"work_item_type_id\":1,\"confidential\":false,\"namespace_id\":100,\"due_date\":200000,\"traversal_path\":\"1/100/\",\"_siphon_replicated_at\":\"2024-01-20 12:00:00\"}
{\"id\":3,\"iid\":3,\"title\":\"Epoch\",\"description\":\"\",\"author_id\":1,\"state_id\":1,\"work_item_type_id\":1,\"confidential\":false,\"namespace_id\":100,\"due_date\":0,\"traversal_path\":\"1/100/\",\"_siphon_replicated_at\":\"2024-01-20 12:00:00\"}";

    // Percent-encode only the spaces in the query string; the rest of the chars we
    // pass are URL-safe (alphanumerics and `_`).
    let url = format!(
        "{}/?database={}&date_time_overflow_behavior=ignore&query=INSERT%20INTO%20work_items%20FORMAT%20JSONEachRow",
        ctx.config.url, ctx.config.database
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .basic_auth(&ctx.config.username, ctx.config.password.as_deref())
        .body(body.to_string())
        .send()
        .await
        .expect("JSONEachRow INSERT request failed");
    assert!(
        resp.status().is_success(),
        "JSONEachRow INSERT failed: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default(),
    );

    // Pre-flight: confirm the source table really does contain out-of-range bytes.
    // Without the clamp, the destination INSERT (ArrowStream) would fail with
    // `Code: 321 VALUE_IS_OUT_OF_RANGE` on the indexer side.
    let preflight = ctx
        .query(
            "SELECT id, toInt64(CAST(due_date AS Int32)) AS days
             FROM work_items FINAL ORDER BY id",
        )
        .await;
    let days =
        ArrowUtils::get_column_by_name::<Int64Array>(&preflight[0], "days").expect("days column");
    assert_eq!(days.value(0), -715643);
    assert_eq!(days.value(1), 200000);
    assert_eq!(days.value(2), 0);

    // The handler must succeed end-to-end despite the poisoned source rows.
    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("namespace_handler must not fail on out-of-range Date32 input");

    assert_node_count(ctx, "gl_work_item", 3).await;

    // Assert each row's due_date: out-of-range rows must clamp to NULL, the in-range
    // row must preserve its value (1970-01-01). We fold `isNull` into a Bool so the
    // Arrow column is BooleanArray (ClickHouse's UInt8 maps to UInt8Array, which
    // would force a different downcast).
    let rows = ctx
        .query(&format!(
            "SELECT id, CAST(isNull(due_date) AS Bool) AS is_null,
                    toString(due_date) AS s
             FROM {} FINAL ORDER BY id",
            t("gl_work_item")
        ))
        .await;
    let batch = &rows[0];
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "id").expect("id column");
    let is_null =
        ArrowUtils::get_column_by_name::<BooleanArray>(batch, "is_null").expect("is_null column");
    let strs = ArrowUtils::get_column_by_name::<StringArray>(batch, "s").expect("s column");

    assert_eq!(ids.value(0), 1);
    assert!(is_null.value(0), "id=1 (-715643 days) should clamp to NULL");

    assert_eq!(ids.value(1), 2);
    assert!(is_null.value(1), "id=2 (200000 days) should clamp to NULL");

    assert_eq!(ids.value(2), 3);
    assert!(!is_null.value(2), "id=3 (epoch) should survive the clamp");
    assert_eq!(strs.value(2), "1970-01-01");
}
