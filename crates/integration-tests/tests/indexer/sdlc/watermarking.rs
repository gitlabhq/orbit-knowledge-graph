use arrow::array::{Int64Array, StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_namespace, handler_context, namespace_envelope, namespace_handler,
};

pub async fn uses_watermark_for_incremental_processing(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('ns.100.Group', '2024-01-19 00:00:00.000000', 'null')",
        t("checkpoint")
    ))
    .await;

    ctx.execute(
        "INSERT INTO siphon_namespaces (id, name, path, type, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (100, 'org1', 'org1', 'Group', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-18 12:00:00'),
        (101, 'new-team', 'new-team', 'Group', 10, 100, NULL, '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_namespace_details (namespace_id, description)
        VALUES (100, 'Old org'), (101, 'New team')",
    )
    .await;

    ctx.execute(
        "INSERT INTO namespace_traversal_paths (id, traversal_path)
        VALUES (100, '1/100/'), (101, '1/100/101/')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT count() as cnt FROM {} FINAL",
            t("gl_group")
        ))
        .await;
    let count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "cnt").expect("cnt column");
    assert_eq!(count.value(0), 1, "should only process new-team, not org1");

    let names = ctx
        .query(&format!("SELECT name FROM {} FINAL", t("gl_group")))
        .await;
    let name =
        ArrowUtils::get_column_by_name::<StringArray>(&names[0], "name").expect("name column");
    assert_eq!(name.value(0), "new-team");
}

/// Validates keyset cursor resume with a composite sort key. Group plans sort
/// by `[traversal_path, id]`, so a saved cursor `["1/100/102/", "102"]` must
/// emit the DNF
/// `(traversal_path > '1/100/102/') OR (traversal_path = '1/100/102/' AND id > '102')`.
/// Groups 100–102 (lexicographically ≤ cursor) must be skipped; groups 103–104
/// (lexicographically greater traversal_path) must process.
pub async fn resumes_from_saved_cursor_skipping_processed_groups(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_namespace(ctx, 101, Some(100), 0, "1/100/101/").await;
    create_namespace(ctx, 102, Some(100), 0, "1/100/102/").await;
    create_namespace(ctx, 103, Some(100), 0, "1/100/103/").await;
    create_namespace(ctx, 104, Some(100), 0, "1/100/104/").await;

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('ns.100.Group', '2024-01-21 00:00:00.000000', '{{\"c\":[\"1/100/102/\", \"102\"]}}')",
        t("checkpoint")
    ))
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let processed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        processed,
        vec![103, 104],
        "composite cursor at (1/100/102/, 102) must skip groups 100-102 and process 103-104"
    );
}
