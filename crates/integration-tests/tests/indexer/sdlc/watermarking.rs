use arrow::array::{StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{TestContext, handler_context, namespace_envelope, namespace_handler};

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
