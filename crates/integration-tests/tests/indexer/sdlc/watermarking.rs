use arrow::array::UInt64Array;

use crate::indexer::common::{
    TestContext, get_string_column, handler_context, namespace_envelope, namespace_handler,
};

pub async fn uses_watermark_for_incremental_processing(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO checkpoint (key, watermark, cursor_values) \
         VALUES ('ns.100.Group', '2024-01-19 00:00:00.000000', 'null')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
        VALUES
        (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-18 12:00:00'),
        (101, 'new-team', 'new-team', 10, 100, NULL, '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
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

    let result = ctx.query("SELECT count() as cnt FROM gl_group FINAL").await;
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");
    assert_eq!(count.value(0), 1, "should only process new-team, not org1");

    let names = ctx.query("SELECT name FROM gl_group FINAL").await;
    let name = get_string_column(&names[0], "name");
    assert_eq!(name.value(0), "new-team");
}
