use arrow::array::{BooleanArray, Int64Array, StringArray};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_node_count, create_namespace, handler_context, namespace_envelope,
    namespace_handler,
};

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
