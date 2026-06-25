use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_namespace, create_user, entity_handler_with_partitions, global_envelope,
    handler_context, namespace_envelope,
};

pub async fn partitioned_initial_load_indexes_all_rows_and_consolidates(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("partitioned handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_user")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let indexed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        indexed,
        (1..=12).collect::<Vec<_>>(),
        "every partition should contribute exactly its slice with no gaps or overlaps"
    );

    let consolidated = ctx
        .query(&format!(
            "SELECT cursor_values FROM {} FINAL WHERE key = 'global.User' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let consolidated_cursor =
        ArrowUtils::get_column_by_name::<StringArray>(&consolidated[0], "cursor_values")
            .expect("cursor_values column");
    assert_eq!(
        consolidated_cursor.len(),
        1,
        "exactly one consolidated checkpoint should exist at global.User"
    );
    assert!(
        consolidated_cursor.value(0).is_empty() || consolidated_cursor.value(0) == "null",
        "consolidated checkpoint should be completed (cursor empty), got: {}",
        consolidated_cursor.value(0)
    );

    let live_partitions = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL \
             WHERE startsWith(key, 'global.User.p') AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let count = ArrowUtils::get_column_by_name::<UInt64Array>(&live_partitions[0], "cnt")
        .expect("cnt column");
    assert_eq!(
        count.value(0),
        0,
        "partition checkpoints should be tombstoned after consolidation"
    );

    let tombstoned = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} \
             WHERE startsWith(key, 'global.User.p') AND _deleted = true",
            t("checkpoint")
        ))
        .await;
    let tombstone_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&tombstoned[0], "cnt").expect("cnt column");
    assert_eq!(
        tombstone_count.value(0),
        4,
        "all four partition rows should have a tombstone marker"
    );
}

pub async fn unfinished_partition_blocks_parent_consolidation(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version) \
         VALUES ('global.User.p5of6', '2024-01-20 12:00:00.000000', '{{\"c\":[\"6\"]}}', \
                 '2024-01-20 12:00:00.000000')",
        t("checkpoint")
    ))
    .await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("deferred consolidation must surface as Ok, not a pipeline error");

    let parent = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL \
             WHERE key = 'global.User' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let parent_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&parent[0], "cnt").expect("cnt column");
    assert_eq!(
        parent_count.value(0),
        0,
        "parent must stay absent so the next dispatch re-triggers partitioning"
    );

    let leftover = ctx
        .query(&format!(
            "SELECT cursor_values FROM {} FINAL \
             WHERE key = 'global.User.p5of6' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let leftover_cursor =
        ArrowUtils::get_column_by_name::<StringArray>(&leftover[0], "cursor_values")
            .expect("cursor_values column");
    assert_eq!(
        leftover_cursor.len(),
        1,
        "the unfinished partition must not be tombstoned, so its range is re-pulled"
    );
    assert!(
        !leftover_cursor.value(0).is_empty() && leftover_cursor.value(0) != "null",
        "the unfinished partition must keep its resume cursor, got: {}",
        leftover_cursor.value(0)
    );
}

/// A partition-configured entity whose parent checkpoint already exists does not
/// re-partition: `should_partition` requires an absent parent. The in-progress
/// parent (cursor + floor) drives the single-pull resume path, which must honor
/// the floor and write no partition checkpoint rows.
pub async fn present_parent_takes_single_pull_path_and_honors_floor(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version) \
         VALUES ('global.User', '2024-01-20 00:00:00.000000', \
                 '{{\"c\":[\"2\"],\"f\":\"2024-01-10T00:00:00Z\"}}', \
                 '2024-01-20 00:00:00.000000')",
        t("checkpoint")
    ))
    .await;

    super::windowing::insert_user_at(ctx, 3, "2024-01-05 00:00:00").await;
    super::windowing::insert_user_at(ctx, 4, "2024-01-15 00:00:00").await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("partitioned handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_user")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let indexed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        indexed,
        vec![4],
        "single-pull resume must stay within (floor, target]: user 3 (below the floor) is skipped"
    );

    let partitions = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} WHERE startsWith(key, 'global.User.p')",
            t("checkpoint")
        ))
        .await;
    let partition_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&partitions[0], "cnt").expect("cnt column");
    assert_eq!(
        partition_count.value(0),
        0,
        "a present parent takes the single-pull path and writes no partition checkpoint rows"
    );
}

/// A partitioned retry with mixed pre-existing partition state. Bucket cuts on
/// ids 1..=12 with count=4 are [1, 4, 7, 10] -> p0=[1,4), p1=[4,7), p2=[7,10),
/// p3=[10,). p0/p1 are pre-completed (skipped), p2 is mid-pull at cursor 6
/// (below its range, so it pulls 7-9 in full), p3 is fresh. When the run
/// finishes every partition, the parent consolidates at the oldest partition
/// watermark and all partition checkpoints are tombstoned.
pub async fn retry_skips_completed_resumes_in_progress_and_pins_watermark(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    for index in 0..2 {
        ctx.execute(&format!(
            "INSERT INTO {} (key, watermark, cursor_values, _version) \
             VALUES ('global.User.p{index}of4', '2024-01-15 00:00:00.000000', 'null', \
                     '2024-01-15 00:00:00.000000')",
            t("checkpoint")
        ))
        .await;
    }
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version) \
         VALUES ('global.User.p2of4', '2024-01-16 00:00:00.000000', '{{\"c\":[\"6\"]}}', \
                 '2024-01-16 00:00:00.000000')",
        t("checkpoint")
    ))
    .await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("partitioned handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_user")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let indexed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        indexed,
        vec![7, 8, 9, 10, 11, 12],
        "p0/p1 skip ids 1-6; p2 pulls 7-9 (cursor 6 sits below its range); p3 indexes 10-12"
    );

    let parent = ctx
        .query(&format!(
            "SELECT toString(watermark) AS w FROM {} FINAL \
             WHERE key = 'global.User' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let parent_watermark = ArrowUtils::get_column_by_name::<StringArray>(&parent[0], "w")
        .expect("w column")
        .value(0)
        .to_string();
    assert!(
        parent_watermark.starts_with("2024-01-15"),
        "parent watermark should pin to oldest partition watermark (2024-01-15...), got: {parent_watermark}"
    );

    let live_partitions = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL \
             WHERE startsWith(key, 'global.User.p') AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let live_count = ArrowUtils::get_column_by_name::<UInt64Array>(&live_partitions[0], "cnt")
        .expect("cnt column");
    assert_eq!(
        live_count.value(0),
        0,
        "every partition finished, so all partition checkpoints are tombstoned"
    );
}

async fn assert_partitions_tombstoned(ctx: &TestContext, key_prefix: &str) {
    let tombstoned = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} WHERE startsWith(key, '{key_prefix}') AND _deleted = true",
            t("checkpoint")
        ))
        .await;
    let tombstone_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&tombstoned[0], "cnt").expect("cnt column");
    assert_eq!(
        tombstone_count.value(0),
        4,
        "all four partition checkpoints under {key_prefix} should be tombstoned"
    );
}

async fn assert_indexed_ids(ctx: &TestContext, table: &str, expected: Vec<i64>, message: &str) {
    let result = ctx
        .query(&format!("SELECT id FROM {} FINAL ORDER BY id", t(table)))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let indexed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(indexed, expected, "{message}");
}

/// Namespaced partitioning over both etl source shapes: `Group` is query-ETL
/// (its partition probe scans `namespace_traversal_paths`), `Milestone` is a
/// namespaced siphon table keyed on `(traversal_path, id)`. Both must scope the
/// partition ranges by `startsWith(traversal_path, ...)` and land every row.
pub async fn namespaced_entities_partition_by_id_within_scope(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    for id in 101..=112 {
        create_namespace(ctx, id, Some(100), 0, &format!("1/100/{id}/")).await;
    }
    for id in 1..=12 {
        ctx.execute(&format!(
            "INSERT INTO siphon_milestones \
             (id, iid, title, description, state, project_id, traversal_path, _siphon_replicated_at) \
             VALUES ({id}, {id}, 'v0.{id}', '', 'active', 1000, '1/100/', '2024-01-20 12:00:00')"
        ))
        .await;
    }

    entity_handler_with_partitions(ctx, "Group", 4)
        .await
        .handle(handler_context(), namespace_envelope(1, 100))
        .await
        .expect("Group partitioned handler should succeed");
    assert_indexed_ids(
        ctx,
        "gl_group",
        (100..=112).collect(),
        "Group query-ETL partitions should land all 13 namespaces in scope",
    )
    .await;
    assert_partitions_tombstoned(ctx, "ns.100.Group.p").await;

    entity_handler_with_partitions(ctx, "Milestone", 4)
        .await
        .handle(handler_context(), namespace_envelope(1, 100))
        .await
        .expect("namespaced partitioned handler should succeed");
    assert_indexed_ids(
        ctx,
        "gl_milestone",
        (1..=12).collect(),
        "every (traversal_path, id) partition slice should land in gl_milestone",
    )
    .await;
    assert_partitions_tombstoned(ctx, "ns.100.Milestone.p").await;
}

pub async fn span_smaller_than_partition_count_falls_back_to_single_run(ctx: &TestContext) {
    create_user(ctx, 1).await;
    create_user(ctx, 2).await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("handler should succeed even when span is too small");

    let result = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL",
            t("gl_user")
        ))
        .await;
    let count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "cnt").expect("cnt column");
    assert_eq!(count.value(0), 2);

    let partitions = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} \
             WHERE startsWith(key, 'global.User.p')",
            t("checkpoint")
        ))
        .await;
    let partition_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&partitions[0], "cnt").expect("cnt column");
    assert_eq!(
        partition_count.value(0),
        0,
        "fallback path must not create partition checkpoint rows"
    );

    let consolidated = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL \
             WHERE key = 'global.User' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let consolidated_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&consolidated[0], "cnt").expect("cnt column");
    assert_eq!(
        consolidated_count.value(0),
        1,
        "fallback should still write the parent checkpoint"
    );
}
