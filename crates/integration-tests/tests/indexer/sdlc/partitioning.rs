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
        .handle(handler_context(ctx), global_envelope())
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

pub async fn incomplete_partition_checkpoint_does_not_advance_watermark_on_resume(
    ctx: &TestContext,
) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User.p2of4', '2024-01-20 12:00:00.000000', '{{\"c\":[\"6\"]}}')",
        t("checkpoint")
    ))
    .await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(ctx), global_envelope())
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
        vec![1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12],
        "re-partition opens from the epoch: id 6 is skipped by the partition cursor, \
         but 7-8 (sharing the in-progress watermark) must still index"
    );
}

pub async fn unfinished_partition_blocks_parent_consolidation(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User.p5of6', '2024-01-20 12:00:00.000000', '{{\"c\":[\"6\"]}}')",
        t("checkpoint")
    ))
    .await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(ctx), global_envelope())
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

pub async fn second_run_after_consolidation_skips_partitioning(ctx: &TestContext) {
    for id in 1..=8 {
        create_user(ctx, id).await;
    }

    let handler = entity_handler_with_partitions(ctx, "User", 4).await;
    handler
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("first run should succeed");

    // Newest partition-checkpoint write from the initial load and its
    // consolidation. Comparing against `_version` is merge-invariant; counting
    // raw rows is not, because background merges collapse the tombstoned
    // duplicates between the two reads.
    let after_first = ctx
        .query(&format!(
            "SELECT toString(max(_version)) AS v FROM {} \
             WHERE startsWith(key, 'global.User.p')",
            t("checkpoint")
        ))
        .await;
    let newest_partition_write =
        ArrowUtils::get_column_by_name::<StringArray>(&after_first[0], "v")
            .expect("v column")
            .value(0)
            .to_string();

    handler
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("second run should succeed");

    let new_rows = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} \
             WHERE startsWith(key, 'global.User.p') AND _version > '{}'",
            t("checkpoint"),
            newest_partition_write
        ))
        .await;
    let cnt =
        ArrowUtils::get_column_by_name::<UInt64Array>(&new_rows[0], "cnt").expect("cnt column");
    assert_eq!(
        cnt.value(0),
        0,
        "incremental run over the consolidated parent must not write new partition checkpoint rows"
    );
}

pub async fn skips_already_completed_partitions_on_retry(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    let prior_watermark = "2024-01-15 00:00:00.000000";
    for index in 0..2 {
        ctx.execute(&format!(
            "INSERT INTO {} (key, watermark, cursor_values) \
             VALUES ('global.User.p{index}of4', '{prior_watermark}', 'null')",
            t("checkpoint")
        ))
        .await;
    }

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

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

    let result = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL",
            t("gl_user")
        ))
        .await;
    let count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "cnt").expect("cnt column");
    // T-digest cuts on ids 1..=12 with count=4 produce [1, 3, 6, 9, 13]
    // → p0=[1,3), p1=[3,6), p2=[6,9), p3=[9,13).
    // p0 and p1 are pre-marked done, so p2+p3 index ids 6..=12 = 7 rows.
    assert_eq!(
        count.value(0),
        7,
        "only p2/p3 should have indexed (ids 6..=12); p0/p1 were skipped"
    );
}

pub async fn all_partitions_completed_runs_consolidate_only(ctx: &TestContext) {
    let prior_watermark = "2024-01-15 00:00:00.000000";
    for index in 0..4 {
        ctx.execute(&format!(
            "INSERT INTO {} (key, watermark, cursor_values) \
             VALUES ('global.User.p{index}of4', '{prior_watermark}', 'null')",
            t("checkpoint")
        ))
        .await;
    }

    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    let inserted_rows = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL",
            t("gl_user")
        ))
        .await;
    let inserted_count = ArrowUtils::get_column_by_name::<UInt64Array>(&inserted_rows[0], "cnt")
        .expect("cnt column");
    assert_eq!(
        inserted_count.value(0),
        0,
        "every partition was already completed: nothing should be extracted"
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
        "parent should consolidate at the partition watermark, got: {parent_watermark}"
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
        "all four partition checkpoints should be tombstoned"
    );
}

pub async fn query_etl_entity_partitions_by_id_within_scope(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    for id in 101..=112 {
        create_namespace(ctx, id, Some(100), 0, &format!("1/100/{id}/")).await;
    }

    entity_handler_with_partitions(ctx, "Group", 4)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("Group partitioned handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let indexed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        indexed,
        (100..=112).collect::<Vec<_>>(),
        "Group partitions probe namespace_traversal_paths (its etl source) and should land all 13 rows"
    );

    let tombstoned = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} \
             WHERE startsWith(key, 'ns.100.Group.p') AND _deleted = true",
            t("checkpoint")
        ))
        .await;
    let tombstone_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&tombstoned[0], "cnt").expect("cnt column");
    assert_eq!(
        tombstone_count.value(0),
        4,
        "all four partition checkpoints should be tombstoned"
    );
}

pub async fn namespaced_entity_partitions_by_id_within_scope(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    for id in 1..=12 {
        ctx.execute(&format!(
            "INSERT INTO siphon_milestones \
             (id, iid, title, description, state, project_id, traversal_path, _siphon_replicated_at) \
             VALUES ({id}, {id}, 'v0.{id}', '', 'active', 1000, '1/100/', '2024-01-20 12:00:00')"
        ))
        .await;
    }

    entity_handler_with_partitions(ctx, "Milestone", 4)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("namespaced partitioned handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_milestone")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let indexed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        indexed,
        (1..=12).collect::<Vec<_>>(),
        "every partition slice on (traversal_path, id) should land in gl_milestone"
    );

    let consolidated = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} FINAL \
             WHERE key = 'ns.100.Milestone' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    let consolidated_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&consolidated[0], "cnt").expect("cnt column");
    assert_eq!(
        consolidated_count.value(0),
        1,
        "parent checkpoint should be consolidated at ns.100.Milestone"
    );

    let tombstoned = ctx
        .query(&format!(
            "SELECT count() AS cnt FROM {} \
             WHERE startsWith(key, 'ns.100.Milestone.p') AND _deleted = true",
            t("checkpoint")
        ))
        .await;
    let tombstone_count =
        ArrowUtils::get_column_by_name::<UInt64Array>(&tombstoned[0], "cnt").expect("cnt column");
    assert_eq!(
        tombstone_count.value(0),
        4,
        "all four namespaced partition checkpoints should be tombstoned"
    );
}

pub async fn span_smaller_than_partition_count_falls_back_to_single_run(ctx: &TestContext) {
    create_user(ctx, 1).await;
    create_user(ctx, 2).await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(ctx), global_envelope())
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
