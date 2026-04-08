//! End-to-end V0 schema reset cycle test.
//!
//! Exercises the full sequence described in the issue:
//!   1. Set up: datalake populated, namespace enabled
//!   2. Initial indexing: SDLC + global data indexed into graph tables
//!   3. Simulate schema change: bump SCHEMA_VERSION in ClickHouse to something old
//!   4. Simulate namespace disable: mark namespace as siphon-deleted
//!   5. Detection: check_version returns ResetReady
//!   6. Reset: schema_reset drops and recreates tables
//!   7. Simulate namespace re-enable: insert enabled namespace row (siphon Insert event)
//!   8. Re-indexing: dispatch cycle fires, all data re-indexed
//!   9. Verify: graph tables contain complete, correct data

use arrow::array::UInt64Array;
use clickhouse_client::ClickHouseConfigurationExt;
use gkg_utils::arrow::ArrowUtils;
use indexer::schema_version::{self, SCHEMA_VERSION};
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use opentelemetry::global;

use crate::indexer::common::{
    assert_node_count, create_namespace, create_project, create_user, global_envelope,
    global_handler, handler_context, namespace_envelope, namespace_handler,
};

pub async fn full_v0_cycle() {
    // Use a fresh context with both siphon and graph schemas.
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    // -------------------------------------------------------------------------
    // Phase 1: Populate datalake and run initial indexing
    // -------------------------------------------------------------------------
    create_user(&ctx, 1).await;
    create_namespace(&ctx, 100, None, 0, "1/100/").await;
    ctx.execute(
        "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
         (id, root_namespace_id, _siphon_deleted, _siphon_replicated_at, created_at, updated_at) \
         VALUES (1, 100, false, now(), now(), now())",
    )
    .await;
    create_project(&ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_project(&ctx, 1001, 100, 1, 0, "1/100/1001/").await;

    // Run global handler to index users.
    global_handler(&ctx)
        .await
        .handle(handler_context(&ctx), global_envelope())
        .await
        .expect("initial global indexing should succeed");

    // Run namespace handler to index projects and groups.
    namespace_handler(&ctx)
        .await
        .handle(handler_context(&ctx), namespace_envelope(1, 100))
        .await
        .expect("initial namespace indexing should succeed");

    // Verify initial data is present.
    assert_node_count(&ctx, "gl_user", 1).await;
    assert_node_count(&ctx, "gl_project", 2).await;

    // Write the current schema version as if we just deployed.
    let graph = ctx.config.build_client();
    schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");
    schema_version::write_schema_version(&graph, SCHEMA_VERSION)
        .await
        .expect("write initial schema version should succeed");

    // -------------------------------------------------------------------------
    // Phase 2: Simulate a schema change — write an old version
    // -------------------------------------------------------------------------
    // In production this is done by deploying a new binary with a bumped SCHEMA_VERSION.
    // In the test we simulate it by writing an old version to ClickHouse so that
    // check_version sees a mismatch.
    schema_version::write_schema_version(&graph, SCHEMA_VERSION - 1)
        .await
        .expect("write old schema version should succeed");

    // -------------------------------------------------------------------------
    // Phase 3: Simulate namespace disable (admin action in Rails)
    // -------------------------------------------------------------------------
    // Truncate the enabled-namespaces table so count_enabled_namespaces returns 0.
    // In production, Rails deletes enabled namespace rows; Siphon replicates these
    // as CDC delete events that mark the row _siphon_deleted = true. The
    // count_enabled_namespaces query does not use FINAL, so TRUNCATE is the
    // simplest way to guarantee the count returns 0 without needing OPTIMIZE.
    ctx.execute("TRUNCATE TABLE siphon_knowledge_graph_enabled_namespaces")
        .await;

    // -------------------------------------------------------------------------
    // Phase 4: Detection — check_version should return ResetReady
    // -------------------------------------------------------------------------
    let meter = global::meter("test");
    let gauge = meter.u64_gauge("test.e2e.mismatch").build();
    let outcome = schema_version::check_version(&graph, &ctx.config.build_client(), &gauge)
        .await
        .expect("check_version should succeed");
    assert_eq!(
        outcome,
        schema_version::CheckOutcome::ResetReady,
        "should be ResetReady after namespace disable with schema mismatch"
    );

    // -------------------------------------------------------------------------
    // Phase 5: Reset — drop and recreate all GKG-owned tables
    // -------------------------------------------------------------------------
    schema_version::schema_reset(&graph, SCHEMA_VERSION)
        .await
        .expect("schema_reset should succeed");

    // Verify tables are empty after reset.
    let user_count_after_reset: Vec<_> = ctx.query("SELECT count() AS cnt FROM gl_user").await;
    let user_cnt = ArrowUtils::get_column_by_name::<UInt64Array>(&user_count_after_reset[0], "cnt")
        .expect("cnt column")
        .value(0);
    assert_eq!(user_cnt, 0, "gl_user must be empty after schema reset");

    let project_count_after_reset: Vec<_> =
        ctx.query("SELECT count() AS cnt FROM gl_project").await;
    let project_cnt =
        ArrowUtils::get_column_by_name::<UInt64Array>(&project_count_after_reset[0], "cnt")
            .expect("cnt column")
            .value(0);
    assert_eq!(
        project_cnt, 0,
        "gl_project must be empty after schema reset"
    );

    let checkpoint_count_after_reset: Vec<_> =
        ctx.query("SELECT count() AS cnt FROM checkpoint").await;
    let cp_cnt =
        ArrowUtils::get_column_by_name::<UInt64Array>(&checkpoint_count_after_reset[0], "cnt")
            .expect("cnt column")
            .value(0);
    assert_eq!(
        cp_cnt, 0,
        "checkpoint table must be empty after schema reset"
    );

    // Version should be recorded after reset.
    ctx.optimize_all().await;
    let version = schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed after reset");
    assert_eq!(
        version,
        Some(SCHEMA_VERSION),
        "new schema version must be recorded after reset"
    );

    // -------------------------------------------------------------------------
    // Phase 6: Simulate namespace re-enable (admin action in Rails)
    // -------------------------------------------------------------------------
    // In production, the admin re-enables namespaces via the Rails UI, which produces
    // fresh Siphon CDC insert events for knowledge_graph_enabled_namespaces.
    ctx.execute(
        "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
         (id, root_namespace_id, _siphon_deleted, _siphon_replicated_at, created_at, updated_at) \
         VALUES (2, 100, false, now(), now(), now())",
    )
    .await;

    // -------------------------------------------------------------------------
    // Phase 7: Re-indexing — existing pipeline handles this naturally
    // -------------------------------------------------------------------------
    // The GlobalHandler starts from epoch-zero (no checkpoint) and re-indexes all users.
    global_handler(&ctx)
        .await
        .handle(handler_context(&ctx), global_envelope())
        .await
        .expect("post-reset global re-indexing should succeed");

    // The NamespaceHandler starts from epoch-zero and re-indexes all projects.
    namespace_handler(&ctx)
        .await
        .handle(handler_context(&ctx), namespace_envelope(1, 100))
        .await
        .expect("post-reset namespace re-indexing should succeed");

    // -------------------------------------------------------------------------
    // Phase 8: Verify — graph tables contain all original data
    // -------------------------------------------------------------------------
    assert_node_count(&ctx, "gl_user", 1).await;
    assert_node_count(&ctx, "gl_project", 2).await;

    // Check version is current after the full cycle.
    let final_outcome = schema_version::check_version(&graph, &ctx.config.build_client(), &gauge)
        .await
        .expect("check_version should succeed");
    assert_eq!(
        final_outcome,
        schema_version::CheckOutcome::Current,
        "schema version should be Current after full re-index cycle"
    );
}
