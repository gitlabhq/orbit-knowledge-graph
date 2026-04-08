use std::sync::Arc;

use indexer::testkit::MockLockService;
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn schema_version_lifecycle() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    let graph = ctx.create_client();

    // Create the version table
    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Fresh install: no persisted version
    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed");
    assert_eq!(version, None, "fresh install should have no version");

    // Write a version
    indexer::schema_version::write_schema_version(&graph, 1)
        .await
        .expect("write_schema_version should succeed");

    // Read it back
    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed");
    assert_eq!(version, Some(1), "should read back version 1");

    // Overwrite with version 2
    indexer::schema_version::write_schema_version(&graph, 2)
        .await
        .expect("write_schema_version should succeed");

    ctx.optimize_all().await;

    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed");
    assert_eq!(version, Some(2), "should read back latest version 2");
}

#[tokio::test]
async fn check_version_detects_mismatch_on_fresh_install() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    let graph = ctx.create_client();
    let datalake = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    let meter = opentelemetry::global::meter("test");
    let gauge = meter.u64_gauge("test.mismatch").build();

    // No persisted version + zero enabled namespaces = ResetReady
    let outcome = indexer::schema_version::check_version(&graph, &datalake, &gauge)
        .await
        .expect("check_version should succeed");

    assert_eq!(
        outcome,
        indexer::schema_version::CheckOutcome::ResetReady,
        "fresh install with zero namespaces should be ResetReady"
    );
}

#[tokio::test]
async fn check_version_current_when_versions_match() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    let graph = ctx.create_client();
    let datalake = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    indexer::schema_version::write_schema_version(&graph, indexer::schema_version::SCHEMA_VERSION)
        .await
        .expect("write_schema_version should succeed");

    let meter = opentelemetry::global::meter("test");
    let gauge = meter.u64_gauge("test.mismatch").build();

    let outcome = indexer::schema_version::check_version(&graph, &datalake, &gauge)
        .await
        .expect("check_version should succeed");

    assert_eq!(
        outcome,
        indexer::schema_version::CheckOutcome::Current,
        "matching versions should report Current"
    );
}

#[tokio::test]
async fn check_version_mismatch_waiting_with_enabled_namespaces() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    let graph = ctx.create_client();
    let datalake = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Write an old version to create a mismatch
    indexer::schema_version::write_schema_version(&graph, 999)
        .await
        .expect("write_schema_version should succeed");

    // Insert an enabled namespace so the check reports MismatchWaiting
    ctx.execute(
        "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
         (root_namespace_id, _siphon_deleted, _siphon_replicated_at) \
         VALUES (1, false, now64(6))",
    )
    .await;

    let meter = opentelemetry::global::meter("test");
    let gauge = meter.u64_gauge("test.mismatch").build();

    let outcome = indexer::schema_version::check_version(&graph, &datalake, &gauge)
        .await
        .expect("check_version should succeed");

    match outcome {
        indexer::schema_version::CheckOutcome::MismatchWaiting {
            persisted,
            enabled_count,
        } => {
            assert_eq!(persisted, Some(999), "should report the persisted version");
            assert!(
                enabled_count > 0,
                "should have at least one enabled namespace"
            );
        }
        other => panic!("expected MismatchWaiting, got {other:?}"),
    }
}

#[tokio::test]
async fn check_version_old_version_zero_namespaces_is_reset_ready() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;

    let graph = ctx.create_client();
    let datalake = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Simulate a previous install with an older schema version.
    indexer::schema_version::write_schema_version(&graph, 0)
        .await
        .expect("write_schema_version should succeed");

    let meter = opentelemetry::global::meter("test");
    let gauge = meter.u64_gauge("test.mismatch").build();

    // Old version persisted + zero enabled namespaces = ResetReady.
    let outcome = indexer::schema_version::check_version(&graph, &datalake, &gauge)
        .await
        .expect("check_version should succeed");

    assert_eq!(
        outcome,
        indexer::schema_version::CheckOutcome::ResetReady,
        "old persisted version with zero namespaces should be ResetReady"
    );
}

/// Verifies that schema_reset drops and recreates all GKG-owned tables,
/// and records the new schema version.
#[tokio::test]
async fn schema_reset_drops_and_recreates_tables() {
    // Graph-only context: siphon tables are external and never touched by reset.
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let graph = ctx.create_client();

    // Ensure the version table exists before reset.
    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Insert a sentinel row into gl_user so we can confirm tables are cleared.
    ctx.execute(
        "INSERT INTO gl_user (id, username, _version, _deleted) \
         VALUES (42, 'sentinel', now64(6), false)",
    )
    .await;

    let rows_before: Vec<_> = ctx.query("SELECT id FROM gl_user").await;
    let row_count_before: u64 = rows_before.iter().map(|b| b.num_rows() as u64).sum();
    assert_eq!(
        row_count_before, 1,
        "sentinel row should exist before reset"
    );

    // Run the schema reset.
    indexer::schema_version::schema_reset(&graph, 99)
        .await
        .expect("schema_reset should succeed");

    // Tables should be empty after reset.
    let rows_after: Vec<_> = ctx.query("SELECT id FROM gl_user").await;
    let row_count_after: u64 = rows_after.iter().map(|b| b.num_rows() as u64).sum();
    assert_eq!(row_count_after, 0, "gl_user should be empty after reset");

    // Version should be recorded.
    ctx.optimize_all().await;
    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed");
    assert_eq!(version, Some(99), "schema version should be 99 after reset");
}

/// Verifies that schema_reset never drops `gkg_schema_version`.
#[tokio::test]
async fn schema_reset_preserves_version_table() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let graph = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Write an old version before reset.
    indexer::schema_version::write_schema_version(&graph, 1)
        .await
        .expect("write_schema_version should succeed");

    indexer::schema_version::schema_reset(&graph, 2)
        .await
        .expect("schema_reset should succeed");

    // gkg_schema_version must still exist and contain the new version.
    ctx.optimize_all().await;
    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed after reset");
    assert_eq!(
        version,
        Some(2),
        "gkg_schema_version should be preserved and contain version 2"
    );
}

/// Verifies that try_schema_reset skips when the lock is already held.
#[tokio::test]
async fn try_schema_reset_skips_when_lock_held() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let graph = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Pre-hold the schema_reset lock.
    let lock_service = Arc::new(MockLockService::new());
    lock_service.set_lock("schema_reset");

    let meter = opentelemetry::global::meter("test");
    let counter = meter.u64_counter("test.schema.reset.total").build();

    let outcome =
        indexer::schema_version::try_schema_reset(&graph, lock_service.as_ref(), 1, &counter)
            .await
            .expect("try_schema_reset should not error when lock is held");

    assert_eq!(
        outcome,
        indexer::schema_version::ResetOutcome::LockNotAcquired,
        "should report LockNotAcquired when lock is already held"
    );

    // Version must not have been written.
    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed");
    assert_eq!(
        version, None,
        "version must not be written when lock is not acquired"
    );
}

/// Verifies that concurrent reset attempts are idempotent: the second reset
/// finds tables already created by the first and succeeds without error.
#[tokio::test]
async fn schema_reset_is_idempotent() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let graph = ctx.create_client();

    indexer::schema_version::ensure_version_table(&graph)
        .await
        .expect("ensure_version_table should succeed");

    // Two consecutive resets must both succeed.
    indexer::schema_version::schema_reset(&graph, 5)
        .await
        .expect("first schema_reset should succeed");

    indexer::schema_version::schema_reset(&graph, 5)
        .await
        .expect("second schema_reset should succeed (idempotent)");

    ctx.optimize_all().await;
    let version = indexer::schema_version::read_persisted_version(&graph)
        .await
        .expect("read_persisted_version should succeed");
    assert_eq!(version, Some(5), "version should be 5 after both resets");
}
