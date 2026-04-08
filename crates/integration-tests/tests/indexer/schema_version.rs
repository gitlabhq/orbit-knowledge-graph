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
