use std::path::PathBuf;

use integration_testkit::TestContext;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/migrations")
}

async fn table_exists(context: &TestContext, table_name: &str) -> bool {
    let results = context
        .query(&format!(
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = '{table_name}'"
        ))
        .await;
    results.iter().any(|batch| batch.num_rows() > 0)
}

#[tokio::test]
async fn apply_runs_all_migrations() {
    let context = TestContext::new(&[]).await;
    let fixtures = fixtures_dir();

    let report = migrations::apply_dir(&context.config, &fixtures)
        .await
        .expect("apply should succeed");

    assert_eq!(report.applied_count, 2);
    assert_eq!(report.already_applied, 0);
    assert!(report.warnings.is_empty());
    assert!(table_exists(&context, "test_users").await);
    assert!(table_exists(&context, "test_orders").await);
}

#[tokio::test]
async fn apply_is_idempotent() {
    let context = TestContext::new(&[]).await;
    let fixtures = fixtures_dir();

    migrations::apply_dir(&context.config, &fixtures)
        .await
        .expect("first apply should succeed");

    let report = migrations::apply_dir(&context.config, &fixtures)
        .await
        .expect("second apply should succeed");

    assert_eq!(report.applied_count, 0);
    assert_eq!(report.already_applied, 2);
    assert!(report.warnings.is_empty());
}

#[tokio::test]
async fn rollback_removes_latest_migration() {
    let context = TestContext::new(&[]).await;
    let fixtures = fixtures_dir();

    migrations::apply_dir(&context.config, &fixtures)
        .await
        .expect("apply should succeed");

    let report =
        migrations::rollback_dir(&context.config, 20260101000000, &fixtures)
            .await
            .expect("rollback should succeed");

    assert_eq!(report.rolled_back_count, 1);
    assert!(report.warnings.is_empty());
    assert!(table_exists(&context, "test_users").await);
    assert!(!table_exists(&context, "test_orders").await);
}

#[tokio::test]
async fn rollback_all() {
    let context = TestContext::new(&[]).await;
    let fixtures = fixtures_dir();

    migrations::apply_dir(&context.config, &fixtures)
        .await
        .expect("apply should succeed");

    let report = migrations::rollback_dir(&context.config, 0, &fixtures)
        .await
        .expect("rollback should succeed");

    assert_eq!(report.rolled_back_count, 2);
    assert!(report.warnings.is_empty());
    assert!(!table_exists(&context, "test_users").await);
    assert!(!table_exists(&context, "test_orders").await);
}
