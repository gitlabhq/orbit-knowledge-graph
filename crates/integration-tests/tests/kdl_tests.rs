use kdl_testkit::runner;

const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/kdl");

/// Discovers all `.kdl` files in the fixtures directory and runs each one
/// against a shared ClickHouse testcontainer. Each file gets a forked database.
#[tokio::test]
async fn kdl_integration_tests() {
    runner::run_kdl_fixtures(FIXTURES_DIR).await;
}

/// Fast parse-only validation: catches KDL syntax errors without starting ClickHouse.
#[test]
fn kdl_fixtures_parse() {
    runner::validate_kdl_fixtures(FIXTURES_DIR);
}
