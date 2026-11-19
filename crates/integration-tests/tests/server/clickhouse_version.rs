use clickhouse_client::{MINIMUM_SERVER_VERSION, require_minimum_server_version};
use integration_testkit::{CLICKHOUSE_TAG, TestContext};
use orbit_server_config::ClickHouseConfiguration;

#[tokio::test]
async fn testkit_clickhouse_meets_the_orbit_minimum_without_the_session_settings() {
    let ctx = TestContext::new(&[]).await;
    let mut config: ClickHouseConfiguration = ctx.config.clone();
    config
        .session_settings
        .insert("setting_from_a_newer_server".to_string(), "1".to_string());

    let version = require_minimum_server_version(&config)
        .await
        .expect("the testkit image must meet the Orbit minimum");

    assert!(version >= MINIMUM_SERVER_VERSION);
    assert_eq!(version.to_string(), CLICKHOUSE_TAG);
}
