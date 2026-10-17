use clickhouse_client::FromArrowColumn;

use super::status::fixtures::{query_cache_usage, use_production_query_settings};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn uncacheable_queries_run_uncached() {
    use_production_query_settings();
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let settings = orbit_server_config::query::default_config()
        .to_http_settings()
        .expect("default settings render");

    for sql in [
        "SELECT toInt64(now() > toDateTime(0))",
        "SELECT toInt64(count()) FROM system.one",
    ] {
        let batches = ctx
            .create_client()
            .query(sql)
            .with_settings(settings.clone())
            .fetch_arrow()
            .await
            .expect("query with the default settings");
        assert_eq!(i64::extract_column(&batches, 0).expect("one column"), [1]);
    }
    assert_eq!(query_cache_usage(&ctx).await, ["None", "None"]);
}
