use std::time::Duration;

use clickhouse_client::FromArrowColumn;
use tokio::time::{sleep, timeout};

use crate::common::TestContext;

const SLOW_QUERY_MARKER: &str = "read_only_client_slow_query";

async fn running_slow_queries(ctx: &TestContext) -> i64 {
    let sql = format!(
        "SELECT toInt64(count()) FROM system.processes \
         WHERE query LIKE '%{SLOW_QUERY_MARKER}%' AND query NOT LIKE '%system.processes%'"
    );
    let batches = ctx.query(&sql).await;
    i64::extract_column(&batches, 0).expect("one column")[0]
}

async fn wait_for_running_slow_queries(ctx: &TestContext, expected: i64) {
    timeout(Duration::from_secs(30), async {
        while running_slow_queries(ctx).await != expected {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("running slow queries never reached {expected}"));
}

#[tokio::test]
async fn read_only_client_query_is_cancelled_when_the_caller_drops_it() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client().read_only();
    let sql = format!(
        "SELECT count() /* {SLOW_QUERY_MARKER} */ FROM numbers(100000000000) WHERE sleepEachRow(0.000001) = 0"
    );

    let dropped = timeout(Duration::from_secs(2), client.query(&sql).fetch_arrow()).await;
    assert!(dropped.is_err(), "the slow query must still be running");

    wait_for_running_slow_queries(&ctx, 0).await;
}

#[tokio::test]
async fn read_only_client_rejects_writes() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client().read_only();

    let error = client
        .execute("CREATE TABLE read_only_client_probe (id UInt64) ENGINE = Memory")
        .await
        .expect_err("a read-only client must not run DDL");

    assert!(error.to_string().contains("READONLY"), "got: {error}");
}
