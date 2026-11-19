use std::time::Duration;

use clickhouse_client::FromArrowColumn;
use tokio::time::{sleep, timeout};

use crate::common::TestContext;

async fn running_queries_marked(ctx: &TestContext, marker: &str) -> i64 {
    let sql = format!(
        "SELECT toInt64(count()) FROM system.processes \
         WHERE query LIKE '%{marker}%' AND query NOT LIKE '%system.processes%'"
    );
    let batches = ctx.query(&sql).await;
    i64::extract_column(&batches, 0).expect("one column")[0]
}

async fn wait_for_running_queries(ctx: &TestContext, marker: &str, expected: i64) {
    timeout(Duration::from_secs(30), async {
        while running_queries_marked(ctx, marker).await != expected {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("running queries never reached {expected}"));
}

#[tokio::test]
async fn read_only_client_query_is_cancelled_when_the_caller_drops_it() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client().read_only();
    let marker = format!("read_only_client_{}", uuid::Uuid::new_v4().simple());
    let sql = format!(
        "SELECT count() /* {marker} */ FROM numbers(100000000000) WHERE cityHash64(number) % 7 = 0 \
         SETTINGS max_execution_time = 60"
    );

    let running = tokio::spawn(async move { client.query(&sql).fetch_arrow().await });
    wait_for_running_queries(&ctx, &marker, 1).await;
    running.abort();

    wait_for_running_queries(&ctx, &marker, 0).await;
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
