use clickhouse_client::FromArrowColumn;
use integration_testkit::{load_ontology, run_subtests};
use query_engine::compiler::{Frontend, compile};

use super::status::fixtures::{
    admin_context, query_cache_usage, seed_namespaces, use_production_query_settings,
};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

const GROUPS_QUERY: &str = r#"{
    "query_type": "traversal",
    "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
    "limit": 10
}"#;

#[tokio::test]
async fn query_cache() {
    use_production_query_settings();
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests!(
        &ctx,
        repeated_graph_query_reads_the_cache,
        uncacheable_queries_run_uncached,
    );
}

async fn repeated_graph_query_reads_the_cache(ctx: &TestContext) {
    let compiled = compile(
        GROUPS_QUERY,
        Frontend::JsonDsl,
        &load_ontology(),
        &admin_context(),
    )
    .expect("query should compile");

    let first = ctx.query_parameterized(&compiled.base).await;
    let second = ctx.query_parameterized(&compiled.base).await;

    assert_eq!(first.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(first, second);
    assert_eq!(query_cache_usage(ctx).await, ["Write", "Read"]);
}

async fn uncacheable_queries_run_uncached(ctx: &TestContext) {
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
    assert_eq!(query_cache_usage(ctx).await, ["None", "None"]);
}
