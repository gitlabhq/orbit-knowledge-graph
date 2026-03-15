mod assertions;
mod context;
mod extract;
pub mod mock_redaction;
mod seed;
pub mod visitor;

pub use assertions::{
    assert_edge_count, assert_edge_count_for_traversal_path, assert_edges_have_traversal_path,
    assert_node_count,
};
pub use context::TestContext;
pub use extract::{get_boolean_column, get_int64_column, get_string_column, get_uint64_column};
pub use seed::load_seed;

pub const SIPHON_SCHEMA_SQL: &str = include_str!(concat!(env!("FIXTURES_DIR"), "/siphon.sql"));
pub const GRAPH_SCHEMA_SQL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph.sql"));

/// Fork a database per subtest and run all subtests in parallel.
///
/// Each subtest gets its own isolated ClickHouse database via
/// [`TestContext::fork`]. Use this when subtests write data beyond the
/// initial seed (e.g. additional INSERTs in specific test cases).
#[macro_export]
macro_rules! run_subtests {
    ($ctx:expr, $($test_fn:path),+ $(,)?) => {
        futures::future::join_all(vec![
            $(
                Box::pin(async {
                    let name = stringify!($test_fn).replace("::", "_").replace(' ', "");
                    let db = $ctx.fork(&name).await;
                    eprintln!("--- {}", name);
                    $test_fn(&db).await;
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
            )+
        ]).await;
    };
}

/// Run all subtests against the same shared database with bounded concurrency.
///
/// Unlike [`run_subtests!`], this does NOT fork a separate database per
/// subtest. All subtests share the caller's [`TestContext`] directly.
/// Use this when all subtests are read-only against pre-seeded data.
///
/// At most `SUBTEST_CONCURRENCY` subtests run at a time (default 8, env
/// var override).
#[macro_export]
macro_rules! run_subtests_shared {
    ($ctx:expr, $($test_fn:path),+ $(,)?) => {{
        use futures::stream::StreamExt as _;

        let _concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        let _futs = vec![
            $(
                Box::pin(async {
                    let _t = std::time::Instant::now();
                    eprintln!("--- {}", stringify!($test_fn));
                    $test_fn($ctx).await;
                    eprintln!("    {} {:.2?}", stringify!($test_fn), _t.elapsed());
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
            )+
        ];

        futures::stream::iter(_futs)
            .buffer_unordered(_concurrency)
            .collect::<Vec<()>>()
            .await;
    }};
}

/// Run a subtest with automatic table truncation afterward.
#[macro_export]
macro_rules! run_subtest {
    ($name:expr, $context:expr, $test_fn:expr) => {{
        eprintln!("--- {}", $name);
        $test_fn($context).await;
        $context.truncate_all_tables().await;
    }};
}
