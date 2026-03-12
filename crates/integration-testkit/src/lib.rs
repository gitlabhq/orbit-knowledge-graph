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

/// Run a subtest with automatic table truncation afterward.
#[macro_export]
macro_rules! run_subtest {
    ($name:expr, $context:expr, $test_fn:expr) => {{
        eprintln!("--- {}", $name);
        $test_fn($context).await;
        $context.truncate_all_tables().await;
    }};
}
