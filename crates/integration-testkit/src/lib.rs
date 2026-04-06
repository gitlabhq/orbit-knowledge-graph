mod assertions;
mod context;
pub mod mock_redaction;
mod seed;
pub mod visitor;

pub use assertions::{
    assert_edge_count, assert_edge_count_for_traversal_path, assert_edges_have_traversal_path,
    assert_node_count,
};
pub use context::TestContext;
pub use seed::load_seed;

pub const SIPHON_SCHEMA_SQL: &str = include_str!(concat!(env!("FIXTURES_DIR"), "/siphon.sql"));
pub const GRAPH_SCHEMA_SQL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph.sql"));

/// Collect spawned task results and panic with a summary of failures.
pub async fn collect_subtest_results(handles: Vec<(&str, tokio::task::JoinHandle<()>)>) {
    let mut failed: Vec<String> = Vec::new();
    for (name, handle) in handles {
        match handle.await {
            Ok(()) => {}
            Err(e) if e.is_panic() => {
                let payload = e.into_panic();
                let msg = if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = payload.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "panic (see stderr)".to_string()
                };
                failed.push(format!("{}: {}", name, msg));
            }
            Err(_) => {
                failed.push(format!("{}: task cancelled", name));
            }
        }
    }

    if !failed.is_empty() {
        panic!(
            "\n{} subtest(s) failed:\n{}",
            failed.len(),
            failed
                .iter()
                .map(|f| format!("  - {f}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

/// Fork a database per subtest and run with bounded concurrency.
///
/// Each subtest gets its own isolated ClickHouse database via
/// [`TestContext::fork`]. Use this when subtests write data beyond the
/// initial seed (e.g. additional INSERTs in specific test cases).
///
/// At most `SUBTEST_CONCURRENCY` subtests run at a time (default 8, env
/// var override).
#[macro_export]
macro_rules! run_subtests {
    ($ctx:expr, $($test_fn:path),+ $(,)?) => {{
        let _concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        let _sem = std::sync::Arc::new(tokio::sync::Semaphore::new(_concurrency));
        let _ctx: std::sync::Arc<_> = std::sync::Arc::new(Clone::clone($ctx));
        let mut _handles: Vec<(&str, tokio::task::JoinHandle<()>)> = Vec::new();

        $(
            {
                let _sem = std::sync::Arc::clone(&_sem);
                let _ctx = std::sync::Arc::clone(&_ctx);
                let _name: &str = stringify!($test_fn);
                let _handle = tokio::task::spawn(async move {
                    let _permit = _sem.acquire_owned().await.unwrap();
                    let _db_name = _name.replace("::", "_").replace(' ', "");
                    let db = _ctx.fork(&_db_name).await;
                    let _t = std::time::Instant::now();
                    eprintln!("--- {}", _name);
                    $test_fn(&db).await;
                    eprintln!("    {} {:.2?}", _name, _t.elapsed());
                });
                _handles.push((_name, _handle));
            }
        )+

        $crate::collect_subtest_results(_handles).await;
    }};
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
        let _concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        let _sem = std::sync::Arc::new(tokio::sync::Semaphore::new(_concurrency));
        let _ctx: std::sync::Arc<_> = std::sync::Arc::new(Clone::clone($ctx));
        let mut _handles: Vec<(&str, tokio::task::JoinHandle<()>)> = Vec::new();

        $(
            {
                let _sem = std::sync::Arc::clone(&_sem);
                let _ctx = std::sync::Arc::clone(&_ctx);
                let _name: &str = stringify!($test_fn);
                let _handle = tokio::task::spawn(async move {
                    let _permit = _sem.acquire_owned().await.unwrap();
                    let _t = std::time::Instant::now();
                    eprintln!("--- {}", _name);
                    $test_fn(&_ctx).await;
                    eprintln!("    {} {:.2?}", _name, _t.elapsed());
                });
                _handles.push((_name, _handle));
            }
        )+

        $crate::collect_subtest_results(_handles).await;
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
