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
        use futures::stream::StreamExt as _;

        let _concurrency: usize = std::env::var("SUBTEST_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        let _futs = vec![
            $(
                Box::pin(async {
                    let name = stringify!($test_fn).replace("::", "_").replace(' ', "");
                    let db = $ctx.fork(&name).await;
                    let _t = std::time::Instant::now();
                    eprintln!("--- {}", name);
                    $test_fn(&db).await;
                    eprintln!("    {} {:.2?}", name, _t.elapsed());
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
            )+
        ];

        futures::stream::iter(_futs)
            .buffer_unordered(_concurrency)
            .collect::<Vec<()>>()
            .await;
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
        let mut _handles: Vec<(&str, tokio::task::JoinHandle<()>)> = Vec::new();

        $(
            let _permit = _sem.clone().acquire_owned().await.unwrap();
            let _name: &str = stringify!($test_fn);
            // SAFETY: ctx outlives all spawned tasks because we join below.
            // The borrow checker can't see this, so we use a raw pointer.
            let _ctx_ptr = $ctx as *const _ as usize;
            let _handle = tokio::task::spawn(async move {
                let _ctx_ref = unsafe { &*(_ctx_ptr as *const _) };
                let _t = std::time::Instant::now();
                eprintln!("--- {}", _name);
                $test_fn(_ctx_ref).await;
                eprintln!("    {} {:.2?}", _name, _t.elapsed());
                drop(_permit);
            });
            _handles.push((_name, _handle));
        )+

        let mut _failed: Vec<String> = Vec::new();
        for (_name, _handle) in _handles {
            match _handle.await {
                Ok(()) => {}
                Err(e) => {
                    let msg = if let Some(s) = e.into_panic().downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "panic (see stderr)".to_string()
                    };
                    _failed.push(format!("{}: {}", _name, msg));
                }
            }
        }

        if !_failed.is_empty() {
            panic!(
                "\n{} subtest(s) failed:\n{}",
                _failed.len(),
                _failed.iter().map(|f| format!("  - {f}")).collect::<Vec<_>>().join("\n")
            );
        }
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
