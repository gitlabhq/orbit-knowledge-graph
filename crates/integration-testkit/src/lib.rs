mod assertions;
pub mod cli;
mod context;
pub mod mock_redaction;
mod seed;
pub mod visitor;

pub use assertions::{
    assert_edge_count, assert_edge_count_for_traversal_path, assert_edge_tags,
    assert_edge_tags_by_source, assert_edge_tags_by_target, assert_edges_have_traversal_path,
    assert_node_count,
};
pub use context::TestContext;
pub use seed::load_seed;

pub fn load_ontology() -> ontology::Ontology {
    let ont = ontology::Ontology::load_embedded().expect("embedded ontology should load");
    let prefix = &*TABLE_PREFIX;
    if prefix.is_empty() {
        ont
    } else {
        ont.with_schema_version_prefix(prefix)
    }
}

pub const SIPHON_SCHEMA_SQL: &str = include_str!(concat!(env!("FIXTURES_DIR"), "/siphon.sql"));

/// The SCHEMA_VERSION from config/SCHEMA_VERSION, parsed at compile time.
pub const SCHEMA_VERSION: u32 =
    const_parse_version(include_str!(concat!(env!("CONFIG_DIR"), "/SCHEMA_VERSION")).as_bytes());

/// Table name prefix for the current SCHEMA_VERSION.
/// Version 0 -> "" (empty), version N -> "vN_".
pub static TABLE_PREFIX: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    if SCHEMA_VERSION == 0 {
        String::new()
    } else {
        format!("v{SCHEMA_VERSION}_")
    }
});

/// Returns the prefixed table name for the current SCHEMA_VERSION.
/// E.g. `t("gl_user")` returns `"v1_gl_user"` when SCHEMA_VERSION=1.
pub fn t(table: &str) -> String {
    format!("{}{}", *TABLE_PREFIX, table)
}

const fn const_parse_version(bytes: &[u8]) -> u32 {
    let mut n: u32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b >= b'0' && b <= b'9' {
            n = n * 10 + (b - b'0') as u32;
        }
        i += 1;
    }
    n
}

/// Graph DDL with the correct table prefix for the current SCHEMA_VERSION.
/// Generated from the ontology so integration tests create the same prefixed
/// tables the indexer writes to at runtime.
pub static GRAPH_SCHEMA_SQL: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
    use query_engine::compiler::{emit_create_table, generate_graph_tables_with_prefix};

    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let tables = generate_graph_tables_with_prefix(&ontology, &TABLE_PREFIX);
    let sql = tables
        .iter()
        .map(|t| format!("{};", emit_create_table(t)))
        .collect::<Vec<_>>()
        .join("\n");

    Box::leak(sql.into_boxed_str())
});

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
