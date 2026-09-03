mod assertions;
pub mod cli;
mod context;
pub mod mock_redaction;
pub mod scenario;
mod seed;
pub mod seeded_resolver;
pub mod visitor;

pub use assertions::{
    assert_edge_count, assert_edge_count_for_traversal_path, assert_edge_tags,
    assert_edge_tags_by_source, assert_edge_tags_by_target, assert_edges_have_traversal_path,
    assert_node_count,
};
pub use context::TestContext;
pub use seed::load_seed;
pub use seeded_resolver::SeededColumnResolver;

/// `GKG_TEST_ONTOLOGY_OVERLAY=<name>` merges `config/seeds/overlays/<name>.yaml` into the ontology.
fn load_unprefixed_ontology() -> ontology::Ontology {
    let Some(name) = std::env::var("GKG_TEST_ONTOLOGY_OVERLAY")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        return ontology::Ontology::load_embedded().expect("embedded ontology should load");
    };
    let path = format!("{}/overlays/{name}.yaml", env!("SEEDS_DIR"));
    let overlay = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("ontology overlay '{name}' not found at {path}: {e}"));
    ontology::Ontology::load_embedded_with_settings_overlay(&overlay)
        .unwrap_or_else(|e| panic!("ontology overlay '{name}' failed to load: {e}"))
}

pub fn load_ontology() -> ontology::Ontology {
    let ont = load_unprefixed_ontology();
    let prefix = &*TABLE_PREFIX;
    if prefix.is_empty() {
        ont
    } else {
        ont.with_schema_version_prefix(prefix)
    }
}

pub const SIPHON_SCHEMA_SQL: &str = include_str!(concat!(env!("FIXTURES_DIR"), "/siphon.sql"));

/// Version 0 -> "" (empty), version N -> "vN_".
pub static TABLE_PREFIX: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| match orbit_versions::VERSIONS.schema {
        0 => String::new(),
        v => format!("v{v}_"),
    });

pub fn t(table: &str) -> String {
    format!("{}{}", *TABLE_PREFIX, table)
}

/// Generated from the ontology so integration tests create the same prefixed
/// tables and materialized views the indexer writes to at runtime.
pub static GRAPH_SCHEMA_SQL: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
    use query_engine::compiler::{
        emit_create_materialized_view, emit_create_table,
        generate_graph_materialized_views_with_prefix, generate_graph_tables_with_prefix,
    };

    let ontology = load_unprefixed_ontology();
    let tables = generate_graph_tables_with_prefix(&ontology, &TABLE_PREFIX);
    let mut stmts: Vec<String> = tables
        .iter()
        .map(|t| format!("{};", emit_create_table(t)))
        .collect();

    let views = generate_graph_materialized_views_with_prefix(&ontology, &TABLE_PREFIX);
    for mv in &views {
        stmts.push(format!("{};", emit_create_materialized_view(mv)));
    }

    let sql = stmts.join("\n");
    Box::leak(sql.into_boxed_str())
});

pub async fn collect_subtest_results(handles: Vec<(String, tokio::task::JoinHandle<()>)>) {
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
        let mut _handles: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();

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
                _handles.push((_name.to_string(), _handle));
            }
        )+

        $crate::collect_subtest_results(_handles).await;
    }};
}

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
        let mut _handles: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();

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
                _handles.push((_name.to_string(), _handle));
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
