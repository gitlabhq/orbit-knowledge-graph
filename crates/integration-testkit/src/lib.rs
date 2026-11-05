mod assertions;
pub mod cli;
mod context;
pub mod mock_redaction;
pub mod plan_shape;
pub mod query_scenario;
pub mod scenario;
mod seed;
pub mod seeded_resolver;
pub mod visitor;

pub use assertions::{
    assert_edge_count, assert_edge_count_for_traversal_path, assert_edge_tags,
    assert_edge_tags_by_source, assert_edge_tags_by_target, assert_edges_have_traversal_path,
    assert_node_count,
};
pub use context::{CLICKHOUSE_IMAGE, CLICKHOUSE_TAG, TestContext};
pub use seed::load_seed;
pub use seeded_resolver::SeededColumnResolver;

/// `GKG_TEST_ONTOLOGY_OVERLAY=<name>` merges `config/seeds/overlays/<name>/` over the ontology.
fn overlay_name() -> Option<String> {
    std::env::var("GKG_TEST_ONTOLOGY_OVERLAY")
        .ok()
        .filter(|v| !v.is_empty())
}

pub(crate) fn load_unprefixed_ontology() -> ontology::Ontology {
    match overlay_name() {
        Some(name) => load_ontology_overlay(&name),
        None => ontology::Ontology::load_embedded().expect("embedded ontology should load"),
    }
}

pub fn load_ontology_overlay(name: &str) -> ontology::Ontology {
    let dir = format!("{}/overlays/{name}", env!("SEEDS_DIR"));
    assert!(
        std::path::Path::new(&dir).is_dir(),
        "ontology overlay '{name}' not found at {dir}"
    );
    ontology::Ontology::load_embedded_with_overlay(&dir)
        .unwrap_or_else(|e| panic!("ontology overlay '{name}' failed to load: {e}"))
}

pub fn load_ontology() -> std::sync::Arc<ontology::Ontology> {
    std::sync::Arc::new(load_unprefixed_ontology().with_schema_version_prefix(&TABLE_PREFIX))
}

pub fn overlay_fragments(name: &str) -> Vec<query_data_model::spec::Fragment> {
    query_data_model::from_ontology::from_ontology(&load_ontology_overlay(name))
        .unwrap_or_else(|e| panic!("ontology overlay '{name}' has no model: {e}"))
}

fn unprefixed_fragments() -> Vec<query_data_model::spec::Fragment> {
    match overlay_name() {
        Some(name) => overlay_fragments(&name),
        None => query_data_model::Packages::embedded()
            .fragments()
            .expect("embedded packages should parse"),
    }
}

fn load_model(
    fragments: &[query_data_model::spec::Fragment],
) -> std::sync::Arc<query_data_model::Model> {
    std::sync::Arc::new(query_data_model::Model::load(fragments).expect("test model should load"))
}

pub fn versioned_data_model(
    fragments: Vec<query_data_model::spec::Fragment>,
) -> std::sync::Arc<query_data_model::ClickHouseDataModel> {
    let fragments =
        query_data_model::implementations::clickhouse::versioned(fragments, &TABLE_PREFIX)
            .expect("test tables should take the schema prefix");
    std::sync::Arc::new(
        query_data_model::ClickHouseDataModel::from_model(&load_model(&fragments))
            .expect("test model should bind its ClickHouse connection"),
    )
}

pub fn graph_schema(
    fragments: &[query_data_model::spec::Fragment],
) -> orbit_migrations::schema::GraphSchema {
    let physical = orbit_migrations::schema::Schema::from_model(&load_model(fragments))
        .expect("test model should resolve a ClickHouse schema");
    orbit_migrations::schema::GraphSchema::new(physical, false)
}

pub fn embedded_schema() -> orbit_migrations::schema::Schema {
    orbit_migrations::schema::Schema::from_model(
        &query_data_model::Packages::embedded()
            .load()
            .expect("embedded packages should load"),
    )
    .expect("embedded packages should resolve a ClickHouse schema")
}

pub fn load_data_model() -> std::sync::Arc<query_data_model::ClickHouseDataModel> {
    versioned_data_model(unprefixed_fragments())
}

pub fn load_local_data_model() -> std::sync::Arc<query_data_model::DuckDbDataModel> {
    std::sync::Arc::new(
        query_data_model::DuckDbDataModel::from_model(&load_model(&unprefixed_fragments()))
            .expect("test model should bind its DuckDB connection"),
    )
}

pub const SIPHON_SCHEMA_SQL: &str = include_str!(concat!(env!("SEEDS_DIR"), "/siphon.sql"));

/// Version 0 -> "" (empty), version N -> "vN_".
pub static TABLE_PREFIX: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| match orbit_versions::VERSIONS.schema {
        0 => String::new(),
        v => format!("v{v}_"),
    });

pub fn t(table: &str) -> String {
    format!("{}{}", *TABLE_PREFIX, table)
}

/// Generated from the model so integration tests create the same prefixed
/// tables and materialized views the indexer writes to at runtime.
pub static GRAPH_SCHEMA_SQL: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
    let schema = graph_schema(&unprefixed_fragments());

    let mut stmts: Vec<String> = schema
        .tables
        .iter()
        .map(|table| format!("{};", table.to_create_sql(&TABLE_PREFIX)))
        .collect();

    let all_table_names: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();
    for view in schema.views.iter().filter(|v| v.versioned) {
        let prefixed = view
            .clone()
            .with_schema_version_prefix(&TABLE_PREFIX, &all_table_names);
        stmts.push(format!("{};", prefixed.to_create_sql()));
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
                    let _outcome = futures::FutureExt::catch_unwind(
                        std::panic::AssertUnwindSafe($test_fn(&db)),
                    ).await;
                    let _cleanup = futures::FutureExt::catch_unwind(
                        std::panic::AssertUnwindSafe(_ctx.drop_fork(&_db_name)),
                    ).await;
                    if let Err(_failure) = _outcome.and(_cleanup) {
                        std::panic::resume_unwind(_failure);
                    }
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

pub fn release(
    version: u32,
    sources: &std::collections::BTreeMap<String, String>,
    packages: &query_data_model::Packages,
) -> orbit_migrations::archive::Release {
    orbit_migrations::archive::Release::new(
        ontology::archive::OntologyArchive::from_sources(version, sources)
            .expect("test ontology archive should build"),
        orbit_migrations::archive::ModelArchive::from_packages(version, packages)
            .expect("test model archive should build"),
    )
    .expect("test archives should form one release")
}

pub fn embedded_release(version: u32) -> orbit_migrations::archive::Release {
    release(
        version,
        &ontology::migrations::embedded_sources(),
        &query_data_model::Packages::embedded(),
    )
}

pub fn derived_release(
    version: u32,
    sources: &std::collections::BTreeMap<String, String>,
) -> orbit_migrations::archive::Release {
    let ontology = ontology::archive::OntologyArchive::from_sources(version, sources)
        .and_then(|archive| archive.load_ontology())
        .expect("test ontology should load");
    let packages = query_data_model::from_ontology::packages(&ontology)
        .expect("test ontology should convert to packages");
    release(version, sources, &packages)
}
