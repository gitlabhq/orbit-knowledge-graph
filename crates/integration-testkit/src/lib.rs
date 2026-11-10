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

/// The stats of a normalized JSON DSL query on the embedded ClickHouse model, before admission.
#[cfg(test)]
pub(crate) fn test_stats(json: &str) -> query_engine::compiler::QueryStats {
    use query_engine::compiler::{
        Frontend, QueryStats, logical::Query, passes::normalize, surface,
    };
    let model = query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap();
    let catalog = model.model().logical().as_admin();
    let statement = surface::parse(json, Frontend::JsonDsl, catalog)
        .unwrap_or_else(|error| panic!("{json}: {error}"));
    let plan = normalize::normalize(statement.plan, catalog.model(), &model).unwrap();
    QueryStats::new(Query::new(&plan, catalog.model()), &model)
}

pub use assertions::{
    assert_edge_count, assert_edge_count_for_traversal_path, assert_edge_tags,
    assert_edge_tags_by_source, assert_edge_tags_by_target, assert_edges_have_traversal_path,
    assert_node_count,
};
pub use context::{CLICKHOUSE_IMAGE, CLICKHOUSE_TAG, TestContext};
pub use seed::load_seed;
pub use seeded_resolver::SeededColumnResolver;

/// `GKG_TEST_MODEL_OVERLAY=<name>` applies `config/seeds/overlays/<name>.yaml` to the packages.
fn overlay_name() -> Option<String> {
    std::env::var("GKG_TEST_MODEL_OVERLAY")
        .ok()
        .filter(|v| !v.is_empty())
}

pub fn overlay_fragments(name: &str) -> Vec<query_data_model::spec::Fragment> {
    query_data_model::testkit::overlay(name)
        .fragments()
        .unwrap_or_else(|e| panic!("model overlay '{name}' does not parse: {e}"))
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

fn versioned_fragments(
    fragments: Vec<query_data_model::spec::Fragment>,
) -> Vec<query_data_model::spec::Fragment> {
    query_data_model::implementations::clickhouse::versioned(fragments, &TABLE_PREFIX)
        .expect("test tables should take the schema prefix")
}

/// The prefixed fragments behind [`load_data_model`].
pub fn load_fragments() -> Vec<query_data_model::spec::Fragment> {
    versioned_fragments(unprefixed_fragments())
}

pub fn versioned_data_model(
    fragments: Vec<query_data_model::spec::Fragment>,
) -> std::sync::Arc<query_data_model::Catalog<query_data_model::ClickHouse>> {
    let fragments = versioned_fragments(fragments);
    std::sync::Arc::new(
        query_data_model::Catalog::<query_data_model::ClickHouse>::from_model(&load_model(
            &fragments,
        ))
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

pub fn load_data_model() -> std::sync::Arc<query_data_model::Catalog<query_data_model::ClickHouse>>
{
    versioned_data_model(unprefixed_fragments())
}

pub fn load_local_data_model() -> std::sync::Arc<query_data_model::Catalog<query_data_model::DuckDb>>
{
    std::sync::Arc::new(
        query_data_model::Catalog::<query_data_model::DuckDb>::from_model(&load_model(
            &unprefixed_fragments(),
        ))
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

pub fn model_archive(
    version: u32,
    packages: &query_data_model::Packages,
) -> orbit_migrations::archive::ModelArchive {
    orbit_migrations::archive::ModelArchive::from_packages(version, packages)
        .expect("test model archive should build")
}

pub fn embedded_archive(version: u32) -> orbit_migrations::archive::ModelArchive {
    model_archive(version, &query_data_model::Packages::embedded())
}

/// The one prefixed edge table that stores every variant of `relationship`.
pub fn edge_table_for(relationship: &str) -> String {
    let model = load_data_model();
    let id = model
        .graph()
        .relationship_id(relationship)
        .unwrap_or_else(|| panic!("unknown relationship {relationship}"));
    match model.edge_tables(&[id]).as_slice() {
        [table] => table.clone(),
        tables => panic!("{relationship} is stored in {tables:?}, not one table"),
    }
}

/// The prefixed edge table of the `source -> target` variant of `relationship`.
pub fn variant_edge_table(relationship: &str, source: &str, target: &str) -> String {
    let model = load_data_model();
    let variant = model
        .graph()
        .variant_named(relationship, source, target)
        .unwrap_or_else(|| panic!("{relationship} has no {source} -> {target} variant"));
    model
        .edge_table(variant.id)
        .unwrap_or_else(|| panic!("{relationship} {source} -> {target} has no edge table"))
        .to_string()
}
