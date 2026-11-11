pub mod builders;
#[cfg(test)]
pub mod logs;
pub mod mocks;

pub use builders::*;
pub use mocks::*;

use opentelemetry::metrics::Meter;

pub fn test_meter() -> Meter {
    opentelemetry::global::meter_provider().meter("test")
}

/// The embedded model with `replace` applied to the text of the package file `path`.
#[cfg(test)]
pub fn patched_model(
    path: &str,
    find: &str,
    replace: &str,
) -> std::sync::Arc<query_data_model::Model> {
    let mut sources = query_data_model::Packages::embedded().sources().clone();
    let source = sources.get_mut(path).unwrap();
    assert!(source.contains(find), "{path} does not contain {find:?}");
    *source = source.replacen(find, replace, 1);
    query_data_model::Packages::new(query_data_model::Tier::Platform, sources)
        .load()
        .unwrap()
}

/// Routes the `File` variants of `CALLS` to `gl_edge`, so one relationship spans two tables.
#[cfg(test)]
pub fn variant_routes_catalog() -> query_data_model::Catalog<query_data_model::ClickHouse> {
    let model = patched_model(
        "gitlab/storage/clickhouse.yaml",
        "  CALLS:\n    Definition -> Definition: {edge_table: gl_code_edge, copies: {project_id: [source.project_id, target.project_id], branch: [source.branch, target.branch]}}\n    File -> Definition: {edge_table: gl_code_edge, copies: {project_id: [source.project_id, target.project_id], branch: [source.branch, target.branch]}}\n    Definition -> ImportedSymbol: {edge_table: gl_code_edge, copies: {project_id: [source.project_id, target.project_id], branch: [source.branch, target.branch]}}\n    File -> ImportedSymbol: {edge_table: gl_code_edge, copies: {project_id: [source.project_id, target.project_id], branch: [source.branch, target.branch]}}\n",
        "  CALLS:\n    Definition -> Definition: {edge_table: gl_code_edge, copies: {project_id: [source.project_id, target.project_id], branch: [source.branch, target.branch]}}\n    File -> Definition: {edge_table: gl_edge}\n    Definition -> ImportedSymbol: {edge_table: gl_code_edge, copies: {project_id: [source.project_id, target.project_id], branch: [source.branch, target.branch]}}\n    File -> ImportedSymbol: {edge_table: gl_edge}\n",
    );
    query_data_model::Catalog::<query_data_model::ClickHouse>::from_model(&model).unwrap()
}

#[expect(
    clippy::expect_used,
    reason = "the embedded model is validated at build time"
)]
pub fn siphon_source() -> query_data_model::catalog::sources::DataSource {
    query_data_model::Packages::embedded()
        .load()
        .expect("embedded model")
        .sources()
        .source_named(orbit_migrations::completion::SIPHON_SOURCE)
        .expect("embedded siphon source")
        .clone()
}
