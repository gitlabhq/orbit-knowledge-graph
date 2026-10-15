use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use duckdb_client::DuckDbClient;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::{Frontend, compile_local, generate_local_ddl};
use query_engine::formatters::{GraphFormatter, ResultFormatter};

use super::{assert_response, pipeline_output, read_scenarios, seed_preset};
use crate::scenario::seed::{group_by_columns, values_insert};
use crate::scenario::{Seed, discover};

pub fn run_duckdb_dir(root: &str, presets: &str, seed: &str) {
    let root = Path::new(root);
    let ontology = Arc::new(crate::load_unprefixed_ontology());
    let base = seed_preset(Path::new(presets), seed);
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    let mut ran = 0;
    for (name, scenario) in files.iter().flat_map(|file| read_scenarios(root, file)) {
        if !scenario.config.duckdb {
            continue;
        }
        let directory = tempfile::tempdir().expect("temporary DuckDB directory");
        let database = DuckDbClient::open(&directory.path().join("scenario.duckdb"))
            .unwrap_or_else(|e| panic!("{name}: open DuckDB: {e}"));
        database
            .initialize_schema(&generate_local_ddl(&ontology, ""))
            .unwrap_or_else(|e| panic!("{name}: local DDL: {e}"));
        seed_local(&database, [&base, &scenario.config.extra_seed], &name);
        for (key, query) in &scenario.query {
            let frontend: Frontend = key
                .parse()
                .unwrap_or_else(|_| panic!("{name}: unknown query language '{key}'"));
            let label = format!("{name} [{key} on duckdb]");
            let compiled = Arc::new(
                compile_local(query, frontend, &ontology)
                    .unwrap_or_else(|e| panic!("{label}: compile failed: {e}")),
            );
            let sql = compiled.base.render();
            let batches = database
                .query_arrow(&sql)
                .unwrap_or_else(|e| panic!("{label}: {e}\n{sql}"));
            let query_result = QueryResult::from_batches(&batches, &compiled.base.result_context);
            let response = GraphFormatter.format(&pipeline_output(
                &compiled,
                query_result,
                0,
                compiled.base.result_context.clone(),
                sql,
            ));
            assert_response(&compiled.input, response, &scenario.expect, &label);
        }
        ran += 1;
    }
    assert!(ran > 0, "no duckdb scenarios under {}", root.display());
}

fn seed_local<'a>(database: &DuckDbClient, seeds: impl IntoIterator<Item = &'a Seed>, name: &str) {
    let batches = database
        .query_arrow("SELECT table_name, column_name FROM information_schema.columns")
        .unwrap_or_else(|e| panic!("{name}: local columns: {e}"));
    let mut columns: HashMap<String, HashSet<String>> = HashMap::new();
    for (table, column) in duckdb_client::string_column(&batches, "table_name")
        .into_iter()
        .zip(duckdb_client::string_column(&batches, "column_name"))
    {
        columns.entry(table).or_default().insert(column);
    }
    for (table, rows) in seeds.into_iter().flatten() {
        let Some(local) = columns.get(table) else {
            continue;
        };
        let rows = rows.iter().map(|row| {
            row.iter()
                .filter(|(column, _)| local.contains(*column))
                .map(|(column, value)| (column.clone(), value.clone()))
                .collect()
        });
        for (column_names, group) in group_by_columns(rows) {
            let quoted: Vec<String> = column_names.iter().map(|c| format!("\"{c}\"")).collect();
            database
                .execute(
                    &values_insert(table, &quoted, &column_names, &group, name),
                    &[],
                )
                .unwrap_or_else(|e| panic!("{name}: seed {table}: {e}"));
        }
    }
}
