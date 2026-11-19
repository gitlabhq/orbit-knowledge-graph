use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use duckdb_client::DuckDbClient;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::{CompiledQueryContext, Frontend, compile_local, generate_local_ddl};
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::PipelineError;
use query_engine::shared::PipelineOutput;

use super::format::Backend;
use super::pagination::{PageCollection, assert_page_bounds, page_query, unpaginated};
use super::{
    QueryStep, assert_compilation, assert_output, assert_pagination, assert_repeated_response,
    pipeline_output, read_scenarios, repeat_count, seed_preset, step_label, validate_response,
};
use crate::scenario::seed::{group_by_columns, values_insert};
use crate::scenario::{Seed, discover};

/// Returns the DuckDB coverage problems: renderable constructs no scenario reached and stale exclusions.
pub fn run_duckdb_dir(root: &str, presets: &str, seed: &str) -> Vec<String> {
    let root = Path::new(root);
    let ontology = Arc::new(crate::load_unprefixed_ontology());
    let base = seed_preset(Path::new(presets), seed);
    let mut files = Vec::new();
    discover(root, &mut files);
    files.sort();
    let mut ran = 0;
    let mut coverage = super::coverage::Coverage::default();
    for (name, scenario) in files.iter().flat_map(|file| read_scenarios(root, file)) {
        if !scenario.config.backends.contains(&Backend::Duckdb) {
            continue;
        }
        let ontology = scenario.config.ontology_overlay.as_deref().map_or_else(
            || Arc::clone(&ontology),
            |name| Arc::new(crate::load_ontology_overlay(name)),
        );
        let directory = tempfile::tempdir().expect("temporary DuckDB directory");
        let database = DuckDbClient::open(&directory.path().join("scenario.duckdb"))
            .unwrap_or_else(|e| panic!("{name}: open DuckDB: {e}"));
        database
            .initialize_schema(&generate_local_ddl(&ontology, ""))
            .unwrap_or_else(|e| panic!("{name}: local DDL: {e}"));
        let (config, steps) = scenario.into_steps(&name);
        let preset = config
            .seed
            .as_ref()
            .map(|preset| seed_preset(Path::new(presets), preset));
        seed_local(
            &database,
            [preset.as_ref().unwrap_or(&base), &config.extra_seed],
            &name,
        );
        for (index, step) in steps.iter().enumerate() {
            seed_local(&database, [&step.seed], &name);
            run_step(
                &database,
                &ontology,
                step,
                &step_label(&name, index, steps.len()),
                &mut coverage,
            );
        }
        ran += 1;
    }
    assert!(ran > 0, "no duckdb scenarios under {}", root.display());
    coverage.problems()
}

fn run_step(
    database: &DuckDbClient,
    ontology: &Arc<ontology::Ontology>,
    step: &QueryStep,
    name: &str,
    coverage: &mut super::coverage::Coverage,
) {
    for (key, query) in &step.query {
        let frontend: Frontend = key
            .parse()
            .unwrap_or_else(|_| panic!("{name}: unknown query language '{key}'"));
        let label = format!("{name} [{key} on duckdb]");
        let expect = &step.expect;
        expect.validate_pages_exclusive(&label);
        let compiled = compile_local(query, frontend, ontology)
            .map(Arc::new)
            .map_err(PipelineError::from);
        let Some(compiled) = assert_compilation(compiled, expect, ("duckdb", key), &label) else {
            continue;
        };
        if expect.pages.is_empty() {
            let output = execute(database, &compiled, &label, coverage);
            let baseline = assert_output(&output, expect, &label);
            for run in 2..=repeat_count(expect, &label) {
                let compiled = compile_local(query, frontend, ontology)
                    .map(Arc::new)
                    .map_err(PipelineError::from);
                let compiled = assert_compilation(compiled, expect, ("duckdb", key), &label)
                    .expect("repeated query should compile");
                let output = execute(database, &compiled, &label, coverage);
                let response = assert_output(&output, expect, &label);
                assert_repeated_response(&baseline, &response, run, &label);
            }
            continue;
        }
        let mut collected = PageCollection::default();
        for (index, page_expect) in expect.pages.iter().enumerate() {
            let page_label = format!("{label} page {}", index + 1);
            let compiled = Arc::new(
                compile_local(&page_query(frontend, query, index), frontend, ontology)
                    .unwrap_or_else(|error| panic!("{page_label}: compile failed: {error}")),
            );
            let output = execute(database, &compiled, &page_label, coverage);
            let response = assert_output(&output, page_expect, &page_label);
            let response = serde_json::from_value(response).expect("response should deserialize");
            collected.push(&response);
            assert_page_bounds(&response, index, expect.pages.len(), &page_label);
        }
        if let Some(all_pages) = &expect.all_pages {
            collected.assert_expect(all_pages, &label);
            if all_pages.matches_unpaginated {
                let query = unpaginated(frontend, query);
                let compiled = Arc::new(compile_local(&query, frontend, ontology).unwrap_or_else(
                    |error| panic!("{label}: unpaginated compile failed: {error}"),
                ));
                let output = execute(database, &compiled, &label, coverage);
                assert_pagination(&output, &label);
                let response =
                    validate_response(&compiled.input, GraphFormatter.format(&output), &label);
                collected.assert_matches(&response, &label);
            }
        }
    }
}

fn execute(
    database: &DuckDbClient,
    compiled: &Arc<CompiledQueryContext>,
    label: &str,
    coverage: &mut super::coverage::Coverage,
) -> PipelineOutput {
    coverage.record(&compiled.ast);
    let sql = compiled.base.render();
    let batches = database
        .query_arrow(&sql)
        .unwrap_or_else(|error| panic!("{label}: {error}\n{sql}"));
    let query_result = QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
    pipeline_output(
        compiled,
        query_result,
        0,
        compiled.base.result_context.clone(),
        sql,
    )
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
