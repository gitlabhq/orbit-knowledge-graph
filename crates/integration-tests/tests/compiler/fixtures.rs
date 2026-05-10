//! Fixture corpus drift tests.

use compiler::{SecurityContext, compile};
use ontology::Ontology;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

const JSON_FIXTURES: &[(&str, &str)] = &[
    (
        "fixtures/queries/corpus-input.json",
        include_str!("../../../../fixtures/queries/corpus-input.json"),
    ),
    (
        "fixtures/queries/cursor_profiling.json",
        include_str!("../../../../fixtures/queries/cursor_profiling.json"),
    ),
    (
        "fixtures/queries/optimization_showcase.json",
        include_str!("../../../../fixtures/queries/optimization_showcase.json"),
    ),
    (
        "fixtures/queries/property_grouping.json",
        include_str!("../../../../fixtures/queries/property_grouping.json"),
    ),
    (
        "fixtures/queries/property_grouping_stress.json",
        include_str!("../../../../fixtures/queries/property_grouping_stress.json"),
    ),
];

const YAML_QUERY_FIXTURES: &[(&str, &str)] = &[
    (
        "fixtures/queries/code_graph_queries.yaml",
        include_str!("../../../../fixtures/queries/code_graph_queries.yaml"),
    ),
    (
        "fixtures/queries/sdlc_queries.yaml",
        include_str!("../../../../fixtures/queries/sdlc_queries.yaml"),
    ),
];

#[derive(Deserialize)]
struct QueryEntry {
    query: String,
}

#[test]
fn fixture_queries_compile_against_embedded_ontology() {
    let ontology = Ontology::load_embedded().expect("embedded ontology loads");
    let ctx = SecurityContext::new(1, vec!["1/".into()]).expect("valid security context");
    let mut errors = Vec::new();
    let mut compiled = 0usize;

    for (path, raw) in JSON_FIXTURES {
        let queries: BTreeMap<String, Value> =
            serde_json::from_str(raw).unwrap_or_else(|err| panic!("{path} parses as JSON: {err}"));

        for (name, mut query) in queries {
            normalize_sample_placeholders(&mut query);
            compiled += compile_fixture_query(path, &name, &query, &ontology, &ctx, &mut errors);
        }
    }

    for (path, raw) in YAML_QUERY_FIXTURES {
        let entries: BTreeMap<String, QueryEntry> =
            serde_yaml::from_str(raw).unwrap_or_else(|err| panic!("{path} parses as YAML: {err}"));

        for (name, entry) in entries {
            let mut query: Value = serde_json::from_str(&entry.query)
                .unwrap_or_else(|err| panic!("{path}::{name} query parses as JSON: {err}"));
            normalize_sample_placeholders(&mut query);
            compiled += compile_fixture_query(path, &name, &query, &ontology, &ctx, &mut errors);
        }
    }

    assert!(
        errors.is_empty(),
        "fixture query compilation failed for {} of {} queries:\n{}",
        errors.len(),
        compiled,
        errors.join("\n")
    );
}

#[test]
fn raw_sql_optimization_fixture_parses() {
    let raw = include_str!("../../../../fixtures/queries/optimization_benchmarks.yaml");
    let parsed: serde_yaml::Value =
        serde_yaml::from_str(raw).expect("optimization benchmark fixture parses as YAML");
    assert!(
        parsed
            .as_mapping()
            .is_some_and(|queries| !queries.is_empty())
    );
}

fn compile_fixture_query(
    path: &str,
    name: &str,
    query: &Value,
    ontology: &Ontology,
    ctx: &SecurityContext,
    errors: &mut Vec<String>,
) -> usize {
    let query_json = serde_json::to_string(query).expect("query serializes");
    if let Err(err) = compile(&query_json, ontology, ctx) {
        errors.push(format!("{path}::{name}: {err}"));
    }
    1
}

fn normalize_sample_placeholders(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "node_ids"
                    && let Some(sample) = child.as_str().and_then(sample_count)
                {
                    *child = Value::Array(
                        (1..=sample)
                            .map(|id| Value::Number(serde_json::Number::from(id)))
                            .collect(),
                    );
                    continue;
                }
                normalize_sample_placeholders(child);
            }
        }
        Value::Array(items) => {
            for child in items {
                normalize_sample_placeholders(child);
            }
        }
        _ => {}
    }
}

fn sample_count(value: &str) -> Option<i64> {
    value.strip_prefix("$sample").map(|suffix| {
        suffix
            .strip_prefix(':')
            .and_then(|count| count.parse::<i64>().ok())
            .unwrap_or(1)
    })
}
