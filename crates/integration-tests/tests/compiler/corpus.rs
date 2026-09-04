//! Every corpus entry's `opencypher` twin compiles to the same SQL as its DSL
//! `query`. The corpus smoke test (Docker) proves the JSON runs end to end;
//! this proves the text frontend builds the same `Input`, so the twin runs
//! too. Entries without a twin must say why in `opencypher_skip`.
//!
//! One canonicalization is applied to the JSON side first: an `id` equality
//! or `in` filter becomes `node_ids`, which is the only spelling `{id: v}`
//! has in openCypher and the plan the planner narrows through.

use std::collections::BTreeMap;

use compiler::input::QueryOptions;
use compiler::{Input, compile, compile_from_input};
use serde::Deserialize;

use super::setup::{admin_ctx, embedded_ontology};

const CORPUS_DIR: &str = concat!(env!("FIXTURES_DIR"), "/queries/corpus");

#[derive(Deserialize)]
struct Entry {
    query: String,
    #[serde(default)]
    expect: Option<String>,
    #[serde(default)]
    opencypher: Option<String>,
    #[serde(default)]
    opencypher_skip: Option<String>,
}

/// `{{TOKEN}}` placeholders become `1` on both sides, as the smoke test does.
fn resolve_tokens(text: &str) -> String {
    regex::Regex::new(r"\{\{[^}]+\}\}")
        .unwrap()
        .replace_all(text, "1")
        .into_owned()
}

/// `"node_ids": "$sample[:N]"` becomes `[1..=N]`, as the smoke test does, and
/// `filters.id` equality/`in` folds into `node_ids`.
fn resolve_json(query: &str) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(&resolve_tokens(query)).unwrap();
    for node in value["nodes"].as_array_mut().into_iter().flatten() {
        if let Some(s) = node.get("node_ids").and_then(|v| v.as_str()) {
            let count: i64 = match s.strip_prefix("$sample:") {
                Some(n) => n.parse().unwrap_or(1),
                None => 1,
            };
            let ids: Vec<i64> = (1..=count).collect();
            node["node_ids"] = serde_json::json!(ids);
        }
        fold_id_filter(node);
    }
    value
}

fn fold_id_filter(node: &mut serde_json::Value) {
    let Some(filters) = node.get_mut("filters").and_then(|f| f.as_object_mut()) else {
        return;
    };
    let Some(id_filter) = filters.get("id").cloned() else {
        return;
    };
    let ids: Vec<serde_json::Value> = match &id_filter {
        serde_json::Value::Object(ops) if ops.len() == 1 => match (ops.get("eq"), ops.get("in")) {
            (Some(v), None) => vec![v.clone()],
            (None, Some(serde_json::Value::Array(items))) => items.clone(),
            _ => return,
        },
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => return,
        scalar => vec![scalar.clone()],
    };
    filters.remove("id");
    if filters.is_empty() {
        node.as_object_mut().unwrap().remove("filters");
    }
    let mut node_ids = node
        .get("node_ids")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    node_ids.extend(ids);
    node["node_ids"] = serde_json::Value::Array(node_ids);
}

/// The twin spells `$sample[:N]` as the parameter `$sample[_N]`, bound to the
/// same id list the JSON side resolves to.
fn sample_params(statement: &str) -> opencypher::Parameters {
    let mut params = opencypher::Parameters::new();
    for cap in regex::Regex::new(r"\$sample(?:_(\d+))?")
        .unwrap()
        .captures_iter(statement)
    {
        let count: i64 = cap.get(1).map_or(1, |n| n.as_str().parse().unwrap());
        let ids: Vec<i64> = (1..=count).collect();
        params.insert(cap[0][1..].to_string(), serde_json::json!(ids));
    }
    params
}

fn lower_twin(
    statement: &str,
    json: &serde_json::Value,
    ontology: &compiler::Ontology,
) -> compiler::Result<Input> {
    let statement = resolve_tokens(statement);
    let mut input = opencypher::lower(&statement, &sample_params(&statement), ontology)?;
    if let Some(cursor) = json.get("cursor") {
        let page_size = cursor["page_size"].as_u64().unwrap_or(30) as u32;
        let after = cursor
            .get("after")
            .and_then(|a| a.as_str())
            .map(str::to_owned);
        opencypher::attach_cursor(&mut input, page_size, after)?;
    }
    if let Some(options) = json.get("options") {
        input.options = QueryOptions::deserialize(options).unwrap();
    }
    Ok(input)
}

#[test]
fn every_corpus_twin_compiles_to_the_same_sql() {
    let ontology = embedded_ontology();
    let ctx = admin_ctx();
    let mut files: Vec<_> = std::fs::read_dir(CORPUS_DIR)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().is_some_and(|x| x == "yaml")
                && p.file_name().is_some_and(|n| n != "raw_sql_ab.yaml")
        })
        .collect();
    files.sort();

    let mut failures = Vec::new();
    let mut checked = 0;
    for path in files {
        let file = path.file_name().unwrap().to_string_lossy().into_owned();
        let entries: BTreeMap<String, Entry> =
            orbit_utils::yaml::from_str(&std::fs::read_to_string(&path).unwrap())
                .unwrap_or_else(|e| panic!("parse {file}: {e}"));
        for (key, entry) in entries {
            let name = format!("{file}::{key}");
            if entry.expect.as_deref() == Some("error") {
                continue;
            }
            let Some(statement) = entry.opencypher.as_deref() else {
                if entry.opencypher_skip.is_none() {
                    failures.push(format!(
                        "{name}: no `opencypher` twin and no `opencypher_skip` reason"
                    ));
                }
                continue;
            };
            let json = resolve_json(&entry.query);
            let expected = match compile(&json.to_string(), &ontology, &ctx) {
                Ok(c) => c,
                Err(e) => {
                    failures.push(format!("{name}: JSON does not compile: {e}"));
                    continue;
                }
            };
            let actual = match lower_twin(statement, &json, &ontology)
                .and_then(|input| compile_from_input(input, &ontology, &ctx))
            {
                Ok(c) => c,
                Err(e) => {
                    failures.push(format!("{name}: openCypher failed: {e}\n{statement}"));
                    continue;
                }
            };
            if actual.base != expected.base
                || actual.query_type != expected.query_type
                || actual.hydration != expected.hydration
            {
                failures.push(format!(
                    "{name}: compiled query differs\n{statement}\nopenCypher: {}\nJSON:       {}",
                    actual.base.sql, expected.base.sql
                ));
                continue;
            }
            checked += 1;
        }
    }
    assert!(
        failures.is_empty(),
        "{} twin(s) failed:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert!(checked > 300, "only {checked} twins checked");
}
