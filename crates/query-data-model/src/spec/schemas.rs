use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::catalog::backends;

/// The JSON schema of each fragment kind, and one storage schema per backend, by file name.
pub fn schemas() -> Vec<(String, String)> {
    let mut schemas = Vec::new();
    for (index, backend) in backends().enumerate() {
        let fragment = Value::from(backend.schema());
        for (kind, variant) in variants(&fragment) {
            let name = match kind {
                "storage" => format!("storage.{}.schema.json", backend.kind()),
                _ if index == 0 => format!("{kind}.schema.json"),
                _ => continue,
            };
            let mut schema = root(&fragment, variant);
            // Whether maps keep insertion order depends on the build's serde_json features.
            schema.sort_all_objects();
            let mut text = serde_json::to_string_pretty(&schema).expect("a schema serializes");
            text.push('\n');
            schemas.push((name, text));
        }
    }
    schemas.sort_by(|a, b| a.0.cmp(&b.0));
    schemas
}

fn variants(fragment: &Value) -> impl Iterator<Item = (&str, &Value)> {
    fragment["oneOf"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|variant| Some((variant["properties"]["kind"]["const"].as_str()?, variant)))
}

/// One variant as a schema of its own, with the definitions it reaches.
fn root(fragment: &Value, variant: &Value) -> Value {
    let definitions = &fragment["$defs"];
    let mut reached = BTreeSet::new();
    let mut pending = vec![variant];
    while let Some(schema) = pending.pop() {
        for name in references(schema) {
            if reached.insert(name.clone()) {
                pending.push(&definitions[name.as_str()]);
            }
        }
    }
    let mut root = Map::new();
    root.insert("$schema".into(), fragment["$schema"].clone());
    if let Some(variant) = variant.as_object() {
        root.extend(variant.clone());
    }
    if !reached.is_empty() {
        let reached: Map<String, Value> = reached
            .into_iter()
            .map(|name| {
                let definition = definitions[name.as_str()].clone();
                (name, definition)
            })
            .collect();
        root.insert("$defs".into(), reached.into());
    }
    root.into()
}

fn references(schema: &Value) -> Vec<String> {
    match schema {
        Value::Object(object) => object
            .iter()
            .flat_map(|(key, value)| match (key.as_str(), value) {
                ("$ref", Value::String(target)) => target
                    .strip_prefix("#/$defs/")
                    .map(str::to_string)
                    .into_iter()
                    .collect(),
                _ => references(value),
            })
            .collect(),
        Value::Array(values) => values.iter().flat_map(references).collect(),
        _ => Vec::new(),
    }
}
