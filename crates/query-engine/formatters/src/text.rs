use std::borrow::Cow;
use std::collections::HashSet;

use orbit_utils::strings::{char_count_if_exceeds, truncate_chars};
use serde_json::{Map, Value};

use crate::graph::GraphEdge;

const LONG_TEXT_LIMIT: usize = 200;
const HARD_VALUE_LIMIT: usize = 1000;
const LONG_TEXT_KEYS: &[&str] = &["body", "description", "name", "note", "title"];

fn column_priority(key: &str) -> u8 {
    match key {
        "iid" | "username" | "name" | "full_path" | "path" | "uuid" => 0,
        "state" | "status" | "visibility_level" => 1,
        "created_at" | "updated_at" | "merged_at" | "closed_at" => 3,
        "title" | "description" | "body" | "note" => 4,
        _ => 2,
    }
}

pub(crate) fn ordered_pairs(properties: &Map<String, Value>) -> Vec<(&str, &Value)> {
    let mut pairs: Vec<(&str, &Value)> = properties
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect();
    pairs.sort_by(|a, b| {
        column_priority(a.0)
            .cmp(&column_priority(b.0))
            .then(a.0.cmp(b.0))
    });
    pairs
}

fn value_limit(key: &str) -> usize {
    if LONG_TEXT_KEYS.contains(&key) {
        LONG_TEXT_LIMIT
    } else {
        HARD_VALUE_LIMIT
    }
}

pub(crate) fn truncate<'a>(raw: &'a str, key: &str) -> Cow<'a, str> {
    truncate_chars(raw, value_limit(key), "...")
}

pub(crate) fn truncated_len(value: &Value, key: &str) -> Option<usize> {
    let Value::String(s) = value else { return None };
    char_count_if_exceeds(s, value_limit(key))
}

pub(crate) fn dedup_and_sort_edges(edges: &[GraphEdge]) -> Vec<&GraphEdge> {
    let mut sorted: Vec<&GraphEdge> = edges.iter().collect();
    sorted.sort_by(|a, b| {
        a.path_id
            .unwrap_or(usize::MAX)
            .cmp(&b.path_id.unwrap_or(usize::MAX))
            .then(
                a.step
                    .unwrap_or(usize::MAX)
                    .cmp(&b.step.unwrap_or(usize::MAX)),
            )
            .then(a.edge_type.cmp(&b.edge_type))
            .then(a.from.cmp(&b.from))
            .then(a.from_id.cmp(&b.from_id))
            .then(a.to.cmp(&b.to))
            .then(a.to_id.cmp(&b.to_id))
            .then(a.depth.cmp(&b.depth))
    });
    let mut seen = HashSet::new();
    sorted.retain(|e| {
        seen.insert((
            e.edge_type.as_str(),
            e.from.as_str(),
            e.from_id,
            e.to.as_str(),
            e.to_id,
            e.path_id,
            e.step,
            e.depth,
        ))
    });
    sorted
}
