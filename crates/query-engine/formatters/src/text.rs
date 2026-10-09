use std::borrow::Cow;

use orbit_utils::strings::{char_count_if_exceeds, truncate_chars};
use serde_json::{Map, Value};

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
