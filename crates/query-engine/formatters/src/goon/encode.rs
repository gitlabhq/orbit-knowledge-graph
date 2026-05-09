use std::collections::BTreeMap;
use std::fmt::Write;

use semver::Version;
use serde_json::{Map, Value};

use super::super::graph::{GraphEdge, GraphNode, GraphResponse};

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

pub fn encode(response: &GraphResponse, format_version: &Version) -> String {
    let mut out = String::with_capacity(estimate_capacity(response));
    write_header(&mut out, response, format_version);
    if response.query_type == "path_finding" {
        write_nodes(&mut out, response);
        write_paths(&mut out, response);
    } else {
        write_nodes(&mut out, response);
        write_edges(&mut out, response);
    }
    out
}

fn estimate_capacity(response: &GraphResponse) -> usize {
    128 + response.nodes.len() * 96 + response.edges.len() * 48
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

fn write_header(out: &mut String, response: &GraphResponse, format_version: &Version) {
    out.push_str("@header\n");
    let _ = writeln!(out, "query_type:{}", response.query_type);
    let _ = writeln!(out, "goon_version:{format_version}");
    let _ = writeln!(out, "nodes:{}", response.nodes.len());
    let _ = writeln!(out, "edges:{}", response.edges.len());
    if let Some(p) = &response.pagination {
        if p.has_more {
            out.push_str("has_more:true\n");
        }
        let _ = writeln!(out, "total_rows:{}", p.total_rows);
    }
    if response.query_type == "aggregation"
        && let Some(cols) = &response.columns
    {
        let parts: Vec<String> = cols
            .iter()
            .map(|c| format!("{}({})", c.name, c.function))
            .collect();
        if !parts.is_empty() {
            let _ = writeln!(out, "aggregations:{}", parts.join(","));
        }
        let inline: Vec<String> = cols
            .iter()
            .filter_map(|c| {
                c.value
                    .as_ref()
                    .map(|v| format!("{}={}", c.name, format_value(v, &c.name)))
            })
            .collect();
        if !inline.is_empty() {
            let _ = writeln!(out, "values:{}", inline.join(" "));
        }
    }
}

fn write_nodes(out: &mut String, response: &GraphResponse) {
    out.push_str("@nodes\n");
    if response.nodes.is_empty() {
        return;
    }
    let preserve_order = response.query_type == "aggregation";
    let groups = group_nodes(response, preserve_order);
    for (entity_type, indices) in groups {
        let _ = writeln!(out, "{}({}):", entity_type, indices.len());
        for idx in indices {
            write_node_row(out, &response.nodes[idx]);
        }
    }
}

fn write_node_row(out: &mut String, node: &GraphNode) {
    let _ = write!(out, "{}", node.id);
    for (key, val) in ordered_pairs(&node.properties) {
        let formatted = format_value(val, key);
        if formatted.is_empty() {
            continue;
        }
        let original_len = string_len_for_breadcrumb(val, key);
        let _ = write!(out, " {key}={formatted}");
        if let Some(len) = original_len {
            let _ = write!(out, " {key}_len={len}");
        }
    }
    out.push('\n');
}

fn write_edges(out: &mut String, response: &GraphResponse) {
    out.push_str("@edges\n");
    let edges = dedup_and_sort_edges(&response.edges);
    if edges.is_empty() {
        return;
    }
    let mut last_type: Option<&str> = None;
    let groups = group_edges_by_type(&edges);
    for (edge_type, slice) in groups {
        if last_type != Some(edge_type) {
            let _ = writeln!(out, "{}({}):", edge_type, slice.len());
            last_type = Some(edge_type);
        }
        for e in slice {
            let _ = writeln!(out, "{}:{} --> {}:{}", e.from, e.from_id, e.to, e.to_id);
        }
    }
}

fn write_paths(out: &mut String, response: &GraphResponse) {
    let edges = dedup_and_sort_edges(&response.edges);
    if !edges.iter().any(|e| e.path_id.is_some()) {
        return;
    }
    let mut by_path: BTreeMap<usize, Vec<&GraphEdge>> = BTreeMap::new();
    for e in &edges {
        if let Some(pid) = e.path_id {
            by_path.entry(pid).or_default().push(e);
        }
    }
    if by_path.is_empty() {
        return;
    }
    out.push_str("@paths\n");
    for (pid, mut steps) in by_path {
        steps.sort_by_key(|s| {
            (
                s.step.unwrap_or(0),
                s.edge_type.as_str().to_owned(),
                s.from_id,
                s.to_id,
            )
        });
        if steps.is_empty() {
            continue;
        }
        let first = steps[0];
        let _ = write!(out, "path={pid}: {}:{}", first.from, first.from_id);
        for s in &steps {
            let _ = write!(out, " --{}--> {}:{}", s.edge_type, s.to, s.to_id);
        }
        out.push('\n');
    }
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

fn group_nodes(response: &GraphResponse, preserve_order: bool) -> Vec<(&str, Vec<usize>)> {
    let mut order: Vec<usize> = (0..response.nodes.len()).collect();
    if !preserve_order {
        order.sort_by(|&a, &b| {
            let na = &response.nodes[a];
            let nb = &response.nodes[b];
            na.entity_type.cmp(&nb.entity_type).then(na.id.cmp(&nb.id))
        });
    }
    let mut groups: Vec<(&str, Vec<usize>)> = Vec::new();
    for idx in order {
        let entity_type = response.nodes[idx].entity_type.as_str();
        match groups.last_mut() {
            Some(last) if last.0 == entity_type => last.1.push(idx),
            _ => groups.push((entity_type, vec![idx])),
        }
    }
    groups
}

fn dedup_and_sort_edges(edges: &[GraphEdge]) -> Vec<&GraphEdge> {
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
    let mut seen = std::collections::HashSet::new();
    sorted.retain(|e| {
        seen.insert((
            e.edge_type.clone(),
            e.from.clone(),
            e.from_id,
            e.to.clone(),
            e.to_id,
            e.path_id,
            e.step,
        ))
    });
    sorted
}

fn group_edges_by_type<'a>(edges: &'a [&'a GraphEdge]) -> Vec<(&'a str, &'a [&'a GraphEdge])> {
    let mut groups: Vec<(&str, &[&GraphEdge])> = Vec::new();
    let mut start = 0;
    for i in 1..=edges.len() {
        let boundary = i == edges.len() || edges[i].edge_type != edges[start].edge_type;
        if boundary {
            groups.push((edges[start].edge_type.as_str(), &edges[start..i]));
            start = i;
        }
    }
    groups
}

fn ordered_pairs(props: &Map<String, Value>) -> Vec<(&str, &Value)> {
    let mut pairs: Vec<(&str, &Value)> = props.iter().map(|(k, v)| (k.as_str(), v)).collect();
    pairs.sort_by(|a, b| {
        column_priority(a.0)
            .cmp(&column_priority(b.0))
            .then(a.0.cmp(b.0))
    });
    pairs
}

// ---------------------------------------------------------------------------
// Value formatting
// ---------------------------------------------------------------------------

fn format_value(value: &Value, key: &str) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        Value::Number(n) if n.is_f64() => match n.as_f64() {
            Some(f) if f.is_finite() => f.to_string(),
            _ => String::new(),
        },
        Value::Number(n) => n.to_string(),
        Value::String(s) => format_string(s, key),
        other => format_string(&serde_json::to_string(other).unwrap_or_default(), key),
    }
}

fn format_string(raw: &str, key: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let truncated = truncate(raw, key);
    if let Some(normalized) = normalize_iso_datetime(&truncated) {
        return normalized;
    }
    // Strings that look like native scalars must be quoted so a reader can
    // tell `state="true"` (string) from a real boolean.
    if !matches!(truncated.as_ref(), "true" | "false" | "null") && is_bare_token(&truncated) {
        return truncated.into_owned();
    }
    quote_escaped(&truncated)
}

fn truncate<'a>(raw: &'a str, key: &str) -> std::borrow::Cow<'a, str> {
    let limit = if LONG_TEXT_KEYS.contains(&key) {
        LONG_TEXT_LIMIT
    } else {
        HARD_VALUE_LIMIT
    };
    if raw.chars().count() <= limit {
        return std::borrow::Cow::Borrowed(raw);
    }
    let take = limit.saturating_sub(3);
    let head: String = raw.chars().take(take).collect();
    std::borrow::Cow::Owned(format!("{head}..."))
}

fn string_len_for_breadcrumb(value: &Value, key: &str) -> Option<usize> {
    let Value::String(s) = value else { return None };
    let limit = if LONG_TEXT_KEYS.contains(&key) {
        LONG_TEXT_LIMIT
    } else {
        HARD_VALUE_LIMIT
    };
    let count = s.chars().count();
    (count > limit).then_some(count)
}

fn quote_escaped(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => {}
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn is_bare_token(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '.' | '/' | '@' | '+'))
}

/// ClickHouse emits datetimes as `2026-05-08 23:13:59.643407`. Convert to
/// ISO 8601 T-form so the value is bare-emittable; spaces inside a value
/// would break the space-delimited `key=value key=value` row format.
fn normalize_iso_datetime(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let valid = bytes.iter().enumerate().all(|(i, &b)| match (i, b) {
        (4 | 7, b'-') => true,
        (10, b' ' | b'T') => true,
        (13 | 16, b':') => true,
        (0..=3 | 5..=6 | 8..=9 | 11..=12 | 14..=15 | 17..=18, b) => b.is_ascii_digit(),
        (19.., b'.' | b'+' | b'-' | b'Z' | b':') => true,
        (19.., b) => b.is_ascii_digit(),
        _ => false,
    });
    if !valid {
        return None;
    }
    if bytes[10] == b' ' {
        let mut buf = String::with_capacity(s.len());
        buf.push_str(&s[..10]);
        buf.push('T');
        buf.push_str(&s[11..]);
        Some(buf)
    } else {
        Some(s.to_string())
    }
}
