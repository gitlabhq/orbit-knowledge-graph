use std::collections::BTreeMap;
use std::fmt::Write;

use semver::Version;
use serde_json::{Map, Value};

use super::super::graph::{
    ColumnDescriptor, GraphEdge, GraphNode, GraphResponse, GroupColumnDescriptor,
};

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
    let extra_nodes = aggregation_grouped_nodes(response);
    write_header(&mut out, response, format_version, extra_nodes.len());
    write_nodes(&mut out, response, &extra_nodes);
    if response.query_type == "path_finding" {
        write_paths(&mut out, response);
    } else {
        write_edges(&mut out, response);
    }
    if response.rows.is_some() {
        write_rows(&mut out, response);
    }
    out
}

/// Aggregation responses keep `nodes` empty — node-kind group cells inline
/// `{type, id, properties}` into each row. Lift those into a deduplicated
/// node list so a reader can resolve `Entity:id` row cells against an
/// `@nodes` section instead of repeating the same properties on every row.
fn aggregation_grouped_nodes(response: &GraphResponse) -> Vec<GraphNode> {
    if response.query_type != "aggregation" {
        return Vec::new();
    }
    let Some(rows) = &response.rows else {
        return Vec::new();
    };
    let Some(groups) = &response.group_columns else {
        return Vec::new();
    };
    let node_keys: Vec<&str> = groups
        .iter()
        .filter(|g| g.kind == "node")
        .map(|g| g.name.as_str())
        .collect();
    if node_keys.is_empty() {
        return Vec::new();
    }
    let mut seen: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        for key in &node_keys {
            let Some(Value::Object(obj)) = row.get(*key) else {
                continue;
            };
            let Some(Value::String(entity_type)) = obj.get("type") else {
                continue;
            };
            let Some(Value::String(id_str)) = obj.get("id") else {
                continue;
            };
            let Ok(id) = id_str.parse::<i64>() else {
                continue;
            };
            if !seen.insert((entity_type.clone(), id)) {
                continue;
            }
            let properties = match obj.get("properties") {
                Some(Value::Object(p)) => p.clone(),
                _ => Map::new(),
            };
            out.push(GraphNode {
                entity_type: entity_type.clone(),
                id,
                properties,
            });
        }
    }
    out
}

fn estimate_capacity(response: &GraphResponse) -> usize {
    let row_count = response.rows.as_ref().map_or(0, Vec::len);
    128 + response.nodes.len() * 96 + response.edges.len() * 48 + row_count * 64
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

fn write_header(
    out: &mut String,
    response: &GraphResponse,
    format_version: &Version,
    extra_node_count: usize,
) {
    out.push_str("@header\n");
    let _ = writeln!(out, "query_type:{}", response.query_type);
    let _ = writeln!(out, "goon_version:{format_version}");
    let _ = writeln!(out, "nodes:{}", response.nodes.len() + extra_node_count);
    let _ = writeln!(out, "edges:{}", response.edges.len());
    if let Some(p) = &response.pagination {
        if p.has_more {
            out.push_str("has_more:true\n");
        }
        if p.truncated {
            out.push_str("truncated:true\n");
        }
        let _ = writeln!(out, "total_rows:{}", p.total_rows);
    }
    if response.query_type == "aggregation" {
        if let Some(rows) = &response.rows {
            let _ = writeln!(out, "rows:{}", rows.len());
        }
        if let Some(groups) = &response.group_columns {
            let parts: Vec<String> = groups.iter().map(format_group_descriptor).collect();
            if !parts.is_empty() {
                let _ = writeln!(out, "group_by:{}", parts.join(","));
            }
        }
        if let Some(cols) = &response.columns {
            let parts: Vec<String> = cols.iter().map(format_aggregation_descriptor).collect();
            if !parts.is_empty() {
                let _ = writeln!(out, "aggregations:{}", parts.join(","));
            }
        }
    }
}

/// Render a metric column descriptor with its target node alias and (when set)
/// the aggregated property, so a reader can tell `count(v)` (count of v rows)
/// from `max(v.updated_at)` (max of v.updated_at) — both used to encode as
/// just `count` / `max`.
fn format_aggregation_descriptor(col: &ColumnDescriptor) -> String {
    let body = match (&col.target, &col.property) {
        (Some(target), Some(property)) => format!("{}:{target}.{property}", col.function),
        (Some(target), None) => format!("{}:{target}", col.function),
        (None, _) => col.function.clone(),
    };
    format!("{}({body})", col.name)
}

/// Render a group-by descriptor. For node kinds the entity type travels as
/// `name(node:Entity)`; for property kinds we surface the underlying ontology
/// property whenever the output column was aliased away from it
/// (`severity_bucket(property:severity)`), so the dimension stays
/// self-describing.
fn format_group_descriptor(group: &GroupColumnDescriptor) -> String {
    match group.kind.as_str() {
        "node" => match &group.entity {
            Some(entity) => format!("{}(node:{})", group.name, entity),
            None => format!("{}(node)", group.name),
        },
        "property" => match group.property.as_deref() {
            Some(property) if property != group.name => {
                format!("{}(property:{property})", group.name)
            }
            _ => format!("{}(property)", group.name),
        },
        other => format!("{}({other})", group.name),
    }
}

fn write_nodes(out: &mut String, response: &GraphResponse, extra: &[GraphNode]) {
    out.push_str("@nodes\n");
    if response.nodes.is_empty() && extra.is_empty() {
        return;
    }
    // For aggregation, server-side row order is meaningful (sort applied);
    // for everything else we sort by (entity_type, id) for determinism.
    let preserve_order = response.query_type == "aggregation";
    let combined: Vec<&GraphNode> = response.nodes.iter().chain(extra.iter()).collect();
    let groups = group_node_refs(&combined, preserve_order);
    for (entity_type, indices) in groups {
        let _ = writeln!(out, "{}({}):", entity_type, indices.len());
        for idx in indices {
            write_node_row(out, combined[idx]);
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
            let _ = write!(out, "{}:{} --> {}:{}", e.from, e.from_id, e.to, e.to_id);
            // Variable-length traversals tag each edge with the hop at which
            // it was found. Surface it so a reader can tell a depth-1 hit
            // from a depth-3 one.
            if let Some(d) = e.depth {
                let _ = write!(out, " depth={d}");
            }
            out.push('\n');
        }
    }
}

/// Aggregation rows are server-ordered (sort comes from `aggregation_sort`),
/// so preserve order. Within a row, render group columns first then metric
/// columns to mirror the table-shape declared in `@header`.
fn write_rows(out: &mut String, response: &GraphResponse) {
    out.push_str("@rows\n");
    let Some(rows) = &response.rows else { return };
    if rows.is_empty() {
        return;
    }
    let group_keys: Vec<&str> = response
        .group_columns
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|g| g.name.as_str())
        .collect();
    let metric_keys: Vec<&str> = response
        .columns
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    for row in rows {
        write_row(out, row, &group_keys, &metric_keys);
    }
}

fn write_row(
    out: &mut String,
    row: &Map<String, Value>,
    group_keys: &[&str],
    metric_keys: &[&str],
) {
    let mut first = true;
    for key in group_keys.iter().chain(metric_keys.iter()) {
        let Some(value) = row.get(*key) else { continue };
        let formatted = format_row_cell(value, key);
        if formatted.is_empty() {
            continue;
        }
        if !first {
            out.push(' ');
        }
        first = false;
        let _ = write!(out, "{key}={formatted}");
    }
    out.push('\n');
}

/// Node-kind group columns arrive as `{type, id, properties}` objects. Render
/// them as `Entity:id` so a row stays one line — full property bodies belong
/// in the @nodes section, not the @rows table. A literal `null` cell is a
/// real bucket value (a row counted "no severity assigned"); render it as
/// the bare token `null` rather than dropping the column, which would make
/// the row look like `count=5` and lose the dimension.
fn format_row_cell(value: &Value, key: &str) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Object(obj) => match (obj.get("type"), obj.get("id")) {
            (Some(Value::String(t)), Some(Value::String(id))) => format!("{t}:{id}"),
            _ => format_value(value, key),
        },
        _ => format_value(value, key),
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

fn group_node_refs<'a>(
    nodes: &'a [&'a GraphNode],
    preserve_order: bool,
) -> Vec<(&'a str, Vec<usize>)> {
    let mut order: Vec<usize> = (0..nodes.len()).collect();
    if !preserve_order {
        order.sort_by(|&a, &b| {
            let na = nodes[a];
            let nb = nodes[b];
            na.entity_type.cmp(&nb.entity_type).then(na.id.cmp(&nb.id))
        });
    }
    let mut groups: Vec<(&str, Vec<usize>)> = Vec::new();
    for idx in order {
        let entity_type = nodes[idx].entity_type.as_str();
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
/// would break the space-delimited `key=value key=value` row format. We
/// validate via chrono but rebuild the output byte-for-byte from the
/// original so microsecond precision (6 digits) is not silently widened
/// to chrono's nanosecond default (9 digits).
fn normalize_iso_datetime(s: &str) -> Option<String> {
    use chrono::{DateTime, NaiveDateTime};
    if DateTime::parse_from_rfc3339(s).is_ok() {
        return Some(s.to_string());
    }
    let parses = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").is_ok()
        || NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok();
    if !parses {
        return None;
    }
    if s.as_bytes().get(10) == Some(&b' ') {
        let mut buf = String::with_capacity(s.len());
        buf.push_str(&s[..10]);
        buf.push('T');
        buf.push_str(&s[11..]);
        Some(buf)
    } else {
        Some(s.to_string())
    }
}
