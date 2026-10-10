use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::LazyLock;

use indexmap::IndexMap;
use orbit_utils::strings::quote_escaped;
use semver::Version;
use serde::Serialize;
use serde_json::{Map, Number, Value};
use shared::PipelineOutput;
use toon_format::utils::format_canonical_number;
use toon_format::{EncodeOptions, encode, is_literal_like, is_valid_unquoted_key};

use super::graph::{
    ColumnDescriptor, GraphEdge, GraphResponse, GroupColumnDescriptor, PaginationResponse,
    group_node_cell,
};
use super::text::{column_order, truncate, truncated_len};
use super::{FormatName, GraphFormatter, ResultFormatter};

pub static TOON_OUTPUT_FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| {
    orbit_versions::VERSIONS
        .toon_output_format
        .parse()
        .expect("TOON_OUTPUT_FORMAT_VERSION must be valid semver")
});

type Properties = Map<String, Value>;
type NodesByType<'a> = BTreeMap<&'a str, IndexMap<i64, &'a Properties>>;

#[derive(Clone, Copy)]
pub struct ToonFormatter;

impl ResultFormatter for ToonFormatter {
    fn format_name(&self) -> FormatName {
        FormatName::Toon
    }

    fn format_version(&self) -> Option<&Version> {
        Some(&TOON_OUTPUT_FORMAT_VERSION)
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        Value::String(encode_response(&GraphFormatter.build_response(output)))
    }
}

#[derive(Serialize)]
struct Descriptors<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<&'a [ColumnDescriptor]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_columns: Option<&'a [GroupColumnDescriptor]>,
}

#[derive(Serialize)]
struct Pagination<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<&'a PaginationResponse>,
}

fn encode_response(response: &GraphResponse) -> String {
    let mut out = String::with_capacity(256 + response.nodes.len() * 128);
    out.push_str("query_type: ");
    write_text(&mut out, &response.query_type);
    out.push('\n');
    let nodes = nodes_by_type(response);
    if !nodes.is_empty() {
        out.push_str("nodes:\n");
        for (entity, entries) in &nodes {
            write_node_table(&mut out, entity, entries);
        }
    }
    if !response.edges.is_empty() {
        write_edge_table(&mut out, &response.edges);
    }
    append_encoded(
        &mut out,
        &Descriptors {
            columns: response.columns.as_deref().filter(|c| !c.is_empty()),
            group_columns: response.group_columns.as_deref().filter(|c| !c.is_empty()),
        },
    );
    if let Some(rows) = &response.rows {
        write_row_table(&mut out, rows);
    }
    append_encoded(
        &mut out,
        &Pagination {
            pagination: response.pagination.as_ref(),
        },
    );
    out
}

fn nodes_by_type(response: &GraphResponse) -> NodesByType<'_> {
    let mut nodes = NodesByType::new();
    for node in &response.nodes {
        nodes
            .entry(node.entity_type.as_str())
            .or_default()
            .insert(node.id, &node.properties);
    }
    for cell in response.rows.iter().flatten().flat_map(Map::values) {
        if let Some((entity, id, properties)) = group_node_cell(cell) {
            nodes
                .entry(entity)
                .or_default()
                .entry(id)
                .or_insert(properties);
        }
    }
    nodes
}

fn write_node_table(out: &mut String, entity: &str, entries: &IndexMap<i64, &Properties>) {
    let mut keys: Vec<&str> = entries
        .values()
        .flat_map(|properties| properties.keys().map(String::as_str))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    keys.sort_by(|a, b| column_order(a, b));
    let truncated: BTreeSet<&str> = keys
        .iter()
        .copied()
        .filter(|key| {
            entries.values().any(|properties| {
                properties
                    .get(*key)
                    .is_some_and(|value| truncated_len(value, key).is_some())
            })
        })
        .collect();
    let mut columns = vec![Cow::Borrowed("id")];
    for key in &keys {
        columns.push(Cow::Borrowed(*key));
        if truncated.contains(key) {
            columns.push(Cow::Owned(format!("{key}_len")));
        }
    }
    write_header(out, 1, entity, entries.len(), &columns);
    for (id, properties) in entries {
        out.push_str("    ");
        let _ = write!(out, "{id}");
        for key in &keys {
            out.push(',');
            let value = properties.get(*key).unwrap_or(&Value::Null);
            match value {
                Value::String(text) => write_text(out, &truncate(text, key)),
                other => write_value(out, other),
            }
            if truncated.contains(key) {
                out.push(',');
                write_optional(out, truncated_len(value, key));
            }
        }
        out.push('\n');
    }
}

fn write_edge_table(out: &mut String, edges: &[GraphEdge]) {
    let has_path = edges.iter().any(|edge| edge.path_id.is_some());
    let has_depth = edges.iter().any(|edge| edge.depth.is_some());
    let mut columns = vec!["from", "from_id", "type", "to", "to_id"];
    if has_path {
        columns.extend(["path_id", "step"]);
    }
    if has_depth {
        columns.push("depth");
    }
    write_header(out, 0, "edges", edges.len(), &columns);
    for edge in edges {
        out.push_str("  ");
        write_text(out, &edge.from);
        let _ = write!(out, ",{},", edge.from_id);
        write_text(out, &edge.edge_type);
        out.push(',');
        write_text(out, &edge.to);
        let _ = write!(out, ",{}", edge.to_id);
        if has_path {
            out.push(',');
            write_optional(out, edge.path_id);
            out.push(',');
            write_optional(out, edge.step);
        }
        if has_depth {
            out.push(',');
            write_optional(out, edge.depth);
        }
        out.push('\n');
    }
}

fn write_row_table(out: &mut String, rows: &[Properties]) {
    let mut columns: Vec<&str> = Vec::new();
    for column in rows.iter().flat_map(Map::keys) {
        if !columns.contains(&column.as_str()) {
            columns.push(column);
        }
    }
    write_header(out, 0, "rows", rows.len(), &columns);
    for row in rows {
        out.push_str("  ");
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let cell = row.get(*column).unwrap_or(&Value::Null);
            match group_node_cell(cell) {
                Some((_, id, _)) => {
                    let _ = write!(out, "{id}");
                }
                None => match cell {
                    Value::String(text) => write_text(out, &truncate(text, column)),
                    other => write_value(out, other),
                },
            }
        }
        out.push('\n');
    }
}

fn write_header(
    out: &mut String,
    depth: usize,
    name: &str,
    len: usize,
    columns: &[impl AsRef<str>],
) {
    out.push_str(&"  ".repeat(depth));
    write_key(out, name);
    let _ = write!(out, "[{len}]");
    if len > 0 && !columns.is_empty() {
        out.push('{');
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            write_key(out, column.as_ref());
        }
        out.push('}');
    }
    out.push_str(":\n");
}

fn write_key(out: &mut String, key: &str) {
    if is_valid_unquoted_key(key) {
        out.push_str(key);
    } else {
        out.push_str(&quote_escaped(key));
    }
}

fn write_text(out: &mut String, text: &str) {
    if needs_quoting(text) {
        out.push_str(&quote_escaped(text));
    } else {
        out.push_str(text);
    }
}

const QUOTED_BYTES: [bool; 256] = {
    let mut table = [false; 256];
    let mut byte = 0;
    while byte < 256 {
        table[byte] = byte < 0x20
            || matches!(
                byte as u8,
                b'[' | b']' | b'{' | b'}' | b':' | b'-' | b'\\' | b'"' | b',' | 0x7f
            );
        byte += 1;
    }
    table
};

fn needs_quoting(text: &str) -> bool {
    let bytes = text.as_bytes();
    let (Some(first), Some(last)) = (text.chars().next(), text.chars().next_back()) else {
        return true;
    };
    first.is_whitespace()
        || last.is_whitespace()
        || bytes.iter().any(|byte| QUOTED_BYTES[usize::from(*byte)])
        || (matches!(bytes[0], b'0'..=b'9' | b't' | b'f' | b'n')
            && (is_literal_like(text)
                || (bytes[0] == b'0' && bytes.get(1).is_some_and(u8::is_ascii_digit))))
}

fn write_optional(out: &mut String, value: Option<impl std::fmt::Display>) {
    match value {
        Some(value) => {
            let _ = write!(out, "{value}");
        }
        None => out.push_str("null"),
    }
}

fn write_value(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(flag) => {
            let _ = write!(out, "{flag}");
        }
        Value::Number(number) => write_number(out, number),
        Value::String(text) => write_text(out, text),
        other => write_text(out, &other.to_string()),
    }
}

fn write_number(out: &mut String, number: &Number) {
    if let Some(integer) = number.as_i64() {
        let _ = write!(out, "{integer}");
    } else if let Some(integer) = number.as_u64() {
        let _ = write!(out, "{integer}");
    } else {
        let float = number
            .as_f64()
            .and_then(toon_format::types::Number::from_f64);
        out.push_str(&float.map_or_else(|| "null".into(), |f| format_canonical_number(&f)));
    }
}

fn append_encoded(out: &mut String, section: &impl Serialize) {
    let text =
        encode(section, &EncodeOptions::default()).expect("TOON section holds only JSON values");
    if !text.is_empty() {
        out.push_str(&text);
        out.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::graph::GraphNode;

    fn node(entity: &str, id: i64, properties: Value) -> GraphNode {
        GraphNode {
            entity_type: entity.into(),
            id,
            properties: properties.as_object().unwrap().clone(),
        }
    }

    fn edge(from_id: i64, to_id: i64, depth: Option<i64>) -> GraphEdge {
        GraphEdge {
            from: "User".into(),
            from_id,
            to: "Group".into(),
            to_id,
            edge_type: "MEMBER_OF".into(),
            depth,
            path_id: None,
            step: None,
        }
    }

    fn response(query_type: &str, nodes: Vec<GraphNode>, edges: Vec<GraphEdge>) -> GraphResponse {
        GraphResponse {
            format_version: String::new(),
            query_type: query_type.into(),
            nodes,
            edges,
            columns: None,
            group_columns: None,
            rows: None,
            pagination: None,
        }
    }

    fn decoded(text: &str) -> Value {
        let value: Value =
            toon_format::decode_strict(text).unwrap_or_else(|error| panic!("{error}: {text}"));
        let canonical = encode(&value, &EncodeOptions::default()).unwrap() + "\n";
        assert_eq!(text, canonical);
        value
    }

    #[test]
    fn quoting_matches_the_toon_crate() {
        let alphabet = [
            'a', 't', 'n', 'f', '0', '7', '.', 'e', '-', ' ', ',', ':', '[', '{', '"', '\\', '\n',
            '\u{a0}', 'é',
        ];
        let mut values = vec![String::new()];
        let mut level = vec![String::new()];
        for _ in 0..4 {
            level = level
                .iter()
                .flat_map(|prefix| alphabet.iter().map(move |c| format!("{prefix}{c}")))
                .collect();
            values.extend(level.iter().cloned());
        }
        values.extend(
            ["true", "null", "false", "1e5", "-0", "007", "0.5", "truex"].map(String::from),
        );
        for value in &values {
            assert_eq!(
                needs_quoting(value),
                toon_format::needs_quoting(value, ','),
                "{value:?}"
            );
            assert_eq!(
                quote_escaped(value),
                toon_format::utils::string::quote_string(value),
                "{value:?}"
            );
        }
    }

    #[test]
    fn node_tables_order_columns_fill_nulls_truncate_and_drop_controls() {
        let text = encode_response(&response(
            "traversal",
            vec![
                node(
                    "MergeRequest",
                    7,
                    json!({"title": format!("\u{1b}[31m{}", "t".repeat(250)), "created_at": "2026-01-01", "web_url": "u", "iid": 3}),
                ),
                node(
                    "MergeRequest",
                    9,
                    json!({"iid": 4, "state": "opened", "title": "a\u{7f}b\tc"}),
                ),
            ],
            vec![],
        ));
        assert!(
            text.starts_with(
                "query_type: traversal\nnodes:\n  MergeRequest[2]{id,iid,state,web_url,created_at,title,title_len}:\n"
            ),
            "{text}"
        );
        let rows = &decoded(&text)["nodes"]["MergeRequest"];
        assert_eq!(rows[0]["state"], Value::Null);
        assert_eq!(rows[0]["title_len"], 255);
        let title = rows[0]["title"].as_str().unwrap();
        assert_eq!(title.chars().count(), 199);
        assert!(
            title.starts_with("[31mttt") && title.ends_with("..."),
            "{title}"
        );
        assert_eq!(rows[1]["title"], "ab\tc");
        assert_eq!(rows[1]["title_len"], Value::Null);
    }

    #[test]
    fn edge_tables_null_fill_optional_columns() {
        let text = encode_response(&response(
            "traversal",
            vec![],
            vec![edge(1, 22, Some(2)), edge(6, 22, None)],
        ));
        assert!(
            text.starts_with(
                "query_type: traversal\nedges[2]{from,from_id,type,to,to_id,depth}:\n"
            ),
            "{text}"
        );
        assert_eq!(decoded(&text)["edges"][1]["depth"], Value::Null);
    }

    #[test]
    fn aggregation_rows_share_one_lifted_node() {
        let group = json!({"type": "Group", "id": "22", "properties": {"name": "Toolbox"}});
        let mut aggregation = response("aggregation", vec![], vec![]);
        aggregation.rows = Some(
            [1, 2]
                .into_iter()
                .map(|n| json!({"g": group, "n": n}).as_object().unwrap().clone())
                .collect(),
        );
        let value = decoded(&encode_response(&aggregation));
        assert_eq!(
            value["nodes"],
            json!({"Group": [{"id": 22, "name": "Toolbox"}]})
        );
        assert_eq!(value["rows"], json!([{"g": 22, "n": 1}, {"g": 22, "n": 2}]));

        aggregation.rows = Some(vec![
            json!({"title": "t".repeat(250), "n": 1})
                .as_object()
                .unwrap()
                .clone(),
        ]);
        let value = decoded(&encode_response(&aggregation));
        assert_eq!(
            value["rows"][0]["title"].as_str().unwrap().chars().count(),
            200
        );
    }
}
