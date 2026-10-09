use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use indexmap::IndexMap;
use semver::Version;
use serde::Serialize;
use serde_json::{Map, Value};
use shared::PipelineOutput;
use toon_format::{EncodeOptions, encode};

use super::graph::{
    ColumnDescriptor, GraphEdge, GraphResponse, GroupColumnDescriptor, PaginationResponse,
};
use super::text::{column_order, truncate, truncated_len};
use super::{FormatName, GraphFormatter, ResultFormatter};

pub static TOON_OUTPUT_FORMAT_VERSION: LazyLock<Version> = LazyLock::new(|| {
    orbit_versions::VERSIONS
        .toon_output_format
        .parse()
        .expect("TOON_OUTPUT_FORMAT_VERSION must be valid semver")
});

type Table = Vec<Map<String, Value>>;
type NodesByType = BTreeMap<String, IndexMap<i64, Map<String, Value>>>;

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
        let response = GraphFormatter.build_response(output);
        Value::String(
            encode(&ToonResponse::from(&response), &EncodeOptions::default())
                .expect("TOON response holds only JSON values"),
        )
    }
}

#[derive(Serialize)]
struct ToonResponse<'a> {
    query_type: &'a str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    nodes: BTreeMap<String, Table>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    edges: Table,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<&'a [ColumnDescriptor]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_columns: Option<&'a [GroupColumnDescriptor]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Table>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<&'a PaginationResponse>,
}

impl<'a> From<&'a GraphResponse> for ToonResponse<'a> {
    fn from(response: &'a GraphResponse) -> Self {
        let mut nodes = NodesByType::new();
        for node in &response.nodes {
            nodes
                .entry(node.entity_type.clone())
                .or_default()
                .insert(node.id, node.properties.clone());
        }
        let rows = response.rows.as_ref().map(|rows| {
            rows.iter()
                .map(|row| lift_group_nodes(row, &mut nodes))
                .collect()
        });
        Self {
            query_type: &response.query_type,
            nodes: nodes
                .into_iter()
                .map(|(entity, entries)| (entity, node_table(entries)))
                .collect(),
            edges: edge_table(&response.edges),
            columns: response.columns.as_deref().filter(|c| !c.is_empty()),
            group_columns: response.group_columns.as_deref().filter(|c| !c.is_empty()),
            rows,
            pagination: response.pagination.as_ref(),
        }
    }
}

fn node_table(entries: IndexMap<i64, Map<String, Value>>) -> Table {
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
                    .is_some_and(|v| truncated_len(v, key).is_some())
            })
        })
        .collect();

    entries
        .iter()
        .map(|(id, properties)| {
            let mut row = Map::with_capacity(keys.len() + truncated.len() + 1);
            row.insert("id".into(), Value::from(*id));
            for key in &keys {
                let value = properties.get(*key).unwrap_or(&Value::Null);
                row.insert((*key).into(), truncated_value(value, key));
                if truncated.contains(key) {
                    let length = truncated_len(value, key).map_or(Value::Null, Value::from);
                    row.insert(format!("{key}_len"), length);
                }
            }
            row
        })
        .collect()
}

fn truncated_value(value: &Value, key: &str) -> Value {
    match value {
        Value::String(s) => Value::String(truncate(s, key).into_owned()),
        other => other.clone(),
    }
}

fn edge_table(edges: &[GraphEdge]) -> Table {
    let has_path = edges.iter().any(|edge| edge.path_id.is_some());
    let has_depth = edges.iter().any(|edge| edge.depth.is_some());
    edges
        .iter()
        .map(|edge| {
            let mut row = Map::with_capacity(8);
            row.insert("from".into(), Value::from(edge.from.as_str()));
            row.insert("from_id".into(), Value::from(edge.from_id));
            row.insert("type".into(), Value::from(edge.edge_type.as_str()));
            row.insert("to".into(), Value::from(edge.to.as_str()));
            row.insert("to_id".into(), Value::from(edge.to_id));
            if has_path {
                row.insert("path_id".into(), edge.path_id.into());
                row.insert("step".into(), edge.step.into());
            }
            if has_depth {
                row.insert("depth".into(), edge.depth.into());
            }
            row
        })
        .collect()
}

fn lift_group_nodes(row: &Map<String, Value>, nodes: &mut NodesByType) -> Map<String, Value> {
    row.iter()
        .map(|(column, cell)| (column.clone(), lift_group_node(cell, nodes)))
        .collect()
}

fn lift_group_node(cell: &Value, nodes: &mut NodesByType) -> Value {
    let Some(object) = cell.as_object() else {
        return cell.clone();
    };
    let (Some(entity), Some(id), Some(properties)) = (
        object.get("type").and_then(Value::as_str),
        object
            .get("id")
            .and_then(Value::as_str)
            .and_then(|id| id.parse::<i64>().ok()),
        object.get("properties").and_then(Value::as_object),
    ) else {
        return cell.clone();
    };
    nodes
        .entry(entity.to_string())
        .or_default()
        .entry(id)
        .or_insert_with(|| properties.clone());
    Value::from(id)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn properties(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn node_tables_share_columns_and_truncate_long_text() {
        let long_title = "t".repeat(250);
        let entries = IndexMap::from([
            (7, properties(json!({"title": long_title, "iid": 3}))),
            (
                9,
                properties(json!({"state": "opened", "iid": 4, "title": "short"})),
            ),
        ]);
        let table = node_table(entries);
        assert_eq!(
            table[0].keys().collect::<Vec<_>>(),
            ["id", "iid", "state", "title", "title_len"]
        );
        assert_eq!(table[0]["state"], Value::Null);
        assert_eq!(table[0]["title"].as_str().unwrap().chars().count(), 200);
        assert_eq!(table[0]["title_len"], 250);
        assert_eq!(table[1]["title"], "short");
        assert_eq!(table[1]["title_len"], Value::Null);

        let text = encode(&json!({"MergeRequest": table}), &EncodeOptions::default()).unwrap();
        assert!(
            text.starts_with("MergeRequest[2]{id,iid,state,title,title_len}:\n"),
            "{text}"
        );
    }
}
