use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use compiler::input::{Direction, group_by_output_names};
use compiler::{QueryType, ResultContext, neighbor_is_outgoing_column, relationship_type_column};
use orbit_utils::arrow::ColumnValue;
use orbit_utils::strings::quote_escaped;
use serde_json::{Map, Value};
use shared::{PaginationMeta, PipelineOutput};
use types::{NodeRef, QueryResultRow};

use crate::column_value_to_json;
use crate::graph::{GraphFormatter, is_reserved_node_key};
use crate::text::{ordered_pairs, truncate};

pub fn encode(output: &PipelineOutput) -> String {
    let (columns, rows) = match output.compiled.input.query_type {
        QueryType::Aggregation => aggregation_table(output),
        QueryType::PathFinding | QueryType::Neighbors => path_table(output),
        _ => node_table(output),
    };
    let mut seen = HashSet::new();
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .filter(|row| seen.insert(row.clone()))
        .collect();
    render(&columns, &rows, output.pagination.as_ref())
}

type Table = (Vec<String>, Vec<Vec<String>>);

fn node_table(output: &PipelineOutput) -> Table {
    let context = &output.result_context;
    let prefixes = edge_prefixes(context);
    let aliases: Vec<String> = output
        .compiled
        .input
        .nodes
        .iter()
        .map(|n| n.id.clone())
        .collect();
    let rows = output
        .query_result
        .authorized_rows()
        .map(|row| {
            aliases
                .iter()
                .map(|alias| row_node(row, context, &prefixes, alias).unwrap_or_else(null))
                .collect()
        })
        .collect();
    (aliases, rows)
}

fn path_table(output: &PipelineOutput) -> Table {
    let prefixes = edge_prefixes(&output.result_context);
    let rows = output
        .query_result
        .authorized_rows()
        .filter_map(|row| match output.compiled.input.query_type {
            QueryType::Neighbors => neighbor_path(row, output, &prefixes),
            _ => shortest_path(row),
        })
        .map(|path| vec![path])
        .collect();
    (vec!["path".into()], rows)
}

fn aggregation_table(output: &PipelineOutput) -> Table {
    let aggregation = &output.compiled.input.aggregation;
    let columns: Vec<String> = group_by_output_names(&aggregation.group_by)
        .into_iter()
        .chain(
            aggregation
                .metrics
                .iter()
                .map(|metric| metric.output_name()),
        )
        .collect();
    let rows = GraphFormatter
        .build_response(output)
        .rows
        .unwrap_or_default()
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| match row.get(column) {
                    Some(Value::Object(cell)) if cell.contains_key("type") => group_node(cell),
                    Some(value) => literal(value, column),
                    None => null(),
                })
                .collect()
        })
        .collect();
    (columns, rows)
}

fn row_node(
    row: &QueryResultRow,
    context: &ResultContext,
    prefixes: &[&str],
    alias: &str,
) -> Option<String> {
    let node = context.get(alias)?;
    let properties = json_properties(&row.entity_properties(alias, prefixes));
    Some(node_literal(
        row.get_type(node)?,
        row.get_public_id(node)?,
        &properties,
    ))
}

fn neighbor_path(
    row: &QueryResultRow,
    output: &PipelineOutput,
    prefixes: &[&str],
) -> Option<String> {
    let input = &output.compiled.input;
    let center = row_node(
        row,
        &output.result_context,
        prefixes,
        &input.nodes.first()?.id,
    )?;
    let neighbor = dynamic_node(row.neighbor_node()?);
    let relationship = row
        .get_column_string(relationship_type_column())
        .unwrap_or_default();
    let outgoing = row
        .get(neighbor_is_outgoing_column())
        .and_then(|value| value.as_int64().copied())
        .map(|value| value != 0)
        .unwrap_or(!matches!(
            input.neighbors.as_ref().map(|n| n.direction),
            Some(Direction::Incoming)
        ));
    Some(if outgoing {
        format!("{center}-[:{relationship}]->{neighbor}")
    } else {
        format!("{center}<-[:{relationship}]-{neighbor}")
    })
}

fn shortest_path(row: &QueryResultRow) -> Option<String> {
    let (first, rest) = row.path_nodes().split_first()?;
    let mut path = dynamic_node(first);
    for (node, relationship) in rest.iter().zip(row.edge_kinds()) {
        let _ = write!(path, "-[:{relationship}]->{}", dynamic_node(node));
    }
    Some(path)
}

fn dynamic_node(node: &NodeRef) -> String {
    node_literal(
        &node.entity_type,
        node.id,
        &json_properties(&node.properties),
    )
}

fn group_node(cell: &Map<String, Value>) -> String {
    let label = cell.get("type").and_then(Value::as_str).unwrap_or_default();
    let id = cell
        .get("id")
        .and_then(Value::as_str)
        .and_then(|id| id.parse().ok());
    let empty = Map::new();
    let properties = cell
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    id.map_or_else(null, |id| node_literal(label, id, properties))
}

fn node_literal(label: &str, id: i64, properties: &Map<String, Value>) -> String {
    let mut out = format!("(:{label} {{id: {id}");
    for (key, value) in ordered_pairs(properties) {
        if !value.is_null() {
            let _ = write!(out, ", {key}: {}", literal(value, key));
        }
    }
    out + "})"
}

fn literal(value: &Value, key: &str) -> String {
    match value {
        Value::Null => null(),
        Value::Bool(true) => "TRUE".into(),
        Value::Bool(false) => "FALSE".into(),
        Value::Number(n) if n.is_f64() => format!("{:?}", n.as_f64().unwrap_or_default()),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_escaped(&truncate(s, key)),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(|item| literal(item, key)).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}: {}", literal(v, k)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
    }
}

fn json_properties(properties: &HashMap<String, ColumnValue>) -> Map<String, Value> {
    properties
        .iter()
        .filter(|(key, _)| !is_reserved_node_key(key))
        .map(|(key, value)| (key.clone(), column_value_to_json(value)))
        .collect()
}

fn edge_prefixes(context: &ResultContext) -> Vec<&str> {
    context
        .edges()
        .iter()
        .map(|edge| edge.column_prefix.as_str())
        .collect()
}

fn null() -> String {
    "NULL".into()
}

fn render(columns: &[String], rows: &[Vec<String>], page: Option<&PaginationMeta>) -> String {
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            rows.iter()
                .map(|row| row[index].chars().count())
                .fold(column.chars().count(), usize::max)
        })
        .collect();
    let line = |cells: &[String]| {
        let mut out = String::from("|");
        for (cell, width) in cells.iter().zip(&widths) {
            let _ = write!(out, " {cell:<width$} |");
        }
        out
    };
    let header = line(columns);
    let border = format!("+{}+", "-".repeat(header.chars().count().saturating_sub(2)));
    let mut out = format!("{border}\n{header}\n{border}\n");
    for row in rows {
        out += &line(row);
        out.push('\n');
    }
    let count = rows.len();
    let _ = write!(
        out,
        "{border}\n\n{count} row{}",
        if count == 1 { "" } else { "s" }
    );
    if let Some(page) = page.filter(|page| page.has_more) {
        out += ", more available";
        if let Some(cursor) = &page.next_cursor {
            let _ = write!(out, "\nnext_cursor: {}", quote_escaped(cursor));
        }
    }
    out + "\n"
}

#[cfg(test)]
mod tests;
