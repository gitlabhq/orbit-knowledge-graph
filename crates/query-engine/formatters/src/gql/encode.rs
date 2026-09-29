use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;

use orbit_utils::strings::quote_escaped;
use semver::Version;
use serde_json::{Map, Value};

use crate::graph::{GraphEdge, GraphResponse};
use crate::text::{dedup_and_sort_edges, ordered_pairs, truncate, truncated_len};

type Rows = Vec<Vec<String>>;

pub fn encode(response: &GraphResponse, format_version: &Version) -> String {
    let mut encoder = Encoder {
        nodes: response
            .nodes
            .iter()
            .map(|node| ((node.entity_type.as_str(), node.id), &node.properties))
            .collect(),
        rendered: HashSet::new(),
    };
    let (columns, rows) = match response.query_type.as_str() {
        "aggregation" => encoder.aggregation_rows(response),
        "path_finding" => (vec!["path".into()], encoder.path_rows(response)),
        _ => encoder.graph_rows(response),
    };
    let mut out = String::new();
    write_metadata(&mut out, response, format_version, rows.len());
    for row in std::iter::once(&columns).chain(&rows) {
        let _ = writeln!(out, "| {} |", row.join(" | "));
    }
    out
}

struct Encoder<'a> {
    nodes: HashMap<(&'a str, i64), &'a Map<String, Value>>,
    rendered: HashSet<(String, i64)>,
}

impl Encoder<'_> {
    fn graph_rows(&mut self, response: &GraphResponse) -> (Vec<String>, Rows) {
        let edges = dedup_and_sort_edges(&response.edges);
        let mut rows: Rows = edges.iter().map(|edge| vec![self.chain(&[edge])]).collect();
        let linked: HashSet<(&str, i64)> = edges
            .iter()
            .flat_map(|e| [(e.from.as_str(), e.from_id), (e.to.as_str(), e.to_id)])
            .collect();
        let mut isolated: Vec<_> = response
            .nodes
            .iter()
            .filter(|node| !linked.contains(&(node.entity_type.as_str(), node.id)))
            .collect();
        isolated.sort_by(|a, b| (&a.entity_type, a.id).cmp(&(&b.entity_type, b.id)));
        for node in isolated {
            rows.push(vec![self.node(
                &node.entity_type,
                node.id,
                Some(&node.properties),
            )]);
        }
        let column = if edges.is_empty() { "node" } else { "path" };
        (vec![column.into()], rows)
    }

    fn path_rows(&mut self, response: &GraphResponse) -> Rows {
        let mut paths: BTreeMap<usize, Vec<&GraphEdge>> = BTreeMap::new();
        for edge in dedup_and_sort_edges(&response.edges) {
            if let Some(path_id) = edge.path_id {
                paths.entry(path_id).or_default().push(edge);
            }
        }
        paths
            .values()
            .map(|steps| vec![self.chain(steps)])
            .collect()
    }

    fn aggregation_rows(&mut self, response: &GraphResponse) -> (Vec<String>, Rows) {
        let groups = response.group_columns.iter().flatten().map(|g| &g.name);
        let metrics = response.columns.iter().flatten().map(|c| &c.name);
        let columns: Vec<String> = groups.chain(metrics).cloned().collect();
        let rows = response
            .rows
            .iter()
            .flatten()
            .map(|row| {
                columns
                    .iter()
                    .map(|key| self.cell(row.get(key), key))
                    .collect()
            })
            .collect();
        (columns, rows)
    }

    fn chain(&mut self, steps: &[&GraphEdge]) -> String {
        let mut out = self.node_ref(&steps[0].from, steps[0].from_id);
        for step in steps {
            let depth = step.depth.map(|d| format!("*{d}")).unwrap_or_default();
            let _ = write!(out, "-[:{}{depth}]->", step.edge_type);
            out += &self.node_ref(&step.to, step.to_id);
        }
        out
    }

    fn cell(&mut self, value: Option<&Value>, key: &str) -> String {
        let Some(value) = value else {
            return "null".into();
        };
        let entity = value.get("type").and_then(Value::as_str);
        let id = value
            .get("id")
            .and_then(Value::as_str)
            .and_then(|id| id.parse().ok());
        match (entity, id) {
            (Some(entity), Some(id)) => self.node(
                entity,
                id,
                value.get("properties").and_then(Value::as_object),
            ),
            _ => literal(value, key),
        }
    }

    fn node_ref(&mut self, entity: &str, id: i64) -> String {
        let properties = self.nodes.get(&(entity, id)).copied();
        self.node(entity, id, properties)
    }

    fn node(&mut self, entity: &str, id: i64, properties: Option<&Map<String, Value>>) -> String {
        let mut out = format!("(:{entity} {{id: {id}");
        if self.rendered.insert((entity.to_string(), id)) {
            for (key, value) in properties.map(ordered_pairs).unwrap_or_default() {
                if value.is_null() || value == "" {
                    continue;
                }
                let _ = write!(out, ", {key}: {}", literal(value, key));
                if let Some(len) = truncated_len(value, key) {
                    let _ = write!(out, ", {key}_len: {len}");
                }
            }
        }
        out + "})"
    }
}

fn literal(value: &Value, key: &str) -> String {
    match value {
        Value::String(s) => quote_escaped(&truncate(s, key)),
        Value::Number(n) if n.is_f64() => format!("{:?}", n.as_f64().unwrap_or_default()),
        Value::Array(_) | Value::Object(_) => quote_escaped(&value.to_string()),
        other => other.to_string(),
    }
}

fn write_metadata(out: &mut String, response: &GraphResponse, version: &Version, rows: usize) {
    let _ = write!(out, "// query_type: {}, rows: {rows}", response.query_type);
    if let Some(page) = &response.pagination {
        if page.has_more {
            out.push_str(", has_more: true");
        }
        if page.truncated {
            out.push_str(", truncated: true");
        }
        if let Some(cursor) = &page.next_cursor {
            let _ = write!(out, ", next_cursor: {}", quote_escaped(cursor));
        }
    }
    let _ = writeln!(out, ", gql_version: {version}");

    let groups = response
        .group_columns
        .iter()
        .flatten()
        .map(|g| match (&g.entity, &g.property) {
            (Some(entity), _) => format!("{} = ({}:{entity})", g.name, g.node),
            (None, Some(property)) => format!("{} = {}.{property}", g.name, g.node),
            (None, None) => format!("{} = {}", g.name, g.node),
        });
    let metrics = response
        .columns
        .iter()
        .flatten()
        .map(|c| match &c.property {
            Some(property) => format!("{} = {}({}.{property})", c.name, c.function, c.target),
            None => format!("{} = {}({})", c.name, c.function, c.target),
        });
    for (label, items) in [
        ("group_by", groups.collect::<Vec<_>>()),
        ("aggregations", metrics.collect()),
    ] {
        if !items.is_empty() {
            let _ = writeln!(out, "// {label}: {}", items.join(", "));
        }
    }
}
