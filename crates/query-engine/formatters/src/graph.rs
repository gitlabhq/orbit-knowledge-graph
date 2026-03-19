use std::collections::HashSet;

use compiler::{
    EdgeMeta, NEIGHBOR_IS_OUTGOING_COLUMN, QueryType, RELATIONSHIP_TYPE_COLUMN, ResultContext,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use shared::PipelineOutput;
use types::{QueryResult, QueryResultRow};

use super::{ResultFormatter, column_value_to_json};

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphResponse {
    pub query_type: String,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphNode {
    #[serde(rename = "type")]
    pub entity_type: String,
    pub id: i64,
    #[serde(flatten)]
    pub properties: serde_json::Map<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub from_id: i64,
    pub to: String,
    pub to_id: i64,
    #[serde(rename = "type")]
    pub edge_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_id: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<usize>,
}

type EdgeKey = (String, i64, String, i64, String, Option<i64>);

#[derive(Clone, Copy)]
pub struct GraphFormatter;

impl ResultFormatter for GraphFormatter {
    fn format(&self, output: &PipelineOutput) -> Value {
        let response = self.build_response(output);
        serde_json::to_value(response).unwrap_or(Value::Null)
    }
}

impl GraphFormatter {
    pub fn build_response(&self, output: &PipelineOutput) -> GraphResponse {
        let result = &output.query_result;
        let result_context = &output.result_context;

        let query_type = result_context
            .query_type
            .map(|qt| qt.to_string())
            .unwrap_or_default();

        let mut node_map: IndexMap<(String, i64), GraphNode> = IndexMap::new();
        let mut edges: Vec<GraphEdge> = Vec::new();
        let mut edge_set: HashSet<EdgeKey> = HashSet::new();

        let aggregations = Some(&output.compiled.input.aggregations);

        let edge_prefixes: Vec<&str> = result_context
            .edges()
            .iter()
            .map(|e| e.column_prefix.as_str())
            .collect();

        match result_context.query_type {
            Some(QueryType::Search) => {
                self.extract_search_nodes(result, result_context, &edge_prefixes, &mut node_map);
            }
            Some(QueryType::Traversal) => {
                self.extract_search_nodes(result, result_context, &edge_prefixes, &mut node_map);
                self.extract_traversal_edges(
                    result,
                    result_context.edges(),
                    &mut edges,
                    &mut edge_set,
                );
            }
            Some(QueryType::Aggregation) => {
                self.extract_aggregation(
                    result,
                    result_context,
                    &edge_prefixes,
                    aggregations,
                    &mut node_map,
                );
            }
            Some(QueryType::PathFinding) => {
                self.extract_path_finding(result, &mut node_map, &mut edges);
            }
            Some(QueryType::Neighbors) => {
                self.extract_neighbors(
                    result,
                    result_context,
                    &edge_prefixes,
                    output,
                    &mut node_map,
                    &mut edges,
                );
            }
            None => {}
        }

        GraphResponse {
            query_type,
            nodes: node_map.into_values().collect(),
            edges,
        }
    }

    fn extract_search_nodes(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        edge_prefixes: &[&str],
        node_map: &mut IndexMap<(String, i64), GraphNode>,
    ) {
        for row in result.authorized_rows() {
            for node in result_context.nodes() {
                let Some(id) = row.get_public_id(node) else {
                    continue;
                };
                let Some(entity_type) = row.get_type(node) else {
                    continue;
                };
                let key = (entity_type.to_string(), id);
                node_map.entry(key).or_insert_with(|| {
                    let properties = Self::extract_node_properties(row, &node.alias, edge_prefixes);
                    GraphNode {
                        entity_type: entity_type.to_string(),
                        id,
                        properties,
                    }
                });
            }
        }
    }

    fn extract_node_properties(
        row: &QueryResultRow,
        alias: &str,
        edge_prefixes: &[&str],
    ) -> serde_json::Map<String, Value> {
        row.entity_properties(alias, edge_prefixes)
            .into_iter()
            .map(|(k, v)| (k, column_value_to_json(&v)))
            .collect()
    }

    fn extract_traversal_edges(
        &self,
        result: &QueryResult,
        edge_metas: &[EdgeMeta],
        edges: &mut Vec<GraphEdge>,
        edge_set: &mut HashSet<EdgeKey>,
    ) {
        for row in result.authorized_rows() {
            for meta in edge_metas {
                let Some(edge_type) = row.get_column_string(&meta.type_column) else {
                    continue;
                };
                let Some(src_id) = row.get_column_i64(&meta.src_column) else {
                    continue;
                };
                let Some(src_type) = row.get_column_string(&meta.src_type_column) else {
                    continue;
                };
                let Some(dst_id) = row.get_column_i64(&meta.dst_column) else {
                    continue;
                };
                let Some(dst_type) = row.get_column_string(&meta.dst_type_column) else {
                    continue;
                };
                let depth = row.get_column_i64(&format!("{}depth", meta.column_prefix));

                let key = (
                    src_type.clone(),
                    src_id,
                    dst_type.clone(),
                    dst_id,
                    edge_type.clone(),
                    depth,
                );
                if !edge_set.insert(key) {
                    continue;
                }

                edges.push(GraphEdge {
                    from: src_type,
                    from_id: src_id,
                    to: dst_type,
                    to_id: dst_id,
                    edge_type,
                    depth,
                    path_id: None,
                    step: None,
                });
            }
        }
    }

    fn extract_aggregation(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        edge_prefixes: &[&str],
        aggregations: Option<&Vec<compiler::input::InputAggregation>>,
        node_map: &mut IndexMap<(String, i64), GraphNode>,
    ) {
        let Some(aggs) = aggregations else { return };

        let agg_col_names: Vec<String> = aggs
            .iter()
            .map(|agg| {
                agg.alias
                    .clone()
                    .unwrap_or_else(|| agg.function.to_string())
            })
            .collect();

        for row in result.authorized_rows() {
            for node in result_context.nodes() {
                let Some(id) = row.get_public_id(node) else {
                    continue;
                };
                let Some(entity_type) = row.get_type(node) else {
                    continue;
                };

                let mut properties = Self::extract_node_properties(row, &node.alias, edge_prefixes);

                for col_name in &agg_col_names {
                    if let Some(value) = row.get(col_name) {
                        properties.insert(col_name.clone(), column_value_to_json(value));
                    }
                }

                let key = (entity_type.to_string(), id);
                node_map.entry(key).or_insert_with(|| GraphNode {
                    entity_type: entity_type.to_string(),
                    id,
                    properties,
                });
            }
        }
    }

    fn extract_path_finding(
        &self,
        result: &QueryResult,
        node_map: &mut IndexMap<(String, i64), GraphNode>,
        edges: &mut Vec<GraphEdge>,
    ) {
        for (row_idx, row) in result.authorized_rows().enumerate() {
            let dynamic_nodes = row.dynamic_nodes();
            let edge_kinds = row.edge_kinds();

            for node_ref in dynamic_nodes {
                let key = (node_ref.entity_type.clone(), node_ref.id);
                node_map.entry(key).or_insert_with(|| {
                    let mut properties = serde_json::Map::new();
                    for (k, value) in &node_ref.properties {
                        properties.insert(k.clone(), column_value_to_json(value));
                    }
                    GraphNode {
                        entity_type: node_ref.entity_type.clone(),
                        id: node_ref.id,
                        properties,
                    }
                });
            }

            for (hop_idx, pair) in dynamic_nodes.windows(2).enumerate() {
                let from = &pair[0];
                let to = &pair[1];
                let edge_type = edge_kinds.get(hop_idx).cloned().unwrap_or_default();

                edges.push(GraphEdge {
                    from: from.entity_type.clone(),
                    from_id: from.id,
                    to: to.entity_type.clone(),
                    to_id: to.id,
                    edge_type,
                    depth: None,
                    path_id: Some(row_idx),
                    step: Some(hop_idx),
                });
            }
        }
    }

    fn extract_neighbors(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        edge_prefixes: &[&str],
        output: &PipelineOutput,
        node_map: &mut IndexMap<(String, i64), GraphNode>,
        edges: &mut Vec<GraphEdge>,
    ) {
        let direction = output
            .compiled
            .input
            .neighbors
            .as_ref()
            .map(|n| n.direction);

        for row in result.authorized_rows() {
            let mut center: Option<(String, i64)> = None;
            for node in result_context.nodes() {
                let Some(id) = row.get_public_id(node) else {
                    continue;
                };
                let Some(entity_type) = row.get_type(node) else {
                    continue;
                };
                if center.is_none() {
                    center = Some((entity_type.to_string(), id));
                }
                let properties = Self::extract_node_properties(row, &node.alias, edge_prefixes);
                let key = (entity_type.to_string(), id);
                node_map.entry(key).or_insert_with(|| GraphNode {
                    entity_type: entity_type.to_string(),
                    id,
                    properties,
                });
            }

            let Some(neighbor) = row.dynamic_nodes().first() else {
                continue;
            };

            let mut neighbor_props = serde_json::Map::new();
            for (key, value) in &neighbor.properties {
                neighbor_props.insert(key.clone(), column_value_to_json(value));
            }
            let neighbor_key = (neighbor.entity_type.clone(), neighbor.id);
            node_map.entry(neighbor_key).or_insert_with(|| GraphNode {
                entity_type: neighbor.entity_type.clone(),
                id: neighbor.id,
                properties: neighbor_props,
            });

            let rel_type = row
                .get_column_string(RELATIONSHIP_TYPE_COLUMN)
                .unwrap_or_default();

            let (center_type, center_id) = center.unwrap_or_default();

            let is_outgoing = row
                .get(NEIGHBOR_IS_OUTGOING_COLUMN)
                .and_then(|value| value.as_int64().copied())
                .map(|value| value != 0)
                .unwrap_or(!matches!(
                    direction,
                    Some(compiler::input::Direction::Incoming)
                ));

            let (from, from_id, to, to_id) = if is_outgoing {
                (
                    center_type,
                    center_id,
                    neighbor.entity_type.clone(),
                    neighbor.id,
                )
            } else {
                (
                    neighbor.entity_type.clone(),
                    neighbor.id,
                    center_type,
                    center_id,
                )
            };

            edges.push(GraphEdge {
                from,
                from_id,
                to,
                to_id,
                edge_type: rel_type,
                depth: None,
                path_id: None,
                step: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use compiler::{CompiledQueryContext, HydrationPlan, ParameterizedQuery, ResultContext};
    use std::sync::Arc;

    fn make_search_output() -> PipelineOutput {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
            Field::new("p_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Project", "Project"])),
                Arc::new(StringArray::from(vec!["Alpha", "Beta"])),
            ],
        )
        .unwrap();

        let mut result_ctx = ResultContext::new();
        result_ctx.add_node("p", "Project");
        result_ctx.query_type = Some(QueryType::Search);

        let qr = QueryResult::from_batches(&[batch], &result_ctx);

        PipelineOutput {
            row_count: qr.authorized_count(),
            redacted_count: 0,
            query_type: "search".to_string(),
            raw_query_strings: vec![],
            compiled: Arc::new(CompiledQueryContext {
                query_type: QueryType::Search,
                base: ParameterizedQuery {
                    sql: String::new(),
                    params: HashMap::new(),
                    result_context: result_ctx.clone(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(serde_json::json!({
                    "query_type": "search",
                    "node": {"id": "p", "entity": "Project"},
                    "limit": 10
                }))
                .unwrap(),
            }),
            query_result: qr,
            result_context: result_ctx,
        }
    }

    #[test]
    fn search_produces_nodes_no_edges() {
        let output = make_search_output();
        let formatter = GraphFormatter;
        let response = formatter.build_response(&output);

        assert_eq!(response.query_type, "search");
        assert_eq!(response.nodes.len(), 2);
        assert!(response.edges.is_empty());
    }

    #[test]
    fn node_properties_exclude_gkg_prefix() {
        let output = make_search_output();
        let formatter = GraphFormatter;
        let response = formatter.build_response(&output);

        for node in &response.nodes {
            assert!(!node.properties.keys().any(|k| k.starts_with("_gkg_")));
            assert!(node.properties.contains_key("name"));
        }
    }
}
