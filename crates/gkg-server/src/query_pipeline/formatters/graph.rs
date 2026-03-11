use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use ontology::DataType;
use query_engine::{
    EdgeMeta, GKG_COLUMN_PREFIX, NEIGHBOR_IS_OUTGOING_COLUMN, QueryType, RELATIONSHIP_TYPE_COLUMN,
    ResultContext,
};
use serde::Serialize;
use serde_json::Value;

use crate::query_pipeline::QueryPipelineContext;
use crate::redaction::{QueryResult, QueryResultRow};

use super::{ResultFormatter, column_value_schema_type, column_value_to_json};

#[derive(Debug, Serialize)]
pub struct GraphResponse {
    pub query_type: String,
    pub row_count: usize,
    pub columns: Vec<GraphColumn>,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Serialize)]
pub struct GraphColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GraphNode {
    #[serde(rename = "type")]
    pub entity_type: String,
    pub id: i64,
    #[serde(flatten)]
    pub properties: serde_json::Map<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct GraphEdge {
    pub from: String,
    pub from_id: i64,
    pub to: String,
    pub to_id: i64,
    #[serde(rename = "type")]
    pub edge_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<usize>,
}

fn edge_hash(
    from: &str,
    from_id: i64,
    to: &str,
    to_id: i64,
    edge_type: &str,
    depth: Option<i64>,
) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    from.hash(&mut h);
    from_id.hash(&mut h);
    to.hash(&mut h);
    to_id.hash(&mut h);
    edge_type.hash(&mut h);
    depth.hash(&mut h);
    h.finish()
}

#[derive(Clone, Copy)]
pub struct GraphFormatter;

impl ResultFormatter for GraphFormatter {
    fn format(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        ctx: &QueryPipelineContext,
    ) -> Value {
        let response = self.build_response(result, result_context, ctx);
        serde_json::to_value(response).unwrap_or(Value::Null)
    }
}

impl GraphFormatter {
    pub(crate) fn build_response(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        ctx: &QueryPipelineContext,
    ) -> GraphResponse {
        let query_type = result_context
            .query_type
            .map(|qt| qt.to_string())
            .unwrap_or_default();

        let row_count = result.authorized_count();

        let mut node_map: HashMap<(String, i64), GraphNode> = HashMap::new();
        let mut edges: Vec<GraphEdge> = Vec::new();
        let mut edge_set: HashSet<u64> = HashSet::new();
        let mut columns: Vec<GraphColumn> = Vec::new();

        let aggregations = ctx.compiled().ok().map(|c| &c.input.aggregations);

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
                    ctx,
                    &mut node_map,
                    &mut columns,
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
                    ctx,
                    &mut node_map,
                    &mut edges,
                );
            }
            None => {}
        }

        GraphResponse {
            query_type,
            row_count,
            columns,
            nodes: node_map.into_values().collect(),
            edges,
        }
    }

    fn extract_search_nodes(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        edge_prefixes: &[&str],
        node_map: &mut HashMap<(String, i64), GraphNode>,
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
                    let properties = self.extract_node_properties(row, &node.alias, edge_prefixes);
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
        &self,
        row: &QueryResultRow,
        alias: &str,
        edge_prefixes: &[&str],
    ) -> serde_json::Map<String, Value> {
        let prefix = format!("{alias}_");
        let mut properties = serde_json::Map::new();

        for (name, value) in row.columns() {
            if name.starts_with(GKG_COLUMN_PREFIX) {
                continue;
            }
            if edge_prefixes.iter().any(|ep| name.starts_with(ep)) {
                continue;
            }
            if let Some(prop_name) = name.strip_prefix(&prefix) {
                properties.insert(prop_name.to_string(), column_value_to_json(value));
            }
        }

        properties
    }

    fn extract_traversal_edges(
        &self,
        result: &QueryResult,
        edge_metas: &[EdgeMeta],
        edges: &mut Vec<GraphEdge>,
        edge_set: &mut HashSet<u64>,
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

                let h = edge_hash(&src_type, src_id, &dst_type, dst_id, &edge_type, depth);
                if !edge_set.insert(h) {
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
        aggregations: Option<&Vec<query_engine::input::InputAggregation>>,
        ctx: &QueryPipelineContext,
        node_map: &mut HashMap<(String, i64), GraphNode>,
        columns: &mut Vec<GraphColumn>,
    ) {
        let Some(aggs) = aggregations else { return };

        let mut agg_col_names: Vec<String> = Vec::new();
        for agg in aggs {
            let col_name = agg_output_name(agg);
            columns.push(GraphColumn {
                name: col_name.clone(),
                data_type: aggregation_schema_type(result, agg, &col_name, ctx).to_string(),
                aggregation: Some(agg_schema_name(agg.function).to_string()),
            });
            agg_col_names.push(col_name);
        }

        for row in result.authorized_rows() {
            for node in result_context.nodes() {
                let Some(id) = row.get_public_id(node) else {
                    continue;
                };
                let Some(entity_type) = row.get_type(node) else {
                    continue;
                };

                let mut properties = self.extract_node_properties(row, &node.alias, edge_prefixes);

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
        node_map: &mut HashMap<(String, i64), GraphNode>,
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
        ctx: &QueryPipelineContext,
        node_map: &mut HashMap<(String, i64), GraphNode>,
        edges: &mut Vec<GraphEdge>,
    ) {
        let direction = ctx
            .compiled()
            .ok()
            .and_then(|c| c.input.neighbors.as_ref().map(|n| n.direction));

        for row in result.authorized_rows() {
            for node in result_context.nodes() {
                let Some(id) = row.get_public_id(node) else {
                    continue;
                };
                let Some(entity_type) = row.get_type(node) else {
                    continue;
                };
                let properties = self.extract_node_properties(row, &node.alias, edge_prefixes);
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

            let (center_type, center_id) = result_context
                .nodes()
                .find_map(|n| Some((row.get_type(n)?.to_string(), row.get_public_id(n)?)))
                .unwrap_or_default();

            let is_outgoing = row
                .get(NEIGHBOR_IS_OUTGOING_COLUMN)
                .and_then(|value| value.as_int64().copied())
                .map(|value| value != 0)
                .unwrap_or(!matches!(
                    direction,
                    Some(query_engine::input::Direction::Incoming)
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

fn agg_schema_name(f: query_engine::input::AggFunction) -> &'static str {
    match f {
        query_engine::input::AggFunction::Count => "count",
        query_engine::input::AggFunction::Sum => "sum",
        query_engine::input::AggFunction::Avg => "avg",
        query_engine::input::AggFunction::Min => "min",
        query_engine::input::AggFunction::Max => "max",
        query_engine::input::AggFunction::Collect => "collect",
    }
}

fn agg_output_name(agg: &query_engine::input::InputAggregation) -> String {
    agg.alias
        .clone()
        .unwrap_or_else(|| agg_schema_name(agg.function).to_string())
}

fn aggregation_schema_type(
    result: &QueryResult,
    agg: &query_engine::input::InputAggregation,
    column_name: &str,
    ctx: &QueryPipelineContext,
) -> &'static str {
    if let Some(data_type) = result
        .authorized_rows()
        .filter_map(|row| row.get(column_name))
        .find_map(column_value_schema_type)
    {
        return data_type;
    }

    aggregation_schema_type_fallback(agg, ctx)
}

fn aggregation_schema_type_fallback(
    agg: &query_engine::input::InputAggregation,
    ctx: &QueryPipelineContext,
) -> &'static str {
    use query_engine::input::AggFunction;

    match agg.function {
        AggFunction::Count => "Int64",
        AggFunction::Avg => "Float64",
        AggFunction::Sum => match aggregation_target_type(agg, ctx) {
            Some(DataType::Float) => "Float64",
            _ => "Int64",
        },
        AggFunction::Min | AggFunction::Max => match aggregation_target_type(agg, ctx) {
            Some(DataType::Float) => "Float64",
            Some(DataType::Int) => "Int64",
            _ => "String",
        },
        AggFunction::Collect => "String",
    }
}

fn aggregation_target_type(
    agg: &query_engine::input::InputAggregation,
    ctx: &QueryPipelineContext,
) -> Option<DataType> {
    let target = agg.target.as_deref()?;
    let property = agg.property.as_deref()?;
    let entity = ctx
        .compiled()
        .ok()?
        .input
        .nodes
        .iter()
        .find(|n| n.id == target)?
        .entity
        .as_deref()?;
    ctx.ontology.get_field_type(entity, property)
}
