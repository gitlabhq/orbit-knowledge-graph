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
        AggFunction::Min | AggFunction::Max => aggregation_target_type(agg, ctx)
            .map(schema_type_for_data_type)
            .unwrap_or("Int64"),
        AggFunction::Collect => "String",
    }
}

fn aggregation_target_type(
    agg: &query_engine::input::InputAggregation,
    ctx: &QueryPipelineContext,
) -> Option<DataType> {
    let property = agg.property.as_deref()?;
    let compiled = ctx.compiled().ok()?;
    let target = agg.target.as_deref()?;
    let entity = compiled
        .input
        .nodes
        .iter()
        .find(|node| node.id == target)?
        .entity
        .as_deref()?;
    ctx.ontology.get_field_type(entity, property)
}

fn schema_type_for_data_type(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int => "Int64",
        DataType::Float => "Float64",
        DataType::String
        | DataType::Bool
        | DataType::Date
        | DataType::DateTime
        | DataType::Enum
        | DataType::Uuid => "String",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use gkg_utils::arrow::ColumnValue;
    use query_engine::{CompiledQueryContext, EdgeMeta, HydrationPlan, ParameterizedQuery};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    static RESPONSE_SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
        std::sync::LazyLock::new(|| {
            let schema: Value =
                serde_json::from_str(include_str!("../../../schemas/query_response.json")).unwrap();
            jsonschema::validator_for(&schema).unwrap()
        });

    fn assert_valid_response(value: &Value) {
        let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(value).collect();
        assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
    }

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn make_pipeline_ctx(query_type: QueryType, input_json: Value) -> QueryPipelineContext {
        QueryPipelineContext {
            compiled: Some(Arc::new(CompiledQueryContext {
                query_type,
                base: ParameterizedQuery {
                    sql: "SELECT 1".to_string(),
                    params: HashMap::new(),
                    result_context: ResultContext::new(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(input_json).unwrap(),
            })),
            ontology: Arc::new(ontology::Ontology::new()),
            client: Arc::new(clickhouse_client::ArrowClickHouseClient::dummy()),
            security_context: None,
        }
    }

    #[test]
    fn test_search_returns_nodes_with_stripped_properties() {
        let batch = make_batch(vec![
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![1, 2]))),
            (
                "_gkg_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ),
            ("p_name", Arc::new(StringArray::from(vec!["Alpha", "Beta"]))),
            ("p_stars", Arc::new(Int64Array::from(vec![10, 20]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Search);
        result_ctx.add_node("p", "Project");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Search,
            json!({"query_type": "search", "node": {"id": "p", "entity": "Project"}}),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "search");
        assert_eq!(value["row_count"], 2);
        assert!(value["edges"].as_array().unwrap().is_empty());
        assert!(value["columns"].as_array().unwrap().is_empty());

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);

        let n1 = nodes.iter().find(|n| n["id"] == 1).unwrap();
        assert_eq!(n1["type"], "Project");
        assert_eq!(n1["name"], "Alpha");
        assert_eq!(n1["stars"], 10);
        assert!(n1["id"].is_i64());
        assert!(n1["name"].is_string());
        assert!(
            n1.get("p_name").is_none(),
            "raw prefixed column must not appear"
        );
        assert!(
            n1.get("_gkg_p_id").is_none(),
            "internal columns must not leak"
        );

        let n2 = nodes.iter().find(|n| n["id"] == 2).unwrap();
        assert_eq!(n2["type"], "Project");
        assert_eq!(n2["name"], "Beta");
        assert_eq!(n2["stars"], 20);
        assert!(n2["id"].is_i64());
    }

    #[test]
    fn test_search_deduplicates_nodes() {
        let batch = make_batch(vec![
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![1, 1]))),
            (
                "_gkg_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ),
            (
                "p_name",
                Arc::new(StringArray::from(vec!["Alpha", "Alpha"])),
            ),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Search);
        result_ctx.add_node("p", "Project");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Search,
            json!({"query_type": "search", "node": {"id": "p", "entity": "Project"}}),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(value["row_count"], 2);

        let node = &nodes[0];
        assert_eq!(node["type"], "Project");
        assert_eq!(node["id"], 1);
        assert!(node["id"].is_i64());
        assert_eq!(node["name"], "Alpha");
    }

    #[test]
    fn test_search_prefers_public_id_when_pk_is_present() {
        let batch = make_batch(vec![
            ("_gkg_d_id", Arc::new(Int64Array::from(vec![9001]))),
            ("_gkg_d_pk", Arc::new(Int64Array::from(vec![42]))),
            (
                "_gkg_d_type",
                Arc::new(StringArray::from(vec!["Definition"])),
            ),
            (
                "d_name",
                Arc::new(StringArray::from(vec!["Definition Alpha"])),
            ),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Search);
        result_ctx.add_node("d", "Definition");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Search,
            json!({"query_type": "search", "node": {"id": "d", "entity": "Definition"}}),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["type"], "Definition");
        assert_eq!(nodes[0]["id"], 42);
        assert_eq!(nodes[0]["name"], "Definition Alpha");
    }

    #[test]
    fn test_traversal_single_hop() {
        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![10]))),
            ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![20]))),
            ("_gkg_p_type", Arc::new(StringArray::from(vec!["Project"]))),
            ("e0_type", Arc::new(StringArray::from(vec!["MEMBER_OF"]))),
            ("e0_src", Arc::new(Int64Array::from(vec![10]))),
            ("e0_src_type", Arc::new(StringArray::from(vec!["User"]))),
            ("e0_dst", Arc::new(Int64Array::from(vec![20]))),
            ("e0_dst_type", Arc::new(StringArray::from(vec!["Project"]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Traversal);
        result_ctx.add_node("u", "User");
        result_ctx.add_node("p", "Project");
        result_ctx.add_edge(EdgeMeta {
            column_prefix: "e0_".to_string(),
            path_column: None,
            rel_types: vec!["MEMBER_OF".to_string()],
            from_alias: "u".to_string(),
            to_alias: "p".to_string(),
            type_column: "e0_type".to_string(),
            src_column: "e0_src".to_string(),
            src_type_column: "e0_src_type".to_string(),
            dst_column: "e0_dst".to_string(),
            dst_type_column: "e0_dst_type".to_string(),
        });

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Traversal,
            json!({
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p"}]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "traversal");
        assert_eq!(value["row_count"], 1);
        assert!(value["columns"].as_array().unwrap().is_empty());

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);

        let user = nodes.iter().find(|n| n["type"] == "User").unwrap();
        assert_eq!(user["id"], 10);
        assert!(user["id"].is_i64());

        let project = nodes.iter().find(|n| n["type"] == "Project").unwrap();
        assert_eq!(project["id"], 20);
        assert!(project["id"].is_i64());

        let edges = value["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["type"], "MEMBER_OF");
        assert_eq!(edges[0]["from"], "User");
        assert_eq!(edges[0]["from_id"], 10);
        assert_eq!(edges[0]["to"], "Project");
        assert_eq!(edges[0]["to_id"], 20);
        assert!(edges[0]["from_id"].is_i64());
        assert!(edges[0]["to_id"].is_i64());
        assert!(
            edges[0].get("path_id").is_none(),
            "traversal edges should not have path_id"
        );
        assert!(
            edges[0].get("step").is_none(),
            "traversal edges should not have step"
        );
        assert!(
            edges[0].get("depth").is_none(),
            "traversal edges should not have depth"
        );

        for node in nodes {
            assert!(
                node.get("e0_type").is_none(),
                "edge columns must not appear as node properties"
            );
            assert!(
                node.get("e0_src").is_none(),
                "edge columns must not appear as node properties"
            );
        }
    }

    #[test]
    fn test_traversal_deduplicates_edges() {
        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![10, 10]))),
            (
                "_gkg_u_type",
                Arc::new(StringArray::from(vec!["User", "User"])),
            ),
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![20, 20]))),
            (
                "_gkg_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ),
            (
                "e0_type",
                Arc::new(StringArray::from(vec!["MEMBER_OF", "MEMBER_OF"])),
            ),
            ("e0_src", Arc::new(Int64Array::from(vec![10, 10]))),
            (
                "e0_src_type",
                Arc::new(StringArray::from(vec!["User", "User"])),
            ),
            ("e0_dst", Arc::new(Int64Array::from(vec![20, 20]))),
            (
                "e0_dst_type",
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Traversal);
        result_ctx.add_node("u", "User");
        result_ctx.add_node("p", "Project");
        result_ctx.add_edge(EdgeMeta {
            column_prefix: "e0_".to_string(),
            path_column: None,
            rel_types: vec!["MEMBER_OF".to_string()],
            from_alias: "u".to_string(),
            to_alias: "p".to_string(),
            type_column: "e0_type".to_string(),
            src_column: "e0_src".to_string(),
            src_type_column: "e0_src_type".to_string(),
            dst_column: "e0_dst".to_string(),
            dst_type_column: "e0_dst_type".to_string(),
        });

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Traversal,
            json!({
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p"}]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let edges = value["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1, "duplicate edges should be deduplicated");
        assert_eq!(edges[0]["from"], "User");
        assert_eq!(edges[0]["from_id"], 10);
        assert_eq!(edges[0]["to"], "Project");
        assert_eq!(edges[0]["to_id"], 20);
        assert_eq!(edges[0]["type"], "MEMBER_OF");

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(
            nodes.len(),
            2,
            "duplicate nodes should also be deduplicated"
        );
    }

    #[test]
    fn test_traversal_includes_depth_for_multi_hop_edges() {
        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![10]))),
            ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ("_gkg_g_id", Arc::new(Int64Array::from(vec![200]))),
            ("_gkg_g_type", Arc::new(StringArray::from(vec!["Group"]))),
            (
                "hop_e0_type",
                Arc::new(StringArray::from(vec!["MEMBER_OF"])),
            ),
            ("hop_e0_src", Arc::new(Int64Array::from(vec![10]))),
            ("hop_e0_src_type", Arc::new(StringArray::from(vec!["User"]))),
            ("hop_e0_dst", Arc::new(Int64Array::from(vec![200]))),
            (
                "hop_e0_dst_type",
                Arc::new(StringArray::from(vec!["Group"])),
            ),
            ("hop_e0_depth", Arc::new(Int64Array::from(vec![2]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Traversal);
        result_ctx.add_node("u", "User");
        result_ctx.add_node("g", "Group");
        result_ctx.add_edge(EdgeMeta {
            column_prefix: "hop_e0_".to_string(),
            path_column: Some("hop_e0_path_nodes".to_string()),
            rel_types: vec!["MEMBER_OF".to_string()],
            from_alias: "u".to_string(),
            to_alias: "g".to_string(),
            type_column: "hop_e0_type".to_string(),
            src_column: "hop_e0_src".to_string(),
            src_type_column: "hop_e0_src_type".to_string(),
            dst_column: "hop_e0_dst".to_string(),
            dst_type_column: "hop_e0_dst_type".to_string(),
        });

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Traversal,
            json!({
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{
                    "type": "MEMBER_OF",
                    "from": "u",
                    "to": "g",
                    "min_hops": 1,
                    "max_hops": 3
                }]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let edges = value["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["type"], "MEMBER_OF");
        assert_eq!(edges[0]["from"], "User");
        assert_eq!(edges[0]["from_id"], 10);
        assert_eq!(edges[0]["to"], "Group");
        assert_eq!(edges[0]["to_id"], 200);
        assert_eq!(edges[0]["depth"], 2);
    }

    #[test]
    fn test_aggregation_builds_columns_and_inlines_values() {
        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![10, 20]))),
            (
                "_gkg_u_type",
                Arc::new(StringArray::from(vec!["User", "User"])),
            ),
            (
                "u_username",
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ),
            ("mr_count", Arc::new(Int64Array::from(vec![5, 12]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Aggregation);
        result_ctx.add_node("u", "User");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Aggregation,
            json!({
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "mr", "entity": "MergeRequest"}
                ],
                "aggregations": [{
                    "function": "count",
                    "target": "mr",
                    "group_by": "u",
                    "alias": "mr_count"
                }]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "aggregation");
        assert_eq!(value["row_count"], 2);
        assert!(value["edges"].as_array().unwrap().is_empty());

        let cols = value["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0]["name"], "mr_count");
        assert_eq!(cols[0]["type"], "Int64");
        assert_eq!(cols[0]["aggregation"], "count");

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);

        let alice = nodes.iter().find(|n| n["id"] == 10).unwrap();
        assert_eq!(alice["type"], "User");
        assert_eq!(alice["username"], "alice");
        assert!(alice["username"].is_string());
        assert_eq!(alice["mr_count"], 5);
        assert!(alice["mr_count"].is_i64());

        let bob = nodes.iter().find(|n| n["id"] == 20).unwrap();
        assert_eq!(bob["type"], "User");
        assert_eq!(bob["username"], "bob");
        assert_eq!(bob["mr_count"], 12);
        assert!(bob["mr_count"].is_i64());
    }

    #[test]
    fn test_aggregation_prefers_public_id_when_pk_is_present() {
        let batch = make_batch(vec![
            ("_gkg_d_id", Arc::new(Int64Array::from(vec![9001]))),
            ("_gkg_d_pk", Arc::new(Int64Array::from(vec![42]))),
            (
                "_gkg_d_type",
                Arc::new(StringArray::from(vec!["Definition"])),
            ),
            (
                "d_name",
                Arc::new(StringArray::from(vec!["Definition Alpha"])),
            ),
            ("def_count", Arc::new(Int64Array::from(vec![3]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Aggregation);
        result_ctx.add_node("d", "Definition");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Aggregation,
            json!({
                "query_type": "aggregation",
                "nodes": [{"id": "d", "entity": "Definition", "columns": ["name"]}],
                "aggregations": [{
                    "function": "count",
                    "target": "d",
                    "group_by": "d",
                    "alias": "def_count"
                }]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["id"], 42);
        assert_eq!(nodes[0]["type"], "Definition");
        assert_eq!(nodes[0]["name"], "Definition Alpha");
        assert_eq!(nodes[0]["def_count"], 3);
    }

    #[test]
    fn test_aggregation_schema_type_uses_observed_float_values() {
        let batch = make_batch(vec![
            ("_gkg_g_id", Arc::new(Int64Array::from(vec![100]))),
            ("_gkg_g_type", Arc::new(StringArray::from(vec!["Group"]))),
            ("g_name", Arc::new(StringArray::from(vec!["Public Group"]))),
            ("score_sum", Arc::new(Float64Array::from(vec![12.5]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Aggregation);
        result_ctx.add_node("g", "Group");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Aggregation,
            json!({
                "query_type": "aggregation",
                "nodes": [
                    {"id": "g", "entity": "Group", "columns": ["name"]},
                    {"id": "u", "entity": "User"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "aggregations": [{
                    "function": "sum",
                    "target": "u",
                    "group_by": "g",
                    "property": "score",
                    "alias": "score_sum"
                }]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let cols = value["columns"].as_array().unwrap();
        assert_eq!(cols[0]["name"], "score_sum");
        assert_eq!(cols[0]["type"], "Float64");
        assert_eq!(value["nodes"][0]["score_sum"], 12.5);
    }

    #[test]
    fn test_aggregation_schema_type_uses_observed_string_values() {
        let batch = make_batch(vec![
            ("_gkg_g_id", Arc::new(Int64Array::from(vec![100]))),
            ("_gkg_g_type", Arc::new(StringArray::from(vec!["Group"]))),
            ("g_name", Arc::new(StringArray::from(vec!["Public Group"]))),
            ("min_username", Arc::new(StringArray::from(vec!["alice"]))),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Aggregation);
        result_ctx.add_node("g", "Group");

        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Aggregation,
            json!({
                "query_type": "aggregation",
                "nodes": [
                    {"id": "g", "entity": "Group", "columns": ["name"]},
                    {"id": "u", "entity": "User"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "aggregations": [{
                    "function": "min",
                    "target": "u",
                    "group_by": "g",
                    "property": "username",
                    "alias": "min_username"
                }]
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let cols = value["columns"].as_array().unwrap();
        assert_eq!(cols[0]["name"], "min_username");
        assert_eq!(cols[0]["type"], "String");
        assert_eq!(value["nodes"][0]["min_username"], "alice");
    }

    #[test]
    fn test_path_finding_builds_path_edges() {
        let ids = Int64Array::from(vec![1, 2, 3]);
        let types = StringArray::from(vec!["Project", "MergeRequest", "Note"]);

        let struct_fields = vec![
            Arc::new(Field::new("1", DataType::Int64, false)),
            Arc::new(Field::new("2", DataType::Utf8, false)),
        ];
        let struct_array = StructArray::new(
            struct_fields.into(),
            vec![Arc::new(ids) as _, Arc::new(types) as _],
            None,
        );

        let path_field = Arc::new(Field::new("item", struct_array.data_type().clone(), true));
        let path_offsets = OffsetBuffer::new(vec![0i32, 3].into());
        let path_list = ListArray::new(path_field, path_offsets, Arc::new(struct_array), None);

        let edge_values = StringArray::from(vec!["HAS_MR", "HAS_NOTE"]);
        let edge_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let edge_offsets = OffsetBuffer::new(vec![0i32, 2].into());
        let edge_list = ListArray::new(edge_field, edge_offsets, Arc::new(edge_values), None);

        let batch = make_batch(vec![
            ("_gkg_path", Arc::new(path_list) as _),
            ("_gkg_edge_kinds", Arc::new(edge_list) as _),
        ]);

        let result_ctx = ResultContext::new().with_query_type(QueryType::PathFinding);
        let result = QueryResult::from_batches(&[batch], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::PathFinding,
            json!({
                "query_type": "path_finding",
                "nodes": [
                    {"id": "start", "entity": "Project", "node_ids": [1]},
                    {"id": "end", "entity": "Note", "node_ids": [3]}
                ],
                "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "path_finding");
        assert_eq!(value["row_count"], 1);
        assert!(value["columns"].as_array().unwrap().is_empty());

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3);

        let proj = nodes.iter().find(|n| n["type"] == "Project").unwrap();
        assert_eq!(proj["id"], 1);
        assert!(proj["id"].is_i64());

        let mr = nodes.iter().find(|n| n["type"] == "MergeRequest").unwrap();
        assert_eq!(mr["id"], 2);
        assert!(mr["id"].is_i64());

        let note = nodes.iter().find(|n| n["type"] == "Note").unwrap();
        assert_eq!(note["id"], 3);
        assert!(note["id"].is_i64());

        let edges = value["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 2);

        let e0 = &edges[0];
        assert_eq!(e0["type"], "HAS_MR");
        assert_eq!(e0["from"], "Project");
        assert_eq!(e0["from_id"], 1);
        assert_eq!(e0["to"], "MergeRequest");
        assert_eq!(e0["to_id"], 2);
        assert_eq!(e0["path_id"], 0);
        assert_eq!(e0["step"], 0);
        assert!(e0.get("depth").is_none());

        let e1 = &edges[1];
        assert_eq!(e1["type"], "HAS_NOTE");
        assert_eq!(e1["from"], "MergeRequest");
        assert_eq!(e1["from_id"], 2);
        assert_eq!(e1["to"], "Note");
        assert_eq!(e1["to_id"], 3);
        assert_eq!(e1["path_id"], 0);
        assert_eq!(e1["step"], 1);
    }

    #[test]
    fn test_neighbors_includes_center_and_neighbor() {
        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![10]))),
            ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ("_gkg_neighbor_id", Arc::new(Int64Array::from(vec![42]))),
            (
                "_gkg_neighbor_type",
                Arc::new(StringArray::from(vec!["MergeRequest"])),
            ),
            (
                "_gkg_relationship_type",
                Arc::new(StringArray::from(vec!["AUTHORED"])),
            ),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Neighbors);
        result_ctx.add_node("u", "User");

        let mut result = QueryResult::from_batches(&[batch], &result_ctx);

        for node in result.rows_mut()[0].dynamic_nodes_mut() {
            node.properties.insert(
                "title".to_string(),
                ColumnValue::String("Fix bug".to_string()),
            );
        }

        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Neighbors,
            json!({
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [10]},
                "neighbors": {"node": "u", "direction": "outgoing"}
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "neighbors");
        assert_eq!(value["row_count"], 1);
        assert!(value["columns"].as_array().unwrap().is_empty());

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);

        let user = nodes.iter().find(|n| n["type"] == "User").unwrap();
        assert_eq!(user["id"], 10);
        assert!(user["id"].is_i64());

        let mr_node = nodes.iter().find(|n| n["type"] == "MergeRequest").unwrap();
        assert_eq!(mr_node["id"], 42);
        assert!(mr_node["id"].is_i64());
        assert_eq!(mr_node["title"], "Fix bug");
        assert!(mr_node["title"].is_string());

        let edges = value["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["type"], "AUTHORED");
        assert_eq!(edges[0]["from"], "User");
        assert_eq!(edges[0]["from_id"], 10);
        assert_eq!(edges[0]["to"], "MergeRequest");
        assert_eq!(edges[0]["to_id"], 42);
        assert!(
            edges[0].get("path_id").is_none(),
            "neighbor edges should not have path_id"
        );
        assert!(
            edges[0].get("step").is_none(),
            "neighbor edges should not have step"
        );
        assert!(
            edges[0].get("depth").is_none(),
            "neighbor edges should not have depth"
        );
    }

    #[test]
    fn test_neighbors_uses_public_id_and_row_orientation_for_both_direction() {
        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![9001]))),
            ("_gkg_u_pk", Arc::new(Int64Array::from(vec![10]))),
            ("_gkg_u_type", Arc::new(StringArray::from(vec!["User"]))),
            ("_gkg_neighbor_id", Arc::new(Int64Array::from(vec![42]))),
            (
                "_gkg_neighbor_type",
                Arc::new(StringArray::from(vec!["Group"])),
            ),
            (
                "_gkg_relationship_type",
                Arc::new(StringArray::from(vec!["MEMBER_OF"])),
            ),
            (
                "_gkg_neighbor_is_outgoing",
                Arc::new(Int64Array::from(vec![0])),
            ),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Neighbors);
        result_ctx.add_node("u", "User");

        let mut result = QueryResult::from_batches(&[batch], &result_ctx);
        for node in result.rows_mut()[0].dynamic_nodes_mut() {
            node.properties.insert(
                "name".to_string(),
                ColumnValue::String("Public Group".to_string()),
            );
        }

        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Neighbors,
            json!({
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [10]},
                "neighbors": {"node": "u", "direction": "both"}
            }),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        let nodes = value["nodes"].as_array().unwrap();
        let user = nodes.iter().find(|n| n["type"] == "User").unwrap();
        assert_eq!(user["id"], 10);

        let edges = value["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["type"], "MEMBER_OF");
        assert_eq!(edges[0]["from"], "Group");
        assert_eq!(edges[0]["from_id"], 42);
        assert_eq!(edges[0]["to"], "User");
        assert_eq!(edges[0]["to_id"], 10);
    }

    #[test]
    fn test_empty_result() {
        let result_ctx = ResultContext::new().with_query_type(QueryType::Search);
        let result = QueryResult::from_batches(&[], &result_ctx);
        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Search,
            json!({"query_type": "search", "node": {"id": "p", "entity": "Project"}}),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "search");
        assert_eq!(value["row_count"], 0);
        assert_eq!(value["nodes"].as_array().unwrap().len(), 0);
        assert_eq!(value["edges"].as_array().unwrap().len(), 0);
        assert_eq!(value["columns"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_response_includes_query_type_and_row_count() {
        let batch = make_batch(vec![
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![1, 2, 3]))),
            (
                "_gkg_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project", "Project"])),
            ),
        ]);

        let mut result_ctx = ResultContext::new().with_query_type(QueryType::Search);
        result_ctx.add_node("p", "Project");

        let mut result = QueryResult::from_batches(&[batch], &result_ctx);
        result.rows_mut()[1].set_unauthorized();

        let pipeline_ctx = make_pipeline_ctx(
            QueryType::Search,
            json!({"query_type": "search", "node": {"id": "p", "entity": "Project"}}),
        );

        let value = GraphFormatter.format(&result, &result_ctx, &pipeline_ctx);
        assert_valid_response(&value);

        assert_eq!(value["query_type"], "search");
        assert_eq!(value["row_count"], 2);

        let nodes = value["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2, "unauthorized row should be excluded");

        let ids: HashSet<i64> = nodes.iter().filter_map(|n| n["id"].as_i64()).collect();
        assert!(ids.contains(&1), "node 1 should be present");
        assert!(ids.contains(&3), "node 3 should be present");
        assert!(
            !ids.contains(&2),
            "node 2 was unauthorized and should be absent"
        );

        for node in nodes {
            assert_eq!(node["type"], "Project");
            assert!(node["id"].is_i64());
        }
    }
}
