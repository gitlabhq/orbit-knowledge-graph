use std::collections::HashSet;

use compiler::{
    EdgeMeta, QueryType, ResultContext, neighbor_is_outgoing_column, relationship_type_column,
};
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::Value;

use shared::PipelineOutput;
use types::{QueryResult, QueryResultRow};

use semver::Version;

use super::{FormatName, ResultFormatter, column_value_to_json};

mod id_as_string {
    use serde::Serializer;

    pub fn serialize<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    #[cfg(feature = "testutils")]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::Deserialize;
        let s = String::deserialize(deserializer)?;
        s.parse::<i64>().map_err(serde::de::Error::custom)
    }
}

/// Keys that collide with `GraphNode`'s fixed struct fields under
/// `#[serde(flatten)]`. If an ontology property strips to one of
/// these names after alias removal, the serialized JSON would get
/// duplicate keys and deserialization would silently shadow the
/// struct field. Filter them out before inserting into `properties`.
fn is_reserved_node_key(key: &str) -> bool {
    key == "type" || key == "id"
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "testutils", derive(serde::Deserialize))]
pub struct GraphResponse {
    pub format_version: String,
    pub query_type: String,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ColumnDescriptor>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "testutils", derive(serde::Deserialize))]
pub struct ColumnDescriptor {
    pub name: String,
    pub function: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "testutils", derive(serde::Deserialize))]
pub struct PaginationResponse {
    pub has_more: bool,
    pub total_rows: usize,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "testutils", derive(serde::Deserialize))]
pub struct GraphNode {
    #[serde(rename = "type")]
    pub entity_type: String,
    #[serde(with = "id_as_string")]
    pub id: i64,
    #[serde(flatten)]
    pub properties: serde_json::Map<String, Value>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "testutils", derive(serde::Deserialize))]
pub struct GraphEdge {
    pub from: String,
    #[serde(with = "id_as_string")]
    pub from_id: i64,
    pub to: String,
    #[serde(with = "id_as_string")]
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
    fn format_name(&self) -> FormatName {
        FormatName::Raw
    }

    fn format_version(&self) -> Option<&Version> {
        Some(&super::RAW_OUTPUT_FORMAT_VERSION)
    }

    fn format(&self, output: &PipelineOutput) -> Value {
        // GraphResponse holds only strings, primitives, and Values that
        // already came from `column_value_to_json` (which filters non-finite
        // floats). Serialization is infallible.
        serde_json::to_value(self.build_response(output))
            .expect("GraphResponse serialization is infallible")
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
        let mut columns: Option<Vec<ColumnDescriptor>> = None;
        let aggregations = Some(&output.compiled.input.aggregations);

        let edge_prefixes: Vec<&str> = result_context
            .edges()
            .iter()
            .map(|e| e.column_prefix.as_str())
            .collect();

        match result_context.query_type {
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
                columns = self.extract_aggregation(
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
                    &mut edge_set,
                );
            }
            Some(QueryType::Hydration) | None => {}
        }

        let pagination = output.pagination.as_ref().map(|p| PaginationResponse {
            has_more: p.has_more,
            total_rows: p.total_rows,
        });

        GraphResponse {
            format_version: super::RAW_OUTPUT_FORMAT_VERSION.to_string(),
            query_type,
            nodes: node_map.into_values().collect(),
            edges,
            columns,
            pagination,
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
            .filter(|(k, _)| !is_reserved_node_key(k))
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

    fn agg_col_names(aggs: &[compiler::input::InputAggregation]) -> Vec<String> {
        aggs.iter()
            .map(|agg| {
                agg.alias
                    .clone()
                    .unwrap_or_else(|| agg.function.to_string())
            })
            .collect()
    }

    fn build_column_descriptors(
        aggs: &[compiler::input::InputAggregation],
    ) -> Vec<ColumnDescriptor> {
        aggs.iter()
            .map(|agg| ColumnDescriptor {
                name: agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| agg.function.to_string()),
                function: agg.function.to_string(),
                target: agg.target.clone(),
                property: agg.property.clone(),
                value: None,
            })
            .collect()
    }

    fn extract_aggregation(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        edge_prefixes: &[&str],
        aggregations: Option<&Vec<compiler::input::InputAggregation>>,
        node_map: &mut IndexMap<(String, i64), GraphNode>,
    ) -> Option<Vec<ColumnDescriptor>> {
        let aggs = aggregations?;
        let agg_col_names = Self::agg_col_names(aggs);
        // The compiler rejects mixed grouped/ungrouped aggregations in the
        // same query, so this is always all-or-nothing.
        let is_ungrouped = aggs.iter().all(|a| a.group_by.is_none());

        if is_ungrouped {
            let mut columns = Self::build_column_descriptors(aggs);
            if let Some(row) = result.authorized_rows().next() {
                for (col, col_name) in columns.iter_mut().zip(&agg_col_names) {
                    if let Some(val) = row.get(col_name) {
                        col.value = Some(column_value_to_json(val));
                    }
                }
            }
            return if columns.is_empty() {
                None
            } else {
                Some(columns)
            };
        }

        // Grouped: values live on entity nodes, columns just describe them.
        let columns = Self::build_column_descriptors(aggs);

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
                    if !is_reserved_node_key(col_name)
                        && let Some(value) = row.get(col_name)
                    {
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

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    fn extract_path_finding(
        &self,
        result: &QueryResult,
        node_map: &mut IndexMap<(String, i64), GraphNode>,
        edges: &mut Vec<GraphEdge>,
    ) {
        // Dedupe paths by canonical (node_seq, edge_kinds) tuple. ReplacingMergeTree
        // edge rows can leak multiple `_version` copies between background merges,
        // which inflates path_finding into N identical logical paths. Collapse here
        // so callers see one path_id per logical path.
        type PathKey = (Vec<(String, i64)>, Vec<String>);
        let mut seen_paths: HashSet<PathKey> = HashSet::new();
        let mut path_id_counter: usize = 0;

        for row in result.authorized_rows() {
            let dynamic_nodes = row.dynamic_nodes();
            let edge_kinds = row.edge_kinds();

            let path_signature: Vec<(String, i64)> = dynamic_nodes
                .iter()
                .map(|n| (n.entity_type.clone(), n.id))
                .collect();
            if !seen_paths.insert((path_signature, edge_kinds.to_vec())) {
                continue;
            }
            let path_id = path_id_counter;
            path_id_counter += 1;

            for node_ref in dynamic_nodes {
                let key = (node_ref.entity_type.clone(), node_ref.id);
                node_map.entry(key).or_insert_with(|| {
                    let mut properties = serde_json::Map::new();
                    for (k, value) in &node_ref.properties {
                        if !is_reserved_node_key(k) {
                            properties.insert(k.clone(), column_value_to_json(value));
                        }
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
                    path_id: Some(path_id),
                    step: Some(hop_idx),
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn extract_neighbors(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        edge_prefixes: &[&str],
        output: &PipelineOutput,
        node_map: &mut IndexMap<(String, i64), GraphNode>,
        edges: &mut Vec<GraphEdge>,
        edge_set: &mut HashSet<EdgeKey>,
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

            let rel_type = row
                .get_column_string(relationship_type_column())
                .unwrap_or_default();

            let (center_type, center_id) = center.unwrap_or_default();

            let is_outgoing = row
                .get(neighbor_is_outgoing_column())
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

            // Collapse multi-`_version` edge rows surfacing as duplicate neighbors.
            // Check dedup before materializing neighbor properties so duplicate
            // rows don't do unnecessary node-map work.
            let key = (
                from.clone(),
                from_id,
                to.clone(),
                to_id,
                rel_type.clone(),
                None,
            );
            if !edge_set.insert(key) {
                continue;
            }

            let mut neighbor_props = serde_json::Map::new();
            for (key, value) in &neighbor.properties {
                if !is_reserved_node_key(key) {
                    neighbor_props.insert(key.clone(), column_value_to_json(value));
                }
            }
            let neighbor_key = (neighbor.entity_type.clone(), neighbor.id);
            node_map.entry(neighbor_key).or_insert_with(|| GraphNode {
                entity_type: neighbor.entity_type.clone(),
                id: neighbor.id,
                properties: neighbor_props,
            });

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
        result_ctx.query_type = Some(QueryType::Traversal);

        let qr = QueryResult::from_batches(&[batch], &result_ctx);

        PipelineOutput {
            row_count: qr.authorized_count(),
            redacted_count: 0,
            query_type: "traversal".to_string(),
            raw_query_strings: vec![],
            compiled: Arc::new(CompiledQueryContext {
                query_type: QueryType::Traversal,
                base: ParameterizedQuery {
                    sql: String::new(),
                    params: HashMap::new(),
                    result_context: result_ctx.clone(),
                    query_config: Default::default(),
                    dialect: Default::default(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(serde_json::json!({
                    "query_type": "traversal",
                    "node": {"id": "p", "entity": "Project"},
                    "limit": 10
                }))
                .unwrap(),
            }),
            query_result: qr,
            result_context: result_ctx,
            execution_log: vec![],
            pagination: None,
        }
    }

    #[test]
    fn search_produces_nodes_no_edges() {
        let output = make_search_output();
        let formatter = GraphFormatter;
        let response = formatter.build_response(&output);

        assert_eq!(response.query_type, "traversal");
        assert_eq!(response.nodes.len(), 2);
        assert!(response.edges.is_empty());
        assert!(response.columns.is_none(), "search should not have columns");
    }

    #[test]
    fn format_name_is_raw() {
        assert_eq!(GraphFormatter.format_name(), FormatName::Raw);
    }

    #[test]
    fn format_version_matches_config_const() {
        let version = GraphFormatter
            .format_version()
            .expect("GraphFormatter must have a version");
        assert_eq!(version, &*super::super::RAW_OUTPUT_FORMAT_VERSION);
    }

    #[test]
    fn build_response_populates_format_version_from_const() {
        let output = make_search_output();
        let response = GraphFormatter.build_response(&output);
        assert_eq!(
            response.format_version,
            super::super::RAW_OUTPUT_FORMAT_VERSION.to_string()
        );
    }

    #[test]
    fn serialized_response_contains_format_version_field() {
        let output = make_search_output();
        let value = GraphFormatter.format(&output);
        let version = value
            .get("format_version")
            .and_then(|v| v.as_str())
            .expect("serialized JSON must include a string format_version");
        semver::Version::parse(version).expect("format_version must be valid semver");
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

    #[test]
    fn property_named_type_does_not_corrupt_entity_type() {
        // If a ClickHouse column strips to "type" after alias removal
        // (e.g. `p_type`), it would collide with GraphNode's `entity_type`
        // field (renamed to "type" via serde). Verify the filter prevents this.
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_id", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["Project"])),
                Arc::new(StringArray::from(vec!["Alpha"])),
                Arc::new(StringArray::from(vec!["wrong_type_value"])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        )
        .unwrap();

        let mut result_ctx = ResultContext::new();
        result_ctx.add_node("p", "Project");
        result_ctx.query_type = Some(QueryType::Traversal);
        let qr = QueryResult::from_batches(&[batch], &result_ctx);

        let output = PipelineOutput {
            row_count: qr.authorized_count(),
            redacted_count: 0,
            query_type: "traversal".to_string(),
            raw_query_strings: vec![],
            compiled: Arc::new(CompiledQueryContext {
                query_type: QueryType::Traversal,
                base: ParameterizedQuery {
                    sql: String::new(),
                    params: HashMap::new(),
                    result_context: result_ctx.clone(),
                    query_config: Default::default(),
                    dialect: Default::default(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(serde_json::json!({
                    "query_type": "traversal",
                    "node": {"id": "p", "entity": "Project"},
                    "limit": 10
                }))
                .unwrap(),
            }),
            query_result: qr,
            result_context: result_ctx,
            execution_log: vec![],
            pagination: None,
        };

        let formatter = GraphFormatter;
        let response = formatter.build_response(&output);

        assert_eq!(response.nodes.len(), 1);
        let node = &response.nodes[0];
        assert_eq!(
            node.entity_type, "Project",
            "entity_type must come from _gkg_p_type"
        );
        assert_eq!(node.id, 42, "id must come from _gkg_p_id");
        assert!(
            !node.properties.contains_key("type"),
            "properties must not contain 'type' (would collide with entity_type under flatten)"
        );
        assert!(
            !node.properties.contains_key("id"),
            "properties must not contain 'id' (would collide with id under flatten)"
        );
        assert_eq!(
            node.properties.get("name").and_then(|v| v.as_str()),
            Some("Alpha"),
            "non-reserved properties should still be present"
        );

        // Verify serialized JSON has exactly one "type" key with correct value
        let json = serde_json::to_value(node).unwrap();
        assert_eq!(json["type"], "Project");
        assert_eq!(json["id"], "42");
        assert_eq!(json["name"], "Alpha");
        assert_eq!(
            json.get("type").and_then(|v| v.as_str()),
            Some("Project"),
            "serialized 'type' must be the entity type, not the p_type column value"
        );
    }

    #[test]
    fn path_finding_dedupes_duplicate_paths() {
        use arrow::array::{Array, ListArray, StructArray};
        use arrow::buffer::OffsetBuffer;

        let n_rows = 4;
        let mut all_ids = Vec::new();
        let mut all_types = Vec::new();
        let mut offsets = vec![0i32];
        for _ in 0..n_rows {
            all_ids.extend_from_slice(&[1_i64, 2, 3]);
            all_types.extend_from_slice(&["User", "Group", "Project"]);
            offsets.push(all_ids.len() as i32);
        }

        let struct_fields = vec![
            Arc::new(Field::new("1", DataType::Int64, false)),
            Arc::new(Field::new("2", DataType::Utf8, false)),
        ];
        let struct_array = StructArray::new(
            struct_fields.into(),
            vec![
                Arc::new(Int64Array::from(all_ids)) as _,
                Arc::new(StringArray::from(all_types)) as _,
            ],
            None,
        );
        let list_field = Arc::new(Field::new("item", struct_array.data_type().clone(), true));
        let path_list = ListArray::new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(struct_array),
            None,
        );

        let edge_struct = StringArray::from(vec![
            "MEMBER_OF",
            "CONTAINS",
            "MEMBER_OF",
            "CONTAINS",
            "MEMBER_OF",
            "CONTAINS",
            "MEMBER_OF",
            "CONTAINS",
        ]);
        let edge_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let edge_offsets = OffsetBuffer::new(vec![0i32, 2, 4, 6, 8].into());
        let edge_list = ListArray::new(edge_field, edge_offsets, Arc::new(edge_struct), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_path", path_list.data_type().clone(), true),
            Field::new("_gkg_edge_kinds", edge_list.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(path_list) as _, Arc::new(edge_list) as _],
        )
        .unwrap();

        let mut result_ctx = ResultContext::new();
        result_ctx.query_type = Some(QueryType::PathFinding);
        let qr = QueryResult::from_batches(&[batch], &result_ctx);

        let output = PipelineOutput {
            row_count: qr.authorized_count(),
            redacted_count: 0,
            query_type: "path_finding".to_string(),
            raw_query_strings: vec![],
            compiled: Arc::new(CompiledQueryContext {
                query_type: QueryType::PathFinding,
                base: ParameterizedQuery {
                    sql: String::new(),
                    params: HashMap::new(),
                    result_context: result_ctx.clone(),
                    query_config: Default::default(),
                    dialect: Default::default(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(serde_json::json!({
                    "query_type": "path_finding",
                    "path": {"type": "any", "from": "u", "to": "p", "max_depth": 2},
                    "nodes": [
                        {"id": "u", "entity": "User"},
                        {"id": "p", "entity": "Project"}
                    ],
                    "limit": 5
                }))
                .unwrap(),
            }),
            query_result: qr,
            result_context: result_ctx,
            execution_log: vec![],
            pagination: None,
        };

        let response = GraphFormatter.build_response(&output);

        // 4 raw rows -> 1 logical path (User -> Group -> Project)
        let path_ids: HashSet<usize> = response.edges.iter().filter_map(|e| e.path_id).collect();
        assert_eq!(
            path_ids.len(),
            1,
            "duplicate paths must collapse to one path_id"
        );
        assert_eq!(response.edges.len(), 2, "one logical path -> two hops");
        assert_eq!(response.nodes.len(), 3, "User, Group, Project");
    }

    #[test]
    fn path_finding_keeps_genuinely_distinct_paths() {
        use arrow::array::{Array, ListArray, StructArray};
        use arrow::buffer::OffsetBuffer;

        // Two paths sharing endpoints but going through different intermediate
        // nodes. Dedup must NOT collapse them.
        // Path A: User(1) -> Group(2) -> Project(99)
        // Path B: User(1) -> Group(3) -> Project(99)
        let all_ids: Vec<i64> = vec![1, 2, 99, 1, 3, 99];
        let all_types = vec!["User", "Group", "Project", "User", "Group", "Project"];
        let offsets = vec![0i32, 3, 6];

        let struct_fields = vec![
            Arc::new(Field::new("1", DataType::Int64, false)),
            Arc::new(Field::new("2", DataType::Utf8, false)),
        ];
        let struct_array = StructArray::new(
            struct_fields.into(),
            vec![
                Arc::new(Int64Array::from(all_ids)) as _,
                Arc::new(StringArray::from(all_types)) as _,
            ],
            None,
        );
        let list_field = Arc::new(Field::new("item", struct_array.data_type().clone(), true));
        let path_list = ListArray::new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(struct_array),
            None,
        );

        let edge_struct = StringArray::from(vec!["MEMBER_OF", "CONTAINS", "MEMBER_OF", "CONTAINS"]);
        let edge_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let edge_offsets = OffsetBuffer::new(vec![0i32, 2, 4].into());
        let edge_list = ListArray::new(edge_field, edge_offsets, Arc::new(edge_struct), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_path", path_list.data_type().clone(), true),
            Field::new("_gkg_edge_kinds", edge_list.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(path_list) as _, Arc::new(edge_list) as _],
        )
        .unwrap();

        let mut result_ctx = ResultContext::new();
        result_ctx.query_type = Some(QueryType::PathFinding);
        let qr = QueryResult::from_batches(&[batch], &result_ctx);

        let output = PipelineOutput {
            row_count: qr.authorized_count(),
            redacted_count: 0,
            query_type: "path_finding".to_string(),
            raw_query_strings: vec![],
            compiled: Arc::new(CompiledQueryContext {
                query_type: QueryType::PathFinding,
                base: ParameterizedQuery {
                    sql: String::new(),
                    params: HashMap::new(),
                    result_context: result_ctx.clone(),
                    query_config: Default::default(),
                    dialect: Default::default(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(serde_json::json!({
                    "query_type": "path_finding",
                    "path": {"type": "any", "from": "u", "to": "p", "max_depth": 2},
                    "nodes": [
                        {"id": "u", "entity": "User"},
                        {"id": "p", "entity": "Project"}
                    ],
                    "limit": 5
                }))
                .unwrap(),
            }),
            query_result: qr,
            result_context: result_ctx,
            execution_log: vec![],
            pagination: None,
        };

        let response = GraphFormatter.build_response(&output);

        let path_ids: HashSet<usize> = response.edges.iter().filter_map(|e| e.path_id).collect();
        assert_eq!(
            path_ids.len(),
            2,
            "distinct paths must keep distinct path_ids"
        );
        assert_eq!(response.edges.len(), 4, "two paths * two hops");
        assert_eq!(response.nodes.len(), 4, "User, Group(2), Group(3), Project");
    }

    #[test]
    fn ids_serialize_as_strings_preserving_precision() {
        let beyond_safe = 9_007_199_254_740_993_i64; // 2^53 + 1

        let node = GraphNode {
            entity_type: "File".to_string(),
            id: beyond_safe,
            properties: serde_json::Map::new(),
        };
        let json = serde_json::to_value(&node).unwrap();
        assert!(json["id"].is_string(), "id must serialize as a string");
        assert_eq!(
            json["id"].as_str().unwrap(),
            "9007199254740993",
            "string must preserve exact digits beyond Number.MAX_SAFE_INTEGER"
        );

        let edge = GraphEdge {
            from: "User".to_string(),
            from_id: beyond_safe,
            to: "File".to_string(),
            to_id: -beyond_safe,
            edge_type: "AUTHORED".to_string(),
            depth: None,
            path_id: None,
            step: None,
        };
        let json = serde_json::to_value(&edge).unwrap();
        assert_eq!(json["from_id"].as_str().unwrap(), "9007199254740993");
        assert_eq!(json["to_id"].as_str().unwrap(), "-9007199254740993");
    }
}
