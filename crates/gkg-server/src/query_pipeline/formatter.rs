use ontology::Ontology;
use query_engine::{
    QueryType, ResultContext, GKG_COLUMN_PREFIX, NEIGHBOR_ID_COLUMN, NEIGHBOR_TYPE_COLUMN,
    RELATIONSHIP_TYPE_COLUMN,
};
use serde_json::{json, Value};

use crate::redaction::{NodeRef, QueryResult, QueryResultRow};
use gkg_utils::arrow::ColumnValue;

pub trait ResultFormatter: Send + Sync {
    fn format(
        &self,
        result: &QueryResult,
        result_context: &ResultContext,
        ontology: &Ontology,
    ) -> Value;
}

#[derive(Clone, Copy)]
pub struct RawRowFormatter;

impl ResultFormatter for RawRowFormatter {
    fn format(&self, result: &QueryResult, ctx: &ResultContext, _: &Ontology) -> Value {
        let rows: Vec<Value> = result
            .authorized_rows()
            .map(|row| row_to_json(row, ctx))
            .collect();
        Value::Array(rows)
    }
}

/// Formats query results for the context engine using GOON format.
/// TODO: Implement GOON format per https://gitlab.com/gitlab-org/gitlab/-/snippets/4929205
#[derive(Clone, Copy)]
pub struct ContextEngineFormatter;

impl ResultFormatter for ContextEngineFormatter {
    fn format(&self, result: &QueryResult, ctx: &ResultContext, ontology: &Ontology) -> Value {
        // Placeholder: delegates to raw format until GOON is implemented
        RawRowFormatter.format(result, ctx, ontology)
    }
}

pub fn row_to_json(row: &QueryResultRow, ctx: &ResultContext) -> Value {
    let mut obj = serde_json::Map::new();

    for (name, value) in row.columns() {
        if name.starts_with(GKG_COLUMN_PREFIX)
            && name != NEIGHBOR_ID_COLUMN
            && name != NEIGHBOR_TYPE_COLUMN
            && name != RELATIONSHIP_TYPE_COLUMN
        {
            continue;
        }
        obj.insert(name.clone(), column_value_to_json(value));
    }

    for node in ctx.nodes() {
        if let Some(id) = row.get_id(node) {
            obj.insert(format!("{}_id", node.alias), json!(id));
        }
        if let Some(entity_type) = row.get_type(node) {
            obj.insert(format!("{}_type", node.alias), json!(entity_type));
        }
    }

    let dynamic_nodes = row.dynamic_nodes();
    if !dynamic_nodes.is_empty() {
        match ctx.query_type {
            Some(QueryType::PathFinding) => {
                let path: Vec<Value> = dynamic_nodes.iter().map(node_ref_to_json).collect();
                obj.insert("path".to_string(), Value::Array(path));

                let edge_kinds = row.edge_kinds();
                if !edge_kinds.is_empty() {
                    let edges: Vec<Value> = edge_kinds.iter().map(|k| json!(k)).collect();
                    obj.insert("edges".to_string(), Value::Array(edges));
                }
            }
            Some(QueryType::Neighbors) => {
                if let Some(neighbor) = dynamic_nodes.first() {
                    for (key, value) in &neighbor.properties {
                        obj.insert(key.clone(), column_value_to_json(value));
                    }
                }
            }
            _ => {}
        }
    }

    Value::Object(obj)
}

fn node_ref_to_json(node: &NodeRef) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("id".to_string(), json!(node.id));
    obj.insert("entity_type".to_string(), json!(node.entity_type));
    for (key, value) in &node.properties {
        obj.insert(key.clone(), column_value_to_json(value));
    }
    Value::Object(obj)
}

fn column_value_to_json(value: &ColumnValue) -> Value {
    match value {
        ColumnValue::Int64(v) => json!(v),
        ColumnValue::String(v) => json!(v),
        ColumnValue::Null => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_result() -> (QueryResult, ResultContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101, 102])),
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ],
        )
        .unwrap();

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");

        (QueryResult::from_batches(&[batch], &ctx), ctx)
    }

    #[test]
    fn row_to_json_includes_node_ids_and_types() {
        let (result, ctx) = make_test_result();
        let row = &result.rows()[0];

        let json = row_to_json(row, &ctx);
        let obj = json.as_object().unwrap();

        assert_eq!(obj.get("p_id").unwrap().as_i64().unwrap(), 101);
        assert_eq!(obj.get("p_type").unwrap().as_str().unwrap(), "Project");
    }

    fn make_test_result_with_columns() -> (QueryResult, ResultContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101, 102])),
                Arc::new(StringArray::from(vec!["Project", "Project"])),
                Arc::new(StringArray::from(vec!["Alpha", "Beta"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");

        (QueryResult::from_batches(&[batch], &ctx), ctx)
    }

    #[test]
    fn row_to_json_includes_all_non_internal_columns() {
        let (result, ctx) = make_test_result_with_columns();
        let row = &result.rows()[0];

        let json = row_to_json(row, &ctx);
        let obj = json.as_object().unwrap();

        assert_eq!(obj.get("p_id").unwrap().as_i64().unwrap(), 101);
        assert_eq!(obj.get("p_type").unwrap().as_str().unwrap(), "Project");
        assert_eq!(obj.get("name").unwrap().as_str().unwrap(), "Alpha");
        assert_eq!(obj.get("count").unwrap().as_i64().unwrap(), 10);
    }

    #[test]
    fn row_to_json_filters_internal_columns() {
        let (result, ctx) = make_test_result_with_columns();
        let row = &result.rows()[0];

        let json = row_to_json(row, &ctx);
        let obj = json.as_object().unwrap();

        assert!(!obj.contains_key("_gkg_p_id"));
        assert!(!obj.contains_key("_gkg_p_type"));
    }

    #[test]
    fn row_to_json_node_keys_win_over_column_collision() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
            Field::new("p_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101])),
                Arc::new(StringArray::from(vec!["Project"])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        )
        .unwrap();

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");

        let result = QueryResult::from_batches(&[batch], &ctx);
        let row = &result.rows()[0];

        let json = row_to_json(row, &ctx);
        let obj = json.as_object().unwrap();

        assert_eq!(obj.get("p_id").unwrap().as_i64().unwrap(), 101);
    }

    #[test]
    fn row_to_json_path_finding_serializes_dynamic_nodes() {
        let ids = Int64Array::from(vec![10, 20]);
        let types = StringArray::from(vec!["Project", "MergeRequest"]);

        let struct_fields = vec![
            Arc::new(Field::new("1", DataType::Int64, false)),
            Arc::new(Field::new("2", DataType::Utf8, false)),
        ];
        let struct_array = StructArray::new(
            struct_fields.into(),
            vec![Arc::new(ids) as _, Arc::new(types) as _],
            None,
        );

        let list_field = Arc::new(Field::new("item", struct_array.data_type().clone(), true));
        let offsets = OffsetBuffer::new(vec![0i32, 2].into());
        let list_array = ListArray::new(list_field, offsets, Arc::new(struct_array), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_path", list_array.data_type().clone(), true),
            Field::new("depth", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(list_array) as _,
                Arc::new(Int64Array::from(vec![2])) as _,
            ],
        )
        .unwrap();

        let ctx = ResultContext::new().with_query_type(QueryType::PathFinding);
        let mut result = QueryResult::from_batches(&[batch], &ctx);

        // Simulate hydration: populate properties on dynamic nodes
        for node in result.rows_mut()[0].dynamic_nodes_mut() {
            node.properties
                .insert("name".to_string(), ColumnValue::String("test".to_string()));
        }

        let json = row_to_json(&result.rows()[0], &ctx);
        let obj = json.as_object().unwrap();

        let path = obj.get("path").unwrap().as_array().unwrap();
        assert_eq!(path.len(), 2);

        let first = path[0].as_object().unwrap();
        assert_eq!(first.get("id").unwrap().as_i64().unwrap(), 10);
        assert_eq!(
            first.get("entity_type").unwrap().as_str().unwrap(),
            "Project"
        );
        assert_eq!(first.get("name").unwrap().as_str().unwrap(), "test");

        let second = path[1].as_object().unwrap();
        assert_eq!(second.get("id").unwrap().as_i64().unwrap(), 20);
        assert_eq!(
            second.get("entity_type").unwrap().as_str().unwrap(),
            "MergeRequest"
        );
    }

    #[test]
    fn row_to_json_path_finding_includes_edges_from_edge_kinds() {
        let ids = Int64Array::from(vec![3, 47, 1020]);
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

        // edge_kinds: Array(String) with 2 hops
        let edge_values = StringArray::from(vec!["IN_PROJECT", "HAS_NOTE"]);
        let edge_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let edge_offsets = OffsetBuffer::new(vec![0i32, 2].into());
        let edge_list = ListArray::new(edge_field, edge_offsets, Arc::new(edge_values), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_path", path_list.data_type().clone(), true),
            Field::new("_gkg_edge_kinds", edge_list.data_type().clone(), true),
            Field::new("depth", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(path_list) as _,
                Arc::new(edge_list) as _,
                Arc::new(Int64Array::from(vec![2])) as _,
            ],
        )
        .unwrap();

        let ctx = ResultContext::new().with_query_type(QueryType::PathFinding);
        let result = QueryResult::from_batches(&[batch], &ctx);

        let json = row_to_json(&result.rows()[0], &ctx);
        let obj = json.as_object().unwrap();

        // Path nodes present
        let path = obj.get("path").unwrap().as_array().unwrap();
        assert_eq!(path.len(), 3);

        // Edges: flat array of relationship kinds, positional with path
        let edges = obj.get("edges").unwrap().as_array().unwrap();
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].as_str().unwrap(), "IN_PROJECT");
        assert_eq!(edges[1].as_str().unwrap(), "HAS_NOTE");
    }

    #[test]
    fn row_to_json_neighbors_serializes_properties_as_top_level() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_neighbor_id", DataType::Int64, false),
            Field::new("_gkg_neighbor_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["MergeRequest"])),
            ],
        )
        .unwrap();

        let ctx = ResultContext::new().with_query_type(QueryType::Neighbors);
        let mut result = QueryResult::from_batches(&[batch], &ctx);

        // Simulate hydration
        for node in result.rows_mut()[0].dynamic_nodes_mut() {
            node.properties.insert(
                "title".to_string(),
                ColumnValue::String("Fix bug".to_string()),
            );
            node.properties
                .insert("iid".to_string(), ColumnValue::Int64(123));
        }

        let json = row_to_json(&result.rows()[0], &ctx);
        let obj = json.as_object().unwrap();

        assert_eq!(obj.get("title").unwrap().as_str().unwrap(), "Fix bug");
        assert_eq!(obj.get("iid").unwrap().as_i64().unwrap(), 123);
        assert!(!obj.contains_key("path"));
    }

    #[test]
    fn row_to_json_no_dynamic_nodes_no_path_key() {
        let (result, ctx) = make_test_result();
        let row = &result.rows()[0];

        let json = row_to_json(row, &ctx);
        let obj = json.as_object().unwrap();

        assert!(!obj.contains_key("path"));
    }
}
