use compiler::{
    INTERNAL_COLUMN_PREFIX, NEIGHBOR_ID_COLUMN, NEIGHBOR_TYPE_COLUMN, QueryType,
    RELATIONSHIP_TYPE_COLUMN, ResultContext,
};
use serde_json::{Value, json};

use types::{NodeRef, QueryResultRow};

use super::column_value_to_json;

pub fn row_to_json(row: &QueryResultRow, ctx: &ResultContext) -> Value {
    let mut obj = serde_json::Map::new();

    for (name, value) in row.columns() {
        if name.starts_with(INTERNAL_COLUMN_PREFIX)
            && name != NEIGHBOR_ID_COLUMN
            && name != NEIGHBOR_TYPE_COLUMN
            && name != RELATIONSHIP_TYPE_COLUMN
        {
            continue;
        }
        obj.insert(name.clone(), column_value_to_json(value));
    }

    for node in ctx.nodes() {
        if let Some(id) = row.get_public_id(node) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use gkg_utils::arrow::ColumnValue;
    use std::sync::Arc;
    use types::QueryResult;

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
    }
}
