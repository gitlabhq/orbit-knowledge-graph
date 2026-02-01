use ontology::Ontology;
use query_engine::ResultContext;
use serde_json::{Value, json};

use crate::redaction::{ColumnValue, QueryResult, QueryResultRow};

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
        if name.starts_with("_gkg_") {
            continue;
        }
        let json_value = match value {
            ColumnValue::Int64(v) => json!(v),
            ColumnValue::String(v) => json!(v),
            ColumnValue::Null => Value::Null,
        };
        obj.insert(name.clone(), json_value);
    }

    for node in ctx.nodes() {
        if let Some(id) = row.get_id(&node.alias) {
            obj.insert(format!("{}_id", node.alias), json!(id));
        }
        if let Some(entity_type) = row.get_type(&node.alias) {
            obj.insert(format!("{}_type", node.alias), json!(entity_type));
        }
    }

    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
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
}
