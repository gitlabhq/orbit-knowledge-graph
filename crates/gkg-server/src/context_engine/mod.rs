use std::collections::HashMap;

use ontology::Ontology;
use query_engine::ResultContext;
use serde_json::{Value, json};

use crate::redaction::{ColumnValue, QueryResult, QueryResultRow, ResourceAuthorization};

#[derive(Debug, Clone, Default)]
pub struct ContextEngine;

impl ContextEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn apply_redaction(
        &self,
        result: &mut QueryResult,
        authorizations: &[ResourceAuthorization],
        entity_to_resource: &HashMap<&str, &str>,
    ) -> usize {
        result.apply_authorizations(authorizations, entity_to_resource)
    }

    pub fn prepare_response(&self, result: Value) -> Value {
        result
    }

    pub fn apply_redaction_and_prepare(
        &self,
        result: &mut QueryResult,
        result_context: &ResultContext,
        authorizations: &[ResourceAuthorization],
        ontology: &Ontology,
    ) -> Value {
        let entity_map = build_entity_to_resource_map(ontology);
        self.apply_redaction(result, authorizations, &entity_map);

        let rows: Vec<Value> = result
            .authorized_rows()
            .map(|row| row_to_json(row, result_context))
            .collect();
        Value::Array(rows)
    }
}

fn build_entity_to_resource_map(ontology: &Ontology) -> HashMap<&str, &str> {
    ontology
        .nodes()
        .filter_map(|node| {
            let redaction = node.redaction.as_ref()?;
            Some((node.name.as_str(), redaction.resource_type.as_str()))
        })
        .collect()
}

fn row_to_json(row: &QueryResultRow, ctx: &ResultContext) -> Value {
    let mut obj = serde_json::Map::new();

    // Insert columns first so node keys can overwrite on collision
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

    // Node IDs and types always win over columns with same name
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
    fn apply_redaction_marks_unauthorized() {
        let engine = ContextEngine::new();
        let (mut result, _ctx) = make_test_result();

        let mut auth = HashMap::new();
        auth.insert(101, true);
        auth.insert(102, false);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "projects".to_string(),
            authorized: auth,
        }];

        let entity_map: HashMap<&str, &str> = [("Project", "projects")].into_iter().collect();

        let count = engine.apply_redaction(&mut result, &authorizations, &entity_map);

        assert_eq!(count, 1);
        assert_eq!(result.authorized_count(), 1);
    }

    #[test]
    fn apply_redaction_no_changes_when_all_authorized() {
        let engine = ContextEngine::new();
        let (mut result, _ctx) = make_test_result();

        let mut auth = HashMap::new();
        auth.insert(101, true);
        auth.insert(102, true);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "projects".to_string(),
            authorized: auth,
        }];

        let entity_map: HashMap<&str, &str> = [("Project", "projects")].into_iter().collect();

        let count = engine.apply_redaction(&mut result, &authorizations, &entity_map);

        assert_eq!(count, 0);
        assert_eq!(result.authorized_count(), 2);
    }

    #[test]
    fn apply_redaction_and_prepare_returns_authorized_rows_as_json() {
        let engine = ContextEngine::new();
        let (mut result, ctx) = make_test_result();
        let ontology = Ontology::load_embedded().unwrap();

        let mut auth = HashMap::new();
        auth.insert(101, true);
        auth.insert(102, false);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "projects".to_string(),
            authorized: auth,
        }];

        let json_result =
            engine.apply_redaction_and_prepare(&mut result, &ctx, &authorizations, &ontology);

        let rows = json_result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("p_id").unwrap().as_i64().unwrap(), 101);
    }

    #[test]
    fn apply_redaction_and_prepare_with_empty_authorizations() {
        let engine = ContextEngine::new();
        let (mut result, ctx) = make_test_result();
        let ontology = Ontology::load_embedded().unwrap();

        let json_result = engine.apply_redaction_and_prepare(&mut result, &ctx, &[], &ontology);

        let rows = json_result.as_array().unwrap();
        assert_eq!(rows.len(), 0);
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
    fn build_entity_to_resource_map_from_ontology() {
        let ontology = Ontology::load_embedded().unwrap();
        let map = build_entity_to_resource_map(&ontology);

        assert_eq!(map.get("Project"), Some(&"projects"));
        assert_eq!(map.get("User"), Some(&"users"));
        assert_eq!(map.get("Group"), Some(&"groups"));
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

        assert!(
            !obj.contains_key("_gkg_p_id"),
            "internal column should be filtered"
        );
        assert!(
            !obj.contains_key("_gkg_p_type"),
            "internal column should be filtered"
        );
    }

    #[test]
    fn columns_iterator_returns_all_columns() {
        let (result, _ctx) = make_test_result_with_columns();
        let row = &result.rows()[0];

        let column_names: Vec<&String> = row.columns().map(|(name, _)| name).collect();

        assert!(column_names.iter().any(|n| *n == "_gkg_p_id"));
        assert!(column_names.iter().any(|n| *n == "_gkg_p_type"));
        assert!(column_names.iter().any(|n| *n == "name"));
        assert!(column_names.iter().any(|n| *n == "count"));
    }

    #[test]
    fn apply_redaction_and_prepare_includes_columns_in_output() {
        let engine = ContextEngine::new();
        let (mut result, ctx) = make_test_result_with_columns();
        let ontology = Ontology::load_embedded().unwrap();

        let mut auth = HashMap::new();
        auth.insert(101, true);
        auth.insert(102, true);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "projects".to_string(),
            authorized: auth,
        }];

        let json_result =
            engine.apply_redaction_and_prepare(&mut result, &ctx, &authorizations, &ontology);

        let rows = json_result.as_array().unwrap();
        assert_eq!(rows.len(), 2);

        let first_row = rows[0].as_object().unwrap();
        assert_eq!(first_row.get("name").unwrap().as_str().unwrap(), "Alpha");
        assert_eq!(first_row.get("count").unwrap().as_i64().unwrap(), 10);

        let second_row = rows[1].as_object().unwrap();
        assert_eq!(second_row.get("name").unwrap().as_str().unwrap(), "Beta");
        assert_eq!(second_row.get("count").unwrap().as_i64().unwrap(), 20);
    }

    #[test]
    fn row_to_json_node_keys_win_over_column_collision() {
        // Create a result where a column has the same name as a node key
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
            Field::new("p_id", DataType::Int64, false), // Collision with node key!
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101])),
                Arc::new(StringArray::from(vec!["Project"])),
                Arc::new(Int64Array::from(vec![999])), // Different value
            ],
        )
        .unwrap();

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");

        let result = QueryResult::from_batches(&[batch], &ctx);
        let row = &result.rows()[0];

        let json = row_to_json(row, &ctx);
        let obj = json.as_object().unwrap();

        // Node key should win, not the column value
        assert_eq!(
            obj.get("p_id").unwrap().as_i64().unwrap(),
            101,
            "node ID should win over column with same name"
        );
    }
}
