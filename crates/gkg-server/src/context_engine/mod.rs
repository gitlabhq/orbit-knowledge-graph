use std::collections::HashMap;

use ontology::Ontology;
use serde_json::{Value, json};

use crate::redaction::{ColumnValue, QueryResult, QueryResultRow, ResourceAuthorization};
use query_engine::ResultContext;

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

    for node in ctx.nodes() {
        if let Some(id) = row.get_id(&node.alias) {
            obj.insert(format!("{}_id", node.alias), json!(id));
        }
        if let Some(entity_type) = row.get_type(&node.alias) {
            obj.insert(format!("{}_type", node.alias), json!(entity_type));
        }
    }

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

    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::ResultContext;
    use std::sync::Arc;

    fn make_test_result() -> QueryResult {
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

        QueryResult::from_batches(&[batch], &ctx)
    }

    #[test]
    fn apply_redaction_marks_unauthorized() {
        let engine = ContextEngine::new();
        let mut result = make_test_result();

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
        let mut result = make_test_result();

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
}
