use std::collections::HashMap;

use serde_json::Value;

use crate::redaction::{QueryResult, ResourceAuthorization};

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
        result: Value,
        authorizations: &[ResourceAuthorization],
    ) -> Value {
        if authorizations.is_empty() {
            return result;
        }

        match result {
            Value::Array(rows) => {
                let filtered: Vec<Value> = rows
                    .into_iter()
                    .filter(|row| {
                        for auth in authorizations {
                            let id_key = match auth.resource_type.as_str() {
                                "projects" => "project_id",
                                "issues" => "issue_id",
                                "merge_requests" => "merge_request_id",
                                "users" => "user_id",
                                "groups" => "group_id",
                                _ => continue,
                            };

                            if let Some(id) = row.get(id_key).and_then(|v| v.as_i64())
                                && auth.authorized.get(&id) == Some(&false)
                            {
                                return false;
                            }
                        }
                        true
                    })
                    .collect();
                Value::Array(filtered)
            }
            other => other,
        }
    }
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
