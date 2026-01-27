use serde_json::{Value, json};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct ResourceAuthorization {
    pub resource_type: String,
    pub authorized: std::collections::HashMap<i64, bool>,
}

#[derive(Debug, Clone, Default)]
pub struct ContextEngine;

impl ContextEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn prepare_response(&self, raw_result: Value) -> Value {
        self.optimize_for_llm(raw_result)
    }

    pub fn apply_redaction_and_prepare(
        &self,
        raw_result: Value,
        authorizations: &[ResourceAuthorization],
    ) -> Value {
        let unauthorized: HashSet<(String, i64)> = authorizations
            .iter()
            .flat_map(|auth| {
                auth.authorized
                    .iter()
                    .filter(|(_, authorized)| !**authorized)
                    .map(|(id, _)| (auth.resource_type.clone(), *id))
            })
            .collect();

        if unauthorized.is_empty() {
            return self.optimize_for_llm(raw_result);
        }

        let redacted = self.filter_unauthorized(raw_result, &unauthorized);
        let redacted_count = unauthorized.len();
        self.add_redaction_metadata(redacted, redacted_count)
    }

    fn filter_unauthorized(&self, value: Value, unauthorized: &HashSet<(String, i64)>) -> Value {
        match value {
            Value::Array(arr) => Value::Array(
                arr.into_iter()
                    .filter(|item| !self.is_unauthorized(item, unauthorized))
                    .map(|item| self.filter_unauthorized(item, unauthorized))
                    .collect(),
            ),
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, self.filter_unauthorized(v, unauthorized)))
                    .collect(),
            ),
            other => other,
        }
    }

    fn is_unauthorized(&self, value: &Value, unauthorized: &HashSet<(String, i64)>) -> bool {
        if let (Some(id), Some(type_val)) = (value.get("id"), value.get("type"))
            && let (Some(id), Some(type_str)) = (id.as_i64(), type_val.as_str())
        {
            let resource_type = self.map_type_to_resource(type_str);
            return unauthorized.contains(&(resource_type, id));
        }
        false
    }

    fn map_type_to_resource(&self, node_type: &str) -> String {
        match node_type {
            "gl_issue" => "issues",
            "gl_mr" | "gl_merge_request" => "merge_requests",
            "gl_project" => "projects",
            "gl_milestone" => "milestones",
            "gl_snippet" => "snippets",
            _ => node_type,
        }
        .to_string()
    }

    fn add_redaction_metadata(&self, mut result: Value, redacted_count: usize) -> Value {
        if let Value::Object(ref mut map) = result {
            map.insert(
                "_context".to_string(),
                json!({
                    "redacted_count": redacted_count,
                    "note": "Some results were filtered due to access permissions"
                }),
            );
        }
        self.optimize_for_llm(result)
    }

    fn optimize_for_llm(&self, value: Value) -> Value {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_prepare_response_no_redaction() {
        let engine = ContextEngine::new();
        let input = json!({"nodes": [{"id": 1, "type": "gl_issue", "title": "Bug"}]});

        let result = engine.prepare_response(input.clone());
        assert_eq!(result, input);
    }

    #[test]
    fn test_apply_redaction_filters_unauthorized() {
        let engine = ContextEngine::new();
        let input = json!({
            "nodes": [
                {"id": 101, "type": "gl_issue", "title": "Visible"},
                {"id": 102, "type": "gl_issue", "title": "Hidden"},
                {"id": 103, "type": "gl_issue", "title": "Also Visible"}
            ]
        });

        let mut auth_map = HashMap::new();
        auth_map.insert(101, true);
        auth_map.insert(102, false); // Not authorized
        auth_map.insert(103, true);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "issues".to_string(),
            authorized: auth_map,
        }];

        let result = engine.apply_redaction_and_prepare(input, &authorizations);

        let nodes = result.get("nodes").unwrap().as_array().unwrap();
        assert_eq!(nodes.len(), 2);

        let ids: Vec<i64> = nodes
            .iter()
            .filter_map(|n| n.get("id").and_then(|v| v.as_i64()))
            .collect();
        assert!(ids.contains(&101));
        assert!(ids.contains(&103));
        assert!(!ids.contains(&102));

        let context = result.get("_context").unwrap();
        assert_eq!(context.get("redacted_count").unwrap().as_u64().unwrap(), 1);
    }

    #[test]
    fn test_apply_redaction_multiple_resource_types() {
        let engine = ContextEngine::new();
        let input = json!({
            "nodes": [
                {"id": 101, "type": "gl_issue", "title": "Issue"},
                {"id": 201, "type": "gl_mr", "title": "MR Hidden"},
                {"id": 1, "type": "gl_project", "name": "Project"}
            ]
        });

        let mut issue_auth = HashMap::new();
        issue_auth.insert(101, true);

        let mut mr_auth = HashMap::new();
        mr_auth.insert(201, false); // Not authorized

        let mut project_auth = HashMap::new();
        project_auth.insert(1, true);

        let authorizations = vec![
            ResourceAuthorization {
                resource_type: "issues".to_string(),
                authorized: issue_auth,
            },
            ResourceAuthorization {
                resource_type: "merge_requests".to_string(),
                authorized: mr_auth,
            },
            ResourceAuthorization {
                resource_type: "projects".to_string(),
                authorized: project_auth,
            },
        ];

        let result = engine.apply_redaction_and_prepare(input, &authorizations);

        let nodes = result.get("nodes").unwrap().as_array().unwrap();
        assert_eq!(nodes.len(), 2);

        let types: Vec<&str> = nodes
            .iter()
            .filter_map(|n| n.get("type").and_then(|v| v.as_str()))
            .collect();
        assert!(types.contains(&"gl_issue"));
        assert!(types.contains(&"gl_project"));
        assert!(!types.contains(&"gl_mr"));
    }

    #[test]
    fn test_all_authorized_no_redaction_metadata() {
        let engine = ContextEngine::new();
        let input = json!({"nodes": [{"id": 101, "type": "gl_issue", "title": "Visible"}]});

        let mut auth_map = HashMap::new();
        auth_map.insert(101, true);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "issues".to_string(),
            authorized: auth_map,
        }];

        let result = engine.apply_redaction_and_prepare(input, &authorizations);

        assert!(result.get("_context").is_none());
    }

    #[test]
    fn test_nested_structure_redaction() {
        let engine = ContextEngine::new();
        let input = json!({
            "center": {"id": 1, "type": "gl_project", "name": "frontend"},
            "neighbors": {
                "gl_issue": [
                    {"id": 101, "type": "gl_issue", "title": "Visible"},
                    {"id": 102, "type": "gl_issue", "title": "Hidden"}
                ]
            }
        });

        let mut auth_map = HashMap::new();
        auth_map.insert(101, true);
        auth_map.insert(102, false);

        let mut project_auth = HashMap::new();
        project_auth.insert(1, true);

        let authorizations = vec![
            ResourceAuthorization {
                resource_type: "issues".to_string(),
                authorized: auth_map,
            },
            ResourceAuthorization {
                resource_type: "projects".to_string(),
                authorized: project_auth,
            },
        ];

        let result = engine.apply_redaction_and_prepare(input, &authorizations);

        let neighbors = result.get("neighbors").unwrap();
        let issues = neighbors.get("gl_issue").unwrap().as_array().unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].get("id").unwrap().as_i64().unwrap(), 101);
    }

    #[test]
    fn test_map_type_to_resource() {
        let engine = ContextEngine::new();

        assert_eq!(engine.map_type_to_resource("gl_issue"), "issues");
        assert_eq!(engine.map_type_to_resource("gl_mr"), "merge_requests");
        assert_eq!(
            engine.map_type_to_resource("gl_merge_request"),
            "merge_requests"
        );
        assert_eq!(engine.map_type_to_resource("gl_project"), "projects");
        assert_eq!(engine.map_type_to_resource("gl_milestone"), "milestones");
        assert_eq!(engine.map_type_to_resource("gl_snippet"), "snippets");
        assert_eq!(engine.map_type_to_resource("unknown"), "unknown");
    }
}
