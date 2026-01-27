use serde_json::{json, Value};
use thiserror::Error;

use crate::auth::Claims;
use crate::redaction::ResourceCheck;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
}

impl ExecutorError {
    pub fn code(&self) -> String {
        match self {
            Self::NotFound(_) => "tool_not_found".to_string(),
            Self::InvalidArguments(_) => "invalid_arguments".to_string(),
            Self::ExecutionFailed(_) => "execution_error".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub raw_result: Value,
    pub resources_to_check: Vec<ResourceCheck>,
}

#[derive(Debug, Clone, Default)]
pub struct ToolExecutor;

impl ToolExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute(
        &self,
        tool_name: &str,
        arguments_json: &str,
        _claims: &Claims,
    ) -> Result<ExecutionResult, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        match tool_name {
            "get_graph_schema" => self.mock_get_graph_schema(),
            "find_nodes" => self.mock_find_nodes(&arguments),
            "traverse_relationships" => self.mock_traverse_relationships(&arguments),
            "explore_neighborhood" => self.mock_explore_neighborhood(&arguments),
            "find_paths" => self.mock_find_paths(&arguments),
            "aggregate_nodes" => self.mock_aggregate_nodes(&arguments),
            _ => Err(ExecutorError::NotFound(tool_name.to_string())),
        }
    }

    fn mock_get_graph_schema(&self) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            raw_result: json!({
                "node_types": [
                    {"name": "gl_project", "properties": ["id", "name", "path", "description"]},
                    {"name": "gl_issue", "properties": ["id", "iid", "title", "state", "author_id"]},
                    {"name": "gl_mr", "properties": ["id", "iid", "title", "state", "author_id"]},
                    {"name": "gl_user", "properties": ["id", "username", "name", "email"]},
                    {"name": "gl_group", "properties": ["id", "name", "path"]},
                    {"name": "gl_epic", "properties": ["id", "iid", "title", "state"]},
                    {"name": "gl_branch", "properties": ["id", "name", "project_id"]}
                ],
                "relationship_types": [
                    {"name": "PROJECT_TO_GROUP", "from_table": "gl_project", "to_table": "gl_group"},
                    {"name": "AUTHORED_ISSUE", "from_table": "gl_user", "to_table": "gl_issue"},
                    {"name": "AUTHORED_MR", "from_table": "gl_user", "to_table": "gl_mr"},
                    {"name": "ISSUE_IN_PROJECT", "from_table": "gl_issue", "to_table": "gl_project"},
                    {"name": "MR_IN_PROJECT", "from_table": "gl_mr", "to_table": "gl_project"},
                    {"name": "CLOSES", "from_table": "gl_mr", "to_table": "gl_issue"}
                ]
            }),
            resources_to_check: vec![],
        })
    }

    fn mock_find_nodes(&self, arguments: &Value) -> Result<ExecutionResult, ExecutorError> {
        let node_label = arguments
            .get("node_label")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExecutorError::InvalidArguments("node_label is required".to_string()))?;

        let (nodes, resources) = match node_label {
            "gl_issue" => (
                json!({
                    "nodes": [
                        {"id": 101, "type": "gl_issue", "iid": 1, "title": "Fix login bug", "state": "opened"},
                        {"id": 102, "type": "gl_issue", "iid": 2, "title": "Add dark mode", "state": "opened"},
                        {"id": 103, "type": "gl_issue", "iid": 3, "title": "Update docs", "state": "closed"}
                    ]
                }),
                vec![ResourceCheck {
                    resource_type: "issues".to_string(),
                    ids: vec![101, 102, 103],
                }],
            ),
            "gl_mr" => (
                json!({
                    "nodes": [
                        {"id": 201, "type": "gl_mr", "iid": 10, "title": "Fix: login validation", "state": "merged"},
                        {"id": 202, "type": "gl_mr", "iid": 11, "title": "Feature: dark mode", "state": "opened"}
                    ]
                }),
                vec![ResourceCheck {
                    resource_type: "merge_requests".to_string(),
                    ids: vec![201, 202],
                }],
            ),
            "gl_project" => (
                json!({
                    "nodes": [
                        {"id": 1, "type": "gl_project", "name": "frontend", "path": "acme/frontend"},
                        {"id": 2, "type": "gl_project", "name": "backend", "path": "acme/backend"}
                    ]
                }),
                vec![ResourceCheck {
                    resource_type: "projects".to_string(),
                    ids: vec![1, 2],
                }],
            ),
            _ => (json!({"nodes": []}), vec![]),
        };

        Ok(ExecutionResult {
            raw_result: nodes,
            resources_to_check: resources,
        })
    }

    fn mock_traverse_relationships(
        &self,
        _arguments: &Value,
    ) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            raw_result: json!({
                "paths": [
                    {
                        "start": {"id": 101, "type": "gl_issue", "title": "Fix login bug"},
                        "relationship": "ISSUE_IN_PROJECT",
                        "end": {"id": 1, "type": "gl_project", "name": "frontend"}
                    }
                ]
            }),
            resources_to_check: vec![
                ResourceCheck {
                    resource_type: "issues".to_string(),
                    ids: vec![101],
                },
                ResourceCheck {
                    resource_type: "projects".to_string(),
                    ids: vec![1],
                },
            ],
        })
    }

    fn mock_explore_neighborhood(
        &self,
        _arguments: &Value,
    ) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            raw_result: json!({
                "center": {"id": 1, "type": "gl_project", "name": "frontend"},
                "neighbors": {
                    "gl_issue": [
                        {"id": 101, "type": "gl_issue", "title": "Fix login bug"},
                        {"id": 102, "type": "gl_issue", "title": "Add dark mode"}
                    ],
                    "gl_mr": [
                        {"id": 201, "type": "gl_mr", "title": "Fix: login validation"}
                    ]
                }
            }),
            resources_to_check: vec![
                ResourceCheck {
                    resource_type: "projects".to_string(),
                    ids: vec![1],
                },
                ResourceCheck {
                    resource_type: "issues".to_string(),
                    ids: vec![101, 102],
                },
                ResourceCheck {
                    resource_type: "merge_requests".to_string(),
                    ids: vec![201],
                },
            ],
        })
    }

    fn mock_find_paths(&self, _arguments: &Value) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            raw_result: json!({
                "paths": [
                    [
                        {"id": 101, "type": "gl_issue", "title": "Fix login bug"},
                        {"relationship": "ISSUE_IN_PROJECT"},
                        {"id": 1, "type": "gl_project", "name": "frontend"},
                        {"relationship": "PROJECT_TO_GROUP"},
                        {"id": 10, "type": "gl_group", "name": "acme"}
                    ]
                ]
            }),
            resources_to_check: vec![
                ResourceCheck {
                    resource_type: "issues".to_string(),
                    ids: vec![101],
                },
                ResourceCheck {
                    resource_type: "projects".to_string(),
                    ids: vec![1],
                },
            ],
        })
    }

    fn mock_aggregate_nodes(&self, _arguments: &Value) -> Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            raw_result: json!({
                "aggregations": [
                    {"group": {"id": 1, "type": "gl_project", "name": "frontend"}, "count": 15},
                    {"group": {"id": 2, "type": "gl_project", "name": "backend"}, "count": 8}
                ]
            }),
            resources_to_check: vec![ResourceCheck {
                resource_type: "projects".to_string(),
                ids: vec![1, 2],
            }],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Claims;

    fn test_claims() -> Claims {
        Claims {
            sub: "user:1".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            exp: 0,
            iat: 0,
            user_id: 1,
            username: "testuser".to_string(),
            admin: false,
            organization_id: None,
            min_access_level: None,
            group_traversal_ids: vec![],
        }
    }

    #[test]
    fn test_execute_unknown_tool() {
        let executor = ToolExecutor::new();
        let result = executor.execute("unknown_tool", "{}", &test_claims());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), "tool_not_found");
    }

    #[test]
    fn test_execute_invalid_json() {
        let executor = ToolExecutor::new();
        let result = executor.execute("find_nodes", "invalid json", &test_claims());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), "invalid_arguments");
    }

    #[test]
    fn test_execute_get_graph_schema() {
        let executor = ToolExecutor::new();
        let result = executor
            .execute("get_graph_schema", "{}", &test_claims())
            .unwrap();

        assert!(result.raw_result.get("node_types").is_some());
        assert!(result.raw_result.get("relationship_types").is_some());
        assert!(result.resources_to_check.is_empty());
    }

    #[test]
    fn test_execute_find_nodes_issues() {
        let executor = ToolExecutor::new();
        let result = executor
            .execute(
                "find_nodes",
                r#"{"node_label": "gl_issue"}"#,
                &test_claims(),
            )
            .unwrap();

        let nodes = result.raw_result.get("nodes").unwrap().as_array().unwrap();
        assert!(!nodes.is_empty());

        assert_eq!(result.resources_to_check.len(), 1);
        assert_eq!(result.resources_to_check[0].resource_type, "issues");
        assert!(!result.resources_to_check[0].ids.is_empty());
    }

    #[test]
    fn test_execute_find_nodes_missing_label() {
        let executor = ToolExecutor::new();
        let result = executor.execute("find_nodes", "{}", &test_claims());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), "invalid_arguments");
    }

    #[test]
    fn test_explore_neighborhood_has_multiple_resource_types() {
        let executor = ToolExecutor::new();
        let result = executor
            .execute(
                "explore_neighborhood",
                r#"{"start_node": {"label": "gl_project", "node_id": 1}}"#,
                &test_claims(),
            )
            .unwrap();

        assert!(result.resources_to_check.len() >= 2);

        let types: Vec<&str> = result
            .resources_to_check
            .iter()
            .map(|r| r.resource_type.as_str())
            .collect();
        assert!(types.contains(&"projects"));
        assert!(types.contains(&"issues"));
    }
}
