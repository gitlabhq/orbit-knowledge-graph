use serde_json::{Value, json};
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
pub struct ToolService;

impl ToolService {
    pub fn new() -> Self {
        Self
    }

    pub fn execute_tool(
        &self,
        tool_name: &str,
        arguments_json: &str,
        _claims: &Claims,
    ) -> Result<ExecutionResult, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        match tool_name {
            "query_graph" => self.execute_query_graph(&arguments),
            "get_graph_entities" => self.execute_get_graph_entities(&arguments),
            _ => Err(ExecutorError::NotFound(tool_name.to_string())),
        }
    }

    fn execute_query_graph(&self, arguments: &Value) -> Result<ExecutionResult, ExecutorError> {
        let operation = arguments
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExecutorError::InvalidArguments("operation is required".to_string()))?;

        Err(ExecutorError::InvalidArguments(format!(
            "Unknown operation: {}",
            operation
        )))
    }

    fn execute_get_graph_entities(
        &self,
        arguments: &Value,
    ) -> Result<ExecutionResult, ExecutorError> {
        let entity_type = arguments
            .get("entity_type")
            .and_then(|v| v.as_str())
            .unwrap_or("schema");

        let result = match entity_type {
            "schema" | "node_types" | "relationship_types" => self.mock_get_graph_schema()?,
            _ => {
                return Err(ExecutorError::InvalidArguments(format!(
                    "Unknown entity_type: {}",
                    entity_type
                )));
            }
        };

        Ok(result)
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
}
