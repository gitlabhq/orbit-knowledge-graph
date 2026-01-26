use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::Claims;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("missing parameter: {0}")]
    MissingParameter(String),

    #[error("execution failed: {0}")]
    Execution(String),

    #[error("access denied: {0}")]
    AccessDenied(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

impl ToolContent {
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content_type: "text".to_string(),
            text: content.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: Vec<ToolContent>,
    pub is_error: bool,
}

impl ToolResult {
    pub fn success(content: Vec<ToolContent>) -> Self {
        Self {
            content,
            is_error: false,
        }
    }

    pub fn success_text(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::text(text)],
            is_error: false,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::text(message)],
            is_error: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInfo {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[async_trait]
pub trait KnowledgeGraphTool: Send + Sync {
    fn name(&self) -> &str;

    fn description(&self) -> &str;

    fn input_schema(&self) -> serde_json::Value;

    fn to_tool_info(&self) -> ToolInfo {
        ToolInfo {
            name: self.name().to_string(),
            description: self.description().to_string(),
            input_schema: self.input_schema(),
        }
    }

    async fn call(
        &self,
        params: serde_json::Value,
        claims: &Claims,
    ) -> Result<ToolResult, ToolError>;
}

pub struct ToolInput {
    params: serde_json::Value,
}

impl ToolInput {
    pub fn new(params: serde_json::Value) -> Self {
        Self { params }
    }

    pub fn get_string(&self, key: &str) -> Result<&str, ToolError> {
        self.params
            .get(key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::MissingParameter(key.to_string()))
    }

    pub fn get_u64(&self, key: &str) -> Result<u64, ToolError> {
        self.params
            .get(key)
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ToolError::MissingParameter(key.to_string()))
    }

    pub fn get_i64(&self, key: &str) -> Result<i64, ToolError> {
        self.params
            .get(key)
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ToolError::MissingParameter(key.to_string()))
    }

    pub fn get_bool(&self, key: &str) -> Result<bool, ToolError> {
        self.params
            .get(key)
            .and_then(|v| v.as_bool())
            .ok_or_else(|| ToolError::MissingParameter(key.to_string()))
    }

    pub fn get_string_array(&self, key: &str) -> Result<Vec<String>, ToolError> {
        let array = self
            .params
            .get(key)
            .and_then(|v| v.as_array())
            .ok_or_else(|| ToolError::MissingParameter(key.to_string()))?;

        Ok(array
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect())
    }

    pub fn get_string_optional(&self, key: &str) -> Option<&str> {
        self.params.get(key).and_then(|v| v.as_str())
    }

    pub fn get_u64_optional(&self, key: &str) -> Option<u64> {
        self.params.get(key).and_then(|v| v.as_u64())
    }

    pub fn get_bool_optional(&self, key: &str) -> Option<bool> {
        self.params.get(key).and_then(|v| v.as_bool())
    }

    pub fn get_string_array_optional(&self, key: &str) -> Option<Vec<String>> {
        self.params.get(key).and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tool_content_text() {
        let content = ToolContent::text("Hello, World!");
        assert_eq!(content.content_type, "text");
        assert_eq!(content.text, "Hello, World!");
    }

    #[test]
    fn test_tool_result_success() {
        let result = ToolResult::success_text("Success message");
        assert!(!result.is_error);
        assert_eq!(result.content.len(), 1);
        assert_eq!(result.content[0].text, "Success message");
    }

    #[test]
    fn test_tool_result_error() {
        let result = ToolResult::error("Error message");
        assert!(result.is_error);
        assert_eq!(result.content.len(), 1);
        assert_eq!(result.content[0].text, "Error message");
    }

    #[test]
    fn test_tool_input_get_string() {
        let params = json!({
            "name": "test",
            "count": 42
        });
        let input = ToolInput::new(params);

        assert_eq!(input.get_string("name").unwrap(), "test");
        assert!(input.get_string("missing").is_err());
    }

    #[test]
    fn test_tool_input_get_u64() {
        let params = json!({
            "count": 42
        });
        let input = ToolInput::new(params);

        assert_eq!(input.get_u64("count").unwrap(), 42);
        assert!(input.get_u64("missing").is_err());
    }

    #[test]
    fn test_tool_input_get_string_array() {
        let params = json!({
            "items": ["a", "b", "c"]
        });
        let input = ToolInput::new(params);

        let items = input.get_string_array("items").unwrap();
        assert_eq!(items, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_tool_input_optional() {
        let params = json!({
            "present": "value"
        });
        let input = ToolInput::new(params);

        assert_eq!(input.get_string_optional("present"), Some("value"));
        assert_eq!(input.get_string_optional("missing"), None);
    }
}
