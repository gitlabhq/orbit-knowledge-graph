use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::Claims;
use crate::error::ServerError;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("missing parameter: {0}")]
    MissingParameter(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: Vec<ToolContent>,
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

impl ToolResult {
    pub fn success(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent {
                content_type: "text".into(),
                text: text.into(),
            }],
            is_error: false,
        }
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent {
                content_type: "text".into(),
                text: text.into(),
            }],
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
    async fn call(
        &self,
        params: serde_json::Value,
        claims: &Claims,
    ) -> Result<ToolResult, ToolError>;
}

pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn KnowledgeGraphTool>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }

    pub fn register(&mut self, tool: impl KnowledgeGraphTool + 'static) {
        self.tools.insert(tool.name().to_string(), Arc::new(tool));
    }

    pub fn list(&self) -> Vec<ToolInfo> {
        self.tools
            .values()
            .map(|t| ToolInfo {
                name: t.name().to_string(),
                description: t.description().to_string(),
                input_schema: t.input_schema(),
            })
            .collect()
    }

    pub async fn call(
        &self,
        name: &str,
        params: serde_json::Value,
        claims: &Claims,
    ) -> Result<ToolResult, ServerError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| ServerError::ToolNotFound(name.into()))?;
        tool.call(params, claims)
            .await
            .map_err(|e| ServerError::ToolExecution(e.to_string()))
    }
}
