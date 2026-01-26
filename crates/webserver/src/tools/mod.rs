pub mod types;

use std::collections::HashMap;
use std::sync::Arc;

pub use types::{KnowledgeGraphTool, ToolContent, ToolError, ToolInfo, ToolInput, ToolResult};

use crate::auth::Claims;
use crate::error::WebserverError;

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
        let name = tool.name().to_string();
        self.tools.insert(name, Arc::new(tool));
    }

    pub fn register_arc(&mut self, tool: Arc<dyn KnowledgeGraphTool>) {
        let name = tool.name().to_string();
        self.tools.insert(name, tool);
    }

    pub fn list_tools(&self) -> Vec<ToolInfo> {
        self.tools
            .values()
            .map(|tool| tool.to_tool_info())
            .collect()
    }

    pub fn get_tool(&self, name: &str) -> Option<Arc<dyn KnowledgeGraphTool>> {
        self.tools.get(name).cloned()
    }

    pub async fn call_tool(
        &self,
        name: &str,
        params: serde_json::Value,
        claims: &Claims,
    ) -> Result<ToolResult, WebserverError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| WebserverError::ToolNotFound(name.to_string()))?;

        tool.call(params, claims)
            .await
            .map_err(|e| WebserverError::ToolExecution(e.to_string()))
    }

    pub fn tool_count(&self) -> usize {
        self.tools.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    struct MockTool;

    #[async_trait]
    impl KnowledgeGraphTool for MockTool {
        fn name(&self) -> &str {
            "mock_tool"
        }

        fn description(&self) -> &str {
            "A mock tool for testing"
        }

        fn input_schema(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {
                    "input": { "type": "string" }
                }
            })
        }

        async fn call(
            &self,
            _params: serde_json::Value,
            _claims: &Claims,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult::success_text("Mock result"))
        }
    }

    fn create_test_claims() -> Claims {
        Claims {
            sub: "user:123".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            iat: 0,
            exp: 0,
            user_id: 123,
            username: "test".to_string(),
            admin: false,
            organization_id: None,
            min_access_level: None,
            group_traversal_ids: vec![],
            project_ids: vec![],
        }
    }

    #[test]
    fn test_registry_register_and_list() {
        let mut registry = ToolRegistry::new();
        registry.register(MockTool);

        let tools = registry.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "mock_tool");
        assert_eq!(tools[0].description, "A mock tool for testing");
    }

    #[test]
    fn test_registry_get_tool() {
        let mut registry = ToolRegistry::new();
        registry.register(MockTool);

        assert!(registry.get_tool("mock_tool").is_some());
        assert!(registry.get_tool("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_registry_call_tool() {
        let mut registry = ToolRegistry::new();
        registry.register(MockTool);

        let claims = create_test_claims();
        let result = registry.call_tool("mock_tool", json!({}), &claims).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(!result.is_error);
        assert_eq!(result.content[0].text, "Mock result");
    }

    #[tokio::test]
    async fn test_registry_call_tool_not_found() {
        let registry = ToolRegistry::new();
        let claims = create_test_claims();

        let result = registry.call_tool("nonexistent", json!({}), &claims).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            WebserverError::ToolNotFound(_)
        ));
    }

    #[test]
    fn test_registry_tool_count() {
        let mut registry = ToolRegistry::new();
        assert_eq!(registry.tool_count(), 0);

        registry.register(MockTool);
        assert_eq!(registry.tool_count(), 1);
    }
}
