use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

pub struct ToolRegistry;

impl ToolRegistry {
    pub fn get_all_tools() -> Vec<ToolDefinition> {
        vec![
            Self::get_graph_schema(),
            Self::find_nodes(),
            Self::traverse_relationships(),
            Self::explore_neighborhood(),
            Self::find_paths(),
            Self::aggregate_nodes(),
        ]
    }

    fn get_graph_schema() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_schema".to_string(),
            description: "placeholder".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn find_nodes() -> ToolDefinition {
        ToolDefinition {
            name: "find_nodes".to_string(),
            description: "placeholder".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn traverse_relationships() -> ToolDefinition {
        ToolDefinition {
            name: "traverse_relationships".to_string(),
            description: "placeholder".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn explore_neighborhood() -> ToolDefinition {
        ToolDefinition {
            name: "explore_neighborhood".to_string(),
            description: "placeholder".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn find_paths() -> ToolDefinition {
        ToolDefinition {
            name: "find_paths".to_string(),
            description: "placeholder".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn aggregate_nodes() -> ToolDefinition {
        ToolDefinition {
            name: "aggregate_nodes".to_string(),
            description: "placeholder".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_tools_have_valid_schemas() {
        let tools = ToolRegistry::get_all_tools();
        assert_eq!(tools.len(), 6, "Should have exactly 6 tools");

        for tool in &tools {
            assert!(!tool.name.is_empty());
            assert!(!tool.description.is_empty());
            assert!(tool.parameters.is_object());
        }
    }

    #[test]
    fn test_tool_names_are_unique() {
        let tools = ToolRegistry::get_all_tools();
        let mut names = std::collections::HashSet::new();

        for tool in &tools {
            assert!(
                names.insert(&tool.name),
                "Duplicate tool name found: {}",
                tool.name
            );
        }
    }

    #[test]
    fn test_tool_names() {
        let tools = ToolRegistry::get_all_tools();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();

        assert!(names.contains(&"get_graph_schema"));
        assert!(names.contains(&"find_nodes"));
        assert!(names.contains(&"traverse_relationships"));
        assert!(names.contains(&"explore_neighborhood"));
        assert!(names.contains(&"find_paths"));
        assert!(names.contains(&"aggregate_nodes"));
    }
}
