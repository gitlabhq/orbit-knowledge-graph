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
        vec![Self::query_graph(), Self::get_graph_entities()]
    }

    fn query_graph() -> ToolDefinition {
        ToolDefinition {
            name: "query_graph".to_string(),
            description: "Execute graph queries to find nodes, traverse relationships, \
                          explore neighborhoods, find paths, or aggregate data."
                .to_string(),
            parameters: json!({}),
        }
    }

    fn get_graph_entities() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_entities".to_string(),
            description: "List the GitLab Knowledge Graph schema. Returns the available nodes \
                          and edges with their source/target types. Use expand_nodes to get \
                          property details for specific types."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "expand_nodes": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Node types to expand with properties and relationships."
                    }
                },
                "additionalProperties": false
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
        assert_eq!(tools.len(), 2, "Should have exactly 2 tools");

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

        assert!(names.contains(&"query_graph"));
        assert!(names.contains(&"get_graph_entities"));
    }

    #[test]
    fn test_get_graph_entities_has_expand_nodes_param() {
        let tools = ToolRegistry::get_all_tools();
        let get_entities = tools
            .iter()
            .find(|t| t.name == "get_graph_entities")
            .expect("get_graph_entities tool should exist");

        let params = &get_entities.parameters;
        assert_eq!(params["type"], "object");
        assert!(params["properties"]["expand_nodes"].is_object());
    }
}
