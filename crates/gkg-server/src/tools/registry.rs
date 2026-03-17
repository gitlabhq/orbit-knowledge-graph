use std::sync::Arc;

use ontology::Ontology;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::schema::condensed_query_schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

pub struct ToolRegistry;

impl ToolRegistry {
    pub fn get_all_tools(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![Self::query_graph(), Self::get_graph_schema()]
    }

    fn query_graph() -> ToolDefinition {
        let base_description = "Execute graph queries to find nodes, traverse relationships, \
                                explore neighborhoods, find paths, or aggregate data. \
                                Use get_graph_schema to discover available entity types and relationships.";

        let description = match condensed_query_schema() {
            Ok(schema) => format!(
                "{}\n\nQuery DSL Schema:\n<toon>\n{}\n</toon>",
                base_description, schema
            ),
            Err(_) => base_description.to_string(),
        };

        ToolDefinition {
            name: "query_graph".to_string(),
            description,
            parameters: json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {
                        "type": "object",
                        "description": "Graph query following the DSL schema"
                    }
                },
                "additionalProperties": false
            }),
        }
    }

    fn get_graph_schema() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_schema".to_string(),
            description: "List the GitLab Knowledge Graph schema. Returns the available nodes \
                          and edges with their source/target types. Use expand_nodes to get \
                          property details for specific types, or pass [\"*\"] to expand all."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "expand_nodes": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Node types to expand with properties and relationships. Pass [\"*\"] to expand all nodes."
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

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("Failed to load ontology"))
    }

    #[test]
    fn test_all_tools_have_valid_schemas() {
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
        assert_eq!(tools.len(), 2, "Should have exactly 2 tools");

        for tool in &tools {
            assert!(!tool.name.is_empty());
            assert!(!tool.description.is_empty());
            assert!(tool.parameters.is_object());
        }
    }

    #[test]
    fn test_tool_names_are_unique() {
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
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
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();

        assert!(names.contains(&"query_graph"));
        assert!(names.contains(&"get_graph_schema"));
    }

    #[test]
    fn test_get_graph_schema_has_expand_nodes_param() {
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
        let get_schema = tools
            .iter()
            .find(|t| t.name == "get_graph_schema")
            .expect("get_graph_schema tool should exist");

        let params = &get_schema.parameters;
        assert_eq!(params["type"], "object");
        assert!(params["properties"]["expand_nodes"].is_object());
    }

    #[test]
    fn test_query_graph_has_query_parameter() {
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
        let query_graph = tools
            .iter()
            .find(|t| t.name == "query_graph")
            .expect("query_graph tool should exist");

        let params = &query_graph.parameters;
        assert_eq!(params["type"], "object");
        assert!(params["properties"]["query"].is_object());

        let required = params["required"].as_array().expect("Should have required");
        assert!(
            required.iter().any(|v| v == "query"),
            "query should be required"
        );
    }

    #[test]
    fn test_query_graph_description_contains_schema() {
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
        let query_graph = tools
            .iter()
            .find(|t| t.name == "query_graph")
            .expect("query_graph tool should exist");

        let desc = &query_graph.description;
        assert!(
            desc.contains("query_type"),
            "Description should contain query_type"
        );
        assert!(
            desc.contains("traversal"),
            "Description should contain traversal"
        );
        assert!(
            desc.contains("get_graph_schema"),
            "Description should reference get_graph_schema for entity discovery"
        );
    }

    #[test]
    fn test_query_graph_excludes_ontology_data() {
        let ontology = test_ontology();
        let tools = ToolRegistry::get_all_tools(&ontology);
        let query_graph = tools
            .iter()
            .find(|t| t.name == "query_graph")
            .expect("query_graph tool should exist");

        let desc = &query_graph.description;
        assert!(
            !desc.contains("username"),
            "Description should not contain entity-specific fields"
        );
        assert!(
            !desc.contains("AUTHORED"),
            "Description should not contain relationship types (use get_graph_schema)"
        );
    }
}
