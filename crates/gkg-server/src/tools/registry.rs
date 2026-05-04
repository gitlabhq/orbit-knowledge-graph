use std::sync::Arc;

use ontology::Ontology;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

mod params {
    use serde_json::{Value, json};

    pub fn format() -> Value {
        json!({
            "type": "string",
            "enum": ["llm", "raw"],
            "description": "Output format. 'llm' (default) returns compact text optimized for AI. 'raw' returns structured JSON."
        })
    }

    pub fn query() -> Value {
        json!({
            "type": "object",
            "description": "Graph query following the DSL schema"
        })
    }

    pub fn expand_nodes() -> Value {
        json!({
            "type": "array",
            "items": { "type": "string" },
            "description": "Node types to expand with properties and relationships."
        })
    }

    pub fn include_response_format() -> Value {
        json!({
            "type": "boolean",
            "description": "When true, include the query response JSON Schema (the formatter output shape) alongside the ontology."
        })
    }
}

pub struct ToolRegistry;

impl ToolRegistry {
    pub fn get_all_tools(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![
            Self::query_graph(),
            Self::get_graph_schema(),
            Self::get_query_dsl(),
        ]
    }

    fn query_graph() -> ToolDefinition {
        ToolDefinition {
            name: "query_graph".into(),
            description: "Execute graph queries to find nodes, traverse relationships, \
                          explore neighborhoods, find paths, or aggregate data. \
                          Call get_query_dsl once per session for the query grammar. \
                          Call get_graph_schema for available entity types and relationships."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": params::query(),
                    "format": params::format()
                },
                "additionalProperties": false
            }),
        }
    }

    fn get_graph_schema() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_schema".into(),
            description: "List the GitLab Knowledge Graph schema. Returns the available nodes \
                          and edges with their source/target types. Use expand_nodes to get \
                          property details for specific types. Set include_response_format to \
                          also return the formatter output JSON Schema."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "expand_nodes": params::expand_nodes(),
                    "include_response_format": params::include_response_format(),
                    "format": params::format()
                },
                "additionalProperties": false
            }),
        }
    }

    fn get_query_dsl() -> ToolDefinition {
        ToolDefinition {
            name: "get_query_dsl".into(),
            description: "Return the query DSL grammar (JSON Schema) used by query_graph. \
                          Call this once per session before composing queries."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "format": params::format()
                },
                "additionalProperties": false
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_tools() -> Vec<ToolDefinition> {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        ToolRegistry::get_all_tools(&ontology)
    }

    fn find_tool(name: &str) -> ToolDefinition {
        all_tools()
            .into_iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("tool '{name}' not found"))
    }

    #[test]
    fn all_tools_have_valid_schemas() {
        let tools = all_tools();
        assert_eq!(tools.len(), 3);

        for tool in &tools {
            assert!(!tool.name.is_empty());
            assert!(!tool.description.is_empty());
            assert!(tool.parameters.is_object());
        }
    }

    #[test]
    fn tool_names_are_unique() {
        let tools = all_tools();
        let mut names = std::collections::HashSet::new();
        for tool in &tools {
            assert!(names.insert(&tool.name), "Duplicate tool: {}", tool.name);
        }
    }

    #[test]
    fn expected_tools_are_registered() {
        let names: Vec<String> = all_tools().into_iter().map(|t| t.name).collect();
        assert!(names.contains(&"query_graph".into()));
        assert!(names.contains(&"get_graph_schema".into()));
        assert!(names.contains(&"get_query_dsl".into()));
    }

    #[test]
    fn all_tools_have_format_parameter() {
        for tool in &all_tools() {
            let format = &tool.parameters["properties"]["format"];
            assert!(format.is_object(), "{} missing format parameter", tool.name);
            assert_eq!(format["type"], "string");

            let values: Vec<&str> = format["enum"]
                .as_array()
                .expect("format should have enum")
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            assert_eq!(values, vec!["llm", "raw"]);
        }
    }

    #[test]
    fn format_is_never_required() {
        for tool in &all_tools() {
            if let Some(required) = tool.parameters.get("required").and_then(|r| r.as_array()) {
                assert!(
                    !required.iter().any(|v| v == "format"),
                    "{} should not require format",
                    tool.name
                );
            }
        }
    }

    #[test]
    fn query_graph_requires_query_parameter() {
        let tool = find_tool("query_graph");
        let params = &tool.parameters;

        assert!(params["properties"]["query"].is_object());
        let required = params["required"].as_array().expect("should have required");
        assert!(required.iter().any(|v| v == "query"));
    }

    #[test]
    fn query_graph_description_points_to_discovery_tools() {
        let tool = find_tool("query_graph");
        assert!(tool.description.contains("get_query_dsl"));
        assert!(tool.description.contains("get_graph_schema"));
    }

    #[test]
    fn query_graph_description_does_not_embed_dsl() {
        // Issue #553: the DSL grammar must not be in the description because
        // some MCP clients silently truncate it. The description must stay
        // small and stable; the grammar lives behind get_query_dsl instead.
        let tool = find_tool("query_graph");
        assert!(
            !tool.description.contains("<toon>"),
            "query_graph description must not embed inline TOON"
        );
        assert!(
            tool.description.len() < 1024,
            "query_graph description should stay well under common MCP truncation limits, got {} bytes",
            tool.description.len()
        );
    }

    #[test]
    fn query_graph_excludes_ontology_data() {
        let tool = find_tool("query_graph");
        assert!(!tool.description.contains("username"));
        assert!(!tool.description.contains("AUTHORED"));
    }

    #[test]
    fn get_graph_schema_has_expand_nodes_param() {
        let tool = find_tool("get_graph_schema");
        assert!(tool.parameters["properties"]["expand_nodes"].is_object());
    }

    #[test]
    fn get_graph_schema_has_include_response_format_param() {
        let tool = find_tool("get_graph_schema");
        let prop = &tool.parameters["properties"]["include_response_format"];
        assert!(prop.is_object());
        assert_eq!(prop["type"], "boolean");
    }

    #[test]
    fn get_query_dsl_has_only_format_param() {
        let tool = find_tool("get_query_dsl");
        let props = tool.parameters["properties"]
            .as_object()
            .expect("properties should be an object");
        assert_eq!(props.len(), 1);
        assert!(props.contains_key("format"));
        assert!(tool.parameters.get("required").is_none());
    }
}
