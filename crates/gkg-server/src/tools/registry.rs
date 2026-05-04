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

    pub fn schema_include() -> Value {
        json!({
            "type": "array",
            "items": { "type": "string", "enum": ["dsl", "response_format"] },
            "description": "Extra blocks to merge into the schema response. \
                            'dsl' adds the query DSL grammar (input shape for query_graph). \
                            'response_format' adds the formatter output JSON Schema and its semver. \
                            Composes any subset in a single call to keep tool counts down."
        })
    }
}

pub struct ToolRegistry;

impl ToolRegistry {
    pub fn get_all_tools(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![
            Self::query_graph(),
            Self::get_graph_schema(),
            Self::get_graph_info(),
        ]
    }

    fn query_graph() -> ToolDefinition {
        // Keep the inline TOON for now so existing MCP clients that already
        // depend on it keep working. The new `get_query_dsl` tool exposes
        // the same grammar through a dedicated call; a follow-up MR will
        // strip the inline schema once the new tool has been adopted.
        let base_description = "Execute graph queries to find nodes, traverse relationships, \
                                explore neighborhoods, find paths, or aggregate data. \
                                Use get_query_dsl for the query grammar. \
                                Use get_graph_schema to discover available entity types and relationships.";

        let description = match condensed_query_schema() {
            Ok(schema) => format!(
                "{}\n\nQuery DSL Schema:\n<toon>\n{}\n</toon>",
                base_description, schema
            ),
            Err(_) => base_description.to_string(),
        };

        ToolDefinition {
            name: "query_graph".into(),
            description,
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
                          property details for specific types."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "expand_nodes": params::expand_nodes(),
                    "format": params::format()
                },
                "additionalProperties": false
            }),
        }
    }

    fn get_graph_info() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_info".into(),
            description: "One-stop discovery tool. Returns the graph ontology and, on demand, \
                          the query DSL grammar and the formatter output JSON Schema in the \
                          same response. Pass include=[\"dsl\", \"response_format\"] to merge \
                          either or both into the result. Prefer this over get_graph_schema \
                          for new agents; get_graph_schema stays for back-compat."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "expand_nodes": params::expand_nodes(),
                    "include": params::schema_include(),
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
        assert!(names.contains(&"get_graph_info".into()));
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
    fn query_graph_description_still_embeds_dsl_for_back_compat() {
        // Issue #553 added `get_query_dsl` so MCP clients that truncate
        // descriptions can still find the grammar. We keep the inline TOON
        // in the description for one release cycle so existing consumers
        // do not break. A follow-up MR strips it once adoption is verified.
        let tool = find_tool("query_graph");
        assert!(tool.description.contains("query_type"));
        assert!(tool.description.contains("traversal"));
        assert!(tool.description.contains("<toon>"));
    }

    #[test]
    fn query_graph_excludes_ontology_data() {
        // The embedded DSL describes query shape, not ontology entities.
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
    fn get_graph_schema_has_no_include_param() {
        // back-compat: clients on get_graph_schema must keep the old shape
        let tool = find_tool("get_graph_schema");
        let props = tool.parameters["properties"]
            .as_object()
            .expect("properties should be an object");
        assert!(!props.contains_key("include"));
        assert!(props.contains_key("expand_nodes"));
        assert!(props.contains_key("format"));
    }

    #[test]
    fn get_graph_info_include_param_lists_known_blocks() {
        let tool = find_tool("get_graph_info");
        let include = &tool.parameters["properties"]["include"];
        assert_eq!(include["type"], "array");

        let values: Vec<&str> = include["items"]["enum"]
            .as_array()
            .expect("include.items.enum must be an array")
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(values, vec!["dsl", "response_format"]);
    }
}
