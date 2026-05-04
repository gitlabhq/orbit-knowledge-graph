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

    pub fn graph_info_sections() -> Value {
        json!({
            "type": "array",
            "minItems": 1,
            "uniqueItems": true,
            "items": {
                "type": "string",
                "enum": ["schema", "dsl", "response_format", "status"]
            },
            "description": "Which discovery sections to return. Pick only what you need."
        })
    }

    pub fn schema_options() -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "description": "Only used when 'schema' is in sections.",
            "properties": {
                "expand_nodes": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string" },
                    "description": "Node names to expand with full property details. Use [\"*\"] to expand every node. Omit for a shape-only listing."
                }
            }
        })
    }

    pub fn status_target() -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "minProperties": 1,
            "maxProperties": 1,
            "description": "Required when 'status' is in sections. Provide exactly one of namespace_id, project_id, or full_path.",
            "properties": {
                "namespace_id": { "type": "integer", "minimum": 1 },
                "project_id":   { "type": "integer", "minimum": 1 },
                "full_path":    { "type": "string", "minLength": 1, "description": "e.g. 'gitlab-org/gitlab'" }
            }
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
            description: "Discovery tool for the GitLab Knowledge Graph. Call this BEFORE \
                          composing a query to learn the graph's shape, query language, response \
                          format, or indexing status. Returns any subset of four sections in one \
                          call.\n\n\
                          Sections (pass via `sections`, pick only what you need):\n\
                          - `schema`: ontology of node/edge types and domains. Add \
                          `schema_options.expand_nodes=[\"Node1\",...]` or `[\"*\"]` to include \
                          property details. Omit for a lightweight listing.\n\
                          - `dsl`: JSON Schema describing valid query inputs. No options.\n\
                          - `response_format`: JSON Schema + semver of the formatter's output. \
                          No options.\n\
                          - `status`: indexing progress and entity counts. REQUIRES \
                          `status_target` with exactly one of {namespace_id:int, project_id:int, \
                          full_path:string}.\n\n\
                          `format` controls output: \"llm\" (default, compact text) or \"raw\" \
                          (JSON). Errors if `status` is requested without `status_target`."
                .into(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["sections"],
                "properties": {
                    "sections": params::graph_info_sections(),
                    "schema_options": params::schema_options(),
                    "status_target": params::status_target(),
                    "format": params::format()
                },
                "allOf": [
                    {
                        "if":   { "properties": { "sections": { "contains": { "const": "status" } } }, "required": ["sections"] },
                        "then": { "required": ["status_target"] }
                    }
                ]
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
    fn get_graph_info_sections_enum_lists_all_four_blocks() {
        let tool = find_tool("get_graph_info");
        let sections = &tool.parameters["properties"]["sections"];
        assert_eq!(sections["type"], "array");

        let values: Vec<&str> = sections["items"]["enum"]
            .as_array()
            .expect("sections.items.enum must be an array")
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(values, vec!["schema", "dsl", "response_format", "status"]);
    }

    #[test]
    fn get_graph_info_requires_sections() {
        let tool = find_tool("get_graph_info");
        let required = tool.parameters["required"]
            .as_array()
            .expect("required should be an array");
        assert!(required.iter().any(|v| v == "sections"));
    }

    #[test]
    fn get_graph_info_status_target_is_oneof() {
        let tool = find_tool("get_graph_info");
        let status_target = &tool.parameters["properties"]["status_target"];
        assert_eq!(status_target["type"], "object");
        assert_eq!(status_target["minProperties"], 1);
        assert_eq!(status_target["maxProperties"], 1);
        let props = status_target["properties"]
            .as_object()
            .expect("status_target.properties is an object");
        for key in ["namespace_id", "project_id", "full_path"] {
            assert!(props.contains_key(key), "missing status_target key: {key}");
        }
    }

    #[test]
    fn get_graph_info_status_implies_status_target_required() {
        // The if/then in the schema requires status_target when sections includes status.
        let tool = find_tool("get_graph_info");
        let all_of = tool.parameters["allOf"]
            .as_array()
            .expect("allOf should be an array");
        assert!(
            !all_of.is_empty(),
            "allOf must encode the status -> status_target rule"
        );
    }
}
