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

    pub fn status_targets() -> Value {
        json!({
            "type": "array",
            "minItems": 1,
            "maxItems": 50,
            "description": "One or more targets to fetch indexing status for. Each target must specify exactly one of namespace_id, project_id, or full_path.",
            "items": {
                "type": "object",
                "additionalProperties": false,
                "minProperties": 1,
                "maxProperties": 1,
                "properties": {
                    "namespace_id": { "type": "integer", "minimum": 1 },
                    "project_id":   { "type": "integer", "minimum": 1 },
                    "full_path":    { "type": "string", "minLength": 1, "description": "e.g. 'gitlab-org/gitlab'" }
                }
            }
        })
    }

    pub fn command_names() -> Value {
        json!({
            "type": "array",
            "items": { "type": "string" },
            "description": "Optional command names to describe. Omit to list every command."
        })
    }

    pub fn command_parameters() -> Value {
        json!({
            "type": "object",
            "description": "Optional downstream command input object. Put the target command inputs here, not alongside command_name."
        })
    }
}

pub struct ToolRegistry;

impl ToolRegistry {
    pub fn get_all_tools(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![
            Self::query_graph(),
            Self::get_graph_schema(),
            Self::get_graph_status(),
            Self::list_commands(),
            Self::invoke_command(),
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

    fn get_graph_status() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_status".into(),
            description: "Indexing progress and entity counts for one or more namespaces or \
                          projects. Omit `targets` to check all Knowledge Graph enabled root \
                          namespaces the current user can access. Otherwise pass `targets` as an \
                          array; each target supplies exactly one of `namespace_id` (int), \
                          `project_id` (int), or `full_path` (string like \"gitlab-org/gitlab\")."
                .into(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "targets": params::status_targets(),
                    "format": params::format()
                }
            }),
        }
    }

    fn list_commands() -> ToolDefinition {
        ToolDefinition {
            name: "list_commands".into(),
            description:
                "List Orbit Knowledge Graph commands with descriptions and input schemas. \
                          Use this before invoke_command to discover available commands."
                    .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command_names": params::command_names()
                },
                "additionalProperties": false
            }),
        }
    }

    fn invoke_command() -> ToolDefinition {
        ToolDefinition {
            name: "invoke_command".into(),
            description: "Execute an Orbit command. This is a wrapper tool: keep only command_name \
                          and parameters at the top level, and put downstream command inputs inside \
                          parameters."
                .into(),
            parameters: json!({
                "type": "object",
                "required": ["command_name"],
                "properties": {
                    "command_name": {
                        "type": "string",
                        "description": "Command name returned by list_commands."
                    },
                    "parameters": params::command_parameters()
                },
                "additionalProperties": false
            }),
        }
    }
}

pub struct CommandRegistry;

impl CommandRegistry {
    pub fn get_all_commands(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![
            ToolRegistry::query_graph(),
            ToolRegistry::get_graph_schema(),
            ToolRegistry::get_graph_status(),
            Self::get_query_dsl(),
            Self::get_response_format(),
        ]
    }

    fn get_query_dsl() -> ToolDefinition {
        ToolDefinition {
            name: "get_query_dsl".into(),
            description: "Return the query_graph JSON DSL grammar and version. Use this before \
                          composing query_graph parameters."
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

    fn get_response_format() -> ToolDefinition {
        ToolDefinition {
            name: "get_response_format".into(),
            description: "Return the JSON Schema and version for query_graph responses.".into(),
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

    fn all_commands() -> Vec<ToolDefinition> {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        CommandRegistry::get_all_commands(&ontology)
    }

    fn find_tool(name: &str) -> ToolDefinition {
        all_tools()
            .into_iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("tool '{name}' not found"))
    }

    fn find_command(name: &str) -> ToolDefinition {
        all_commands()
            .into_iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("command '{name}' not found"))
    }

    #[test]
    fn all_tools_have_valid_schemas() {
        let tools = all_tools();
        assert_eq!(tools.len(), 5);

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
        assert!(names.contains(&"get_graph_status".into()));
        assert!(names.contains(&"list_commands".into()));
        assert!(names.contains(&"invoke_command".into()));
    }

    #[test]
    fn expected_commands_are_registered() {
        let names: Vec<String> = all_commands().into_iter().map(|t| t.name).collect();
        assert!(names.contains(&"query_graph".into()));
        assert!(names.contains(&"get_graph_schema".into()));
        assert!(names.contains(&"get_graph_status".into()));
        assert!(names.contains(&"get_query_dsl".into()));
        assert!(names.contains(&"get_response_format".into()));
    }

    #[test]
    fn all_tools_have_format_parameter() {
        for tool in &all_tools() {
            if ["list_commands", "invoke_command"].contains(&tool.name.as_str()) {
                continue;
            }

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
        let tool = find_command("query_graph");
        let params = &tool.parameters;

        assert!(params["properties"]["query"].is_object());
        let required = params["required"].as_array().expect("should have required");
        assert!(required.iter().any(|v| v == "query"));
    }

    #[test]
    fn query_graph_description_points_to_discovery_tools() {
        let tool = find_command("query_graph");
        assert!(tool.description.contains("get_query_dsl"));
        assert!(tool.description.contains("get_graph_schema"));
    }

    #[test]
    fn query_graph_description_still_embeds_dsl_for_back_compat() {
        // Issue #553 added `get_query_dsl` so MCP clients that truncate
        // descriptions can still find the grammar. We keep the inline TOON
        // in the description for one release cycle so existing consumers
        // do not break. A follow-up MR strips it once adoption is verified.
        let tool = find_command("query_graph");
        assert!(tool.description.contains("query_type"));
        assert!(tool.description.contains("traversal"));
        assert!(tool.description.contains("<toon>"));
    }

    #[test]
    fn query_graph_excludes_ontology_data() {
        // The embedded DSL describes query shape, not ontology entities.
        let tool = find_command("query_graph");
        assert!(!tool.description.contains("username"));
        assert!(!tool.description.contains("AUTHORED"));
    }

    #[test]
    fn get_graph_schema_has_expand_nodes_param() {
        let tool = find_command("get_graph_schema");
        assert!(tool.parameters["properties"]["expand_nodes"].is_object());
    }

    #[test]
    fn get_graph_schema_has_no_include_param() {
        // back-compat: clients on get_graph_schema must keep the old shape
        let tool = find_command("get_graph_schema");
        let props = tool.parameters["properties"]
            .as_object()
            .expect("properties should be an object");
        assert!(!props.contains_key("include"));
        assert!(props.contains_key("expand_nodes"));
        assert!(props.contains_key("format"));
    }

    #[test]
    fn get_graph_status_accepts_optional_target_array() {
        let tool = find_command("get_graph_status");
        assert!(tool.parameters.get("required").is_none());

        let targets = &tool.parameters["properties"]["targets"];
        assert_eq!(targets["type"], "array");
        assert_eq!(targets["minItems"], 1);

        let item = &targets["items"];
        assert_eq!(item["type"], "object");
        assert_eq!(item["minProperties"], 1);
        assert_eq!(item["maxProperties"], 1);
        let props = item["properties"]
            .as_object()
            .expect("target item properties is an object");
        for key in ["namespace_id", "project_id", "full_path"] {
            assert!(props.contains_key(key), "missing target key: {key}");
        }
    }

    #[test]
    fn list_commands_accepts_optional_command_names() {
        let tool = find_tool("list_commands");
        let command_names = &tool.parameters["properties"]["command_names"];
        assert_eq!(command_names["type"], "array");
    }

    #[test]
    fn invoke_command_requires_command_name() {
        let tool = find_tool("invoke_command");
        let params = &tool.parameters;

        assert!(params["properties"]["command_name"].is_object());
        assert!(params["properties"]["parameters"].is_object());
        let required = params["required"].as_array().expect("should have required");
        assert!(required.iter().any(|v| v == "command_name"));
    }
}
