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

pub(super) const COMMAND_SUMMARIES: [(&str, &str); 4] = [
    (
        "query_graph",
        "Execute permission-aware graph queries over GitLab SDLC and code entities.",
    ),
    (
        "get_graph_schema",
        "Return graph nodes, edges, and expanded node properties.",
    ),
    (
        "get_query_dsl",
        "Return the query_graph JSON DSL grammar and version.",
    ),
    (
        "get_response_format",
        "Return the query_graph response JSON Schema and version.",
    ),
];

pub(super) fn list_commands_description() -> String {
    let commands = COMMAND_SUMMARIES
        .iter()
        .map(|(name, summary)| format!("- {name}: {summary}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "List Orbit Knowledge Graph commands with descriptions and input schemas. \
         Use this before invoke_command to discover available command details.\n\n\
         Available commands:\n{commands}"
    )
}

pub(super) mod params {
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
            "description": "Entity types to expand with their properties and relationships. Pass the names you intend to query (e.g. [\"MergeRequest\", \"User\"]) to get their filterable fields and types."
        })
    }

    pub fn entity_types() -> Value {
        json!({
            "type": "array",
            "items": { "type": "string" },
            "description": "Alias for expand_nodes. Entity types to expand with their properties and relationships."
        })
    }

    pub fn get_graph_schema_parameters() -> Value {
        json!({
            "type": "object",
            "properties": {
                "expand_nodes": expand_nodes(),
                "entity_types": entity_types(),
                "format": format()
            },
            "additionalProperties": false
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
            Self::list_commands(),
            Self::invoke_command(),
        ]
    }

    pub(super) fn query_graph() -> ToolDefinition {
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

    pub(super) fn get_graph_schema() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_schema".into(),
            description: "List the GitLab Knowledge Graph schema. Returns the available nodes \
                          and edges with their source/target types. Pass expand_nodes (or its \
                          alias entity_types) with specific type names to get their filterable \
                          properties and types before composing a query_graph call."
                .into(),
            parameters: params::get_graph_schema_parameters(),
        }
    }

    fn list_commands() -> ToolDefinition {
        ToolDefinition {
            name: "list_commands".into(),
            description: list_commands_description(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command_names": params::command_names(),
                    "format": params::format()
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
    use crate::tools::{V2CommandRegistry, V2ToolRegistry};

    fn all_tools() -> Vec<ToolDefinition> {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        ToolRegistry::get_all_tools(&ontology)
    }

    fn all_commands() -> Vec<ToolDefinition> {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        V2CommandRegistry::get_all_commands(&ontology)
    }

    fn all_v2_tools() -> Vec<ToolDefinition> {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        V2ToolRegistry::get_all_tools(&ontology)
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
        assert_eq!(tools.len(), 4);

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
        assert!(names.contains(&"list_commands".into()));
        assert!(names.contains(&"invoke_command".into()));
    }

    #[test]
    fn expected_v2_tools_are_registered() {
        let names: Vec<String> = all_v2_tools().into_iter().map(|t| t.name).collect();
        assert!(names.contains(&"query_graph".into()));
        assert!(names.contains(&"get_graph_schema".into()));
        assert!(names.contains(&"list_commands".into()));
        assert!(names.contains(&"invoke_command".into()));
    }

    #[test]
    fn expected_commands_are_registered() {
        let names: Vec<String> = all_commands().into_iter().map(|t| t.name).collect();
        assert!(names.contains(&"query_graph".into()));
        assert!(names.contains(&"get_graph_schema".into()));
        assert!(names.contains(&"get_query_dsl".into()));
        assert!(names.contains(&"get_response_format".into()));
    }

    #[test]
    fn command_summary_mapping_matches_registered_commands() {
        for command in all_commands() {
            assert!(
                COMMAND_SUMMARIES
                    .iter()
                    .any(|(name, _summary)| *name == command.name),
                "{} missing from command summaries",
                command.name
            );
        }
    }

    #[test]
    fn list_commands_description_includes_command_summaries() {
        for tool in all_tools()
            .into_iter()
            .chain(all_v2_tools())
            .filter(|tool| tool.name == "list_commands")
        {
            for (name, summary) in COMMAND_SUMMARIES {
                assert!(
                    tool.description.contains(name),
                    "{} missing command name {name}",
                    tool.name
                );
                assert!(
                    tool.description.contains(summary),
                    "{} missing command summary for {name}",
                    tool.name
                );
            }
        }
    }

    #[test]
    fn all_commands_have_short_descriptions() {
        for command in &all_commands() {
            assert!(
                !command.description.is_empty(),
                "{} missing description",
                command.name
            );
            assert!(
                command.description.len() < 400,
                "{} command description is too long",
                command.name
            );
            assert!(
                !command.description.contains("<toon>")
                    && !command.description.contains("Query DSL Schema"),
                "{} should keep large schemas out of the command description",
                command.name
            );
        }
    }

    #[test]
    fn all_tools_have_format_parameter() {
        for tool in &all_tools() {
            if tool.name == "invoke_command" {
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
        let tool = find_tool("query_graph");
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
    fn get_graph_schema_advertises_entity_types_alias() {
        let tool = find_command("get_graph_schema");
        assert!(
            tool.parameters["properties"]["entity_types"].is_object(),
            "entity_types alias should be advertised so agents that reach for it self-correct"
        );
    }

    #[test]
    fn list_commands_accepts_optional_command_names() {
        let tool = find_tool("list_commands");
        let command_names = &tool.parameters["properties"]["command_names"];
        assert_eq!(command_names["type"], "array");
    }

    #[test]
    fn list_commands_accepts_optional_format() {
        for tool in all_tools()
            .into_iter()
            .chain(all_v2_tools())
            .filter(|tool| tool.name == "list_commands")
        {
            let format = &tool.parameters["properties"]["format"];
            assert_eq!(format["type"], "string");
            assert_eq!(format["enum"], json!(["llm", "raw"]));
        }
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
