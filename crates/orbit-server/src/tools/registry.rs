use query_engine::compiler::Frontend;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::prompt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

pub(super) fn command_summaries(frontend: Frontend) -> Vec<(&'static str, &'static str)> {
    let query_prompt = match frontend {
        Frontend::JsonDsl => "tools/query_graph",
        Frontend::Gql => "tools/query_graph_gql",
    };
    let mut commands = vec![
        ("query_graph", prompt(query_prompt).summary()),
        (
            "get_graph_schema",
            prompt("tools/get_graph_schema").summary(),
        ),
        ("get_query_dsl", prompt("tools/get_query_dsl").summary()),
        (
            "get_response_format",
            prompt("tools/get_response_format").summary(),
        ),
    ];
    if frontend == Frontend::Gql {
        commands.retain(|(name, _)| *name != "get_query_dsl");
    }
    commands
}

fn render_prompt(key: &str, context: minijinja::Value) -> String {
    let mut environment = minijinja::Environment::new();
    environment.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
    environment
        .render_str(prompt(key).description(), context)
        .expect("template placeholders are validated against the prompt file at build time")
}

pub(super) fn list_commands_description(frontend: Frontend) -> String {
    let commands = command_summaries(frontend)
        .iter()
        .map(|(name, summary)| format!("- {name}: {summary}"))
        .collect::<Vec<_>>()
        .join("\n");

    render_prompt(
        "list_commands",
        minijinja::context! { commands, catalog => "", relationships => "" },
    )
}

/// Catalog entry shape shared by the TOON `list_commands` response and the
/// inlined description. The input schema is named `input_schema` so it is not
/// confused with `invoke_command`'s own `parameters` argument.
#[derive(Serialize)]
pub(super) struct CommandCatalogEntry {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

impl From<&ToolDefinition> for CommandCatalogEntry {
    fn from(command: &ToolDefinition) -> Self {
        Self {
            name: command.name.clone(),
            description: command.description.clone(),
            input_schema: command.parameters.clone(),
        }
    }
}

/// Description for callers that cannot afford a discovery turn: the full
/// command catalog is inlined as compact JSON, followed by the graph
/// relationship patterns when given.
pub(super) fn inline_list_commands_description(
    frontend: Frontend,
    relationships: Option<&str>,
) -> String {
    let entries: Vec<CommandCatalogEntry> = CommandRegistry::commands_for(frontend)
        .iter()
        .map(CommandCatalogEntry::from)
        .collect();
    let catalog = serde_json::to_string(&entries).expect("command definitions serialize to JSON");
    let relationships = relationships.unwrap_or_default();

    render_prompt(
        "list_commands",
        minijinja::context! { commands => "", catalog, relationships },
    )
}

pub(super) mod params {
    use query_engine::compiler::Frontend;
    use serde_json::{Value, json};

    pub fn format() -> Value {
        json!({
            "type": "string",
            "enum": ["llm", "raw"],
            "description": "Output format. 'llm' (default) returns compact text optimized for AI. 'raw' returns structured JSON."
        })
    }

    pub fn query_parameters(frontend: Frontend) -> Value {
        let query = match frontend {
            Frontend::JsonDsl => json!({"type": "object", "description": "JSON Query DSL object."}),
            Frontend::Gql => json!({"type": "string", "description": "Read-only GQL query text."}),
        };
        json!({
            "type": "object",
            "required": ["query"],
            "properties": {"query": query, "format": format()},
            "additionalProperties": false
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
    pub fn get_all_tools() -> Vec<ToolDefinition> {
        Self::tools_for(Frontend::JsonDsl)
    }

    pub fn tools_for(frontend: Frontend) -> Vec<ToolDefinition> {
        Self::tools_with_catalog(frontend, false, None)
    }

    /// When `inline_catalog` is set, `list_commands` carries the full command
    /// catalog, plus the graph relationship patterns when given, in its
    /// description so the caller can skip the discovery and schema turns.
    pub fn tools_with_catalog(
        frontend: Frontend,
        inline_catalog: bool,
        relationships: Option<&str>,
    ) -> Vec<ToolDefinition> {
        vec![
            Self::list_commands(frontend, inline_catalog, relationships),
            Self::invoke_command(),
        ]
    }

    fn list_commands(
        frontend: Frontend,
        inline_catalog: bool,
        relationships: Option<&str>,
    ) -> ToolDefinition {
        let description = if inline_catalog {
            inline_list_commands_description(frontend, relationships)
        } else {
            list_commands_description(frontend)
        };
        ToolDefinition {
            name: "list_commands".into(),
            description,
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
            description: prompt("invoke_command").description().into(),
            parameters: json!({
                "type": "object",
                "required": ["command_name"],
                "properties": {
                    "command_name": {
                        "type": "string",
                        "description": "Command name from the Orbit command catalog."
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
    pub fn get_all_commands() -> Vec<ToolDefinition> {
        Self::commands_for(Frontend::JsonDsl)
    }

    pub fn commands_for(frontend: Frontend) -> Vec<ToolDefinition> {
        let mut commands = vec![Self::query_graph(frontend), Self::get_graph_schema()];
        if frontend == Frontend::JsonDsl {
            commands.push(Self::get_query_dsl());
        }
        commands.push(Self::get_response_format());
        commands
    }

    fn query_graph(frontend: Frontend) -> ToolDefinition {
        let prompt_key = match frontend {
            Frontend::JsonDsl => "tools/query_graph",
            Frontend::Gql => "tools/query_graph_gql",
        };
        ToolDefinition {
            name: "query_graph".into(),
            description: prompt(prompt_key).description().into(),
            parameters: params::query_parameters(frontend),
        }
    }

    fn get_graph_schema() -> ToolDefinition {
        ToolDefinition {
            name: "get_graph_schema".into(),
            description: prompt("tools/get_graph_schema").description().into(),
            parameters: params::get_graph_schema_parameters(),
        }
    }

    fn get_query_dsl() -> ToolDefinition {
        ToolDefinition {
            name: "get_query_dsl".into(),
            description: prompt("tools/get_query_dsl").description().into(),
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
            description: prompt("tools/get_response_format").description().into(),
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
    use ontology::Ontology;
    use ontology::introspection::{IntrospectionScope, build_relationship_patterns};

    use super::*;

    fn all_tools() -> Vec<ToolDefinition> {
        ToolRegistry::get_all_tools()
    }

    fn all_commands() -> Vec<ToolDefinition> {
        CommandRegistry::get_all_commands()
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
        assert_eq!(tools.len(), 2);

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
        assert_eq!(names, ["list_commands", "invoke_command"]);
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
                command_summaries(Frontend::JsonDsl)
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
            .filter(|tool| tool.name == "list_commands")
        {
            for (name, summary) in command_summaries(Frontend::JsonDsl) {
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
    fn descriptions_are_bounded_and_carry_no_schema() {
        for frontend in [Frontend::JsonDsl, Frontend::Gql] {
            for definition in ToolRegistry::tools_for(frontend)
                .into_iter()
                .chain(CommandRegistry::commands_for(frontend))
            {
                assert!(
                    !definition.description.is_empty(),
                    "{frontend:?}: {} missing description",
                    definition.name
                );
                if definition.name != "list_commands" {
                    let budget = match (frontend, definition.name.as_str()) {
                        (Frontend::Gql, "query_graph") => 512,
                        _ => 400,
                    };
                    assert!(
                        definition.description.len() < budget,
                        "{frontend:?}: {} description exceeds {budget} bytes",
                        definition.name
                    );
                }
                assert!(
                    !definition.description.contains("<toon>")
                        && !definition.description.contains("Query DSL Schema"),
                    "{frontend:?}: {} should keep large schemas out of the description",
                    definition.name
                );
            }
        }
    }

    fn relationship_patterns() -> String {
        let ontology = Ontology::load_embedded().expect("embedded ontology loads");
        build_relationship_patterns(&ontology, IntrospectionScope::All).join("\n")
    }

    fn inline_description(frontend: Frontend) -> String {
        inline_description_with(frontend, Some(&relationship_patterns()))
    }

    fn inline_description_with(frontend: Frontend, relationships: Option<&str>) -> String {
        ToolRegistry::tools_with_catalog(frontend, true, relationships)
            .into_iter()
            .find(|tool| tool.name == "list_commands")
            .expect("list_commands tool")
            .description
    }

    // Tripping this budget usually means the ontology gained edges. Either
    // raise the limits after checking the inlined description is still worth
    // its tokens, or shorten the pattern format in `build_relationship_patterns`.
    #[test]
    fn inlined_description_stays_under_size_budget() {
        for frontend in [Frontend::JsonDsl, Frontend::Gql] {
            let catalog = inline_description_with(frontend, None).len();
            assert!(
                catalog < 4096,
                "{frontend:?} inlined catalog is {catalog} B"
            );
            let full = inline_description(frontend).len();
            assert!(
                full < 8192,
                "{frontend:?} inlined catalog with relationships is {full} B"
            );
        }
    }

    #[test]
    fn inlined_description_carries_relationship_patterns() {
        let patterns = relationship_patterns();
        for frontend in [Frontend::JsonDsl, Frontend::Gql] {
            let description = inline_description(frontend);
            assert!(description.contains(&patterns));
            assert!(description.contains("(User)-[:AUTHORED]->("));
            assert!(!inline_description_with(frontend, None).contains("Graph relationships"));
        }
    }

    #[test]
    fn inlined_description_carries_every_command_name_and_schema() {
        for frontend in [Frontend::JsonDsl, Frontend::Gql] {
            let description = inline_description(frontend);
            for command in CommandRegistry::commands_for(frontend) {
                let entry = serde_json::to_string(&CommandCatalogEntry::from(&command)).unwrap();
                assert!(
                    description.contains(&entry),
                    "{frontend:?} missing catalog entry for {}",
                    command.name
                );
            }
            assert!(!description.contains("Call this before invoke_command"));
            assert!(!description.contains("\"parameters\":"));
        }
    }

    #[test]
    fn default_tools_do_not_inline_the_catalog() {
        let description = ToolRegistry::tools_for(Frontend::JsonDsl)
            .into_iter()
            .find(|tool| tool.name == "list_commands")
            .unwrap()
            .description;
        assert!(description.contains("Call this before invoke_command"));
        assert!(
            !description.contains("\"inputSchema\"") && !description.contains("\"parameters\"")
        );
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
    fn commands_advertise_llm_and_raw_formats_in_every_mode() {
        for frontend in [Frontend::JsonDsl, Frontend::Gql] {
            for command in CommandRegistry::commands_for(frontend) {
                assert_eq!(
                    command.parameters["properties"]["format"]["enum"],
                    json!(["llm", "raw"])
                );
            }
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
    fn gql_commands_use_text_queries_without_the_json_dsl() {
        let commands = CommandRegistry::commands_for(Frontend::Gql);
        assert!(
            !commands
                .iter()
                .any(|command| command.name == "get_query_dsl")
        );
        assert_eq!(
            commands[0].parameters["properties"]["query"]["type"],
            "string"
        );
        assert!(commands[0].description.contains("CALL db.schema()"));
    }

    #[test]
    fn query_graph_excludes_ontology_data() {
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
