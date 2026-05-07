use std::sync::Arc;

use ontology::Ontology;
use serde_json::json;

use super::registry::{ToolDefinition, ToolRegistry, list_commands_description, params};

pub struct V2ToolRegistry;

impl V2ToolRegistry {
    pub fn get_all_tools(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![
            ToolRegistry::query_graph(),
            ToolRegistry::get_graph_schema(),
            Self::list_commands(),
            Self::invoke_command(),
        ]
    }

    fn list_commands() -> ToolDefinition {
        ToolDefinition {
            name: "list_commands".into(),
            description: list_commands_description(),
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

pub struct V2CommandRegistry;

impl V2CommandRegistry {
    pub fn get_all_commands(_ontology: &Arc<Ontology>) -> Vec<ToolDefinition> {
        vec![
            Self::query_graph(),
            Self::get_graph_schema(),
            Self::get_query_dsl(),
            Self::get_response_format(),
        ]
    }

    fn query_graph() -> ToolDefinition {
        ToolDefinition {
            name: "query_graph".into(),
            description: "Execute a graph query. Before composing a query, call get_query_dsl \
                          for the DSL and get_graph_schema for the node and edge names relevant \
                          to the user's question."
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
            description: "Return the graph schema. Use expand_nodes for the node types relevant \
                          to the user's question to include properties and relationships."
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
