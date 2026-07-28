use std::sync::Arc;

use ontology::Ontology;
use serde_json::json;

use super::prompt;
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
            description: prompt("tools/query_graph_v2").description().into(),
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
            description: prompt("tools/get_graph_schema_v2").description().into(),
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
