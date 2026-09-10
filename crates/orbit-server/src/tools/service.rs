use std::collections::HashMap;
use std::sync::Arc;

use jsonschema::Validator;
use ontology::Ontology;
use ontology::introspection::{IntrospectionScope, build_schema_response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use toon_format::{EncodeOptions, encode};

use super::registry::ToolDefinition;
use super::schema::{condensed_query_schema, query_dsl_version, raw_query_schema};
use super::{V2CommandRegistry, V2ToolRegistry};

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Command is handled by Rails interceptor: {0}")]
    InterceptedCommand(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Llm,
    Raw,
}

impl OutputFormat {
    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "raw" => Self::Raw,
            _ => Self::Llm,
        }
    }
}

#[derive(Debug)]
pub(crate) enum AgentCommand {
    GraphSchema {
        expand_nodes: Vec<String>,
        format: OutputFormat,
    },
    QueryLanguage {
        format: OutputFormat,
    },
    ResponseFormat {
        format: OutputFormat,
    },
}

struct CommandSchema {
    validator: Validator,
    /// Carried so a validation error can name the valid parameters.
    property_names: Vec<String>,
}

// `jsonschema::Validator` is not `Debug`, so derive it manually to keep
// `ToolService` (a public type) `Debug`.
impl std::fmt::Debug for CommandSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSchema")
            .field("property_names", &self.property_names)
            .finish_non_exhaustive()
    }
}

impl CommandSchema {
    fn compile(definition: &ToolDefinition) -> Self {
        let validator = jsonschema::validator_for(&definition.parameters)
            .expect("advertised command schema must compile");

        let property_names = definition.parameters["properties"]
            .as_object()
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default();

        Self {
            validator,
            property_names,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolService {
    schemas: Arc<HashMap<String, CommandSchema>>,
}

impl Default for ToolService {
    fn default() -> Self {
        let definitions = V2CommandRegistry::get_all_commands()
            .into_iter()
            .chain(V2ToolRegistry::get_all_tools());

        let mut schemas = HashMap::new();
        for definition in definitions {
            schemas
                .entry(definition.name.clone())
                .or_insert_with(|| CommandSchema::compile(&definition));
        }

        Self {
            schemas: Arc::new(schemas),
        }
    }
}

impl ToolService {
    /// The error lists the valid parameter names so an agent that passed an
    /// unknown one (e.g. a hallucinated `node_types`) can self-correct without
    /// first calling `list_commands`.
    fn validate_arguments(
        &self,
        command_name: &str,
        arguments: &Value,
    ) -> Result<(), ExecutorError> {
        let Some(schema) = self.schemas.get(command_name) else {
            return Ok(());
        };

        let errors: Vec<String> = schema
            .validator
            .iter_errors(arguments)
            .map(|error| error.to_string())
            .collect();

        if errors.is_empty() {
            return Ok(());
        }

        let valid_parameters = if schema.property_names.is_empty() {
            "none".to_string()
        } else {
            schema.property_names.join(", ")
        };

        Err(ExecutorError::InvalidArguments(format!(
            "`{command_name}` rejected the given parameters: {}. \
             Valid parameters: {valid_parameters}. Call list_commands for the full schema.",
            errors.join("; "),
        )))
    }

    pub(crate) fn parse_command(
        &self,
        command_name: &str,
        arguments_json: &str,
    ) -> Result<AgentCommand, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        self.validate_arguments(command_name, &arguments)?;
        let format = parse_format(&arguments);

        match command_name {
            "query_graph" => Err(ExecutorError::InterceptedCommand(command_name.to_string())),
            "get_graph_schema" => {
                let parameters: GetGraphSchemaArgs = serde_json::from_value(arguments)
                    .map_err(|error| ExecutorError::InvalidArguments(error.to_string()))?;
                Ok(AgentCommand::GraphSchema {
                    expand_nodes: parameters.resolve_expand_nodes(),
                    format,
                })
            }
            "get_query_dsl" => Ok(AgentCommand::QueryLanguage { format }),
            "get_response_format" => Ok(AgentCommand::ResponseFormat { format }),
            _ => Err(ExecutorError::NotFound(command_name.to_string())),
        }
    }

    pub fn build_schema_toon(
        ontology: &Ontology,
        expand_nodes: &[String],
    ) -> Result<String, ExecutorError> {
        let response = build_schema_response(ontology, IntrospectionScope::All, expand_nodes);
        let options = EncodeOptions::default();
        encode(&response, &options)
            .map_err(|e| ExecutorError::InvalidArguments(format!("Failed to encode as toon: {e}")))
    }

    pub fn build_command_catalog_toon(
        commands: &[ToolDefinition],
    ) -> Result<String, ExecutorError> {
        #[derive(Serialize)]
        struct CommandCatalogToon {
            commands: Vec<CommandToon>,
        }

        #[derive(Serialize)]
        struct CommandToon {
            name: String,
            description: String,
            input_schema: Value,
        }

        let catalog = CommandCatalogToon {
            commands: commands
                .iter()
                .map(|command| CommandToon {
                    name: command.name.clone(),
                    description: command.description.clone(),
                    input_schema: command.parameters.clone(),
                })
                .collect(),
        };

        encode(&catalog, &EncodeOptions::default()).map_err(|e| {
            ExecutorError::InvalidArguments(format!(
                "Failed to encode command catalog as toon: {e}"
            ))
        })
    }

    /// TOON-encoded condensed query DSL grammar (issue #553).
    pub fn build_query_dsl_toon() -> Result<String, ExecutorError> {
        let version = Self::build_query_dsl_version();
        let schema = condensed_query_schema().map_err(ExecutorError::InvalidArguments)?;
        Ok(format!("QueryDSL v{version}:\n{schema}"))
    }

    /// Full query DSL JSON Schema as a JSON string (verbatim from disk).
    pub fn build_query_dsl_raw() -> &'static str {
        raw_query_schema()
    }

    /// Semver string for the query DSL grammar. Matches `config/QUERY_DSL_VERSION`.
    pub fn build_query_dsl_version() -> String {
        query_dsl_version()
    }

    /// JSON Schema describing the query response shape (formatter output).
    /// Returned verbatim from `config/schemas/query_response.json`.
    pub fn build_response_format_schema() -> &'static str {
        super::schema::query_response_schema()
    }

    /// Semver string for the response format. Matches `config/RAW_OUTPUT_FORMAT_VERSION`
    /// and the `format_version` field stamped on every query response.
    pub fn build_response_format_version() -> String {
        query_engine::formatters::RAW_OUTPUT_FORMAT_VERSION.to_string()
    }

    pub fn render_graph_schema(
        ontology: &Ontology,
        expand_nodes: &[String],
        format: OutputFormat,
    ) -> Result<Value, ExecutorError> {
        match format {
            OutputFormat::Llm => Self::build_schema_toon(ontology, expand_nodes).map(Value::String),
            OutputFormat::Raw => {
                let response =
                    build_schema_response(ontology, IntrospectionScope::All, expand_nodes);
                serde_json::to_value(response)
                    .map_err(|error| ExecutorError::InvalidArguments(error.to_string()))
            }
        }
    }

    pub(crate) fn render_query_language(format: OutputFormat) -> Result<Value, ExecutorError> {
        let result = match format {
            OutputFormat::Llm => json!(Self::build_query_dsl_toon()?),
            OutputFormat::Raw => {
                let mut schema: Value =
                    serde_json::from_str(Self::build_query_dsl_raw()).map_err(|e| {
                        ExecutorError::InvalidArguments(format!("Failed to parse DSL schema: {e}"))
                    })?;
                if let Value::Object(ref mut object) = schema {
                    object.insert(
                        "version".to_string(),
                        Value::String(Self::build_query_dsl_version()),
                    );
                }
                schema
            }
        };

        Ok(result)
    }

    pub(crate) fn render_response_format(format: OutputFormat) -> Result<Value, ExecutorError> {
        let version = Self::build_response_format_version();
        let schema = Self::build_response_format_schema();
        let result = match format {
            OutputFormat::Llm => json!(format!(
                "ResponseFormat v{version} (JSON Schema):\n{schema}"
            )),
            OutputFormat::Raw => {
                let parsed_schema: Value = serde_json::from_str(schema).map_err(|e| {
                    ExecutorError::InvalidArguments(format!(
                        "Failed to parse response format schema: {e}"
                    ))
                })?;
                json!({
                    "schema": parsed_schema,
                    "version": version,
                })
            }
        };

        Ok(result)
    }
}

fn parse_format(arguments: &Value) -> OutputFormat {
    arguments
        .get("format")
        .and_then(|v| v.as_str())
        .map(OutputFormat::from_str_lossy)
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct GetGraphSchemaArgs {
    #[serde(default)]
    expand_nodes: Option<Vec<String>>,
    #[serde(default)]
    entity_types: Option<Vec<String>>,
}

impl GetGraphSchemaArgs {
    fn resolve_expand_nodes(self) -> Vec<String> {
        let mut nodes = self.expand_nodes.unwrap_or_default();
        if let Some(entity_types) = self.entity_types {
            for node in entity_types {
                if !nodes.contains(&node) {
                    nodes.push(node);
                }
            }
        }
        nodes
    }
}
