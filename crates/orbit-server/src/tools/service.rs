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
pub enum ToolPlan {
    RunGraphQuery {
        query_json: String,
        format: OutputFormat,
    },
    GraphSchema {
        expand_nodes: Vec<String>,
        format: OutputFormat,
    },
    Immediate {
        result: Value,
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

    pub fn resolve(
        &self,
        tool_name: &str,
        arguments_json: &str,
    ) -> Result<ToolPlan, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        self.validate_arguments(tool_name, &arguments)?;

        match tool_name {
            "query_graph" => self.resolve_query_graph(&arguments),
            "get_graph_schema" => Self::resolve_graph_schema(&arguments),
            _ => Err(ExecutorError::NotFound(tool_name.to_string())),
        }
    }

    pub fn resolve_command(
        &self,
        command_name: &str,
        arguments_json: &str,
    ) -> Result<ToolPlan, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        self.validate_arguments(command_name, &arguments)?;

        match command_name {
            "query_graph" => Err(ExecutorError::InterceptedCommand(command_name.to_string())),
            "get_graph_schema" => Self::resolve_graph_schema(&arguments),
            "get_query_dsl" => self.execute_get_query_dsl(&arguments),
            "get_response_format" => self.execute_get_response_format(&arguments),
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

    fn resolve_query_graph(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let query = arguments
            .get("query")
            .ok_or_else(|| ExecutorError::InvalidArguments("missing 'query' field".to_string()))?;

        let query_json = serde_json::to_string(query)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        let format = parse_format(arguments);

        Ok(ToolPlan::RunGraphQuery { query_json, format })
    }

    fn resolve_graph_schema(arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let parameters: GetGraphSchemaArgs = serde_json::from_value(arguments.clone())
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        Ok(ToolPlan::GraphSchema {
            expand_nodes: parameters.resolve_expand_nodes(),
            format: parse_format(arguments),
        })
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

    fn execute_get_query_dsl(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let format = parse_format(arguments);
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

        Ok(ToolPlan::Immediate { result })
    }

    fn execute_get_response_format(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let format = parse_format(arguments);
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

        Ok(ToolPlan::Immediate { result })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_graph_preserves_the_query_and_format() {
        let service = ToolService::default();
        let query = json!({"match": {}});

        for (parameters, expected_format) in [
            (json!({"query": query}), OutputFormat::Llm),
            (json!({"query": query, "format": "raw"}), OutputFormat::Raw),
        ] {
            let ToolPlan::RunGraphQuery { query_json, format } = service
                .resolve("query_graph", &parameters.to_string())
                .unwrap()
            else {
                panic!("expected a graph query plan");
            };
            assert_eq!(serde_json::from_str::<Value>(&query_json).unwrap(), query);
            assert_eq!(format, expected_format);
        }
    }

    #[test]
    fn query_graph_requires_a_query() {
        let error = ToolService::default()
            .resolve("query_graph", r#"{"match":{}}"#)
            .unwrap_err();

        assert!(matches!(error, ExecutorError::InvalidArguments(_)));
        assert!(error.to_string().contains("query"));
        assert!(error.to_string().contains("list_commands"));
    }

    #[test]
    fn schema_plans_preserve_expansion_aliases_and_format() {
        let service = ToolService::default();

        for (parameters, expected_nodes, expected_format) in [
            (json!({}), vec![], OutputFormat::Llm),
            (json!({"format": "llm"}), vec![], OutputFormat::Llm),
            (json!({"format": "raw"}), vec![], OutputFormat::Raw),
            (
                json!({"expand_nodes": ["User"]}),
                vec!["User"],
                OutputFormat::Llm,
            ),
            (
                json!({"entity_types": ["MergeRequest"]}),
                vec!["MergeRequest"],
                OutputFormat::Llm,
            ),
            (
                json!({"expand_nodes": ["User"], "entity_types": ["User", "Project"]}),
                vec!["User", "Project"],
                OutputFormat::Llm,
            ),
            (json!({"expand_nodes": ["*"]}), vec!["*"], OutputFormat::Llm),
        ] {
            for plan in [
                service.resolve("get_graph_schema", &parameters.to_string()),
                service.resolve_command("get_graph_schema", &parameters.to_string()),
            ] {
                let ToolPlan::GraphSchema {
                    expand_nodes,
                    format,
                } = plan.unwrap()
                else {
                    panic!("expected a graph schema plan");
                };
                assert_eq!(expand_nodes, expected_nodes);
                assert_eq!(format, expected_format);
            }
        }
    }

    #[test]
    fn invalid_schema_arguments_include_discovery_help() {
        let service = ToolService::default();

        for (parameters, invalid_parameter) in [
            (r#"{"format":"raw","include":["dsl"]}"#, "include"),
            (r#"{"node_types":["Job"]}"#, "node_types"),
            (r#"{"expand_nodes":"User"}"#, "expand_nodes"),
        ] {
            for result in [
                service.resolve("get_graph_schema", parameters),
                service.resolve_command("get_graph_schema", parameters),
            ] {
                let error = result.unwrap_err();
                assert!(matches!(error, ExecutorError::InvalidArguments(_)));
                let message = error.to_string();
                for expected in [invalid_parameter, "expand_nodes", "format", "list_commands"] {
                    assert!(message.contains(expected), "{message}");
                }
            }
        }
    }

    #[test]
    fn unknown_tools_and_commands_are_not_found() {
        let service = ToolService::default();
        for result in [
            service.resolve("nonexistent_tool", "{}"),
            service.resolve_command("nonexistent_command", "{}"),
        ] {
            assert!(matches!(result, Err(ExecutorError::NotFound(_))));
        }
    }

    #[test]
    fn metadata_commands_include_their_versions() {
        let service = ToolService::default();
        for (command, expected_title, expected_version) in [
            (
                "get_query_dsl",
                "GraphQueryAsJSON",
                ToolService::build_query_dsl_version(),
            ),
            (
                "get_response_format",
                "Orbit unified query response",
                ToolService::build_response_format_version(),
            ),
        ] {
            let ToolPlan::Immediate { result } = service
                .resolve_command(command, r#"{"format":"raw"}"#)
                .unwrap()
            else {
                panic!("expected immediate metadata");
            };
            let schema = result.get("schema").unwrap_or(&result);
            assert_eq!(schema["title"], expected_title);
            assert_eq!(result["version"], expected_version);
        }
    }

    #[test]
    fn query_commands_require_rails_interception() {
        let result = ToolService::default().resolve_command("query_graph", r#"{"query":{}}"#);
        assert!(matches!(result, Err(ExecutorError::InterceptedCommand(_))));
    }
}
