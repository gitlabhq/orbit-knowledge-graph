use std::collections::HashMap;
use std::sync::Arc;

use jsonschema::Validator;
use ontology::Ontology;
use ontology::introspection::{
    IntrospectionScope, SchemaDomain, SchemaResponse, build_schema_response,
};
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
    ontology: Arc<Ontology>,
    schemas: Arc<HashMap<String, CommandSchema>>,
}

impl ToolService {
    pub fn new(ontology: Arc<Ontology>) -> Self {
        let definitions = V2CommandRegistry::get_all_commands(&ontology)
            .into_iter()
            .chain(V2ToolRegistry::get_all_tools(&ontology));

        let mut schemas = HashMap::new();
        for definition in definitions {
            schemas
                .entry(definition.name.clone())
                .or_insert_with(|| CommandSchema::compile(&definition));
        }

        Self {
            ontology,
            schemas: Arc::new(schemas),
        }
    }

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
            "get_graph_schema" => self.execute_get_graph_schema(&arguments),
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
            "get_graph_schema" => self.execute_get_graph_schema(&arguments),
            "get_query_dsl" => self.execute_get_query_dsl(&arguments),
            "get_response_format" => self.execute_get_response_format(&arguments),
            _ => Err(ExecutorError::NotFound(command_name.to_string())),
        }
    }

    pub fn build_schema_toon(&self, expand_nodes: &[String]) -> Result<String, ExecutorError> {
        let response = self.build_graph_schema_response(expand_nodes);
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

    fn execute_get_graph_schema(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let args: GetGraphSchemaArgs = serde_json::from_value(arguments.clone())
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        let format = parse_format(arguments);
        let expand_nodes = args.resolve_expand_nodes();
        let response = self.build_graph_schema_response(&expand_nodes);

        let result = match format {
            OutputFormat::Llm => {
                let toon = encode(&response, &EncodeOptions::default()).map_err(|e| {
                    ExecutorError::InvalidArguments(format!("Failed to encode as toon: {e}"))
                })?;
                json!(toon)
            }
            OutputFormat::Raw => serde_json::to_value(&response)
                .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?,
        };

        Ok(ToolPlan::Immediate { result })
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

    fn build_graph_schema_response(&self, expand_nodes: &[String]) -> SchemaResponse {
        build_schema_response(&self.ontology, IntrospectionScope::All, expand_nodes)
    }

    pub fn build_domains(&self, expand_nodes: &[String]) -> Vec<SchemaDomain> {
        self.build_graph_schema_response(expand_nodes).domains
    }

    pub fn build_edges(&self) -> Vec<String> {
        self.build_graph_schema_response(&[]).edges
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
    use ontology::introspection::SchemaNode;

    fn get_toon_output(args: &str) -> String {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("get_graph_schema", args)
            .expect("Should resolve");

        match plan {
            ToolPlan::Immediate { result } => result.as_str().unwrap().to_string(),
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn test_base_call_contains_domains_and_edges() {
        let output = get_toon_output("{}");

        assert!(output.contains("domains"), "Missing domains section");
        assert!(output.contains("edges"), "Missing edges section");
    }

    #[test]
    fn test_base_call_contains_known_domains() {
        let output = get_toon_output("{}");

        assert!(output.contains("core"), "Missing core domain");
        assert!(output.contains("plan"), "Missing plan domain");
        assert!(output.contains("ci"), "Missing ci domain");
    }

    #[test]
    fn test_base_call_contains_known_nodes() {
        let output = get_toon_output("{}");

        assert!(output.contains("User"), "Missing User node");
        assert!(output.contains("Project"), "Missing Project node");
        assert!(output.contains("MergeRequest"), "Missing MergeRequest node");
        assert!(output.contains("WorkItem"), "Missing WorkItem node");
    }

    #[test]
    fn test_base_call_contains_known_edges() {
        let output = get_toon_output("{}");

        assert!(output.contains("AUTHORED"), "Missing AUTHORED edge");
        assert!(output.contains("CONTAINS"), "Missing CONTAINS edge");
    }

    #[test]
    fn test_edges_are_name_only() {
        let output = get_toon_output("{}");

        assert!(output.contains("AUTHORED"), "Missing AUTHORED edge name");
        assert!(output.contains("CONTAINS"), "Missing CONTAINS edge name");
    }

    #[test]
    fn test_expand_nodes_shows_properties() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        assert!(output.contains("props"), "Expanded node should have props");
        assert!(
            output.contains("username"),
            "User should have username property"
        );
        assert!(output.contains("id"), "User should have id property");
    }

    #[test]
    fn test_entity_types_alias_shows_properties() {
        let output = get_toon_output(r#"{"entity_types": ["User"]}"#);

        assert!(output.contains("props"), "entity_types should expand props");
        assert!(
            output.contains("username"),
            "User should have username property via entity_types: {output}"
        );
    }

    #[test]
    fn test_entity_types_and_expand_nodes_union() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"], "entity_types": ["Project"]}"#);

        assert!(
            output.contains("username"),
            "User should be expanded from expand_nodes"
        );
        assert!(
            output.contains("Project,{") || output.contains("path"),
            "Project should be expanded from entity_types: {output}"
        );
    }

    #[test]
    fn resolve_command_accepts_entity_types() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);

        assert!(
            service
                .resolve_command("get_graph_schema", r#"{"entity_types": ["MergeRequest"]}"#)
                .is_ok(),
            "entity_types alias should be a valid parameter"
        );
    }

    #[test]
    fn test_expand_nodes_shows_relationships() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        assert!(
            output.contains("out") || output.contains("in"),
            "Expanded node should have relationship info"
        );
    }

    #[test]
    fn test_property_format_has_type() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        assert!(
            output.contains("id:int") || output.contains("id:integer"),
            "Properties should include type: {}",
            output
        );
    }

    #[test]
    fn test_unexpanded_nodes_are_compact() {
        let output = get_toon_output(r#"{"expand_nodes": ["User"]}"#);

        let project_in_output = output.contains("Project");
        assert!(project_in_output, "Project should be in output");

        let project_props = output.contains("Project") && output.contains("Project,{");
        assert!(
            !project_props || output.contains("User,{"),
            "Only expanded nodes should have properties block"
        );
    }

    #[test]
    fn test_output_is_not_json() {
        let output = get_toon_output("{}");

        assert!(
            !output.starts_with('{'),
            "Output should be TOON format, not JSON"
        );
    }

    #[test]
    fn test_query_graph_returns_run_graph_query_plan() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("query_graph", r#"{"query":{"match":{}}}"#)
            .expect("Should resolve");

        match plan {
            ToolPlan::RunGraphQuery { query_json, format } => {
                assert!(query_json.contains("match"));
                assert_eq!(format, OutputFormat::Llm);
            }
            _ => panic!("Expected RunGraphQuery plan"),
        }
    }

    #[test]
    fn test_query_graph_requires_query_field() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let result = service.resolve("query_graph", r#"{"match":{}}"#);
        assert!(matches!(result, Err(ExecutorError::InvalidArguments(_))));

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("query"),
            "error should name the required param: {err}"
        );
        assert!(
            err.contains("list_commands"),
            "error should point at discovery: {err}"
        );
    }

    #[test]
    fn test_build_schema_toon_returns_string() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let toon = service.build_schema_toon(&[]).expect("Should succeed");
        assert!(toon.contains("domains"));
        assert!(toon.contains("edges"));
    }

    #[test]
    fn test_build_schema_toon_with_expand() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let toon = service
            .build_schema_toon(&["User".to_string()])
            .expect("Should succeed");
        assert!(toon.contains("username"));
        assert!(toon.contains("props"));
    }

    #[test]
    fn test_build_domains_groups_nodes_by_domain() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let domains = service.build_domains(&[]);
        assert!(!domains.is_empty());

        let core = domains.iter().find(|d| d.name == "core");
        assert!(core.is_some(), "Should have a core domain");
    }

    #[test]
    fn test_build_edges_returns_edge_names() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let edges = service.build_edges();
        assert!(!edges.is_empty());
        assert!(
            edges.iter().any(|e| e == "AUTHORED"),
            "Should have AUTHORED edge"
        );
    }

    #[test]
    fn test_resolve_unknown_tool_returns_not_found() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let result = service.resolve("nonexistent_tool", "{}");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("nonexistent_tool"));
    }

    #[test]
    fn test_build_schema_toon_with_unknown_expand_node() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let toon = service
            .build_schema_toon(&["FakeNode".to_string()])
            .expect("Should succeed without error");
        assert!(toon.contains("domains"), "Should still return valid schema");
    }

    #[test]
    fn get_graph_schema_raw_format_returns_json() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("get_graph_schema", r#"{"format": "raw"}"#)
            .expect("Should resolve");

        match plan {
            ToolPlan::Immediate { result } => {
                assert!(result.is_object(), "Raw format should return a JSON object");
                assert!(result.get("domains").is_some(), "Should have domains key");
                assert!(result.get("edges").is_some(), "Should have edges key");
            }
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn get_graph_schema_llm_format_returns_toon() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("get_graph_schema", r#"{"format": "llm"}"#)
            .expect("Should resolve");

        match plan {
            ToolPlan::Immediate { result } => {
                assert!(result.is_string(), "LLM format should return a TOON string");
                let text = result.as_str().unwrap();
                assert!(text.contains("domains"));
            }
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn get_graph_schema_default_format_is_llm() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("get_graph_schema", r#"{}"#)
            .expect("Should resolve");

        match plan {
            ToolPlan::Immediate { result } => {
                assert!(result.is_string(), "Default format should be TOON string");
            }
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn query_graph_raw_format_is_carried_in_plan() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let plan = service
            .resolve("query_graph", r#"{"query":{"match":{}}, "format": "raw"}"#)
            .expect("Should resolve");

        match plan {
            ToolPlan::RunGraphQuery { format, .. } => {
                assert_eq!(format, OutputFormat::Raw);
            }
            _ => panic!("Expected RunGraphQuery plan"),
        }
    }

    #[test]
    fn test_expand_all_wildcard() {
        let output = get_toon_output(r#"{"expand_nodes": ["*"]}"#);

        assert!(output.contains("props"), "Wildcard should expand nodes");
        assert!(output.contains("username"), "User should be expanded");
    }

    #[test]
    fn test_build_domains_wildcard_expands_all() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let domains = service.build_domains(&["*".to_string()]);

        for domain in &domains {
            for node in &domain.nodes {
                assert!(
                    matches!(node, SchemaNode::Expanded { .. }),
                    "All nodes should be expanded with wildcard"
                );
            }
        }
    }

    fn resolve_command_immediate(args: &str, command: &str) -> Value {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);
        match service
            .resolve_command(command, args)
            .expect("Should resolve command")
        {
            ToolPlan::Immediate { result } => result,
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn get_graph_schema_rejects_unknown_parameter() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);
        let result = service.resolve(
            "get_graph_schema",
            r#"{"format": "raw", "include": ["dsl"]}"#,
        );

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("include"),
            "error should name the bad param: {err}"
        );
        assert!(
            err.contains("expand_nodes") && err.contains("format"),
            "error should list valid params: {err}"
        );
    }

    #[test]
    fn resolve_command_rejects_hallucinated_parameter() {
        // Regression: an agent invoked get_graph_schema with a non-existent
        // `node_types` parameter and silently received the full schema.
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);
        let result = service.resolve_command("get_graph_schema", r#"{"node_types": ["Job"]}"#);

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("node_types"),
            "error should name the bad param: {err}"
        );
        assert!(
            err.contains("expand_nodes") && err.contains("format"),
            "error should list valid params: {err}"
        );
        assert!(
            err.contains("list_commands"),
            "error should point at discovery: {err}"
        );
    }

    #[test]
    fn resolve_command_rejects_wrong_argument_type() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);
        let result = service.resolve_command("get_graph_schema", r#"{"expand_nodes": "User"}"#);

        assert!(matches!(result, Err(ExecutorError::InvalidArguments(_))));
    }

    #[test]
    fn valid_arguments_still_resolve() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);

        for args in [
            r#"{}"#,
            r#"{"expand_nodes": ["User"]}"#,
            r#"{"format": "raw"}"#,
        ] {
            assert!(
                service.resolve_command("get_graph_schema", args).is_ok(),
                "valid args should resolve: {args}"
            );
        }
    }

    #[test]
    fn every_advertised_schema_compiles() {
        // Guards the `expect` in CommandSchema::compile: constructing the
        // service compiles every advertised command and tool schema, so a
        // malformed schema fails this test instead of panicking in production.
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);

        for name in [
            "query_graph",
            "get_graph_schema",
            "get_query_dsl",
            "get_response_format",
            "list_commands",
            "invoke_command",
        ] {
            assert!(
                service.schemas.contains_key(name),
                "{name} should have a compiled schema"
            );
        }
    }

    #[test]
    fn resolve_command_executes_get_query_dsl() {
        let result = resolve_command_immediate(r#"{"format": "raw"}"#, "get_query_dsl");
        assert_eq!(
            result.get("title").and_then(Value::as_str),
            Some("GraphQueryAsJSON")
        );
        assert_eq!(
            result.get("version").and_then(Value::as_str),
            Some(ToolService::build_query_dsl_version().as_str())
        );
    }

    #[test]
    fn resolve_command_executes_get_response_format() {
        let result = resolve_command_immediate(r#"{"format": "raw"}"#, "get_response_format");
        assert_eq!(
            result
                .get("schema")
                .and_then(|s| s.get("title"))
                .and_then(Value::as_str),
            Some("GKG unified query response")
        );
        assert_eq!(
            result.get("version").and_then(Value::as_str),
            Some(ToolService::build_response_format_version().as_str())
        );
    }

    #[test]
    fn resolve_command_rejects_rails_intercepted_commands() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);

        let result = service.resolve_command("query_graph", r#"{"query": {}}"#);
        assert!(matches!(result, Err(ExecutorError::InterceptedCommand(_))));
    }
}
