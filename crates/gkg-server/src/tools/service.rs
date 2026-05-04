use std::sync::Arc;

use ontology::Ontology;
use ontology::introspection::{
    IntrospectionScope, SchemaDomain, SchemaEdge, SchemaResponse, build_schema_edges,
    build_schema_response,
};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use toon_format::{EncodeOptions, encode};

use super::schema::{condensed_query_schema, raw_query_schema};

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

impl ExecutorError {
    pub fn code(&self) -> String {
        match self {
            Self::NotFound(_) => "tool_not_found".to_string(),
            Self::InvalidArguments(_) => "invalid_arguments".to_string(),
        }
    }
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

#[derive(Debug, Clone)]
pub struct ToolService {
    ontology: Arc<Ontology>,
}

impl ToolService {
    pub fn new(ontology: Arc<Ontology>) -> Self {
        Self { ontology }
    }

    pub fn resolve(
        &self,
        tool_name: &str,
        arguments_json: &str,
    ) -> Result<ToolPlan, ExecutorError> {
        let arguments: Value = serde_json::from_str(arguments_json)
            .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;

        match tool_name {
            "query_graph" => self.resolve_query_graph(&arguments),
            "get_graph_schema" => self.execute_get_graph_schema(&arguments),
            "get_query_dsl" => self.execute_get_query_dsl(&arguments),
            _ => Err(ExecutorError::NotFound(tool_name.to_string())),
        }
    }

    pub fn build_schema_toon(&self, expand_nodes: &[String]) -> Result<String, ExecutorError> {
        let response = self.build_graph_schema_response(expand_nodes);
        let options = EncodeOptions::default();
        encode(&response, &options)
            .map_err(|e| ExecutorError::InvalidArguments(format!("Failed to encode as toon: {e}")))
    }

    /// TOON-encoded condensed query DSL grammar (issue #553).
    pub fn build_query_dsl_toon() -> Result<String, ExecutorError> {
        condensed_query_schema().map_err(ExecutorError::InvalidArguments)
    }

    /// Full query DSL JSON Schema as a JSON string (verbatim from disk).
    pub fn build_query_dsl_raw() -> &'static str {
        raw_query_schema()
    }

    /// JSON Schema describing the query response shape (formatter output).
    /// Returned verbatim from `crates/gkg-server/schemas/query_response.json`.
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
        let expand_nodes = args.expand_nodes.as_deref().unwrap_or(&[]);
        let response = self.build_graph_schema_response(expand_nodes);

        let result = match format {
            OutputFormat::Llm => {
                let mut toon = encode(&response, &EncodeOptions::default()).map_err(|e| {
                    ExecutorError::InvalidArguments(format!("Failed to encode as toon: {e}"))
                })?;
                if args.include_response_format {
                    toon.push_str("\n\nResponseFormat v");
                    toon.push_str(&Self::build_response_format_version());
                    toon.push_str(" (JSON Schema):\n");
                    toon.push_str(Self::build_response_format_schema());
                }
                json!(toon)
            }
            OutputFormat::Raw => {
                let mut value = serde_json::to_value(&response)
                    .map_err(|e| ExecutorError::InvalidArguments(e.to_string()))?;
                if args.include_response_format {
                    let response_format: Value = serde_json::from_str(
                        Self::build_response_format_schema(),
                    )
                    .map_err(|e| {
                        ExecutorError::InvalidArguments(format!(
                            "Failed to parse response format schema: {e}"
                        ))
                    })?;
                    if let Some(obj) = value.as_object_mut() {
                        obj.insert("response_format".to_string(), response_format);
                        obj.insert(
                            "response_format_version".to_string(),
                            Value::String(Self::build_response_format_version()),
                        );
                    }
                }
                value
            }
        };

        Ok(ToolPlan::Immediate { result })
    }

    fn execute_get_query_dsl(&self, arguments: &Value) -> Result<ToolPlan, ExecutorError> {
        let format = parse_format(arguments);

        let result = match format {
            OutputFormat::Llm => json!(Self::build_query_dsl_toon()?),
            OutputFormat::Raw => {
                serde_json::from_str(Self::build_query_dsl_raw()).map_err(|e| {
                    ExecutorError::InvalidArguments(format!("Failed to parse DSL schema: {e}"))
                })?
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

    pub fn build_edges(&self) -> Vec<SchemaEdge> {
        build_schema_edges(&self.ontology, IntrospectionScope::All)
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
    include_response_format: bool,
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
    fn test_edges_show_source_and_target_nodes() {
        let output = get_toon_output("{}");

        assert!(
            output.contains("from") && output.contains("to"),
            "Edges should have from/to fields"
        );
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
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.to_string().contains("missing 'query' field"));
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
    fn test_build_edges_returns_all_relationships() {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let service = ToolService::new(ontology);

        let edges = service.build_edges();
        assert!(!edges.is_empty());

        let authored = edges.iter().find(|e| e.name == "AUTHORED");
        assert!(authored.is_some(), "Should have AUTHORED edge");
        assert!(
            !authored.unwrap().from.is_empty(),
            "AUTHORED should have source types"
        );
        assert!(
            !authored.unwrap().to.is_empty(),
            "AUTHORED should have target types"
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

    fn resolve_immediate(args: &str, tool: &str) -> Value {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let service = ToolService::new(ontology);
        match service.resolve(tool, args).expect("Should resolve") {
            ToolPlan::Immediate { result } => result,
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn get_query_dsl_default_returns_toon_string() {
        let result = resolve_immediate("{}", "get_query_dsl");
        let toon = result.as_str().expect("LLM format returns a string");
        assert!(toon.contains("query_type"));
        assert!(toon.contains("traversal"));
        assert!(toon.contains("NodeSelector"));
    }

    #[test]
    fn get_query_dsl_raw_returns_full_json_schema() {
        let result = resolve_immediate(r#"{"format": "raw"}"#, "get_query_dsl");
        assert!(result.is_object(), "RAW format returns the JSON Schema");
        assert!(result.get("$schema").is_some());
        assert_eq!(
            result.get("title").and_then(Value::as_str),
            Some("GraphQueryAsJSON")
        );
    }

    #[test]
    fn get_query_dsl_raw_and_llm_describe_same_grammar() {
        let raw = resolve_immediate(r#"{"format": "raw"}"#, "get_query_dsl");
        let llm = resolve_immediate(r#"{"format": "llm"}"#, "get_query_dsl");

        // Both forms must mention the same core DSL surface; only the encoding differs.
        let raw_str = serde_json::to_string(&raw).unwrap();
        let llm_str = llm.as_str().unwrap();
        for token in [
            "query_type",
            "traversal",
            "aggregation",
            "path_finding",
            "neighbors",
            "NodeSelector",
        ] {
            assert!(raw_str.contains(token), "raw missing token: {token}");
            assert!(llm_str.contains(token), "llm missing token: {token}");
        }
    }

    #[test]
    fn get_graph_schema_with_include_response_format_raw() {
        let result = resolve_immediate(
            r#"{"format": "raw", "include_response_format": true}"#,
            "get_graph_schema",
        );
        let response_format = result
            .get("response_format")
            .expect("response_format key should be present when flag is set");
        assert!(response_format.is_object());
        assert_eq!(
            response_format.get("title").and_then(Value::as_str),
            Some("GKG unified query response")
        );

        let version = result
            .get("response_format_version")
            .and_then(Value::as_str)
            .expect("response_format_version should be present when flag is set");
        assert!(
            !version.is_empty() && version.chars().any(|c| c.is_ascii_digit()),
            "response_format_version should look like a semver, got {version:?}"
        );
        assert_eq!(
            version,
            ToolService::build_response_format_version(),
            "version should match RAW_OUTPUT_FORMAT_VERSION"
        );
    }

    #[test]
    fn get_graph_schema_without_include_response_format_omits_it() {
        let result = resolve_immediate(r#"{"format": "raw"}"#, "get_graph_schema");
        assert!(
            result.get("response_format").is_none(),
            "response_format must be absent unless include_response_format = true"
        );
        assert!(
            result.get("response_format_version").is_none(),
            "response_format_version must be absent unless include_response_format = true"
        );
    }

    #[test]
    fn get_graph_schema_llm_with_include_response_format_appends_section() {
        let result = resolve_immediate(
            r#"{"format": "llm", "include_response_format": true}"#,
            "get_graph_schema",
        );
        let toon = result.as_str().expect("LLM format returns a string");
        let version = ToolService::build_response_format_version();
        assert!(
            toon.contains(&format!("ResponseFormat v{version}")),
            "TOON should embed the response format version, got: ...{}",
            &toon[toon.len().saturating_sub(200)..]
        );
        assert!(toon.contains("GKG unified query response"));
    }
}
