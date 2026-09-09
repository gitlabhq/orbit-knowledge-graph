use super::*;
use serde_json::Value;

async fn command_response(
    command_name: &str,
    parameters_json: &str,
) -> Result<InvokeAgentCommandResponse, Status> {
    test_service()
        .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
            command_name: command_name.into(),
            parameters_json: parameters_json.into(),
        }))
        .await
        .map(Response::into_inner)
}

async fn graph_schema_text(parameters_json: &str) -> String {
    let response = command_response("get_graph_schema", parameters_json)
        .await
        .unwrap();
    let Some(invoke_agent_command_response::Content::FormattedText(text)) = response.content else {
        panic!("expected formatted schema text");
    };
    text
}

async fn command_json(command_name: &str, parameters_json: &str) -> Value {
    let response = command_response(command_name, parameters_json)
        .await
        .unwrap();
    let Some(invoke_agent_command_response::Content::ResultJson(encoded)) = response.content else {
        panic!("expected JSON command response");
    };
    serde_json::from_str(&encoded).unwrap()
}

#[tokio::test]
async fn graph_schema_raw_format_returns_domains_and_edge_names() {
    let result = command_json("get_graph_schema", r#"{"format":"raw"}"#).await;
    let domains = result["domains"].as_array().unwrap();
    assert!(!domains.is_empty());
    assert!(domains.iter().any(|domain| domain["name"] == "core"));

    let edges = result["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(edges.iter().all(Value::is_string));
    assert!(edges.iter().any(|edge| edge == "AUTHORED"));
}

#[tokio::test]
async fn graph_schema_defaults_to_formatted_text() {
    for parameters in ["", " ", "{}", r#"{"format":"llm"}"#] {
        let text = graph_schema_text(parameters).await;
        assert!(text.contains("domains"));
        assert!(text.contains("edges"));
    }
}

#[tokio::test]
async fn graph_schema_ignores_unknown_expansion_nodes() {
    let text = graph_schema_text(r#"{"expand_nodes":["FakeNode"]}"#).await;
    assert!(text.contains("domains"));
}

#[tokio::test]
async fn graph_schema_wildcard_expands_every_node() {
    let result = command_json(
        "get_graph_schema",
        r#"{"format":"raw","expand_nodes":["*"]}"#,
    )
    .await;
    let domains = result["domains"].as_array().unwrap();
    assert!(!domains.is_empty());
    for domain in domains {
        for node in domain["nodes"].as_array().unwrap() {
            assert!(node.is_object(), "wildcard must expand every node: {node}");
        }
    }
}

#[tokio::test]
async fn graph_schema_rejects_invalid_parameters_with_discovery_guidance() {
    for (parameters, invalid_parameter) in [
        (r#"{"format":"raw","include":["dsl"]}"#, "include"),
        (r#"{"node_types":["Job"]}"#, "node_types"),
        (r#"{"expand_nodes":"User"}"#, "expand_nodes"),
    ] {
        let error = command_response("get_graph_schema", parameters)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains(invalid_parameter));
        assert!(error.message().contains("expand_nodes"));
        assert!(error.message().contains("format"));
        assert!(error.message().contains("list_commands"));
    }
}

#[tokio::test]
async fn intercepted_query_commands_still_validate_arguments() {
    let error = command_response("query_graph", r#"{"match":{}}"#)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("query"));
    assert!(error.message().contains("list_commands"));
}

#[tokio::test]
async fn unknown_commands_return_not_found() {
    let error = command_response("nonexistent_command", "{}")
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    assert!(error.message().contains("nonexistent_command"));
}

#[tokio::test]
async fn commands_reject_malformed_json() {
    let error = command_response("get_graph_schema", "{").await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn advertised_tools_and_commands_have_valid_parameter_schemas() {
    let service = test_service();
    let tools = service
        .list_tools(authed_request(ListToolsRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let commands = service
        .list_agent_commands(authed_request(ListAgentCommandsRequest::default()))
        .await
        .unwrap()
        .into_inner();

    for definition in tools.tools.into_iter().chain(commands.commands) {
        let schema: Value = serde_json::from_str(&definition.parameters_json_schema).unwrap();
        assert!(
            jsonschema::validator_for(&schema).is_ok(),
            "{}",
            definition.name
        );
    }
}

#[tokio::test]
async fn query_language_command_returns_the_versioned_schema() {
    let result = command_json("get_query_dsl", r#"{"format":"raw"}"#).await;
    assert_eq!(result["title"], "GraphQueryAsJSON");
    assert_eq!(result["version"], ToolService::build_query_dsl_version());
}

#[tokio::test]
async fn response_format_command_returns_the_versioned_schema() {
    let result = command_json("get_response_format", r#"{"format":"raw"}"#).await;
    assert_eq!(result["schema"]["title"], "Orbit unified query response");
    assert_eq!(
        result["version"],
        ToolService::build_response_format_version()
    );

    let response = command_response("get_response_format", "{}").await.unwrap();
    let Some(invoke_agent_command_response::Content::FormattedText(text)) = response.content else {
        panic!("expected formatted response schema");
    };
    assert!(text.starts_with(&format!(
        "ResponseFormat v{} (JSON Schema):\n",
        ToolService::build_response_format_version()
    )));
    assert!(text.contains("Orbit unified query response"));
}

#[tokio::test]
async fn test_base_call_contains_known_domains() {
    let output = graph_schema_text("{}").await;

    assert!(output.contains("core"), "Missing core domain");
    assert!(output.contains("plan"), "Missing plan domain");
    assert!(output.contains("ci"), "Missing ci domain");
}

#[tokio::test]
async fn test_base_call_contains_known_nodes() {
    let output = graph_schema_text("{}").await;

    assert!(output.contains("User"), "Missing User node");
    assert!(output.contains("Project"), "Missing Project node");
    assert!(output.contains("MergeRequest"), "Missing MergeRequest node");
    assert!(output.contains("WorkItem"), "Missing WorkItem node");
}

#[tokio::test]
async fn test_base_call_contains_known_edges() {
    let output = graph_schema_text("{}").await;

    assert!(output.contains("AUTHORED"), "Missing AUTHORED edge");
    assert!(output.contains("CONTAINS"), "Missing CONTAINS edge");
}

#[tokio::test]
async fn test_expand_nodes_shows_properties() {
    let output = graph_schema_text(r#"{"expand_nodes": ["User"]}"#).await;

    assert!(output.contains("props"), "Expanded node should have props");
    assert!(
        output.contains("username"),
        "User should have username property"
    );
    assert!(output.contains("id"), "User should have id property");
}

#[tokio::test]
async fn test_entity_types_alias_shows_properties() {
    let output = graph_schema_text(r#"{"entity_types": ["User"]}"#).await;

    assert!(output.contains("props"), "entity_types should expand props");
    assert!(
        output.contains("username"),
        "User should have username property via entity_types: {output}"
    );
}

#[tokio::test]
async fn test_entity_types_and_expand_nodes_union() {
    let output =
        graph_schema_text(r#"{"expand_nodes": ["User"], "entity_types": ["Project"]}"#).await;

    assert!(
        output.contains("username"),
        "User should be expanded from expand_nodes"
    );
    assert!(
        output.contains("Project,{") || output.contains("path"),
        "Project should be expanded from entity_types: {output}"
    );
}

#[tokio::test]
async fn test_expand_nodes_shows_relationships() {
    let output = graph_schema_text(r#"{"expand_nodes": ["User"]}"#).await;

    assert!(
        output.contains("out") || output.contains("in"),
        "Expanded node should have relationship info"
    );
}

#[tokio::test]
async fn test_property_format_has_type() {
    let output = graph_schema_text(r#"{"expand_nodes": ["User"]}"#).await;

    assert!(
        output.contains("id:int") || output.contains("id:integer"),
        "Properties should include type: {}",
        output
    );
}

#[tokio::test]
async fn graph_schema_keeps_unexpanded_nodes_compact() {
    let result = command_json(
        "get_graph_schema",
        r#"{"format":"raw","expand_nodes":["User"]}"#,
    )
    .await;
    let nodes: Vec<&Value> = result["domains"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|domain| domain["nodes"].as_array().unwrap())
        .collect();

    assert!(nodes.iter().any(|node| node.as_str() == Some("Project")));
    let expanded: Vec<_> = nodes.iter().filter(|node| node.is_object()).collect();
    assert_eq!(expanded.len(), 1);
    assert_eq!(expanded[0]["name"], "User");
    assert!(!expanded[0]["props"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_output_is_not_json() {
    let output = graph_schema_text("{}").await;

    assert!(
        !output.starts_with('{'),
        "Output should be TOON format, not JSON"
    );
}

#[tokio::test]
async fn test_expand_all_wildcard() {
    let output = graph_schema_text(r#"{"expand_nodes": ["*"]}"#).await;

    assert!(output.contains("props"), "Wildcard should expand nodes");
    assert!(output.contains("username"), "User should be expanded");
}

#[tokio::test]
async fn schema_rpc_and_command_use_the_supplied_ontology() {
    let service = OrbitServiceImpl::new(
        Arc::new(mock_validator()),
        Arc::new(Ontology::new().with_nodes(["CustomNode"])),
        &test_config(),
        ClusterHealthChecker::default().into_arc(),
        60,
        Arc::new(orbit_server_config::AppConfig::embedded_defaults().analytics),
    );
    let response = service
        .get_graph_schema(authed_request(GetGraphSchemaRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let Some(get_graph_schema_response::Content::Structured(schema)) = response.content else {
        panic!("expected structured schema");
    };
    assert_eq!(schema.nodes.len(), 1);
    assert_eq!(schema.nodes[0].name, "CustomNode");

    for format in ["raw", "llm"] {
        let response = service
            .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
                command_name: "get_graph_schema".into(),
                parameters_json: serde_json::json!({"format": format}).to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        match response.content.unwrap() {
            invoke_agent_command_response::Content::ResultJson(encoded) => {
                assert_eq!(format, "raw");
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
                    serde_json::json!({
                        "domains": [{"name": "other", "nodes": ["CustomNode"]}],
                        "edges": [],
                    })
                );
            }
            invoke_agent_command_response::Content::FormattedText(text) => {
                assert_eq!(format, "llm");
                assert!(text.contains("CustomNode"));
                assert!(!text.contains("Project"));
            }
        }
    }
}

#[tokio::test]
async fn list_agent_commands_filters_known_command() {
    let service = test_service();
    let response = service
        .list_agent_commands(authed_request(ListAgentCommandsRequest {
            command_names: vec!["get_query_dsl".into()],
            format: ResponseFormat::Raw as i32,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.commands.len(), 1);
    assert_eq!(response.commands[0].name, "get_query_dsl");
    assert!(!response.commands[0].description.is_empty());
}

#[tokio::test]
async fn list_agent_commands_returns_short_command_descriptions() {
    let service = test_service();
    let response = service
        .list_agent_commands(authed_request(ListAgentCommandsRequest {
            command_names: vec![],
            format: ResponseFormat::Raw as i32,
        }))
        .await
        .unwrap()
        .into_inner();

    let query_graph = response
        .commands
        .iter()
        .find(|command| command.name == "query_graph")
        .expect("query_graph command should be listed");

    assert!(!query_graph.description.is_empty());
    assert!(!query_graph.description.contains("<toon>"));
    assert!(!query_graph.description.contains("Query DSL Schema"));
}

#[tokio::test]
async fn list_agent_commands_returns_toon_for_llm_format() {
    let service = test_service();
    let response = service
        .list_agent_commands(authed_request(ListAgentCommandsRequest {
            command_names: vec!["get_query_dsl".into()],
            format: ResponseFormat::Llm as i32,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.commands.len(), 1);
    assert!(response.formatted_text.contains("commands[1]"));
    assert!(response.formatted_text.contains("name: get_query_dsl"));
    assert!(response.formatted_text.contains("input_schema"));
}

#[tokio::test]
async fn list_agent_commands_rejects_unknown_command() {
    let service = test_service();
    let status = service
        .list_agent_commands(authed_request(ListAgentCommandsRequest {
            command_names: vec!["typo".into()],
            format: ResponseFormat::Raw as i32,
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains("typo"));
}

#[tokio::test]
async fn invoke_agent_command_maps_intercepted_command_to_failed_precondition() {
    let service = test_service();
    let status = service
        .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
            command_name: "query_graph".into(),
            parameters_json: r#"{"query":{}}"#.into(),
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn invoke_agent_command_preserves_raw_and_llm_content_shapes() {
    let service = test_service();
    let raw = service
        .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
            command_name: "get_query_dsl".into(),
            parameters_json: r#"{"format":"raw"}"#.into(),
        }))
        .await
        .unwrap()
        .into_inner();

    let Some(invoke_agent_command_response::Content::ResultJson(json)) = raw.content else {
        panic!("expected raw command result JSON");
    };
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        parsed.get("version").and_then(serde_json::Value::as_str),
        Some(ToolService::build_query_dsl_version().as_str())
    );

    let llm = service
        .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
            command_name: "get_query_dsl".into(),
            parameters_json: "{}".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    let Some(invoke_agent_command_response::Content::FormattedText(text)) = llm.content else {
        panic!("expected LLM command result text");
    };
    assert!(text.contains("QueryDSL v"));
}
