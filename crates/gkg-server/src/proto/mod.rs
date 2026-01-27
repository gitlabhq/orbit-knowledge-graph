#[allow(clippy::all)]
#[allow(clippy::derive_partial_eq_without_eq)]
mod gkg_v1 {
    include!("gkg.v1.rs");
}

pub use gkg_v1::ExecuteToolMessage;
pub use gkg_v1::ExecuteToolRequest;
pub use gkg_v1::ListToolsRequest;
pub use gkg_v1::ListToolsResponse;
pub use gkg_v1::RedactionRequired;
pub use gkg_v1::RedactionResponse;
pub use gkg_v1::ResourceAuthorization;
pub use gkg_v1::ResourceCheck;
pub use gkg_v1::ToolDefinition;
pub use gkg_v1::ToolError;
pub use gkg_v1::ToolResult;
pub use gkg_v1::execute_tool_message;
pub use gkg_v1::knowledge_graph_service_server;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_tools_request_is_empty() {
        let request = ListToolsRequest {};
        assert_eq!(std::mem::size_of_val(&request), 0);
    }

    #[test]
    fn test_tool_definition_fields() {
        let tool = ToolDefinition {
            name: "find_nodes".to_string(),
            description: "Find nodes in the graph".to_string(),
            parameters_json_schema: r#"{"type": "object"}"#.to_string(),
        };

        assert_eq!(tool.name, "find_nodes");
        assert!(!tool.description.is_empty());
        assert!(tool.parameters_json_schema.contains("object"));
    }

    #[test]
    fn test_execute_tool_message_variants() {
        let request_msg = ExecuteToolMessage {
            message: Some(execute_tool_message::Message::Request(ExecuteToolRequest {
                tool_name: "test".to_string(),
                arguments_json: "{}".to_string(),
            })),
        };
        assert!(matches!(
            request_msg.message,
            Some(execute_tool_message::Message::Request(_))
        ));

        let error_msg = ExecuteToolMessage {
            message: Some(execute_tool_message::Message::Error(ToolError {
                code: "not_found".to_string(),
                message: "Tool not found".to_string(),
            })),
        };
        assert!(matches!(
            error_msg.message,
            Some(execute_tool_message::Message::Error(_))
        ));
    }

    #[test]
    fn test_redaction_required_structure() {
        let redaction = RedactionRequired {
            result_id: "uuid-123".to_string(),
            resources: vec![
                ResourceCheck {
                    resource_type: "issues".to_string(),
                    ids: vec![1, 2, 3],
                },
                ResourceCheck {
                    resource_type: "merge_requests".to_string(),
                    ids: vec![10, 20],
                },
            ],
        };

        assert_eq!(redaction.result_id, "uuid-123");
        assert_eq!(redaction.resources.len(), 2);
        assert_eq!(redaction.resources[0].ids.len(), 3);
    }

    #[test]
    fn test_resource_authorization_map() {
        use std::collections::HashMap;

        let mut authorized = HashMap::new();
        authorized.insert(1, true);
        authorized.insert(2, false);

        let auth = ResourceAuthorization {
            resource_type: "issues".to_string(),
            authorized,
        };

        assert!(auth.authorized.get(&1) == Some(&true));
        assert!(auth.authorized.get(&2) == Some(&false));
    }
}
