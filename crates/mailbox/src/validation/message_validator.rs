//! Message payload validation against plugin schemas.

use serde_json::Value;

use crate::error::MailboxError;
use crate::types::{EdgePayload, MailboxMessage, NodePayload, Plugin, PropertyType};

const MAX_NODES_PER_MESSAGE: usize = 1000;
const MAX_EDGES_PER_MESSAGE: usize = 1000;

pub struct MessageValidator;

impl MessageValidator {
    pub fn validate(message: &MailboxMessage, plugin: &Plugin) -> Result<(), MailboxError> {
        Self::validate_batch_size(message)?;
        Self::validate_plugin_id_matches(message, plugin)?;
        Self::validate_nodes(message, plugin)?;
        Self::validate_edges(message, plugin)?;
        Self::validate_delete_nodes(message, plugin)?;
        Self::validate_delete_edges(message, plugin)?;
        Ok(())
    }

    fn validate_batch_size(message: &MailboxMessage) -> Result<(), MailboxError> {
        let total_nodes = message.node_count() + message.delete_node_count();
        if total_nodes > MAX_NODES_PER_MESSAGE {
            return Err(MailboxError::validation(format!(
                "message contains {} node operations (create + delete), maximum allowed is {}",
                total_nodes, MAX_NODES_PER_MESSAGE
            )));
        }

        let total_edges = message.edge_count() + message.delete_edge_count();
        if total_edges > MAX_EDGES_PER_MESSAGE {
            return Err(MailboxError::validation(format!(
                "message contains {} edge operations (create + delete), maximum allowed is {}",
                total_edges, MAX_EDGES_PER_MESSAGE
            )));
        }

        Ok(())
    }

    fn validate_plugin_id_matches(
        message: &MailboxMessage,
        plugin: &Plugin,
    ) -> Result<(), MailboxError> {
        if message.plugin_id != plugin.plugin_id {
            return Err(MailboxError::validation(format!(
                "message plugin_id '{}' does not match authenticated plugin '{}'",
                message.plugin_id, plugin.plugin_id
            )));
        }
        Ok(())
    }

    fn validate_nodes(message: &MailboxMessage, plugin: &Plugin) -> Result<(), MailboxError> {
        for node in &message.nodes {
            Self::validate_node(node, plugin)?;
        }
        Ok(())
    }

    fn validate_node(node: &NodePayload, plugin: &Plugin) -> Result<(), MailboxError> {
        let node_definition = plugin.schema.get_node(&node.node_kind).ok_or_else(|| {
            MailboxError::validation(format!(
                "node kind '{}' is not defined in plugin schema",
                node.node_kind
            ))
        })?;

        let properties = node.properties.as_object().ok_or_else(|| {
            MailboxError::validation(format!(
                "node '{}' properties must be an object",
                node.external_id
            ))
        })?;

        for property_definition in &node_definition.properties {
            let value = properties.get(&property_definition.name);

            match value {
                None | Some(Value::Null) if !property_definition.nullable => {
                    return Err(MailboxError::validation(format!(
                        "node '{}' missing required property '{}'",
                        node.external_id, property_definition.name
                    )));
                }
                Some(v) if !v.is_null() => {
                    Self::validate_property_value(
                        v,
                        property_definition.property_type,
                        &property_definition.name,
                        &node.external_id,
                        property_definition.enum_values.as_deref(),
                    )?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn validate_property_value(
        value: &Value,
        property_type: PropertyType,
        property_name: &str,
        node_id: &str,
        enum_values: Option<&[String]>,
    ) -> Result<(), MailboxError> {
        let is_valid = match property_type {
            PropertyType::String => value.is_string(),
            PropertyType::Int64 => value.is_i64(),
            PropertyType::Float => value.is_f64() || value.is_i64(),
            PropertyType::Boolean => value.is_boolean(),
            PropertyType::Date | PropertyType::Timestamp => value.is_string(),
            PropertyType::Enum => {
                if let Some(s) = value.as_str() {
                    enum_values.is_none_or(|values| values.contains(&s.to_string()))
                } else {
                    false
                }
            }
        };

        if !is_valid {
            return Err(MailboxError::validation(format!(
                "node '{}' property '{}' has invalid type, expected {}",
                node_id, property_name, property_type
            )));
        }

        Ok(())
    }

    fn validate_edges(message: &MailboxMessage, plugin: &Plugin) -> Result<(), MailboxError> {
        for edge in &message.edges {
            Self::validate_edge(edge, plugin)?;
        }
        Ok(())
    }

    fn validate_edge(edge: &EdgePayload, plugin: &Plugin) -> Result<(), MailboxError> {
        let edge_definition = plugin
            .schema
            .get_edge(&edge.relationship_kind)
            .ok_or_else(|| {
                MailboxError::validation(format!(
                    "edge relationship_kind '{}' is not defined in plugin schema",
                    edge.relationship_kind
                ))
            })?;

        if !edge_definition
            .from_node_kinds
            .contains(&edge.source.node_kind)
        {
            return Err(MailboxError::validation(format!(
                "edge '{}' source node_kind '{}' is not allowed for relationship '{}'",
                edge.external_id, edge.source.node_kind, edge.relationship_kind
            )));
        }

        if !edge_definition
            .to_node_kinds
            .contains(&edge.target.node_kind)
        {
            return Err(MailboxError::validation(format!(
                "edge '{}' target node_kind '{}' is not allowed for relationship '{}'",
                edge.external_id, edge.target.node_kind, edge.relationship_kind
            )));
        }

        Ok(())
    }

    fn validate_delete_nodes(
        message: &MailboxMessage,
        plugin: &Plugin,
    ) -> Result<(), MailboxError> {
        for node_ref in &message.delete_nodes {
            if plugin.schema.get_node(&node_ref.node_kind).is_none() {
                return Err(MailboxError::validation(format!(
                    "delete_nodes references unknown node kind '{}'",
                    node_ref.node_kind
                )));
            }
        }
        Ok(())
    }

    fn validate_delete_edges(
        message: &MailboxMessage,
        plugin: &Plugin,
    ) -> Result<(), MailboxError> {
        for edge_ref in &message.delete_edges {
            if plugin
                .schema
                .get_edge(&edge_ref.relationship_kind)
                .is_none()
            {
                return Err(MailboxError::validation(format!(
                    "delete_edges references unknown relationship_kind '{}'",
                    edge_ref.relationship_kind
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        EdgeDefinition, NodeDefinition, NodeReference, PluginSchema, PropertyDefinition,
    };
    use serde_json::json;

    fn test_plugin() -> Plugin {
        let schema = PluginSchema::new()
            .with_node(
                NodeDefinition::new("test_Vulnerability")
                    .with_property(PropertyDefinition::new("score", PropertyType::Float))
                    .with_property(
                        PropertyDefinition::new("cve_id", PropertyType::String).nullable(),
                    )
                    .with_property(
                        PropertyDefinition::new("severity", PropertyType::Enum)
                            .with_enum_values(vec!["low".into(), "medium".into(), "high".into()]),
                    ),
            )
            .with_edge(
                EdgeDefinition::new("test_AFFECTS")
                    .from_kinds(vec!["test_Vulnerability".into()])
                    .to_kinds(vec!["Project".into()]),
            );

        Plugin::new("test", 42, "hash", schema)
    }

    #[test]
    fn validates_correct_message() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test")
            .with_node(
                NodePayload::new("vuln-1", "test_Vulnerability")
                    .with_properties(json!({"score": 8.5, "severity": "high"})),
            )
            .with_edge(EdgePayload::new(
                "edge-1",
                "test_AFFECTS",
                NodeReference::new("test_Vulnerability", "vuln-1"),
                NodeReference::new("Project", "42"),
            ));

        assert!(MessageValidator::validate(&message, &plugin).is_ok());
    }

    #[test]
    fn rejects_missing_required_property() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test").with_node(
            NodePayload::new("vuln-1", "test_Vulnerability")
                .with_properties(json!({"cve_id": "CVE-2024-001"})),
        );

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing required property")
        );
    }

    #[test]
    fn allows_null_for_nullable_property() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test").with_node(
            NodePayload::new("vuln-1", "test_Vulnerability")
                .with_properties(json!({"score": 8.5, "cve_id": null, "severity": "low"})),
        );

        assert!(MessageValidator::validate(&message, &plugin).is_ok());
    }

    #[test]
    fn rejects_invalid_enum_value() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test").with_node(
            NodePayload::new("vuln-1", "test_Vulnerability")
                .with_properties(json!({"score": 8.5, "severity": "invalid_severity"})),
        );

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid type"));
    }

    #[test]
    fn rejects_unknown_node_kind() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test")
            .with_node(NodePayload::new("vuln-1", "test_UnknownType"));

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not defined in plugin schema")
        );
    }

    #[test]
    fn rejects_mismatched_plugin_id() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "different_plugin")
            .with_node(NodePayload::new("vuln-1", "test_Vulnerability"));

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not match"));
    }

    #[test]
    fn rejects_oversized_batch() {
        let plugin = test_plugin();

        let nodes: Vec<NodePayload> = (0..1001)
            .map(|i| {
                NodePayload::new(format!("vuln-{}", i), "test_Vulnerability")
                    .with_properties(json!({"score": 5.0, "severity": "low"}))
            })
            .collect();

        let message = MailboxMessage::new("msg-1", "test").with_nodes(nodes);

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("maximum allowed"));
    }

    #[test]
    fn validates_delete_nodes_with_valid_kind() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test")
            .with_delete_node(NodeReference::new("test_Vulnerability", "vuln-1"));

        assert!(MessageValidator::validate(&message, &plugin).is_ok());
    }

    #[test]
    fn rejects_delete_nodes_with_unknown_kind() {
        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test")
            .with_delete_node(NodeReference::new("test_UnknownType", "vuln-1"));

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("delete_nodes references unknown node kind")
        );
    }

    #[test]
    fn validates_delete_edges_with_valid_kind() {
        use crate::types::EdgeReference;

        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test")
            .with_delete_edge(EdgeReference::new("test_AFFECTS", "edge-1"));

        assert!(MessageValidator::validate(&message, &plugin).is_ok());
    }

    #[test]
    fn rejects_delete_edges_with_unknown_kind() {
        use crate::types::EdgeReference;

        let plugin = test_plugin();

        let message = MailboxMessage::new("msg-1", "test")
            .with_delete_edge(EdgeReference::new("test_UNKNOWN_EDGE", "edge-1"));

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("delete_edges references unknown relationship_kind")
        );
    }

    #[test]
    fn counts_delete_operations_in_batch_limit() {
        let plugin = test_plugin();

        let nodes: Vec<NodePayload> = (0..500)
            .map(|i| {
                NodePayload::new(format!("vuln-{}", i), "test_Vulnerability")
                    .with_properties(json!({"score": 5.0, "severity": "low"}))
            })
            .collect();

        let delete_nodes: Vec<NodeReference> = (0..501)
            .map(|i| NodeReference::new("test_Vulnerability", format!("del-vuln-{}", i)))
            .collect();

        let message = MailboxMessage::new("msg-1", "test")
            .with_nodes(nodes)
            .with_delete_nodes(delete_nodes);

        let result = MessageValidator::validate(&message, &plugin);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("node operations"));
    }
}
