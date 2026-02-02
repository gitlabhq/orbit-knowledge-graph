//! Plugin schema validation.

use ontology::Ontology;

use crate::error::MailboxError;
use crate::types::{NodeDefinition, PluginSchema, PropertyType};

pub struct SchemaValidator {
    ontology: Ontology,
}

impl SchemaValidator {
    pub fn new(ontology: Ontology) -> Self {
        Self { ontology }
    }

    pub fn validate(&self, plugin_id: &str, schema: &PluginSchema) -> Result<(), MailboxError> {
        self.validate_node_names_prefixed(plugin_id, schema)?;
        self.validate_edge_relationship_kinds_prefixed(plugin_id, schema)?;
        self.validate_edge_targets(plugin_id, schema)?;
        self.validate_property_types(schema)?;
        Ok(())
    }

    fn validate_node_names_prefixed(
        &self,
        plugin_id: &str,
        schema: &PluginSchema,
    ) -> Result<(), MailboxError> {
        let required_prefix = format!("{}_", plugin_id.replace('-', "_"));

        for node in &schema.nodes {
            if !node.name.starts_with(&required_prefix) {
                return Err(MailboxError::validation(format!(
                    "node name '{}' must start with '{}'",
                    node.name, required_prefix
                )));
            }

            self.validate_node_name_format(&node.name)?;
        }

        Ok(())
    }

    fn validate_node_name_format(&self, name: &str) -> Result<(), MailboxError> {
        if name.is_empty() {
            return Err(MailboxError::validation("node name cannot be empty"));
        }

        let valid_chars = name.chars().all(|c| c.is_alphanumeric() || c == '_');

        if !valid_chars {
            return Err(MailboxError::validation(format!(
                "node name '{}' contains invalid characters (only alphanumeric and underscore allowed)",
                name
            )));
        }

        Ok(())
    }

    fn validate_edge_relationship_kinds_prefixed(
        &self,
        plugin_id: &str,
        schema: &PluginSchema,
    ) -> Result<(), MailboxError> {
        let required_prefix = format!("{}_", plugin_id.replace('-', "_"));

        for edge in &schema.edges {
            if !edge.relationship_kind.starts_with(&required_prefix) {
                return Err(MailboxError::validation(format!(
                    "edge relationship_kind '{}' must start with '{}'",
                    edge.relationship_kind, required_prefix
                )));
            }
        }

        Ok(())
    }

    fn validate_edge_targets(
        &self,
        plugin_id: &str,
        schema: &PluginSchema,
    ) -> Result<(), MailboxError> {
        let plugin_node_names: Vec<&str> = schema.node_names().collect();

        for edge in &schema.edges {
            self.validate_edge_endpoint_kinds(
                plugin_id,
                &edge.from_node_kinds,
                &plugin_node_names,
                &edge.relationship_kind,
                "from",
            )?;
            self.validate_edge_endpoint_kinds(
                plugin_id,
                &edge.to_node_kinds,
                &plugin_node_names,
                &edge.relationship_kind,
                "to",
            )?;
        }

        Ok(())
    }

    fn validate_edge_endpoint_kinds(
        &self,
        plugin_id: &str,
        kinds: &[String],
        plugin_node_names: &[&str],
        relationship_kind: &str,
        direction: &str,
    ) -> Result<(), MailboxError> {
        let plugin_prefix = format!("{}_", plugin_id.replace('-', "_"));

        for kind in kinds {
            if kind.starts_with(&plugin_prefix) {
                if !plugin_node_names.contains(&kind.as_str()) {
                    return Err(MailboxError::validation(format!(
                        "edge '{}' {}_node_kinds references unknown plugin node '{}'",
                        relationship_kind, direction, kind
                    )));
                }
            } else if kind.contains('_') && !self.ontology.has_node(kind) {
                return Err(MailboxError::validation(format!(
                    "edge '{}' {}_node_kinds references node '{}' from another plugin in the same namespace, which is not allowed",
                    relationship_kind, direction, kind
                )));
            } else if !self.ontology.has_node(kind) {
                return Err(MailboxError::validation(format!(
                    "edge '{}' {}_node_kinds references unknown system node '{}'",
                    relationship_kind, direction, kind
                )));
            }
        }

        Ok(())
    }

    fn validate_property_types(&self, schema: &PluginSchema) -> Result<(), MailboxError> {
        for node in &schema.nodes {
            self.validate_node_properties(node)?;
        }
        Ok(())
    }

    fn validate_node_properties(&self, node: &NodeDefinition) -> Result<(), MailboxError> {
        let mut seen_names = std::collections::HashSet::new();

        for property in &node.properties {
            if !seen_names.insert(&property.name) {
                return Err(MailboxError::validation(format!(
                    "node '{}' has duplicate property '{}'",
                    node.name, property.name
                )));
            }

            self.validate_property_name(&property.name, &node.name)?;

            if property.property_type == PropertyType::Enum && property.enum_values.is_none() {
                return Err(MailboxError::validation(format!(
                    "property '{}' on node '{}' has type 'enum' but no enum_values specified",
                    property.name, node.name
                )));
            }
        }

        Ok(())
    }

    fn validate_property_name(&self, name: &str, node_name: &str) -> Result<(), MailboxError> {
        if name.is_empty() {
            return Err(MailboxError::validation(format!(
                "property name cannot be empty on node '{}'",
                node_name
            )));
        }

        let reserved = ["id", "traversal_path", "_version", "_deleted"];
        if reserved.contains(&name) {
            return Err(MailboxError::validation(format!(
                "property '{}' on node '{}' uses reserved column name",
                name, node_name
            )));
        }

        let valid_chars = name.chars().all(|c| c.is_alphanumeric() || c == '_');

        if !valid_chars {
            return Err(MailboxError::validation(format!(
                "property '{}' on node '{}' contains invalid characters",
                name, node_name
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EdgeDefinition, PropertyDefinition};

    fn test_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User", "Project", "File"])
            .with_edges(["AUTHORED", "CONTAINS"])
    }

    #[test]
    fn validates_correct_schema() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new()
            .with_node(
                NodeDefinition::new("security_scanner_Vulnerability")
                    .with_property(PropertyDefinition::new("score", PropertyType::Float))
                    .with_property(
                        PropertyDefinition::new("severity", PropertyType::Enum)
                            .with_enum_values(vec!["low".into(), "high".into()]),
                    ),
            )
            .with_edge(
                EdgeDefinition::new("security_scanner_AFFECTS")
                    .from_kinds(vec!["security_scanner_Vulnerability".into()])
                    .to_kinds(vec!["File".into(), "Project".into()]),
            );

        assert!(validator.validate("security_scanner", &schema).is_ok());
    }

    #[test]
    fn rejects_unprefixed_node_name() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new().with_node(NodeDefinition::new("Vulnerability"));

        let result = validator.validate("security_scanner", &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must start with"));
    }

    #[test]
    fn rejects_unprefixed_edge_relationship_kind() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new()
            .with_node(NodeDefinition::new("security_scanner_Vulnerability"))
            .with_edge(
                EdgeDefinition::new("AFFECTS")
                    .from_kinds(vec!["security_scanner_Vulnerability".into()])
                    .to_kinds(vec!["Project".into()]),
            );

        let result = validator.validate("security_scanner", &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must start with"));
    }

    #[test]
    fn rejects_unknown_system_node_target() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new()
            .with_node(NodeDefinition::new("security_scanner_Vulnerability"))
            .with_edge(
                EdgeDefinition::new("security_scanner_AFFECTS")
                    .from_kinds(vec!["security_scanner_Vulnerability".into()])
                    .to_kinds(vec!["UnknownSystemNode".into()]),
            );

        let result = validator.validate("security_scanner", &schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown system node")
        );
    }

    #[test]
    fn rejects_enum_without_values() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new().with_node(
            NodeDefinition::new("security_scanner_Vulnerability")
                .with_property(PropertyDefinition::new("severity", PropertyType::Enum)),
        );

        let result = validator.validate("security_scanner", &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no enum_values"));
    }

    #[test]
    fn rejects_reserved_property_name() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new().with_node(
            NodeDefinition::new("security_scanner_Vulnerability")
                .with_property(PropertyDefinition::new("id", PropertyType::Int64)),
        );

        let result = validator.validate("security_scanner", &schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("reserved column name")
        );
    }

    #[test]
    fn rejects_duplicate_property_name() {
        let validator = SchemaValidator::new(test_ontology());

        let schema = PluginSchema::new().with_node(
            NodeDefinition::new("security_scanner_Vulnerability")
                .with_property(PropertyDefinition::new("score", PropertyType::Float))
                .with_property(PropertyDefinition::new("score", PropertyType::Int64)),
        );

        let result = validator.validate("security_scanner", &schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("duplicate property")
        );
    }
}
