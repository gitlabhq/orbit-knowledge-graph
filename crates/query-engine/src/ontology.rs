//! Ontology loading from YAML files
//!
//! This module provides utilities to load ontology definitions from YAML files
//! and convert them into a Schema for validation.

use crate::schema::{PropertyDef, Schema};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Main ontology schema from schema.yaml
#[derive(Debug, Deserialize)]
pub struct OntologySchema {
    #[serde(rename = "type")]
    pub schema_type: String,
    #[serde(default)]
    pub schema_version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub domains: HashMap<String, DomainDef>,
    pub edges: HashMap<String, String>,
}

/// Domain definition containing node references
#[derive(Debug, Deserialize)]
pub struct DomainDef {
    #[serde(default)]
    pub description: Option<String>,
    pub nodes: HashMap<String, String>,
}

/// Node definition from individual YAML files
#[derive(Debug, Deserialize)]
pub struct NodeDef {
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, YamlPropertyDef>,
    #[serde(default)]
    pub additional_properties: HashMap<String, YamlPropertyDef>,
}

/// Property definition from YAML
#[derive(Debug, Clone, Deserialize)]
pub struct YamlPropertyDef {
    #[serde(rename = "type")]
    pub property_type: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub status: Option<String>,
}

/// Load ontology from a directory and create a Schema
///
/// The directory should contain:
/// - `schema.yaml` - main ontology definition
/// - Node definition files referenced in schema.yaml
///
/// # Example
///
/// ```ignore
/// use query_engine::ontology::load_ontology_from_dir;
///
/// let schema = load_ontology_from_dir("fixtures/ontology")?;
/// ```
pub fn load_ontology_from_dir(dir: impl AsRef<Path>) -> Result<Schema, OntologyError> {
    let dir = dir.as_ref();

    // Load main schema.yaml
    let schema_path = dir.join("schema.yaml");
    let schema_content = std::fs::read_to_string(&schema_path).map_err(|e| {
        OntologyError::IoError(format!("Failed to read {}: {}", schema_path.display(), e))
    })?;

    let ontology: OntologySchema = serde_yaml::from_str(&schema_content).map_err(|e| {
        OntologyError::ParseError(format!("Failed to parse {}: {}", schema_path.display(), e))
    })?;

    // Collect node labels and load their properties
    let mut node_labels = Vec::new();
    let mut node_properties: HashMap<String, HashMap<String, PropertyDef>> = HashMap::new();

    for domain in ontology.domains.values() {
        for (node_name, node_path) in &domain.nodes {
            node_labels.push(node_name.clone());

            // Load node definition
            let node_file = dir.join(node_path);
            let content = std::fs::read_to_string(&node_file).map_err(|e| {
                OntologyError::IoError(format!("Failed to read {}: {}", node_file.display(), e))
            })?;

            let node_def: NodeDef = serde_yaml::from_str(&content).map_err(|e| {
                OntologyError::ParseError(format!("Failed to parse {}: {}", node_file.display(), e))
            })?;

            let mut props = HashMap::new();

            // Add properties
            for (prop_name, prop_def) in node_def.properties {
                props.insert(
                    prop_name,
                    PropertyDef {
                        property_type: prop_def.property_type,
                        description: prop_def.description,
                    },
                );
            }

            // Add additional_properties
            for (prop_name, prop_def) in node_def.additional_properties {
                props.insert(
                    prop_name,
                    PropertyDef {
                        property_type: prop_def.property_type,
                        description: prop_def.description,
                    },
                );
            }

            node_properties.insert(node_name.clone(), props);
        }
    }

    // Collect relationship types
    let relationship_types: Vec<String> = ontology.edges.keys().cloned().collect();

    Ok(Schema::from_ontology(
        node_labels,
        relationship_types,
        node_properties,
    ))
}

/// Load ontology from YAML strings (useful for embedded ontologies)
///
/// # Arguments
/// * `schema_yaml` - Contents of schema.yaml
/// * `node_yamls` - Map of node name to node YAML content
pub fn load_ontology_from_strings(
    schema_yaml: &str,
    node_yamls: &HashMap<String, String>,
) -> Result<Schema, OntologyError> {
    let ontology: OntologySchema = serde_yaml::from_str(schema_yaml)
        .map_err(|e| OntologyError::ParseError(format!("Failed to parse schema: {e}")))?;

    let mut node_labels = Vec::new();
    let mut node_properties: HashMap<String, HashMap<String, PropertyDef>> = HashMap::new();

    for domain in ontology.domains.values() {
        for node_name in domain.nodes.keys() {
            node_labels.push(node_name.clone());

            if let Some(content) = node_yamls.get(node_name) {
                let node_def: NodeDef = serde_yaml::from_str(content).map_err(|e| {
                    OntologyError::ParseError(format!("Failed to parse node {node_name}: {e}"))
                })?;

                let mut props = HashMap::new();

                for (prop_name, prop_def) in node_def.properties {
                    props.insert(
                        prop_name,
                        PropertyDef {
                            property_type: prop_def.property_type,
                            description: prop_def.description,
                        },
                    );
                }

                for (prop_name, prop_def) in node_def.additional_properties {
                    props.insert(
                        prop_name,
                        PropertyDef {
                            property_type: prop_def.property_type,
                            description: prop_def.description,
                        },
                    );
                }

                node_properties.insert(node_name.clone(), props);
            }
        }
    }

    let relationship_types: Vec<String> = ontology.edges.keys().cloned().collect();

    Ok(Schema::from_ontology(
        node_labels,
        relationship_types,
        node_properties,
    ))
}

/// Errors that can occur when loading an ontology
#[derive(Debug, thiserror::Error)]
pub enum OntologyError {
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
}

/// Load ontology from the test fixtures directory.
///
/// This is useful for tests that need a real ontology schema.
#[cfg(test)]
pub fn load_test_ontology() -> Schema {
    let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures/ontology");

    load_ontology_from_dir(&fixtures_dir)
        .unwrap_or_else(|e| panic!("Failed to load test ontology: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_ontology() {
        let schema = load_test_ontology();

        // Verify node labels
        assert!(schema.node_labels.contains("User"));
        assert!(schema.node_labels.contains("Group"));
        assert!(schema.node_labels.contains("Project"));
        assert!(schema.node_labels.contains("Note"));

        // Verify relationship types
        assert!(schema.relationship_types.contains("AUTHORED"));
        assert!(schema.relationship_types.contains("CONTAINS"));
        assert!(schema.relationship_types.contains("MEMBER_OF"));
        assert!(schema.relationship_types.contains("CREATOR"));
        assert!(schema.relationship_types.contains("OWNER"));
    }

    #[test]
    fn test_node_properties_loaded() {
        let schema = load_test_ontology();

        // Check User properties
        let user_props = schema.node_properties.get("User").unwrap();
        assert!(user_props.contains_key("id"));
        assert!(user_props.contains_key("username"));
        assert!(user_props.contains_key("email"));
        assert!(user_props.contains_key("name"));
        assert!(user_props.contains_key("created_at")); // from additional_properties
        assert!(user_props.contains_key("state")); // from additional_properties

        // Check Project properties
        let project_props = schema.node_properties.get("Project").unwrap();
        assert!(project_props.contains_key("id"));
        assert!(project_props.contains_key("name"));
        assert!(project_props.contains_key("description"));
        assert!(project_props.contains_key("created_at"));

        // Check Note properties
        let note_props = schema.node_properties.get("Note").unwrap();
        assert!(note_props.contains_key("id"));
        assert!(note_props.contains_key("note"));
        assert!(note_props.contains_key("system"));
        assert!(note_props.contains_key("confidential"));
    }

    #[test]
    fn test_column_validation_valid() {
        let schema = load_test_ontology();

        // Valid columns should pass
        assert!(schema.validate_column("User", "username").is_ok());
        assert!(schema.validate_column("User", "email").is_ok());
        assert!(schema.validate_column("User", "created_at").is_ok());
        assert!(schema.validate_column("Project", "name").is_ok());
        assert!(schema.validate_column("Note", "system").is_ok());

        // Reserved columns should always pass
        assert!(schema.validate_column("User", "id").is_ok());
        assert!(schema.validate_column("User", "label").is_ok());
    }

    #[test]
    fn test_column_validation_invalid() {
        let schema = load_test_ontology();

        // Invalid columns should fail
        let err = schema
            .validate_column("User", "nonexistent_column")
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"));

        let err = schema
            .validate_column("Project", "invalid_property")
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn test_type_filter_validation() {
        let schema = load_test_ontology();

        // Valid node labels
        assert!(schema.validate_type_filter("User").is_ok());
        assert!(schema.validate_type_filter("Project").is_ok());
        assert!(schema.validate_type_filter("Note").is_ok());

        // Valid relationship types
        assert!(schema.validate_type_filter("AUTHORED").is_ok());
        assert!(schema.validate_type_filter("CONTAINS").is_ok());

        // Invalid types
        let err = schema.validate_type_filter("NonexistentType").unwrap_err();
        assert!(err.to_string().contains("not a valid"));

        let err = schema.validate_type_filter("INVALID_REL").unwrap_err();
        assert!(err.to_string().contains("not a valid"));
    }

    #[test]
    fn test_derive_schema_with_ontology() {
        let schema = load_test_ontology();
        let derived = schema.derive_json_schema().unwrap();

        // Check $defs
        let defs = derived.get("$defs").unwrap().as_object().unwrap();

        // NodeLabel should contain our labels
        let node_label = defs.get("NodeLabel").unwrap().as_object().unwrap();
        let labels = node_label.get("enum").unwrap().as_array().unwrap();
        assert!(labels.iter().any(|v| v.as_str() == Some("User")));
        assert!(labels.iter().any(|v| v.as_str() == Some("Project")));
        assert!(labels.iter().any(|v| v.as_str() == Some("Note")));

        // RelationshipTypeName should contain our types
        let rel_type = defs
            .get("RelationshipTypeName")
            .unwrap()
            .as_object()
            .unwrap();
        let types = rel_type.get("enum").unwrap().as_array().unwrap();
        assert!(types.iter().any(|v| v.as_str() == Some("AUTHORED")));
        assert!(types.iter().any(|v| v.as_str() == Some("CONTAINS")));

        // NodeProperties should be populated
        let node_props = defs.get("NodeProperties").unwrap().as_object().unwrap();
        assert!(node_props.contains_key("User"));
        assert!(node_props.contains_key("Project"));

        // User should have properties
        let user_props = node_props.get("User").unwrap().as_object().unwrap();
        assert!(user_props.contains_key("username"));
        assert!(user_props.contains_key("email"));
    }
}
