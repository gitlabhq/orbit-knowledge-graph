//! Ontology loading from YAML fixtures (for testing)
//!
//! This module provides utilities to load ontology definitions from YAML files
//! and convert them into a Schema for validation.

#[cfg(test)]
pub mod tests {
    use crate::schema::{PropertyDef, Schema};
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::path::Path;

    /// Main ontology schema from schema.yaml
    #[derive(Debug, Deserialize)]
    struct OntologySchema {
        #[serde(rename = "type")]
        _type: String,
        domains: HashMap<String, DomainDef>,
        edges: HashMap<String, String>,
    }

    #[derive(Debug, Deserialize)]
    struct DomainDef {
        nodes: HashMap<String, String>,
    }

    /// Node definition from individual YAML files
    #[derive(Debug, Deserialize)]
    struct NodeDef {
        #[serde(default)]
        properties: HashMap<String, YamlPropertyDef>,
        #[serde(default)]
        additional_properties: HashMap<String, YamlPropertyDef>,
    }

    #[derive(Debug, Deserialize)]
    struct YamlPropertyDef {
        #[serde(rename = "type")]
        property_type: String,
        #[serde(default)]
        description: Option<String>,
    }

    /// Load ontology from fixtures directory and create a Schema
    pub fn load_ontology_schema() -> Schema {
        let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("fixtures/ontology");

        // Load main schema.yaml
        let schema_path = fixtures_dir.join("schema.yaml");
        let schema_content = std::fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path.display(), e));
        let ontology: OntologySchema = serde_yaml::from_str(&schema_content)
            .unwrap_or_else(|e| panic!("Failed to parse {}: {}", schema_path.display(), e));

        // Collect node labels and load their properties
        let mut node_labels = Vec::new();
        let mut node_properties: HashMap<String, HashMap<String, PropertyDef>> = HashMap::new();

        for domain in ontology.domains.values() {
            for (node_name, node_path) in &domain.nodes {
                node_labels.push(node_name.clone());

                // Load node definition
                let node_file = fixtures_dir.join(node_path);
                if let Ok(content) = std::fs::read_to_string(&node_file) {
                    if let Ok(node_def) = serde_yaml::from_str::<NodeDef>(&content) {
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
            }
        }

        // Collect relationship types
        let relationship_types: Vec<String> = ontology.edges.keys().cloned().collect();

        Schema::from_ontology(node_labels, relationship_types, node_properties)
    }

    #[test]
    fn test_load_ontology() {
        let schema = load_ontology_schema();

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
        let schema = load_ontology_schema();

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
        let schema = load_ontology_schema();

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
        let schema = load_ontology_schema();

        // Invalid columns should fail
        let err = schema.validate_column("User", "nonexistent_column").unwrap_err();
        assert!(err.to_string().contains("does not exist"));

        let err = schema.validate_column("Project", "invalid_property").unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn test_type_filter_validation() {
        let schema = load_ontology_schema();

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
        let schema = load_ontology_schema();
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
        let rel_type = defs.get("RelationshipTypeName").unwrap().as_object().unwrap();
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
