//! Schema management and validation
//!
//! Handles JSON schema validation and ontology-based column validation.

use crate::error::QueryError;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

/// Base JSON schema embedded at compile time
const BASE_SCHEMA_JSON: &str = include_str!("../conf/schema.json");

/// Schema for validating queries against the ontology
#[derive(Debug, Clone)]
pub struct Schema {
    /// Valid node labels
    pub node_labels: HashSet<String>,
    /// Valid relationship types
    pub relationship_types: HashSet<String>,
    /// Properties per node label: label -> property -> type
    pub node_properties: HashMap<String, HashMap<String, PropertyDef>>,
    /// Reserved columns that exist on all nodes/edges
    reserved_columns: HashSet<&'static str>,
}

#[derive(Debug, Clone)]
pub struct PropertyDef {
    pub property_type: String,
    pub description: Option<String>,
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Schema {
    pub fn new() -> Self {
        Self {
            node_labels: HashSet::new(),
            relationship_types: HashSet::new(),
            node_properties: HashMap::new(),
            reserved_columns: ["id", "label", "from_id", "to_id", "type"]
                .into_iter()
                .collect(),
        }
    }

    /// Create a schema from ontology data
    pub fn from_ontology(
        node_labels: impl IntoIterator<Item = impl Into<String>>,
        relationship_types: impl IntoIterator<Item = impl Into<String>>,
        node_properties: HashMap<String, HashMap<String, PropertyDef>>,
    ) -> Self {
        Self {
            node_labels: node_labels.into_iter().map(Into::into).collect(),
            relationship_types: relationship_types.into_iter().map(Into::into).collect(),
            node_properties,
            reserved_columns: ["id", "label", "from_id", "to_id", "type"]
                .into_iter()
                .collect(),
        }
    }

    /// Validate that a column exists for a given node label
    pub fn validate_column(&self, node_label: &str, column: &str) -> Result<(), QueryError> {
        // Reserved columns exist on all nodes/edges
        if self.reserved_columns.contains(column) {
            return Ok(());
        }

        // If no label specified, we can't validate
        if node_label.is_empty() {
            return Err(QueryError::Validation(
                "no node label specified for column validation".into(),
            ));
        }

        // If we don't have property definitions for this node, skip validation
        // (will be caught at runtime if the column doesn't exist)
        let Some(properties) = self.node_properties.get(node_label) else {
            return Ok(());
        };

        // Check if column exists in properties
        if properties.contains_key(column) {
            return Ok(());
        }

        Err(QueryError::Validation(format!(
            "column \"{column}\" does not exist on node type \"{node_label}\""
        )))
    }

    /// Validate that a type filter is valid
    pub fn validate_type_filter(&self, type_filter: &str) -> Result<(), QueryError> {
        if self.node_labels.contains(type_filter) {
            return Ok(());
        }
        if self.relationship_types.contains(type_filter) {
            return Ok(());
        }
        Err(QueryError::Validation(format!(
            "type \"{type_filter}\" is not a valid node label or relationship type"
        )))
    }

    /// Validate JSON data against the base JSON schema
    pub fn validate_json(&self, json_data: &Value) -> Result<(), QueryError> {
        // For now, just check that it's an object with required fields
        // Full JSON schema validation can be added later
        let obj = json_data
            .as_object()
            .ok_or_else(|| QueryError::Validation("input must be an object".into()))?;

        if !obj.contains_key("query_type") {
            return Err(QueryError::Validation(
                "missing required field: query_type".into(),
            ));
        }

        if !obj.contains_key("nodes") {
            return Err(QueryError::Validation(
                "missing required field: nodes".into(),
            ));
        }

        Ok(())
    }

    /// Derive the full JSON schema with ontology values populated.
    ///
    /// This loads the base schema.json and populates:
    /// - `$defs.NodeLabel.enum` with valid node labels
    /// - `$defs.RelationshipTypeName.enum` with valid relationship types
    /// - `$defs.NodeProperties` with property definitions per node type
    pub fn derive_json_schema(&self) -> Result<Value, QueryError> {
        // Parse the base schema
        let mut schema: Value = serde_json::from_str(BASE_SCHEMA_JSON)
            .map_err(|e| QueryError::Validation(format!("failed to parse base schema: {e}")))?;

        // Get $defs section
        let defs = schema
            .get_mut("$defs")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| QueryError::Validation("schema missing $defs".into()))?;

        // Populate NodeLabel enum
        if let Some(node_label) = defs.get_mut("NodeLabel").and_then(Value::as_object_mut) {
            let labels: Vec<Value> = self
                .node_labels
                .iter()
                .map(|s| Value::String(s.clone()))
                .collect();
            node_label.insert("enum".to_string(), Value::Array(labels));
        }

        // Populate RelationshipTypeName enum
        if let Some(rel_type) = defs
            .get_mut("RelationshipTypeName")
            .and_then(Value::as_object_mut)
        {
            let types: Vec<Value> = self
                .relationship_types
                .iter()
                .map(|s| Value::String(s.clone()))
                .collect();
            rel_type.insert("enum".to_string(), Value::Array(types));
        }

        // Populate NodeProperties with property definitions per node type
        let node_props = self.build_node_properties_schema();
        defs.insert("NodeProperties".to_string(), node_props);

        Ok(schema)
    }

    /// Build the NodeProperties schema object from property definitions
    fn build_node_properties_schema(&self) -> Value {
        let mut node_props = Map::new();

        for (node_label, properties) in &self.node_properties {
            let mut prop_map = Map::new();

            for (prop_name, prop_def) in properties {
                let mut prop_schema = Map::new();
                prop_schema.insert(
                    "type".to_string(),
                    Value::String(map_type_to_json_schema(&prop_def.property_type)),
                );
                if let Some(ref desc) = prop_def.description {
                    prop_schema.insert("description".to_string(), Value::String(desc.clone()));
                }
                prop_map.insert(prop_name.clone(), Value::Object(prop_schema));
            }

            node_props.insert(node_label.clone(), Value::Object(prop_map));
        }

        Value::Object(node_props)
    }
}

/// Map ontology types to JSON Schema types
fn map_type_to_json_schema(ontology_type: &str) -> String {
    match ontology_type {
        "int64" | "int32" | "int16" | "int8" | "integer" => "integer".to_string(),
        "float64" | "float32" | "number" => "number".to_string(),
        "string" | "text" => "string".to_string(),
        "boolean" | "bool" => "boolean".to_string(),
        "timestamp" | "date" | "datetime" => "string".to_string(),
        "enum" => "string".to_string(),
        "array" => "array".to_string(),
        "object" | "json" | "jsonb" => "object".to_string(),
        _ => "string".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let mut node_properties = HashMap::new();
        node_properties.insert(
            "User".to_string(),
            [
                ("id", "integer"),
                ("username", "string"),
                ("email", "string"),
                ("created_at", "string"),
            ]
            .into_iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    PropertyDef {
                        property_type: v.to_string(),
                        description: None,
                    },
                )
            })
            .collect(),
        );
        node_properties.insert(
            "Project".to_string(),
            [("id", "integer"), ("name", "string")]
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        PropertyDef {
                            property_type: v.to_string(),
                            description: None,
                        },
                    )
                })
                .collect(),
        );

        Schema::from_ontology(
            ["User", "Project", "Note", "Group"],
            ["AUTHORED", "CONTAINS", "MEMBER_OF"],
            node_properties,
        )
    }

    #[test]
    fn test_validate_reserved_column() {
        let schema = test_schema();
        assert!(schema.validate_column("User", "id").is_ok());
        assert!(schema.validate_column("User", "label").is_ok());
    }

    #[test]
    fn test_validate_valid_column() {
        let schema = test_schema();
        assert!(schema.validate_column("User", "username").is_ok());
        assert!(schema.validate_column("User", "email").is_ok());
    }

    #[test]
    fn test_validate_invalid_column() {
        let schema = test_schema();
        let err = schema.validate_column("User", "nonexistent").unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn test_validate_type_filter() {
        let schema = test_schema();
        assert!(schema.validate_type_filter("User").is_ok());
        assert!(schema.validate_type_filter("AUTHORED").is_ok());
        assert!(schema.validate_type_filter("InvalidType").is_err());
    }

    #[test]
    fn test_derive_json_schema() {
        let schema = test_schema();
        let derived = schema.derive_json_schema().unwrap();

        // Check that $defs exists
        let defs = derived.get("$defs").unwrap().as_object().unwrap();

        // Check NodeLabel enum is populated
        let node_label = defs.get("NodeLabel").unwrap().as_object().unwrap();
        let labels = node_label.get("enum").unwrap().as_array().unwrap();
        assert!(labels.iter().any(|v| v.as_str() == Some("User")));
        assert!(labels.iter().any(|v| v.as_str() == Some("Project")));

        // Check RelationshipTypeName enum is populated
        let rel_type = defs
            .get("RelationshipTypeName")
            .unwrap()
            .as_object()
            .unwrap();
        let types = rel_type.get("enum").unwrap().as_array().unwrap();
        assert!(types.iter().any(|v| v.as_str() == Some("AUTHORED")));
        assert!(types.iter().any(|v| v.as_str() == Some("CONTAINS")));

        // Check NodeProperties is populated
        let node_props = defs.get("NodeProperties").unwrap().as_object().unwrap();
        assert!(node_props.contains_key("User"));
        assert!(node_props.contains_key("Project"));

        // Check User properties
        let user_props = node_props.get("User").unwrap().as_object().unwrap();
        assert!(user_props.contains_key("username"));
        assert!(user_props.contains_key("email"));
    }
}
