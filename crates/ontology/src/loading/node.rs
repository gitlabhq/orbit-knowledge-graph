use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

use crate::OntologyError;
use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::entities::{
    DataType, EnumType, Field, FieldSource, NodeEntity, NodeStyle, RedactionConfig, VirtualSource,
};
use crate::etl::{EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope};

use super::EtlSettings;

#[derive(Debug, Deserialize)]
pub(crate) struct NodeYaml {
    #[allow(dead_code)]
    node_type: String,
    domain: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    label: String,
    destination_table: String,
    #[serde(default)]
    properties: BTreeMap<String, PropertyYaml>,
    #[serde(default)]
    default_columns: Vec<String>,
    #[serde(default)]
    sort_key: Option<Vec<String>>,
    #[serde(default)]
    etl: Option<EtlYaml>,
    #[serde(default)]
    redaction: Option<RedactionConfig>,
    #[serde(default)]
    style: NodeStyle,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum EtlYaml {
    #[serde(rename = "table")]
    Table {
        scope: EtlScope,
        source: String,
        #[serde(default)]
        watermark: Option<String>,
        #[serde(default)]
        deleted: Option<String>,
        #[serde(default)]
        order_by: Option<Vec<String>>,
        #[serde(default)]
        edges: BTreeMap<String, EdgeMappingYaml>,
    },
    #[serde(rename = "query")]
    Query {
        scope: EtlScope,
        select: String,
        from: String,
        #[serde(default, rename = "where")]
        where_clause: Option<String>,
        #[serde(default)]
        watermark: Option<String>,
        #[serde(default)]
        deleted: Option<String>,
        #[serde(default)]
        order_by: Option<Vec<String>>,
        #[serde(default)]
        traversal_path_filter: Option<String>,
        #[serde(default)]
        edges: BTreeMap<String, EdgeMappingYaml>,
    },
}

#[derive(Debug, Deserialize)]
struct EdgeMappingYaml {
    #[serde(rename = "to")]
    target_literal: Option<String>,
    #[serde(rename = "to_column")]
    target_column: Option<String>,
    #[serde(rename = "as")]
    relationship_kind: String,
    #[serde(default)]
    direction: EdgeDirection,
    #[serde(default)]
    delimiter: Option<String>,
    #[serde(default)]
    array_field: Option<String>,
    #[serde(default)]
    array: bool,
}

#[derive(Debug, Deserialize)]
struct PropertyYaml {
    #[serde(rename = "type")]
    data_type: DataType,
    /// Source column name. Required for column-backed fields, absent for virtual fields.
    #[serde(default)]
    source: Option<String>,
    /// Virtual source configuration. Present only for fields resolved from a
    /// remote service. Mutually exclusive with `source`.
    #[serde(default, rename = "virtual")]
    virtual_config: Option<VirtualSourceYaml>,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    values: Option<BTreeMap<i64, String>>,
    #[serde(default)]
    enum_type: EnumType,
    #[serde(default = "PropertyYaml::default_like_allowed")]
    like_allowed: bool,
    #[serde(default = "PropertyYaml::default_filterable")]
    filterable: bool,
}

impl PropertyYaml {
    fn default_like_allowed() -> bool {
        true
    }
    fn default_filterable() -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
struct VirtualSourceYaml {
    service: String,
    lookup: String,
    #[serde(default)]
    disabled: bool,
    /// Column-backed properties this virtual field needs in the property map
    /// for resolution. The compiler ensures these are fetched during hydration.
    #[serde(default)]
    depends_on: Vec<String>,
}

impl NodeYaml {
    pub(crate) fn into_entity(
        self,
        name: String,
        default_entity_sort_key: &[String],
        etl_settings: &EtlSettings,
        internal_column_prefix: &str,
    ) -> Result<NodeEntity, OntologyError> {
        let mut primary_keys = Vec::new();

        let fields: Vec<Field> = self
            .properties
            .into_iter()
            .map(|(prop_name, prop_def)| {
                if prop_name == DEFAULT_PRIMARY_KEY {
                    primary_keys.push(prop_name.clone());
                }

                let source = match (prop_def.source, prop_def.virtual_config) {
                    (Some(col), None) => FieldSource::DatabaseColumn(col),
                    (None, Some(v)) => FieldSource::Virtual(VirtualSource {
                        service: v.service,
                        lookup: v.lookup,
                        disabled: v.disabled,
                        depends_on: v.depends_on,
                    }),
                    (Some(_), Some(_)) => {
                        return Err(OntologyError::Validation(format!(
                            "property '{prop_name}' on node '{name}': \
                             use 'source' or 'virtual', not both"
                        )));
                    }
                    (None, None) => {
                        return Err(OntologyError::Validation(format!(
                            "property '{prop_name}' on node '{name}': \
                             requires 'source' or 'virtual'"
                        )));
                    }
                };

                Ok(Field {
                    name: prop_name,
                    source,
                    data_type: prop_def.data_type,
                    nullable: prop_def.nullable,
                    enum_values: prop_def.values,
                    enum_type: prop_def.enum_type,
                    like_allowed: prop_def.like_allowed,
                    filterable: prop_def.filterable,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Reject field names that collide with the internal redaction column prefix.
        for field in &fields {
            if field.name.starts_with(internal_column_prefix) {
                return Err(OntologyError::Validation(format!(
                    "field '{}' on node '{}' uses reserved prefix '{internal_column_prefix}'",
                    field.name, name
                )));
            }
        }

        if primary_keys.is_empty() {
            primary_keys.push(DEFAULT_PRIMARY_KEY.to_string());
        }

        for pk in &primary_keys {
            if !fields.iter().any(|f| &f.name == pk) {
                return Err(OntologyError::Validation(format!(
                    "primary key '{}' not found in fields for node '{}'",
                    pk, name
                )));
            }
        }

        let field_names: HashSet<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        for col in &self.default_columns {
            if !field_names.contains(col.as_str()) {
                return Err(OntologyError::Validation(format!(
                    "default_columns entry '{}' is not a declared property of node '{}'",
                    col, name
                )));
            }
        }

        // Validate that every depends_on entry on a virtual field references
        // an existing database-backed column on this same node.
        for field in &fields {
            if let FieldSource::Virtual(vs) = &field.source {
                for dep in &vs.depends_on {
                    match fields.iter().find(|f| f.name == *dep) {
                        None => {
                            return Err(OntologyError::Validation(format!(
                                "virtual field '{}' on node '{}': depends_on references \
                                 unknown field '{dep}'",
                                field.name, name
                            )));
                        }
                        Some(dep_field) if dep_field.is_virtual() => {
                            return Err(OntologyError::Validation(format!(
                                "virtual field '{}' on node '{}': depends_on references \
                                 virtual field '{dep}' (must be database-backed)",
                                field.name, name
                            )));
                        }
                        _ => {}
                    }
                }
            }
        }

        let sort_key = self
            .sort_key
            .unwrap_or_else(|| default_entity_sort_key.to_vec());

        let etl = self.etl.map(|e| e.into_config(etl_settings)).transpose()?;

        let has_traversal_path = fields
            .iter()
            .any(|f| f.name == crate::constants::TRAVERSAL_PATH_COLUMN);

        Ok(NodeEntity {
            name,
            domain: self.domain,
            description: self.description,
            label: self.label,
            fields,
            primary_keys,
            default_columns: self.default_columns,
            sort_key,
            destination_table: self.destination_table,
            etl,
            redaction: self.redaction,
            style: self.style,
            has_traversal_path,
        })
    }
}

fn convert_edge_mappings(
    raw: BTreeMap<String, EdgeMappingYaml>,
) -> Result<BTreeMap<String, EdgeMapping>, OntologyError> {
    raw.into_iter()
        .map(|(column, mapping)| {
            let target = match (mapping.target_literal, mapping.target_column) {
                (Some(lit), None) => EdgeTarget::Literal(lit),
                (None, Some(col)) => EdgeTarget::Column(col),
                (Some(_), Some(_)) => {
                    return Err(OntologyError::Validation(format!(
                        "edge '{}': use 'to' or 'to_column', not both",
                        column
                    )));
                }
                (None, None) => {
                    return Err(OntologyError::Validation(format!(
                        "edge '{}': requires 'to' or 'to_column'",
                        column
                    )));
                }
            };
            let multi_value_options = [
                mapping.delimiter.is_some(),
                mapping.array_field.is_some(),
                mapping.array,
            ];
            if multi_value_options.iter().filter(|&&v| v).count() > 1 {
                return Err(OntologyError::Validation(format!(
                    "edge '{}': use only one of 'delimiter', 'array_field', or 'array'",
                    column
                )));
            }
            Ok((
                column,
                EdgeMapping {
                    target,
                    relationship_kind: mapping.relationship_kind,
                    direction: mapping.direction,
                    delimiter: mapping.delimiter,
                    array_field: mapping.array_field,
                    array: mapping.array,
                },
            ))
        })
        .collect()
}

impl EtlYaml {
    fn into_config(self, etl_settings: &EtlSettings) -> Result<EtlConfig, OntologyError> {
        match self {
            EtlYaml::Table {
                scope,
                source,
                watermark,
                deleted,
                order_by,
                edges,
            } => Ok(EtlConfig::Table {
                scope,
                source,
                watermark: watermark.unwrap_or_else(|| etl_settings.watermark.clone()),
                deleted: deleted.unwrap_or_else(|| etl_settings.deleted.clone()),
                order_by: order_by.unwrap_or_else(|| etl_settings.order_by.clone()),
                edges: convert_edge_mappings(edges)?,
            }),
            EtlYaml::Query {
                scope,
                select,
                from,
                where_clause,
                watermark,
                deleted,
                order_by,
                traversal_path_filter,
                edges,
            } => Ok(EtlConfig::Query {
                scope,
                select,
                from,
                where_clause,
                watermark: watermark.unwrap_or_else(|| etl_settings.watermark.clone()),
                deleted: deleted.unwrap_or_else(|| etl_settings.deleted.clone()),
                order_by: order_by.unwrap_or_else(|| etl_settings.order_by.clone()),
                traversal_path_filter,
                edges: convert_edge_mappings(edges)?,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Ontology;

    fn test_etl_settings() -> EtlSettings {
        EtlSettings {
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["id".to_string()],
        }
    }

    #[test]
    fn embedded_ontology_depends_on_references_are_valid() {
        // The real ontology should pass all validation including depends_on.
        let ontology = Ontology::load_embedded().expect("embedded ontology should load");
        // File.content has depends_on -- verify the field exists and has deps.
        let file = ontology.get_node("File").expect("File node should exist");
        let content = file.fields.iter().find(|f| f.name == "content");
        assert!(content.is_some(), "File should have a content field");
        if let Some(f) = content
            && let FieldSource::Virtual(vs) = &f.source
        {
            assert!(
                !vs.depends_on.is_empty(),
                "File.content should have depends_on"
            );
        }
    }

    fn parse_test_node(yaml: &str) -> Result<NodeEntity, OntologyError> {
        let node: NodeYaml = serde_yaml::from_str(yaml).unwrap();
        node.into_entity(
            "TestNode".to_string(),
            &["id".to_string()],
            &test_etl_settings(),
            "_gkg_",
        )
    }

    #[test]
    fn depends_on_rejects_unknown_field() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              content:
                type: string
                virtual:
                  service: gitaly
                  lookup: blob_content
                  depends_on: [nonexistent_field]
                nullable: true
            "#,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nonexistent_field"),
            "error should mention the bad field name, got: {err}"
        );
    }

    #[test]
    fn depends_on_rejects_virtual_dependency() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              other_virtual:
                type: string
                virtual:
                  service: foo
                  lookup: bar
                nullable: true
              content:
                type: string
                virtual:
                  service: gitaly
                  lookup: blob_content
                  depends_on: [other_virtual]
                nullable: true
            "#,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must be database-backed"),
            "error should say virtual deps not allowed, got: {err}"
        );
    }

    #[test]
    fn depends_on_accepts_valid_db_column() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              project_id:
                type: int64
                source: project_id
              content:
                type: string
                virtual:
                  service: gitaly
                  lookup: blob_content
                  depends_on: [project_id]
                nullable: true
            "#,
        );
        assert!(
            result.is_ok(),
            "should accept valid depends_on: {:?}",
            result.err()
        );
    }
}
