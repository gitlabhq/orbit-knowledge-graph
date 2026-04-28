mod edge;
mod node;
mod schema;

use rust_embed::Embed;
use serde::Deserialize;
use std::path::Path;

use crate::entities::{DomainInfo, EdgeColumn};
use crate::{Ontology, OntologyError};

pub(crate) use edge::EdgeYaml;
pub(crate) use node::NodeYaml;
use schema::SchemaYaml;

#[derive(Embed)]
#[folder = "$ONTOLOGY_DIR"]
struct EmbeddedOntology;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EtlSettings {
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
}

pub(crate) trait ReadOntologyFile {
    fn read(&self, path: &str) -> Result<String, OntologyError>;
}

pub(crate) struct DirReader<'a>(pub &'a Path);

impl ReadOntologyFile for DirReader<'_> {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        let full_path = self.0.join(path);
        std::fs::read_to_string(&full_path).map_err(|e| OntologyError::Io {
            path: full_path.to_string_lossy().to_string(),
            source: e,
        })
    }
}

struct EmbeddedReader;

impl ReadOntologyFile for EmbeddedReader {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        let file = EmbeddedOntology::get(path).ok_or_else(|| OntologyError::Io {
            path: path.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("embedded file not found: {}", path),
            ),
        })?;

        String::from_utf8(file.data.to_vec()).map_err(|e| OntologyError::Io {
            path: path.to_string(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        })
    }
}

pub(crate) fn parse_yaml<T: for<'de> Deserialize<'de>>(
    content: &str,
    path: &str,
) -> Result<T, OntologyError> {
    serde_yaml::from_str(content).map_err(|e| OntologyError::Yaml {
        path: path.to_string(),
        source: e,
    })
}

pub(super) fn load_from_dir(dir: &Path) -> Result<Ontology, OntologyError> {
    load_with(&DirReader(dir))
}

pub(super) fn load_embedded() -> Result<Ontology, OntologyError> {
    load_with(&EmbeddedReader)
}

pub(crate) fn load_with(reader: &impl ReadOntologyFile) -> Result<Ontology, OntologyError> {
    let schema_content = reader.read("schema.yaml")?;
    let schema: SchemaYaml = parse_yaml(&schema_content, "schema.yaml")?;

    let denormalization_entries = schema.settings.denormalization.clone();
    let denorm_default_as = schema.settings.denorm_default_as.clone();

    let mut ontology = Ontology::new();
    ontology.schema_version = schema.schema_version.unwrap_or_default();
    ontology.table_prefix = schema.settings.table_prefix.clone();
    ontology.default_edge_table = schema.settings.default_edge_table;
    ontology.default_entity_sort_key = schema.settings.default_entity_sort_key;

    // Load edge table configs.
    ontology.edge_table_configs = schema
        .settings
        .edge_tables
        .into_iter()
        .map(|(name, cfg)| {
            let storage = cfg.storage.map(|s| crate::entities::EdgeTableStorage {
                index_granularity: s.index_granularity,
                primary_key: s.primary_key,
                columns: s
                    .columns
                    .into_iter()
                    .map(|col| crate::entities::StorageColumn {
                        name: col.name,
                        ch_type: col.ch_type,
                        default: col.default,
                        codec: col.codec,
                    })
                    .collect(),
                indexes: s
                    .indexes
                    .into_iter()
                    .map(node::convert_storage_index)
                    .collect(),
                projections: s
                    .projections
                    .into_iter()
                    .map(node::convert_storage_projection)
                    .collect(),
                denormalized_columns: s
                    .denormalized_columns
                    .into_iter()
                    .map(|col| crate::entities::StorageColumn {
                        name: col.name,
                        ch_type: col.ch_type,
                        default: col.default,
                        codec: col.codec,
                    })
                    .collect(),
                denormalized_indexes: s
                    .denormalized_indexes
                    .into_iter()
                    .map(node::convert_storage_index)
                    .collect(),
                denormalized_projections: s
                    .denormalized_projections
                    .into_iter()
                    .map(node::convert_storage_projection)
                    .collect(),
            });
            let config = crate::EdgeTableConfig {
                sort_key: cfg.sort_key,
                columns: cfg
                    .columns
                    .into_iter()
                    .map(|c| crate::entities::EdgeColumn {
                        name: c.name,
                        data_type: c.data_type,
                    })
                    .collect(),
                storage: storage.unwrap_or_default(),
            };
            (name, config)
        })
        .collect();

    let etl_settings = EtlSettings {
        watermark: schema.settings.etl.default_watermark,
        deleted: schema.settings.etl.default_deleted,
        order_by: schema.settings.etl.default_etl_order_by,
    };
    ontology.etl_settings = etl_settings.clone();
    ontology.internal_column_prefix = schema.settings.internal_column_prefix;

    // Validate edge table names: must start with table_prefix and contain
    // only lowercase ASCII letters and underscores (safe for SQL identifiers).
    for table_name in ontology.edge_table_configs.keys() {
        if !table_name.starts_with(&ontology.table_prefix) {
            return Err(OntologyError::Validation(format!(
                "edge table '{}' does not start with table_prefix '{}'",
                table_name, ontology.table_prefix
            )));
        }
        if !table_name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '_')
        {
            return Err(OntologyError::Validation(format!(
                "edge table '{}' contains invalid characters (only a-z and _ allowed)",
                table_name
            )));
        }
    }
    if !ontology
        .edge_table_configs
        .contains_key(&ontology.default_edge_table)
    {
        return Err(OntologyError::Validation(format!(
            "default_edge_table '{}' is not defined in edge_tables",
            ontology.default_edge_table
        )));
    }

    // Validate default edge table columns match EDGE_RESERVED_COLUMNS.
    let actual_names: Vec<&str> = ontology
        .edge_columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let expected: &[&str] = crate::constants::EDGE_RESERVED_COLUMNS;
    if actual_names != expected {
        return Err(OntologyError::Validation(format!(
            "default edge table columns {:?} do not match EDGE_RESERVED_COLUMNS {:?}",
            actual_names, expected
        )));
    }

    for (domain_name, domain) in &schema.domains {
        let mut node_names = Vec::new();

        for (node_name, node_path) in &domain.nodes {
            if ontology.nodes.contains_key(node_name) {
                return Err(OntologyError::Validation(format!(
                    "duplicate node definition: '{}'",
                    node_name
                )));
            }

            let content = reader.read(node_path)?;
            let node_def: NodeYaml = parse_yaml(&content, node_path)?;

            let entity = node_def.into_entity(
                node_name.clone(),
                &ontology.default_entity_sort_key,
                &etl_settings,
                &ontology.internal_column_prefix,
            )?;

            if !entity.destination_table.starts_with(&ontology.table_prefix) {
                return Err(OntologyError::Validation(format!(
                    "node '{}' has destination_table '{}' which does not start with \
                     table_prefix '{}'",
                    node_name, entity.destination_table, ontology.table_prefix
                )));
            }

            ontology.nodes.insert(node_name.clone(), entity);
            node_names.push(node_name.clone());
        }

        node_names.sort();
        ontology.domains.insert(
            domain_name.clone(),
            DomainInfo {
                name: domain_name.clone(),
                description: domain.description.clone().unwrap_or_default(),
                node_names,
            },
        );
    }

    for (edge_name, edge_path) in &schema.edges {
        let content = reader.read(edge_path)?;
        let edge_def: EdgeYaml = parse_yaml(&content, edge_path)?;

        let entities = edge_def.to_entities(edge_name.clone(), ontology.edge_table());

        for entity in &entities {
            if !ontology.nodes.contains_key(&entity.source_kind) {
                return Err(OntologyError::Validation(format!(
                    "edge '{}' references unknown source node '{}'",
                    edge_name, entity.source_kind
                )));
            }
            if !ontology.nodes.contains_key(&entity.target_kind) {
                return Err(OntologyError::Validation(format!(
                    "edge '{}' references unknown target node '{}'",
                    edge_name, entity.target_kind
                )));
            }
            if !ontology
                .edge_table_configs
                .contains_key(&entity.destination_table)
            {
                return Err(OntologyError::Validation(format!(
                    "edge '{}' references unknown edge table '{}'",
                    edge_name, entity.destination_table
                )));
            }
        }

        if ontology.edges.contains_key(edge_name) {
            return Err(OntologyError::Validation(format!(
                "duplicate edge definition: '{edge_name}'"
            )));
        }
        ontology.edges.insert(edge_name.clone(), entities);

        if let Some(desc) = &edge_def.description {
            ontology
                .edge_descriptions
                .insert(edge_name.clone(), desc.clone());
        }

        let etl_configs = edge_def.into_etl_configs(&etl_settings)?;
        if !etl_configs.is_empty() {
            ontology
                .edge_etl_configs
                .insert(edge_name.clone(), etl_configs);
        }
    }

    // Auto-derive denormalized properties from central denormalization list.
    let has_denorm = !denormalization_entries.is_empty();

    for entry in &denormalization_entries {
        let node = ontology.nodes.get(&entry.node).ok_or_else(|| {
            OntologyError::Validation(format!("denormalization: unknown node '{}'", entry.node))
        })?;
        let field = node
            .fields
            .iter()
            .find(|f| f.name == entry.property)
            .ok_or_else(|| {
                OntologyError::Validation(format!(
                    "denormalization: unknown property '{}' on node '{}'",
                    entry.property, entry.node
                ))
            })?;
        let enum_values = field.enum_values.clone();

        let tag_key = entry
            .column_alias
            .as_deref()
            .or(denorm_default_as.as_deref())
            .unwrap_or(&entry.property)
            .to_string();

        for (edge_name, variants) in &ontology.edges {
            for variant in variants {
                let mut directions = Vec::new();
                if variant.source_kind == entry.node {
                    directions.push(crate::entities::DenormDirection::Source);
                }
                if variant.target_kind == entry.node {
                    directions.push(crate::entities::DenormDirection::Target);
                }
                for direction in directions {
                    let edge_column = match direction {
                        crate::entities::DenormDirection::Source => "source_tags",
                        crate::entities::DenormDirection::Target => "target_tags",
                    }
                    .to_string();

                    let already_exists = ontology.denormalized_properties.iter().any(|dp| {
                        dp.relationship_kind == *edge_name
                            && dp.node_kind == entry.node
                            && dp.property_name == entry.property
                            && dp.direction == direction
                    });
                    if !already_exists {
                        ontology.denormalized_properties.push(
                            crate::entities::DenormalizedProperty {
                                relationship_kind: edge_name.clone(),
                                node_kind: entry.node.clone(),
                                property_name: entry.property.clone(),
                                direction,
                                edge_column,
                                tag_key: tag_key.clone(),
                                enum_values: enum_values.clone(),
                            },
                        );
                    }
                }
            }
        }
    }

    // Auto-populate denormalized Array columns and text indexes on all edge tables.
    let auto_columns: Vec<crate::entities::StorageColumn> = if has_denorm {
        vec![
            crate::entities::StorageColumn {
                name: "source_tags".to_string(),
                ch_type: "Array(LowCardinality(String))".to_string(),
                default: None,
                codec: None,
            },
            crate::entities::StorageColumn {
                name: "target_tags".to_string(),
                ch_type: "Array(LowCardinality(String))".to_string(),
                default: None,
                codec: None,
            },
        ]
    } else {
        vec![]
    };

    let auto_indexes: Vec<crate::entities::StorageIndex> = if has_denorm {
        vec![
            crate::entities::StorageIndex {
                name: "source_tags_idx".to_string(),
                column: "source_tags".to_string(),
                index_type: "text(tokenizer = 'array')".to_string(),
                granularity: 64,
            },
            crate::entities::StorageIndex {
                name: "target_tags_idx".to_string(),
                column: "target_tags".to_string(),
                index_type: "text(tokenizer = 'array')".to_string(),
                granularity: 64,
            },
        ]
    } else {
        vec![]
    };

    for config in ontology.edge_table_configs.values_mut() {
        config.storage.denormalized_columns = auto_columns.clone();
        config.storage.denormalized_indexes = auto_indexes.clone();
        config.storage.denormalized_projections = vec![];
    }

    // Resolve skip_security_filter_for_entities → physical table names.
    for entity_name in &schema.settings.skip_security_filter_for_entities {
        let node = ontology.nodes.get(entity_name).ok_or_else(|| {
            OntologyError::Validation(format!(
                "skip_security_filter_for_entities: unknown entity '{entity_name}'"
            ))
        })?;
        ontology
            .skip_security_filter_for_tables
            .push(node.destination_table.clone());
    }

    // Validate and store local_db entity settings.
    if let Some(local_db) = schema.settings.local_db {
        for entry in local_db.entities {
            let node = ontology.nodes.get(&entry.name).ok_or_else(|| {
                OntologyError::Validation(format!(
                    "local_db.entities: unknown entity '{}'",
                    entry.name
                ))
            })?;

            // Validate exclude_properties reference actual fields.
            let field_names: std::collections::HashSet<&str> =
                node.fields.iter().map(|f| f.name.as_str()).collect();
            for prop in &entry.exclude_properties {
                if !field_names.contains(prop.as_str()) {
                    return Err(OntologyError::Validation(format!(
                        "local_db.entities: exclude_properties entry '{}' \
                         is not a declared property of '{}'",
                        prop, entry.name
                    )));
                }
            }

            ontology
                .local_entities
                .insert(entry.name, entry.exclude_properties);
        }

        if let Some(edge_table) = local_db.edge_table {
            // Validate no duplicate column names.
            let mut seen = std::collections::HashSet::new();
            for col in &edge_table.columns {
                if !seen.insert(&col.name) {
                    return Err(OntologyError::Validation(format!(
                        "local_db.edge_table: duplicate column name '{}'",
                        col.name
                    )));
                }
            }

            ontology.local_edge_table_name = Some(edge_table.name);
            ontology.local_edge_columns = edge_table
                .columns
                .into_iter()
                .map(|c| EdgeColumn {
                    name: c.name,
                    data_type: c.data_type,
                })
                .collect();
        }
    }

    // Load auxiliary tables.
    ontology.auxiliary_tables = schema
        .settings
        .auxiliary_tables
        .into_iter()
        .map(|t| crate::entities::AuxiliaryTable {
            name: t.name,
            columns: t
                .columns
                .into_iter()
                .map(|c| crate::entities::AuxiliaryColumn {
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.nullable,
                    codec: c.codec,
                    default: c.default,
                })
                .collect(),
            order_by: t.order_by,
            version_only_engine: t.version_only_engine,
            version_type: t.version_type,
            projections: t
                .projections
                .into_iter()
                .map(node::convert_storage_projection)
                .collect(),
        })
        .collect();

    // Validate storage columns match declared properties.
    validate_storage_columns(&ontology)?;

    Ok(ontology)
}

/// Checks that every node's storage columns correspond 1:1 with its
/// non-virtual properties. Catches drift between the logical schema
/// (properties) and physical schema (storage).
fn validate_storage_columns(ontology: &crate::Ontology) -> Result<(), OntologyError> {
    for node in ontology.nodes() {
        if node.storage.columns.is_empty() {
            continue;
        }

        let property_names: Vec<&str> = node
            .fields
            .iter()
            .filter(|f| !f.is_virtual())
            .map(|f| f.name.as_str())
            .collect();

        let storage_names: Vec<String> = node
            .storage
            .columns
            .iter()
            .map(|c| c.name.trim_matches('`').to_string())
            .collect();

        for storage_col in &storage_names {
            if !property_names.contains(&storage_col.as_str()) {
                return Err(OntologyError::Validation(format!(
                    "{}: storage column '{}' has no matching property",
                    node.name, storage_col
                )));
            }
        }

        for prop in &property_names {
            if !storage_names.iter().any(|s| s == prop) {
                return Err(OntologyError::Validation(format!(
                    "{}: property '{}' has no matching storage column",
                    node.name, prop
                )));
            }
        }
    }

    Ok(())
}
