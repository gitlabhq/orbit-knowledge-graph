mod edge;
mod node;
mod schema;

use rust_embed::Embed;
use serde::Deserialize;
use std::path::Path;

use crate::entities::DomainInfo;
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

    let mut ontology = Ontology::new();
    ontology.schema_version = schema.schema_version.unwrap_or_default();
    ontology.table_prefix = schema.settings.table_prefix;
    ontology.edge_table = schema.settings.edge_table;
    ontology.default_entity_sort_key = schema.settings.default_entity_sort_key;
    ontology.edge_sort_key = schema.settings.edge_sort_key;

    let etl_settings = EtlSettings {
        watermark: schema.settings.etl.default_watermark,
        deleted: schema.settings.etl.default_deleted,
        order_by: schema.settings.etl.default_etl_order_by,
    };
    ontology.etl_settings = etl_settings.clone();

    if !ontology.edge_table.starts_with(&ontology.table_prefix) {
        return Err(OntologyError::Validation(format!(
            "edge_table '{}' does not start with table_prefix '{}'",
            ontology.edge_table, ontology.table_prefix
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

        let entities = edge_def.to_entities(edge_name.clone());

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
        }

        ontology.edges.insert(edge_name.clone(), entities);

        if let Some(desc) = &edge_def.description {
            ontology
                .edge_descriptions
                .insert(edge_name.clone(), desc.clone());
        }

        if let Some(etl_config) = edge_def.into_etl_config(&etl_settings)? {
            ontology
                .edge_etl_configs
                .insert(edge_name.clone(), etl_config);
        }
    }

    Ok(ontology)
}
