use serde::Deserialize;
use std::collections::BTreeMap;

use crate::OntologyError;
use crate::entities::{EdgeEntity, EdgeVariantScope};
use crate::etl::{EtlScope, Pipeline, ReindexSource};

use super::node::{IndexerYaml, PipelineYaml, convert_reindex_on};
use super::{EtlSettings, ReadOntologyFile};

#[derive(Debug, Deserialize)]
pub(crate) struct EdgeYaml {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    variants: Vec<EdgeVariantYaml>,
    #[serde(default)]
    indexer: Option<IndexerYaml>,
    #[serde(default)]
    pipelines: Vec<PipelineYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EdgeVariantYaml {
    #[serde(default, rename = "description")]
    _description: Option<String>,
    from_node: EdgeNodeRef,
    to_node: EdgeNodeRef,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    fk_column: Option<String>,
    #[serde(default)]
    scope: Option<EdgeVariantScope>,
}

#[derive(Debug, Deserialize)]
struct EdgeNodeRef {
    #[serde(rename = "type")]
    node_type: String,
    id: String,
}

impl EdgeYaml {
    pub(crate) fn to_entities(
        &self,
        relationship_kind: String,
        default_table: &str,
        node_domains: &BTreeMap<String, String>,
        domain_tables: &BTreeMap<String, String>,
    ) -> Vec<EdgeEntity> {
        self.variants
            .iter()
            .map(|variant| {
                let domain_table = node_domains
                    .get(&variant.from_node.node_type)
                    .filter(|domain| node_domains.get(&variant.to_node.node_type) == Some(*domain))
                    .and_then(|domain| domain_tables.get(domain));
                let destination_table = variant
                    .table
                    .as_ref()
                    .or(self.table.as_ref())
                    .or(domain_table)
                    .map_or_else(|| default_table.to_string(), Clone::clone);

                EdgeEntity {
                    relationship_kind: relationship_kind.clone(),
                    source: variant.from_node.id.clone(),
                    source_kind: variant.from_node.node_type.clone(),
                    target: variant.to_node.id.clone(),
                    target_kind: variant.to_node.node_type.clone(),
                    destination_table,
                    fk_column: variant.fk_column.clone(),
                    scope: variant.scope,
                }
            })
            .collect()
    }

    pub(crate) fn into_pipelines(
        self,
        relationship_kind: &str,
        yaml_path: &str,
        etl_settings: &EtlSettings,
        reader: &impl ReadOntologyFile,
    ) -> Result<(Vec<Pipeline>, Vec<ReindexSource>), OntologyError> {
        if let Some(indexer) = &self.indexer {
            indexer.validate(relationship_kind)?;
        }
        let pipelines = self
            .pipelines
            .into_iter()
            .map(|p| {
                p.into_edge_pipeline(
                    relationship_kind,
                    yaml_path,
                    etl_settings,
                    EtlScope::Namespaced,
                    reader,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reindex_on = convert_reindex_on(
            relationship_kind,
            self.indexer.map(|i| i.reindex).unwrap_or_default(),
        )?;
        Ok((pipelines, reindex_on))
    }
}
