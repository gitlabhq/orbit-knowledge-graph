use serde::Deserialize;

use crate::OntologyError;
use crate::entities::{EdgeEntity, EdgeSourceEtlConfig, EdgeVariantScope};

use super::EtlSettings;
use super::node::{IndexerYaml, PipelineYaml};

#[derive(Debug, Deserialize)]
pub(crate) struct EdgeYaml {
    #[serde(default)]
    pub description: Option<String>,
    /// Optional override for the ClickHouse table storing this edge type.
    /// Defaults to the global `edge_table` from settings.
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
struct EdgeVariantYaml {
    from_node: EdgeNodeRef,
    to_node: EdgeNodeRef,
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
    ) -> Vec<EdgeEntity> {
        let table = self.table.as_deref().unwrap_or(default_table).to_string();
        self.variants
            .iter()
            .map(|v| EdgeEntity {
                relationship_kind: relationship_kind.clone(),
                source: v.from_node.id.clone(),
                source_kind: v.from_node.node_type.clone(),
                target: v.to_node.id.clone(),
                target_kind: v.to_node.node_type.clone(),
                destination_table: table.clone(),
                fk_column: v.fk_column.clone(),
                scope: v.scope,
            })
            .collect()
    }

    pub(crate) fn into_etl_configs(
        self,
        relationship_kind: &str,
        etl_settings: &EtlSettings,
    ) -> Result<Vec<EdgeSourceEtlConfig>, OntologyError> {
        if let Some(indexer) = &self.indexer {
            indexer.validate(relationship_kind)?;
            if self.pipelines.is_empty() {
                return Err(OntologyError::Validation(format!(
                    "edge '{relationship_kind}' declares an indexer block but no pipelines"
                )));
            }
        }
        let indexer = self.indexer;
        self.pipelines
            .into_iter()
            .map(|p| p.into_edge_config(relationship_kind, etl_settings, indexer.as_ref()))
            .collect()
    }
}
