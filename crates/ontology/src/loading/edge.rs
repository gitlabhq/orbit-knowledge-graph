use serde::Deserialize;
use std::collections::BTreeMap;

use crate::OntologyError;
use crate::entities::{EdgeEndpoint, EdgeEndpointType, EdgeEntity, EdgeFk, EdgeSourceEtlConfig};
use crate::etl::EtlScope;

use super::EtlSettings;

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
    etl: Vec<EdgeEtlYaml>,
}

#[derive(Debug, Deserialize)]
struct EdgeVariantYaml {
    from_node: EdgeNodeRef,
    to_node: EdgeNodeRef,
    #[serde(default)]
    fk: Option<EdgeFkYaml>,
}

#[derive(Debug, Deserialize)]
struct EdgeFkYaml {
    node: String,
    column: String,
}

#[derive(Debug, Deserialize)]
struct EdgeNodeRef {
    #[serde(rename = "type")]
    node_type: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct EdgeEtlYaml {
    scope: EtlScope,
    source: String,
    #[serde(default)]
    watermark: Option<String>,
    #[serde(default)]
    deleted: Option<String>,
    order_by: Vec<String>,
    from: EdgeEndpointYaml,
    to: EdgeEndpointYaml,
}

#[derive(Debug, Deserialize)]
struct EdgeEndpointYaml {
    id: String,
    #[serde(rename = "type")]
    type_literal: Option<String>,
    #[serde(rename = "type_column")]
    type_column: Option<String>,
    #[serde(default)]
    type_mapping: BTreeMap<String, String>,
    /// Columns to enrich from this endpoint's node datalake table.
    #[serde(default)]
    enrich: Vec<String>,
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
                fk: v.fk.as_ref().map(|fk| EdgeFk {
                    node: fk.node.clone(),
                    column: fk.column.clone(),
                }),
            })
            .collect()
    }

    pub(crate) fn into_etl_configs(
        self,
        etl_settings: &EtlSettings,
    ) -> Result<Vec<EdgeSourceEtlConfig>, OntologyError> {
        self.etl
            .into_iter()
            .map(|etl| {
                let from = convert_endpoint(etl.from, "from")?;
                let to = convert_endpoint(etl.to, "to")?;

                let watermark = etl
                    .watermark
                    .unwrap_or_else(|| etl_settings.watermark.clone());
                let deleted = etl.deleted.unwrap_or_else(|| etl_settings.deleted.clone());

                Ok(EdgeSourceEtlConfig {
                    scope: etl.scope,
                    source: etl.source,
                    watermark,
                    deleted,
                    order_by: etl.order_by,
                    from,
                    to,
                })
            })
            .collect()
    }
}

fn convert_endpoint(
    ep: EdgeEndpointYaml,
    endpoint_name: &str,
) -> Result<EdgeEndpoint, OntologyError> {
    let node_type = match (ep.type_literal, ep.type_column) {
        (Some(lit), None) => EdgeEndpointType::Literal(lit),
        (None, Some(col)) => EdgeEndpointType::Column {
            column: col,
            type_mapping: ep.type_mapping,
        },
        (Some(_), Some(_)) => {
            return Err(OntologyError::Validation(format!(
                "edge source endpoint '{}': use 'type' or 'type_column', not both",
                endpoint_name
            )));
        }
        (None, None) => {
            return Err(OntologyError::Validation(format!(
                "edge source endpoint '{}': requires 'type' or 'type_column'",
                endpoint_name
            )));
        }
    };

    Ok(EdgeEndpoint {
        id_column: ep.id,
        node_type,
        enrich: ep.enrich,
    })
}
