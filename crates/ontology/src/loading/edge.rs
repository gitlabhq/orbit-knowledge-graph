use serde::Deserialize;
use std::collections::BTreeMap;

use crate::OntologyError;
use crate::entities::{
    EdgeEndpoint, EdgeEndpointType, EdgeEntity, EdgeSourceEtlConfig, EdgeVariantScope,
};
use crate::etl::EtlScope;

use super::EtlSettings;
use super::node::{ReindexOnYaml, convert_reindex_on, render_etl_placeholders};

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

#[derive(Debug, Deserialize)]
struct EdgeEtlYaml {
    scope: EtlScope,
    source: String,
    /// Optional raw `FROM` expression (join or aliased subquery) scanned instead
    /// of `source` directly. See [`EdgeSourceEtlConfig::from_table`].
    #[serde(default)]
    from_table: Option<String>,
    #[serde(default)]
    watermark: Option<String>,
    #[serde(default)]
    deleted: Option<String>,
    order_by: Vec<String>,
    #[serde(rename = "where", default)]
    filter: Option<String>,
    #[serde(default)]
    reindex_on: Vec<ReindexOnYaml>,
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
        let wm = &etl_settings.watermark;
        let del = &etl_settings.deleted;
        self.etl
            .into_iter()
            .map(|etl| {
                let from = convert_endpoint(etl.from, "from")?;
                let to = convert_endpoint(etl.to, "to")?;

                let watermark = match etl.watermark {
                    Some(w) => {
                        render_etl_placeholders(relationship_kind, "etl.watermark", &w, wm, del)?
                    }
                    None => wm.clone(),
                };
                let deleted = match etl.deleted {
                    Some(d) => {
                        render_etl_placeholders(relationship_kind, "etl.deleted", &d, wm, del)?
                    }
                    None => del.clone(),
                };
                let reindex_on = convert_reindex_on(
                    relationship_kind,
                    etl.reindex_on,
                    (etl.scope == EtlScope::Namespaced).then_some(etl.source.as_str()),
                )?;

                Ok(EdgeSourceEtlConfig {
                    scope: etl.scope,
                    source: etl.source,
                    from_table: etl.from_table,
                    watermark,
                    deleted,
                    order_by: etl.order_by,
                    filter: etl.filter,
                    reindex_on,
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

#[cfg(test)]
mod tests {
    use super::EdgeYaml;
    use crate::loading::EtlSettings;

    fn etl_settings() -> EtlSettings {
        EtlSettings {
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: Vec::new(),
        }
    }

    #[test]
    fn from_table_round_trips_into_etl_config() {
        // An attribute-join edge: the Package id is resolved by joining the
        // SBOM source to siphon_packages_packages on (name, type, version),
        // projected via the raw `from_table` subquery rather than an FK column.
        let yaml = r"
etl:
  - scope: namespaced
    source: siphon_sbom_occurrences_vulnerabilities
    from_table: >
      (SELECT pkg.id AS source_id, v.vulnerability_id AS target_id,
              pkg.traversal_path, v._siphon_replicated_at, v._siphon_deleted
       FROM gkg_sbom_vuln_components v
       INNER JOIN siphon_packages_packages pkg
         ON pkg.name = v.component_name AND pkg.package_type = v.package_type) AS e
    order_by: [traversal_path, source_id, target_id]
    from: { id: source_id, type: Package }
    to: { id: target_id, type: Vulnerability }
";
        let edge: EdgeYaml = serde_yaml::from_str(yaml).expect("parse edge yaml");
        let configs = edge
            .into_etl_configs("HAS_VULNERABILITY", &etl_settings())
            .expect("into_etl_configs");

        assert_eq!(configs.len(), 1);
        let cfg = &configs[0];
        let from_table = cfg
            .from_table
            .as_deref()
            .expect("from_table should be Some");
        assert!(from_table.contains("INNER JOIN siphon_packages_packages pkg"));
        assert!(from_table.contains("pkg.id AS source_id"));
        // `source` still names the concrete watermark/partitioning table.
        assert_eq!(cfg.source, "siphon_sbom_occurrences_vulnerabilities");
        assert_eq!(cfg.from.id_column, "source_id");
        assert_eq!(cfg.to.id_column, "target_id");
    }

    #[test]
    fn from_table_defaults_to_none_for_plain_fk_edge() {
        let yaml = r"
etl:
  - scope: namespaced
    source: siphon_packages_dependency_links
    order_by: [traversal_path, id]
    from: { id: package_id, type: Package }
    to: { id: dependency_id, type: Dependency }
";
        let edge: EdgeYaml = serde_yaml::from_str(yaml).expect("parse edge yaml");
        let configs = edge
            .into_etl_configs("DECLARES_DEPENDENCY", &etl_settings())
            .expect("into_etl_configs");

        assert_eq!(configs[0].from_table, None);
    }
}
