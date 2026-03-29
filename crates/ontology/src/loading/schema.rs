use crate::entities::DataType;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub(super) struct SchemaYaml {
    #[serde(default)]
    pub schema_version: Option<String>,
    pub settings: SettingsYaml,
    #[serde(default)]
    pub domains: BTreeMap<String, DomainYaml>,
    #[serde(default)]
    pub edges: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct EdgeColumnYaml {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
}

#[derive(Debug, Deserialize)]
pub(super) struct SettingsYaml {
    pub table_prefix: String,
    pub edge_table: String,
    pub default_entity_sort_key: Vec<String>,
    pub edge_sort_key: Vec<String>,
    pub edge_columns: Vec<EdgeColumnYaml>,
    #[serde(default)]
    pub skip_security_filter_for_entities: Vec<String>,
    pub etl: EtlSettingsYaml,
}

#[derive(Debug, Deserialize)]
pub(super) struct EtlSettingsYaml {
    pub default_watermark: String,
    pub default_deleted: String,
    pub default_etl_order_by: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DomainYaml {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub nodes: BTreeMap<String, String>,
}
