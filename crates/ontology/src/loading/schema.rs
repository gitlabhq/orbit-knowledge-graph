use crate::entities::DataType;
use crate::loading::node::{StorageColumnYaml, StorageIndexYaml, StorageProjectionYaml};
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

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EdgeColumnYaml {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EdgeTableYaml {
    pub sort_key: Vec<String>,
    pub columns: Vec<EdgeColumnYaml>,
    #[serde(default)]
    pub storage: Option<EdgeTableStorageYaml>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EdgeTableStorageYaml {
    #[serde(default)]
    pub index_granularity: Option<u32>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub columns: Vec<StorageColumnYaml>,
    #[serde(default)]
    pub indexes: Vec<StorageIndexYaml>,
    #[serde(default)]
    pub projections: Vec<StorageProjectionYaml>,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SettingsYaml {
    pub table_prefix: String,
    pub default_edge_table: String,
    pub default_entity_sort_key: Vec<String>,
    pub edge_tables: BTreeMap<String, EdgeTableYaml>,
    #[serde(default)]
    pub denormalization: Vec<DenormalizationEntryYaml>,
    #[serde(default)]
    pub statistics: Option<StatisticsYaml>,
    #[serde(default)]
    pub partition: Option<PartitionYaml>,
    pub internal_column_prefix: String,
    #[serde(default)]
    pub local_db: Option<LocalSettingsYaml>,
    pub etl: EtlSettingsYaml,
    #[serde(default)]
    pub auxiliary_tables: Vec<AuxiliaryTableYaml>,
    #[serde(default)]
    pub materialized_views: Vec<MaterializedViewYaml>,
    #[serde(default)]
    pub auxiliary_dictionaries: Vec<AuxiliaryDictionaryYaml>,
    #[serde(default)]
    pub gc_preserve_patterns: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct MaterializedViewYaml {
    pub name: String,
    #[serde(default)]
    pub versioned: bool,
    pub select_file: String,
    #[serde(flatten)]
    pub kind: MaterializedViewKindYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum MaterializedViewKindYaml {
    InsertTrigger {
        #[serde(default)]
        to_table: Option<String>,
        #[serde(default)]
        engine: Option<String>,
        #[serde(default)]
        engine_args: Vec<String>,
        #[serde(default)]
        order_by: Vec<String>,
        #[serde(default)]
        populate: bool,
    },
    Refreshable {
        append_to: String,
        refresh: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct StatisticsYaml {
    pub stats_table: String,
    pub histogram_table: String,
    pub token_table: String,
    pub dictionary: String,
    pub lifetime: DictionaryLifetimeYaml,
    pub histogram_buckets: u16,
    pub top_k_tokens: u16,
    pub partition_key: String,
    #[serde(default)]
    pub exclude: Vec<StatisticsExcludeYaml>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct StatisticsExcludeYaml {
    pub node: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct PartitionYaml {
    pub strategy: PartitionStrategyYaml,
    #[serde(default)]
    pub include_entities: Vec<String>,
    #[serde(default)]
    pub include_edge_tables: Vec<String>,
}

/// One optional block per strategy; exactly one must be set (enforced in the
/// loader). Serde's externally-tagged enums require a YAML `!tag`, which reads
/// worse than a named nested block, so this models the choice as optionals.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct PartitionStrategyYaml {
    #[serde(default)]
    pub hash_bucket: Option<HashBucketYaml>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HashBucketYaml {
    pub buckets: u16,
    pub column: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct DenormalizationEntryYaml {
    pub node: String,
    pub property: String,
    /// Edge column suffix. Defaults to `property` if omitted.
    /// The full column name is `{direction}_{as}`, e.g. `source_status`.
    #[serde(rename = "as", default)]
    pub column_alias: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct AuxiliaryTableYaml {
    pub name: String,
    #[serde(default = "default_true")]
    pub versioned: bool,
    pub columns: Vec<AuxiliaryColumnYaml>,
    pub order_by: Vec<String>,
    #[serde(default)]
    pub version_only_engine: bool,
    #[serde(default)]
    pub version_type: Option<String>,
    #[serde(default)]
    pub projections: Vec<StorageProjectionYaml>,
    #[serde(default = "default_true")]
    pub include_system_columns: bool,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub engine_args: Vec<String>,
    #[serde(default)]
    pub ttl: Option<String>,
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub(super) struct LocalSettingsYaml {
    #[serde(default)]
    pub entities: Vec<LocalEntityYaml>,
    #[serde(default)]
    pub edge_table: Option<LocalEdgeTableYaml>,
}

#[derive(Debug, Deserialize)]
pub(super) struct LocalEdgeTableYaml {
    pub name: String,
    pub columns: Vec<EdgeColumnYaml>,
}

#[derive(Debug, Deserialize)]
pub(super) struct LocalEntityYaml {
    pub name: String,
    #[serde(default)]
    pub exclude_properties: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct AuxiliaryColumnYaml {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub codec: Option<Vec<String>>,
    #[serde(default)]
    pub default: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct AuxiliaryDictionaryYaml {
    pub name: String,
    pub source_table: String,
    pub key: String,
    #[serde(default)]
    pub key_type: Option<DataType>,
    pub attributes: Vec<AuxiliaryColumnYaml>,
    pub layout: DictionaryLayoutYaml,
    pub lifetime: DictionaryLifetimeYaml,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct DictionaryLayoutYaml {
    pub kind: String,
    #[serde(default)]
    pub size_in_cells: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct DictionaryLifetimeYaml {
    pub min: u32,
    pub max: u32,
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
    #[serde(default)]
    pub derived: BTreeMap<String, String>,
}
