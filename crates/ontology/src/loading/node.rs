use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

use crate::OntologyError;
use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::entities::{
    DataType, EnumType, Field, FieldSelectivity, FieldSource, NodeEntity, NodeStorage, NodeStyle,
    RedactionConfig, StorageIndex, StorageProjection, TraversalPathKind, TraversalPathLookupSpec,
    VirtualSource,
};
use crate::etl::{DEFAULT_TRANSFORM, EtlConfig, EtlScope};

use super::{EtlSettings, ReadOntologyFile};

#[derive(Debug, Deserialize)]
pub(crate) struct NodeYaml {
    /// Canonical entity name (e.g. `Project`, `MergeRequest`). Mirrors the
    /// `schema.yaml` registry key the loader actually reads for node identity;
    /// kept here as human-facing self-documentation so each node file states
    /// which entity it defines without cross-referencing `schema.yaml`.
    #[expect(
        dead_code,
        reason = "human-facing self-documentation; the entity name is read from the schema.yaml registry key, this field mirrors it for readability in the node file"
    )]
    node_type: String,
    domain: String,
    #[serde(default)]
    global: bool,
    #[serde(default)]
    description: String,
    #[serde(default)]
    label: String,
    destination_table: String,
    #[serde(default)]
    properties: IndexMap<String, PropertyYaml>,
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
    #[serde(default)]
    storage: Option<NodeStorageYaml>,
}

/// Two-tier ETL surface: declarative base (`source`/`scope`/`filter`) or a
/// complete SELECT in a sibling `query:` file. Edges live in `edges/*.yaml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EtlYaml {
    source: String,
    scope: EtlScope,
    #[serde(default)]
    order_by: Option<Vec<String>>,
    /// Plain WHERE on the base table; the only SQL string allowed in YAML.
    #[serde(default)]
    filter: Option<String>,
    /// Sibling .sql file emitting the property source columns plus the
    /// watermark/deleted columns; the engine wraps it for scope/window/page.
    #[serde(default)]
    query: Option<String>,
    /// Rust transform name; derived entities only.
    #[serde(default)]
    transform: Option<String>,
    /// Output columns of `query`; derived entities only (no properties to
    /// infer them from).
    #[serde(default)]
    columns: Option<Vec<String>>,
    /// Engine-owned enrichment joined below the page LIMIT (#830: a plain
    /// join in `query` would build the join table per batch, namespace-wide).
    #[serde(default)]
    page_join: Option<Box<PageJoinYaml>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageJoinYaml {
    table: String,
    alias: String,
    fk_column: String,
    select: Vec<String>,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    traversal_path_column: Option<String>,
}

impl EtlYaml {
    pub(crate) fn transform(&self) -> Option<&str> {
        self.transform.as_deref()
    }

    pub(crate) fn columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }
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
    #[serde(default)]
    admin_only: bool,
    #[serde(default)]
    selectivity: Option<FieldSelectivity>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    traversal_path_lookup: Option<TraversalPathLookupYaml>,
    #[serde(default)]
    mutable: bool,
    #[serde(default)]
    terminal_values: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct TraversalPathLookupYaml {
    kind: TraversalPathKind,
    #[serde(default)]
    dictionary: Option<String>,
    source_table: String,
    key_column: String,
}

impl PropertyYaml {
    fn default_like_allowed() -> bool {
        true
    }
    fn default_filterable() -> bool {
        true
    }
}

#[derive(Debug, Default, Deserialize)]
struct NodeStorageYaml {
    #[serde(default)]
    version_only_engine: bool,
    #[serde(default)]
    primary_key: Option<Vec<String>>,
    #[serde(default)]
    columns: Vec<StorageColumnYaml>,
    #[serde(default)]
    indexes: Vec<StorageIndexYaml>,
    #[serde(default)]
    projections: Vec<StorageProjectionYaml>,
    #[serde(default)]
    settings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StorageColumnYaml {
    pub name: String,
    /// Exact ClickHouse type, e.g. `"Int64"`, `"Nullable(String)"`, `"LowCardinality(String)"`.
    #[serde(rename = "type")]
    pub ch_type: String,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub codec: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StorageIndexYaml {
    pub name: String,
    pub column: String,
    #[serde(rename = "type")]
    pub index_type: String,
    #[serde(default = "default_granularity")]
    pub granularity: u32,
}

fn default_granularity() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum StorageProjectionYaml {
    #[serde(rename = "reorder")]
    Reorder { name: String, order_by: Vec<String> },
    #[serde(rename = "lightweight")]
    Lightweight {
        name: String,
        /// Raw ORDER BY columns. Mutually exclusive with `versioned_sort_key`.
        #[serde(default)]
        order_by: Vec<String>,
        /// Prefix columns for a dedup-compatible LWP. The table sort key and
        /// `_version` are appended automatically, producing an ORDER BY of
        /// `(prefix..., sort_key..., _version)`. Mutually exclusive with
        /// `order_by`.
        #[serde(default)]
        versioned_sort_key: Vec<String>,
    },
    #[serde(rename = "aggregate")]
    Aggregate {
        name: String,
        select: Vec<String>,
        group_by: Vec<String>,
    },
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
    /// Filter operators allowed on this virtual column. When omitted, all
    /// default virtual ops are allowed.
    #[serde(default)]
    allowed_ops: Vec<String>,
}

impl NodeYaml {
    pub(crate) fn into_entity(
        self,
        name: String,
        default_entity_sort_key: &[String],
        etl_settings: &EtlSettings,
        internal_column_prefix: &str,
        reader: &impl ReadOntologyFile,
        yaml_dir: &str,
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
                    (None, Some(v)) => {
                        let allowed_ops = if v.allowed_ops.is_empty() {
                            VirtualSource::DEFAULT_ALLOWED_OPS
                                .iter()
                                .map(|s| (*s).to_string())
                                .collect()
                        } else {
                            v.allowed_ops
                        };
                        FieldSource::Virtual(VirtualSource {
                            service: v.service,
                            lookup: v.lookup,
                            disabled: v.disabled,
                            depends_on: v.depends_on,
                            allowed_ops,
                        })
                    }
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

                let selectivity = prop_def
                    .selectivity
                    .unwrap_or_else(|| FieldSelectivity::from_data_type(prop_def.data_type));

                Ok(Field {
                    name: prop_name,
                    source,
                    data_type: prop_def.data_type,
                    nullable: prop_def.nullable,
                    enum_values: prop_def.values,
                    enum_type: prop_def.enum_type,
                    like_allowed: prop_def.like_allowed,
                    filterable: prop_def.filterable,
                    admin_only: prop_def.admin_only,
                    selectivity,
                    description: prop_def.description,
                    traversal_path_lookup: prop_def.traversal_path_lookup.map(|l| {
                        TraversalPathLookupSpec {
                            kind: l.kind,
                            dictionary: l.dictionary,
                            source_table: l.source_table,
                            key_column: l.key_column,
                        }
                    }),
                    mutable: prop_def.mutable,
                    terminal_values: prop_def.terminal_values,
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

        match self.etl.as_ref().and_then(|e| e.transform()) {
            Some(transform) if transform != DEFAULT_TRANSFORM => {
                return Err(OntologyError::Validation(format!(
                    "node '{name}' sets etl.transform '{transform}'; custom transforms are only for derived entities"
                )));
            }
            _ => {}
        }
        if self.etl.as_ref().is_some_and(|e| e.columns().is_some()) {
            return Err(OntologyError::Validation(format!(
                "node '{name}' sets etl.columns; output columns are inferred from properties \
                 (etl.columns is only for derived entities)"
            )));
        }

        let source_columns: Vec<String> = fields
            .iter()
            .filter_map(|f| f.column_name().map(str::to_string))
            .collect();

        let etl = self
            .etl
            .map(|e| e.into_config(&name, etl_settings, reader, yaml_dir, &source_columns))
            .transpose()?;

        let has_traversal_path = fields
            .iter()
            .any(|f| f.name == crate::constants::TRAVERSAL_PATH_COLUMN);

        // A global hub must be non-namespaced; a traversal_path would let elision drop a scope filter.
        if self.global && has_traversal_path {
            return Err(OntologyError::Validation(format!(
                "node '{name}' is `global: true` but declares a `traversal_path` column; global hubs must be non-namespaced"
            )));
        }

        let storage = convert_node_storage(self.storage.unwrap_or_default(), &sort_key);

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
            global: self.global,
            storage,
        })
    }
}

impl EtlYaml {
    /// Lowers the surface onto [`EtlConfig`]: bare → `Table`, `filter` →
    /// synthesized `Query`, `query:` file → `Query` over a derived table.
    pub(crate) fn into_config(
        self,
        entity_name: &str,
        etl_settings: &EtlSettings,
        reader: &impl ReadOntologyFile,
        yaml_dir: &str,
        select_columns: &[String],
    ) -> Result<EtlConfig, OntologyError> {
        let order_by = self
            .order_by
            .unwrap_or_else(|| etl_settings.order_by.clone());
        let page_join = self
            .page_join
            .map(|pj| convert_page_join(entity_name, *pj, etl_settings))
            .transpose()?
            .map(Box::new);

        match (self.filter, self.query) {
            (Some(_), Some(_)) => Err(OntologyError::Validation(format!(
                "entity '{entity_name}': 'filter' and 'query' are mutually exclusive; \
                 move the filter into the query file"
            ))),
            (None, None) => {
                if page_join.is_some() {
                    return Err(OntologyError::Validation(format!(
                        "entity '{entity_name}': 'page_join' requires 'filter' or 'query'"
                    )));
                }
                Ok(EtlConfig::Table {
                    scope: self.scope,
                    source: self.source,
                    watermark: etl_settings.watermark.clone(),
                    deleted: etl_settings.deleted.clone(),
                    order_by,
                    edges: BTreeMap::new(),
                })
            }
            (Some(filter), None) => {
                let filter = render_etl_placeholders(
                    entity_name,
                    "filter",
                    &filter,
                    &etl_settings.watermark,
                    &etl_settings.deleted,
                )?;
                let select = synthesized_select(entity_name, select_columns)?;
                Ok(EtlConfig::Query {
                    scope: self.scope,
                    source: self.source.clone(),
                    select,
                    from: self.source,
                    where_clause: Some(filter),
                    watermark: etl_settings.watermark.clone(),
                    deleted: etl_settings.deleted.clone(),
                    order_by,
                    traversal_path_filter: None,
                    table_alias: None,
                    page_join,
                    edges: BTreeMap::new(),
                })
            }
            (None, Some(query_file)) => {
                let path = if yaml_dir.is_empty() {
                    query_file.clone()
                } else {
                    format!("{yaml_dir}/{query_file}")
                };
                let raw_sql = reader.read(&path)?;
                let raw_sql = raw_sql.trim();
                if raw_sql.is_empty() {
                    return Err(OntologyError::Validation(format!(
                        "entity '{entity_name}': query file '{path}' is empty"
                    )));
                }
                let sql = render_etl_placeholders(
                    entity_name,
                    &path,
                    raw_sql,
                    &etl_settings.watermark,
                    &etl_settings.deleted,
                )?;
                let select = synthesized_select(entity_name, select_columns)?;
                Ok(EtlConfig::Query {
                    scope: self.scope,
                    source: self.source,
                    select,
                    from: format!("(\n{sql}\n) AS src"),
                    where_clause: None,
                    watermark: etl_settings.watermark.clone(),
                    deleted: etl_settings.deleted.clone(),
                    order_by,
                    traversal_path_filter: None,
                    table_alias: None,
                    page_join,
                    edges: BTreeMap::new(),
                })
            }
        }
    }
}

fn convert_page_join(
    entity_name: &str,
    pj: PageJoinYaml,
    etl_settings: &EtlSettings,
) -> Result<crate::etl::PageJoin, OntologyError> {
    let where_clause = pj
        .where_clause
        .map(|w| {
            render_etl_placeholders(
                entity_name,
                "page_join.where",
                &w,
                &etl_settings.watermark,
                &etl_settings.deleted,
            )
        })
        .transpose()?;
    Ok(crate::etl::PageJoin {
        table: pj.table,
        alias: pj.alias,
        fk_column: pj.fk_column,
        select: pj.select,
        where_clause,
        watermark: etl_settings.watermark.clone(),
        traversal_path_column: pj.traversal_path_column,
    })
}

/// Substitutes the watermark/deleted placeholders; hardcoded names are
/// rejected (checked pre-substitution) so a rename stays a schema.yaml edit.
pub(crate) fn render_etl_placeholders(
    entity: &str,
    field: &str,
    raw: &str,
    watermark: &str,
    deleted: &str,
) -> Result<String, OntologyError> {
    if raw.contains(watermark) {
        return Err(OntologyError::Validation(format!(
            "entity '{entity}' field '{field}' hardcodes watermark column \
             '{watermark}'; use {{{{watermark_column}}}} placeholder instead"
        )));
    }
    if raw.contains(deleted) {
        return Err(OntologyError::Validation(format!(
            "entity '{entity}' field '{field}' hardcodes deleted column \
             '{deleted}'; use {{{{deleted_column}}}} placeholder instead"
        )));
    }
    let rendered = raw
        .replace("{{watermark_column}}", watermark)
        .replace("{{deleted_column}}", deleted);
    if rendered.contains("{{") {
        return Err(OntologyError::Validation(format!(
            "entity '{entity}' field '{field}' contains unresolved placeholder '{{{{..}}}}'",
        )));
    }
    Ok(rendered)
}

fn synthesized_select(
    entity_name: &str,
    select_columns: &[String],
) -> Result<String, OntologyError> {
    if select_columns.is_empty() {
        return Err(OntologyError::Validation(format!(
            "entity '{entity_name}': 'filter'/'query' ETL needs output columns \
             (declared properties, or 'columns' on a derived entity)"
        )));
    }
    Ok(select_columns.join(", "))
}

fn convert_node_storage(yaml: NodeStorageYaml, sort_key: &[String]) -> NodeStorage {
    NodeStorage {
        version_only_engine: yaml.version_only_engine,
        primary_key: yaml.primary_key,
        columns: yaml
            .columns
            .into_iter()
            .map(|col| crate::entities::StorageColumn {
                name: col.name,
                ch_type: col.ch_type,
                default: col.default,
                codec: col.codec,
            })
            .collect(),
        indexes: yaml
            .indexes
            .into_iter()
            .map(convert_storage_index)
            .collect(),
        projections: yaml
            .projections
            .into_iter()
            .map(|p| convert_storage_projection(p, sort_key))
            .collect(),
        settings: yaml.settings,
    }
}

pub(crate) fn convert_storage_index(yaml: StorageIndexYaml) -> StorageIndex {
    StorageIndex {
        name: yaml.name,
        column: yaml.column,
        index_type: yaml.index_type,
        granularity: yaml.granularity,
    }
}

pub(crate) fn convert_storage_projection(
    yaml: StorageProjectionYaml,
    sort_key: &[String],
) -> StorageProjection {
    match yaml {
        StorageProjectionYaml::Reorder { name, order_by } => {
            StorageProjection::Reorder { name, order_by }
        }
        StorageProjectionYaml::Lightweight {
            name,
            order_by,
            versioned_sort_key,
        } => {
            let resolved = if !versioned_sort_key.is_empty() {
                let mut cols = versioned_sort_key;
                for col in sort_key {
                    if !cols.contains(col) {
                        cols.push(col.clone());
                    }
                }
                let version = crate::constants::VERSION_COLUMN.to_string();
                if !cols.contains(&version) {
                    cols.push(version);
                }
                cols
            } else {
                order_by
            };
            StorageProjection::Lightweight {
                name,
                order_by: resolved,
            }
        }
        StorageProjectionYaml::Aggregate {
            name,
            select,
            group_by,
        } => StorageProjection::Aggregate {
            name,
            select,
            group_by,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Ontology;

    fn test_etl_settings() -> EtlSettings {
        EtlSettings {
            watermark: crate::constants::siphon_watermark_column().to_string(),
            deleted: crate::constants::siphon_deleted_column().to_string(),
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

    struct TestReader(std::collections::HashMap<String, String>);

    impl ReadOntologyFile for TestReader {
        fn read(&self, path: &str) -> Result<String, OntologyError> {
            self.0.get(path).cloned().ok_or_else(|| OntologyError::Io {
                path: path.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing test file"),
            })
        }
    }

    fn parse_test_node_with_files(
        yaml: &str,
        files: &[(&str, &str)],
    ) -> Result<NodeEntity, OntologyError> {
        let node: NodeYaml = serde_yaml::from_str(yaml).unwrap();
        let reader = TestReader(
            files
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        node.into_entity(
            "TestNode".to_string(),
            &["id".to_string()],
            &test_etl_settings(),
            "_gkg_",
            &reader,
            "nodes/test",
        )
    }

    fn parse_test_node(yaml: &str) -> Result<NodeEntity, OntologyError> {
        parse_test_node_with_files(yaml, &[])
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
    fn global_node_with_traversal_path_is_rejected() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            global: true
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              traversal_path:
                type: string
                source: traversal_path
            "#,
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("global") && err.contains("traversal_path"),
            "got: {err}"
        );
    }

    #[test]
    fn global_defaults_false_and_parses_when_true() {
        let scoped = parse_test_node(
            "node_type: entity\ndomain: test\ndestination_table: gl_test\nproperties:\n  id: {type: int64, source: id}\n",
        )
        .unwrap();
        assert!(!scoped.global);

        let hub = parse_test_node(
            "node_type: entity\ndomain: test\nglobal: true\ndestination_table: gl_test\nproperties:\n  id: {type: int64, source: id}\n",
        )
        .unwrap();
        assert!(hub.global);
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
    fn node_rejects_custom_etl_transform() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            etl:
              source: siphon_test
              scope: namespaced
              transform: system_notes
            "#,
        );
        let err = result.expect_err("custom transform on a node should be rejected");
        assert!(
            err.to_string()
                .contains("custom transforms are only for derived entities"),
            "got: {err}"
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

    const PLAIN_NODE: &str = r#"
        node_type: entity
        domain: test
        destination_table: gl_test
        properties:
          id:
            type: int64
            source: id
          name:
            type: string
            source: name
          traversal_path:
            type: string
            source: traversal_path
    "#;

    fn node_yaml(etl: &str) -> String {
        format!("{PLAIN_NODE}\n{etl}")
    }

    #[test]
    fn bare_etl_becomes_table_config() {
        let node = parse_test_node(&node_yaml(
            r#"
        etl:
          source: siphon_test
          scope: namespaced
        "#,
        ))
        .expect("bare etl should parse");
        let etl = node.etl.unwrap();
        assert!(matches!(etl, EtlConfig::Table { .. }));
        assert_eq!(etl.source(), "siphon_test");
        assert_eq!(etl.watermark(), crate::constants::SIPHON_WATERMARK_COLUMN);
        assert_eq!(etl.order_by(), ["id"]);
        assert!(etl.edges().is_empty());
    }

    #[test]
    fn filter_synthesizes_query_over_property_columns() {
        let node = parse_test_node(&node_yaml(
            r#"
        etl:
          source: siphon_test
          scope: namespaced
          filter: "system = false"
        "#,
        ))
        .expect("filter etl should parse");
        let EtlConfig::Query {
            select,
            from,
            where_clause,
            traversal_path_filter,
            ..
        } = node.etl.unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(select, "id, name, traversal_path");
        assert_eq!(from, "siphon_test");
        assert_eq!(where_clause.as_deref(), Some("system = false"));
        assert_eq!(traversal_path_filter, None);
    }

    #[test]
    fn query_file_wraps_sql_as_derived_table() {
        let node = parse_test_node_with_files(
            &node_yaml(
                r#"
        etl:
          source: siphon_test
          scope: namespaced
          query: test.sql
        "#,
            ),
            &[("nodes/test/test.sql", "SELECT 1 AS id\n")],
        )
        .expect("query etl should parse");
        let EtlConfig::Query { select, from, .. } = node.etl.unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(select, "id, name, traversal_path");
        assert_eq!(from, "(\nSELECT 1 AS id\n) AS src");
    }

    #[test]
    fn query_file_missing_is_an_error() {
        let err = parse_test_node(&node_yaml(
            r#"
        etl:
          source: siphon_test
          scope: namespaced
          query: missing.sql
        "#,
        ))
        .expect_err("missing query file should fail");
        assert!(err.to_string().contains("missing.sql"), "got: {err}");
    }

    #[test]
    fn query_file_renders_watermark_and_deleted_placeholders() {
        let node = parse_test_node_with_files(
            &node_yaml(
                r#"
        etl:
          source: siphon_test
          scope: namespaced
          query: test.sql
        "#,
            ),
            &[(
                "nodes/test/test.sql",
                "SELECT id, t.{{watermark_column}} AS {{watermark_column}}, \
                 t.{{deleted_column}} AS {{deleted_column}} FROM t",
            )],
        )
        .expect("placeholders in sql file should render");
        let EtlConfig::Query { from, .. } = node.etl.unwrap() else {
            panic!("expected Query");
        };
        assert!(
            from.contains("t._siphon_watermark AS _siphon_watermark"),
            "got: {from}"
        );
        assert!(
            !from.contains("{{"),
            "placeholders must be resolved: {from}"
        );
    }

    #[test]
    fn query_file_hardcoding_watermark_is_rejected() {
        let err = parse_test_node_with_files(
            &node_yaml(
                r#"
        etl:
          source: siphon_test
          scope: namespaced
          query: test.sql
        "#,
            ),
            &[("nodes/test/test.sql", "SELECT id, _siphon_watermark FROM t")],
        )
        .expect_err("hardcoded watermark in sql file should fail");
        assert!(
            err.to_string().contains("hardcodes watermark column"),
            "got: {err}"
        );
    }

    #[test]
    fn query_file_with_unknown_placeholder_is_rejected() {
        let err = parse_test_node_with_files(
            &node_yaml(
                r#"
        etl:
          source: siphon_test
          scope: namespaced
          query: test.sql
        "#,
            ),
            &[(
                "nodes/test/test.sql",
                "SELECT argMax(x, {{typo_column}}) AS x FROM t",
            )],
        )
        .expect_err("unknown placeholder should fail");
        assert!(
            err.to_string().contains("unresolved placeholder"),
            "got: {err}"
        );
    }

    #[test]
    fn filter_and_query_are_mutually_exclusive() {
        let err = parse_test_node_with_files(
            &node_yaml(
                r#"
        etl:
          source: siphon_test
          scope: namespaced
          filter: "x = 1"
          query: test.sql
        "#,
            ),
            &[("nodes/test/test.sql", "SELECT 1 AS id")],
        )
        .expect_err("filter+query should fail");
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn node_rejects_etl_columns() {
        let err = parse_test_node(&node_yaml(
            r#"
        etl:
          source: siphon_test
          scope: namespaced
          columns: [id]
        "#,
        ))
        .expect_err("columns on a node should fail");
        assert!(
            err.to_string().contains("only for derived entities"),
            "got: {err}"
        );
    }

    #[test]
    fn legacy_etl_surface_is_rejected() {
        let result: Result<NodeYaml, _> = serde_yaml::from_str(&node_yaml(
            r#"
        etl:
          type: table
          source: siphon_test
          scope: namespaced
        "#,
        ));
        assert!(result.is_err(), "old 'type:' key must not silently parse");
    }

    #[test]
    fn lwp_versioned_sort_key_builds_full_ordering() {
        let sort_key = vec!["traversal_path".into(), "id".into()];
        let proj = convert_storage_projection(
            StorageProjectionYaml::Lightweight {
                name: "by_project_id".into(),
                order_by: vec![],
                versioned_sort_key: vec!["project_id".into()],
            },
            &sort_key,
        );
        match proj {
            StorageProjection::Lightweight { order_by, .. } => {
                assert_eq!(
                    order_by,
                    vec!["project_id", "traversal_path", "id", "_version"]
                );
            }
            _ => panic!("expected Lightweight"),
        }
    }

    #[test]
    fn lwp_versioned_sort_key_does_not_duplicate_overlap() {
        let sort_key = vec!["traversal_path".into(), "id".into()];
        let proj = convert_storage_projection(
            StorageProjectionYaml::Lightweight {
                name: "by_tp".into(),
                order_by: vec![],
                versioned_sort_key: vec!["traversal_path".into()],
            },
            &sort_key,
        );
        match proj {
            StorageProjection::Lightweight { order_by, .. } => {
                assert_eq!(order_by, vec!["traversal_path", "id", "_version"]);
            }
            _ => panic!("expected Lightweight"),
        }
    }

    #[test]
    fn lwp_versioned_sort_key_does_not_duplicate_version() {
        let sort_key = vec!["traversal_path".into(), "id".into()];
        let proj = convert_storage_projection(
            StorageProjectionYaml::Lightweight {
                name: "by_ver".into(),
                order_by: vec![],
                versioned_sort_key: vec!["project_id".into(), "_version".into()],
            },
            &sort_key,
        );
        match proj {
            StorageProjection::Lightweight { order_by, .. } => {
                assert_eq!(
                    order_by,
                    vec!["project_id", "_version", "traversal_path", "id"]
                );
            }
            _ => panic!("expected Lightweight"),
        }
    }

    #[test]
    fn lwp_raw_order_by_passes_through() {
        let sort_key = vec!["traversal_path".into(), "id".into()];
        let proj = convert_storage_projection(
            StorageProjectionYaml::Lightweight {
                name: "by_raw".into(),
                order_by: vec!["project_id".into()],
                versioned_sort_key: vec![],
            },
            &sort_key,
        );
        match proj {
            StorageProjection::Lightweight { order_by, .. } => {
                assert_eq!(order_by, vec!["project_id"]);
            }
            _ => panic!("expected Lightweight"),
        }
    }

    #[test]
    fn reorder_and_aggregate_pass_through_unchanged() {
        let sort_key = vec!["traversal_path".into(), "id".into()];

        let reorder = convert_storage_projection(
            StorageProjectionYaml::Reorder {
                name: "r".into(),
                order_by: vec!["col_a".into()],
            },
            &sort_key,
        );
        assert!(matches!(reorder, StorageProjection::Reorder { .. }));

        let agg = convert_storage_projection(
            StorageProjectionYaml::Aggregate {
                name: "a".into(),
                select: vec!["x".into()],
                group_by: vec!["y".into()],
            },
            &sort_key,
        );
        assert!(matches!(agg, StorageProjection::Aggregate { .. }));
    }
}
