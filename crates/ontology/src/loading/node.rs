use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

use crate::OntologyError;
use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::entities::{
    DataType, EdgeEndpoint, EdgeEndpointType, EdgeSourceEtlConfig, EnumType, Field,
    FieldSelectivity, FieldSource, NodeEntity, NodeStorage, NodeStyle, RedactionConfig,
    StorageIndex, StorageProjection, TraversalPathKind, TraversalPathLookupSpec, VirtualSource,
};
use crate::etl::{
    DEFAULT_TRANSFORM, EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope, PathResolution,
    ReindexSource,
};

use super::EtlSettings;

const GENERATED_QUERY: &str = "generated";

#[derive(Debug, Deserialize)]
pub(crate) struct NodeYaml {
    /// Mirrors the `schema.yaml` registry key, which is the value the loader
    /// actually reads for node identity; this field is documentation only.
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
    indexer: Option<IndexerYaml>,
    #[serde(default)]
    pipelines: Vec<PipelineYaml>,
    #[serde(default)]
    redaction: Option<RedactionConfig>,
    #[serde(default)]
    style: NodeStyle,
    #[serde(default)]
    storage: Option<NodeStorageYaml>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IndexerYaml {
    #[serde(default)]
    reindex: Option<String>,
    /// Explicit reindex trigger tables. Required, and only allowed, when
    /// `reindex: use_specified_tables`.
    #[serde(default)]
    tables: Vec<ReindexOnYaml>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PipelineYaml {
    name: String,
    extract: ExtractYaml,
    transform: TransformYaml,
}

#[derive(Debug, Deserialize)]
struct ExtractYaml {
    #[serde(rename = "type")]
    source_type: ExtractSourceYaml,
    /// `tables[0]` is the extraction source (and partition-probe base); the
    /// rest document secondary tables the query reads (joins, enrichment).
    tables: Vec<String>,
    order_by: Vec<String>,
    query: ExtractQueryYaml,
    /// Extra WHERE predicate on the source scan. Only standalone edges
    /// support it today.
    #[serde(default)]
    filter: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExtractSourceYaml {
    ClickHouse,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExtractQueryYaml {
    Marker(String),
    Authored(AuthoredQueryYaml),
}

#[derive(Debug, Deserialize)]
struct AuthoredQueryYaml {
    select: String,
    from: String,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    watermark: Option<String>,
    #[serde(default)]
    deleted: Option<String>,
    #[serde(default)]
    traversal_path_filter: Option<String>,
    #[serde(default)]
    table_alias: Option<String>,
    #[serde(default)]
    page_join: Option<Box<PageJoinYaml>>,
}

#[derive(Debug, Deserialize)]
struct TransformYaml {
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    edges: Vec<TransformEdgeYaml>,
}

#[derive(Debug, Deserialize)]
struct TransformEdgeYaml {
    from: EndpointYaml,
    to: EndpointYaml,
    label: String,
    #[serde(default)]
    array_field: Option<String>,
    #[serde(default)]
    mutable: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EndpointYaml {
    field: String,
    kind: EndpointKindYaml,
    /// Columns to enrich from this endpoint's node datalake table. Only
    /// standalone edges support it today.
    #[serde(default)]
    enrich: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EndpointKindYaml {
    Literal(String),
    Derived {
        derive: String,
        mapping: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum ReindexOnYaml {
    Bare(String),
    Detailed(DetailedReindexYaml),
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DetailedReindexYaml {
    table: String,
    #[serde(default)]
    traversal_path: Option<PathResolutionYaml>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PathResolutionYaml {
    #[serde(default)]
    column: Option<String>,
    #[serde(default)]
    dictionary: Option<String>,
    #[serde(default)]
    key_column: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PageJoinYaml {
    table: String,
    alias: String,
    fk_column: String,
    select: Vec<String>,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    watermark: Option<String>,
    #[serde(default)]
    traversal_path_column: Option<String>,
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
    #[serde(default)]
    binary: bool,
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

                if prop_def.binary && prop_def.data_type != DataType::String {
                    return Err(OntologyError::Validation(format!(
                        "property '{prop_name}' on node '{name}': \
                         'binary: true' is only valid for 'type: string', got {:?}",
                        prop_def.data_type
                    )));
                }

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
                    binary: prop_def.binary,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

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

        let node_scope = if self.global {
            EtlScope::Global
        } else {
            EtlScope::Namespaced
        };
        let indexer = self.indexer;
        if let Some(indexer) = &indexer {
            indexer.validate(&name)?;
            if self.pipelines.is_empty() {
                return Err(OntologyError::Validation(format!(
                    "node '{name}' declares an indexer block but no pipelines"
                )));
            }
        }
        if self.pipelines.len() > 1 {
            return Err(OntologyError::Validation(format!(
                "node '{name}' declares {} pipelines; nodes support exactly one",
                self.pipelines.len()
            )));
        }
        let etl = self
            .pipelines
            .into_iter()
            .next()
            .map(|pipeline| {
                if pipeline.transform_type() != DEFAULT_TRANSFORM {
                    return Err(OntologyError::Validation(format!(
                        "node '{name}' sets transform '{}'; custom transforms are only for derived entities",
                        pipeline.transform_type()
                    )));
                }
                pipeline.into_config(&name, etl_settings, node_scope, indexer.as_ref())
            })
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

impl PipelineYaml {
    pub(crate) fn transform_type(&self) -> &str {
        &self.transform.type_name
    }

    /// Lowers a node or derived-entity pipeline to the legacy `EtlConfig`.
    /// The caller validates the transform kind (nodes require `datafusion`,
    /// derived entities a custom name).
    pub(crate) fn into_config(
        self,
        entity_name: &str,
        etl_settings: &EtlSettings,
        scope: EtlScope,
        indexer: Option<&IndexerYaml>,
    ) -> Result<EtlConfig, OntologyError> {
        if self.name != entity_name {
            return Err(OntologyError::Validation(format!(
                "entity '{entity_name}' declares pipeline '{}'; node and derived pipelines must be named after their entity",
                self.name
            )));
        }
        if self.transform.type_name != DEFAULT_TRANSFORM && !self.transform.edges.is_empty() {
            return Err(OntologyError::Validation(format!(
                "entity '{entity_name}': transform '{}' does not take edge mappings",
                self.transform.type_name
            )));
        }
        let edges = convert_transform_edges(entity_name, self.transform.edges)?;
        let Some(source) = self.extract.tables.first().cloned() else {
            return Err(OntologyError::Validation(format!(
                "entity '{entity_name}': extract.tables cannot be empty"
            )));
        };
        let reindex_on = resolve_reindex_on(entity_name, indexer, scope, &source)?;
        self.extract
            .into_config(entity_name, etl_settings, scope, source, reindex_on, edges)
    }

    pub(crate) fn into_edge_config(
        self,
        relationship_kind: &str,
        etl_settings: &EtlSettings,
        indexer: Option<&IndexerYaml>,
    ) -> Result<EdgeSourceEtlConfig, OntologyError> {
        let PipelineYaml {
            name,
            extract,
            transform,
        } = self;
        if transform.type_name != DEFAULT_TRANSFORM {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline '{name}' sets transform '{}'; standalone edges require '{DEFAULT_TRANSFORM}'",
                transform.type_name
            )));
        }
        let mapping = match <[TransformEdgeYaml; 1]>::try_from(transform.edges) {
            Ok([mapping]) => mapping,
            Err(edges) => {
                return Err(OntologyError::Validation(format!(
                    "edge '{relationship_kind}' pipeline '{name}' must declare exactly one edge mapping, found {}",
                    edges.len()
                )));
            }
        };
        if mapping.label != relationship_kind {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline '{name}' declares label '{}'; the label must match the relationship kind",
                mapping.label
            )));
        }
        if mapping.array_field.is_some() || mapping.mutable {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline '{name}': standalone edges do not support 'array_field' or 'mutable'"
            )));
        }
        let ExtractSourceYaml::ClickHouse = extract.source_type;
        let Some(source) = extract.tables.first().cloned() else {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline '{name}': extract.tables cannot be empty"
            )));
        };
        if extract.order_by.is_empty() {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline '{name}': extract.order_by cannot be empty"
            )));
        }
        match &extract.query {
            ExtractQueryYaml::Marker(marker) if marker == GENERATED_QUERY => {}
            _ => {
                return Err(OntologyError::Validation(format!(
                    "edge '{relationship_kind}' pipeline '{name}': standalone edges require 'query: generated'"
                )));
            }
        }
        let reindex_on =
            resolve_reindex_on(relationship_kind, indexer, EtlScope::Namespaced, &source)?;
        let config = EdgeSourceEtlConfig {
            scope: EtlScope::Namespaced,
            source,
            watermark: etl_settings.watermark.clone(),
            deleted: etl_settings.deleted.clone(),
            order_by: extract.order_by,
            filter: extract.filter,
            reindex_on,
            from: mapping.from.into_endpoint(),
            to: mapping.to.into_endpoint(),
        };
        // Pipeline names are handler names and checkpoint keys; the YAML spells
        // the composed name out, but the composition stays the contract.
        let composed = crate::pipelines::standalone_edge_pipeline_name(relationship_kind, &config);
        if name != composed {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline is named '{name}' but the composed checkpoint key is '{composed}'"
            )));
        }
        Ok(config)
    }
}

impl EndpointYaml {
    fn into_endpoint(self) -> EdgeEndpoint {
        let node_type = match self.kind {
            EndpointKindYaml::Literal(kind) => EdgeEndpointType::Literal(kind),
            EndpointKindYaml::Derived { derive, mapping } => EdgeEndpointType::Column {
                column: derive,
                type_mapping: mapping,
            },
        };
        EdgeEndpoint {
            id_column: self.field,
            node_type,
            enrich: self.enrich,
        }
    }
}

/// Rebuilds the legacy FK-keyed edge mappings from explicit from/to endpoint
/// pairs. One endpoint must be the node itself (`field: id` with the node's
/// kind); the other carries the FK column. The node on the FK-less side being
/// `from` or `to` is what the legacy model called direction.
fn convert_transform_edges(
    node_name: &str,
    edges: Vec<TransformEdgeYaml>,
) -> Result<BTreeMap<String, Vec<EdgeMapping>>, OntologyError> {
    let mut result: BTreeMap<String, Vec<EdgeMapping>> = BTreeMap::new();
    let mut seen: HashSet<(String, String, EdgeDirection)> = HashSet::new();
    for edge in edges {
        let label = edge.label.clone();
        let (column, mapping) = convert_transform_edge(node_name, edge)?;
        if !seen.insert((
            column.clone(),
            mapping.relationship_kind.clone(),
            mapping.direction,
        )) {
            return Err(OntologyError::Validation(format!(
                "node '{node_name}': duplicate edge (label={label}, field={column})"
            )));
        }
        result.entry(column).or_default().push(mapping);
    }
    Ok(result)
}

fn convert_transform_edge(
    node_name: &str,
    edge: TransformEdgeYaml,
) -> Result<(String, EdgeMapping), OntologyError> {
    let TransformEdgeYaml {
        from,
        to,
        label,
        array_field,
        mutable,
    } = edge;
    let is_self = |ep: &EndpointYaml| {
        ep.field == DEFAULT_PRIMARY_KEY
            && matches!(&ep.kind, EndpointKindYaml::Literal(kind) if kind == node_name)
    };
    let (direction, fk_endpoint, self_endpoint) = match (is_self(&from), is_self(&to)) {
        (true, false) => (EdgeDirection::Outgoing, to, from),
        (false, true) => (EdgeDirection::Incoming, from, to),
        (true, true) => {
            return Err(OntologyError::Validation(format!(
                "node '{node_name}' edge '{label}': both endpoints are the node's id column"
            )));
        }
        (false, false) => {
            return Err(OntologyError::Validation(format!(
                "node '{node_name}' edge '{label}': one endpoint must be the node itself \
                 (field: {DEFAULT_PRIMARY_KEY}, kind: {node_name})"
            )));
        }
    };
    if !fk_endpoint.enrich.is_empty() || !self_endpoint.enrich.is_empty() {
        return Err(OntologyError::Validation(format!(
            "node '{node_name}' edge '{label}': 'enrich' is only supported on standalone edges"
        )));
    }
    let target = match fk_endpoint.kind {
        EndpointKindYaml::Literal(kind) => EdgeTarget::Literal(kind),
        EndpointKindYaml::Derived { derive, mapping } => EdgeTarget::Column {
            column: derive,
            type_mapping: mapping,
        },
    };
    Ok((
        fk_endpoint.field,
        EdgeMapping {
            target,
            relationship_kind: label,
            direction,
            delimiter: None,
            array_field,
            array: false,
            mutable,
        },
    ))
}

fn resolve_reindex_on(
    entity_name: &str,
    indexer: Option<&IndexerYaml>,
    scope: EtlScope,
    source: &str,
) -> Result<Vec<ReindexSource>, OntologyError> {
    let specified = indexer
        .map(IndexerYaml::specified_reindex)
        .unwrap_or_default();
    convert_reindex_on(
        entity_name,
        specified,
        (scope == EtlScope::Namespaced).then_some(source),
    )
}

impl IndexerYaml {
    pub(crate) fn validate(&self, entity_name: &str) -> Result<(), OntologyError> {
        match self.reindex.as_deref() {
            None | Some("use_source_tables") => {
                if self.tables.is_empty() {
                    Ok(())
                } else {
                    Err(OntologyError::Validation(format!(
                        "entity '{entity_name}' declares indexer.tables but reindex is \
                         'use_source_tables'; set reindex to 'use_specified_tables' to list \
                         explicit reindex trigger tables"
                    )))
                }
            }
            Some("use_specified_tables") => {
                if self.tables.is_empty() {
                    Err(OntologyError::Validation(format!(
                        "entity '{entity_name}' sets indexer.reindex 'use_specified_tables' but \
                         declares no indexer.tables"
                    )))
                } else {
                    Ok(())
                }
            }
            Some(value) => Err(OntologyError::Validation(format!(
                "entity '{entity_name}' sets unsupported indexer.reindex '{value}'"
            ))),
        }
    }

    fn specified_reindex(&self) -> Vec<ReindexOnYaml> {
        self.tables.clone()
    }
}

pub(crate) fn convert_reindex_on(
    entity_name: &str,
    raw: Vec<ReindexOnYaml>,
    default_table: Option<&str>,
) -> Result<Vec<ReindexSource>, OntologyError> {
    let raw = if raw.is_empty() {
        default_table
            .map(|table| vec![ReindexOnYaml::Bare(table.to_string())])
            .unwrap_or_default()
    } else {
        raw
    };

    raw.into_iter()
        .map(|entry| match entry {
            ReindexOnYaml::Bare(table) => Ok(ReindexSource {
                table,
                target: entity_name.to_string(),
                traversal_path: PathResolution::Column(TRAVERSAL_PATH_COLUMN.to_string()),
            }),
            ReindexOnYaml::Detailed(detailed) => Ok(ReindexSource {
                table: detailed.table,
                target: entity_name.to_string(),
                traversal_path: convert_path_resolution(entity_name, detailed.traversal_path)?,
            }),
        })
        .collect()
}

fn convert_path_resolution(
    entity_name: &str,
    raw: Option<PathResolutionYaml>,
) -> Result<PathResolution, OntologyError> {
    let Some(raw) = raw else {
        return Ok(PathResolution::Column(TRAVERSAL_PATH_COLUMN.to_string()));
    };

    match (raw.column, raw.dictionary, raw.key_column) {
        (Some(column), None, None) => Ok(PathResolution::Column(column)),
        (None, Some(dictionary), Some(key_column)) => Ok(PathResolution::Dictionary {
            dictionary,
            key_column,
        }),
        (Some(_), Some(_), _) => Err(OntologyError::Validation(format!(
            "{entity_name}: reindex_on.traversal_path must use column or dictionary, not both"
        ))),
        (None, Some(_), None) => Err(OntologyError::Validation(format!(
            "{entity_name}: reindex_on.traversal_path.dictionary requires key_column"
        ))),
        (None, None, Some(_)) => Err(OntologyError::Validation(format!(
            "{entity_name}: reindex_on.traversal_path.key_column requires dictionary"
        ))),
        (Some(_), None, Some(_)) => Err(OntologyError::Validation(format!(
            "{entity_name}: reindex_on.traversal_path.column cannot use key_column"
        ))),
        (None, None, None) => Ok(PathResolution::Column(TRAVERSAL_PATH_COLUMN.to_string())),
    }
}

/// Replaces `{{watermark_column}}` and `{{deleted_column}}` placeholders with
/// the configured column names, rejecting ETL SQL that hardcodes either literal.
///
/// Both literals are checked on the raw input *before* any substitution so that
/// the check doesn't false-positive on the rendered output (substitution
/// reintroduces the literal).
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

fn render_optional(
    entity: &str,
    field: &str,
    raw: Option<String>,
    watermark: &str,
    deleted: &str,
) -> Result<Option<String>, OntologyError> {
    raw.map(|s| render_etl_placeholders(entity, field, &s, watermark, deleted))
        .transpose()
}

impl ExtractYaml {
    fn into_config(
        self,
        entity_name: &str,
        etl_settings: &EtlSettings,
        scope: EtlScope,
        source: String,
        reindex_on: Vec<ReindexSource>,
        edges: BTreeMap<String, Vec<EdgeMapping>>,
    ) -> Result<EtlConfig, OntologyError> {
        let ExtractSourceYaml::ClickHouse = self.source_type;
        let wm = &etl_settings.watermark;
        let del = &etl_settings.deleted;
        if self.order_by.is_empty() {
            return Err(OntologyError::Validation(format!(
                "entity '{entity_name}': extract.order_by cannot be empty"
            )));
        }
        if self.filter.is_some() {
            return Err(OntologyError::Validation(format!(
                "entity '{entity_name}': extract.filter is only supported on standalone edges"
            )));
        }
        match self.query {
            ExtractQueryYaml::Marker(marker) if marker == GENERATED_QUERY => Ok(EtlConfig::Table {
                scope,
                source,
                watermark: wm.clone(),
                deleted: del.clone(),
                order_by: self.order_by,
                reindex_on,
                edges,
            }),
            ExtractQueryYaml::Marker(other) => Err(OntologyError::Validation(format!(
                "entity '{entity_name}': unsupported extract.query '{other}'"
            ))),
            ExtractQueryYaml::Authored(query) => {
                let select =
                    render_etl_placeholders(entity_name, "select", &query.select, wm, del)?;
                let from = render_etl_placeholders(entity_name, "from", &query.from, wm, del)?;
                let where_clause =
                    render_optional(entity_name, "where", query.where_clause, wm, del)?;
                let watermark = match query.watermark {
                    Some(w) => render_etl_placeholders(entity_name, "watermark", &w, wm, del)?,
                    None => wm.clone(),
                };
                let deleted = match query.deleted {
                    Some(d) => render_etl_placeholders(entity_name, "deleted", &d, wm, del)?,
                    None => del.clone(),
                };
                let traversal_path_filter = render_optional(
                    entity_name,
                    "traversal_path_filter",
                    query.traversal_path_filter,
                    wm,
                    del,
                )?;

                let page_join = query
                    .page_join
                    .map(|pj| {
                        let pj = *pj;
                        let pj_where = render_optional(
                            entity_name,
                            "page_join.where",
                            pj.where_clause,
                            wm,
                            del,
                        )?;
                        let pj_watermark = match pj.watermark {
                            Some(w) => render_etl_placeholders(
                                entity_name,
                                "page_join.watermark",
                                &w,
                                wm,
                                del,
                            )?,
                            None => wm.clone(),
                        };
                        Ok(Box::new(crate::etl::PageJoin {
                            table: pj.table,
                            alias: pj.alias,
                            fk_column: pj.fk_column,
                            select: pj.select,
                            where_clause: pj_where,
                            watermark: pj_watermark,
                            traversal_path_column: pj.traversal_path_column,
                        }))
                    })
                    .transpose()?;

                Ok(EtlConfig::Query {
                    scope,
                    source,
                    select,
                    from,
                    where_clause,
                    watermark,
                    deleted,
                    order_by: self.order_by,
                    reindex_on,
                    traversal_path_filter,
                    table_alias: query.table_alias,
                    page_join,
                    edges,
                })
            }
        }
    }
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
        let ontology = Ontology::load_embedded().expect("embedded ontology should load");
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

    #[test]
    fn every_namespaced_entity_declares_reindex_on() {
        let ontology = Ontology::load_embedded().expect("embedded ontology should load");
        let missing_node_sources: Vec<&str> = ontology
            .nodes()
            .filter_map(|node| {
                let etl = node.etl.as_ref()?;
                (etl.scope() == EtlScope::Namespaced && etl.reindex_on().is_empty())
                    .then_some(node.name.as_str())
            })
            .collect();
        assert!(missing_node_sources.is_empty(), "{missing_node_sources:?}");

        let missing_derived_sources: Vec<&str> = ontology
            .derived_entities()
            .filter_map(|entity| {
                (entity.etl.scope() == EtlScope::Namespaced && entity.etl.reindex_on().is_empty())
                    .then_some(entity.name.as_str())
            })
            .collect();
        assert!(
            missing_derived_sources.is_empty(),
            "{missing_derived_sources:?}"
        );

        let missing_edge_sources: Vec<&str> = ontology
            .edge_etl_configs()
            .filter_map(|(relationship_kind, config)| {
                (config.scope == EtlScope::Namespaced && config.reindex_on.is_empty())
                    .then_some(relationship_kind)
            })
            .collect();
        assert!(missing_edge_sources.is_empty(), "{missing_edge_sources:?}");

        let system_note = ontology
            .derived_entities()
            .find(|entity| entity.name == "SystemNote")
            .expect("SystemNote derived entity");
        let system_note_tables: Vec<&str> = system_note
            .etl
            .reindex_on()
            .iter()
            .map(|source_table| source_table.table.as_str())
            .collect();
        assert!(system_note_tables.contains(&"siphon_notes"));
        assert!(system_note_tables.contains(&"siphon_system_note_metadata"));

        let has_label = ontology.get_edge_etl("HAS_LABEL").unwrap();
        assert_eq!(has_label[0].reindex_on[0].table, "siphon_label_links");
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
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [siphon_test]
                  order_by: [id]
                  query: generated
                transform:
                  type: system_notes
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

    fn parse_etl_yaml(yaml: &str) -> Result<EtlConfig, OntologyError> {
        let node = parse_test_node(yaml)?;
        Ok(node.etl.expect("etl block expected"))
    }

    #[test]
    fn reindex_on_defaults_to_primary_source_and_traversal_path_column() {
        let etl = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        )
        .expect("source table should default from the extract source table");

        let [source_table] = etl.reindex_on() else {
            panic!("expected one source table");
        };
        assert_eq!(source_table.table, "source_table");
        assert_eq!(
            source_table.traversal_path,
            PathResolution::Column("traversal_path".to_string())
        );
    }

    #[test]
    fn reindex_on_accepts_bare_table_names() {
        let etl = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            indexer:
              reindex: use_specified_tables
              tables: [merge_requests, siphon_merge_request_metrics]
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [merge_requests]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        )
        .expect("bare table names should parse");

        let tables: Vec<&str> = etl
            .reindex_on()
            .iter()
            .map(|source| source.table.as_str())
            .collect();
        assert_eq!(tables, ["merge_requests", "siphon_merge_request_metrics"]);
        assert!(
            etl.reindex_on().iter().all(|source| source.traversal_path
                == PathResolution::Column("traversal_path".to_string()))
        );
    }

    #[test]
    fn reindex_on_accepts_dictionary_traversal_path() {
        let etl = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            indexer:
              reindex: use_specified_tables
              tables:
                - table: siphon_projects
                  traversal_path:
                    dictionary: project_traversal_paths_dict
                    key_column: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [project_namespace_traversal_paths]
                  order_by: [id]
                  query:
                    select: "id, traversal_path"
                    from: "project_namespace_traversal_paths"
                transform:
                  type: datafusion
            "#,
        )
        .expect("dictionary source table should parse");

        let [source_table] = etl.reindex_on() else {
            panic!("expected one source table");
        };
        assert_eq!(
            source_table.traversal_path,
            PathResolution::Dictionary {
                dictionary: "project_traversal_paths_dict".to_string(),
                key_column: "id".to_string()
            }
        );
    }

    #[test]
    fn reindex_on_rejects_ambiguous_traversal_path() {
        let result = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            indexer:
              reindex: use_specified_tables
              tables:
                - table: source_table
                  traversal_path:
                    column: traversal_path
                    dictionary: traversal_paths_dict
                    key_column: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("ambiguous traversal path should fail");
        assert!(err.to_string().contains("column or dictionary"));
    }

    #[test]
    fn fk_edges_accept_single_mapping() {
        let etl = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
                  edges:
                    - from: { field: id, kind: TestNode }
                      to: { field: project_id, kind: Project }
                      label: IN_PROJECT
            "#,
        )
        .expect("single mapping should parse");

        let mappings = etl.edges().get("project_id").expect("project_id present");
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].relationship_kind, "IN_PROJECT");
        assert_eq!(mappings[0].direction, EdgeDirection::Outgoing);
    }

    #[test]
    fn fk_edges_accept_multiple_mappings_per_column() {
        let etl = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
                  edges:
                    - from: { field: id, kind: TestNode }
                      to: { field: pipeline_id, kind: Pipeline }
                      label: IN_PIPELINE
                    - from: { field: pipeline_id, kind: Pipeline }
                      to: { field: id, kind: TestNode }
                      label: HAS_JOB
            "#,
        )
        .expect("repeated FK fields should parse");

        let mappings = etl.edges().get("pipeline_id").expect("pipeline_id present");
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].relationship_kind, "IN_PIPELINE");
        assert_eq!(mappings[0].direction, EdgeDirection::Outgoing);
        assert_eq!(mappings[1].relationship_kind, "HAS_JOB");
        assert_eq!(mappings[1].direction, EdgeDirection::Incoming);
    }

    #[test]
    fn fk_edges_reject_duplicate_emission_on_same_column() {
        let result = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
                  edges:
                    - from: { field: id, kind: TestNode }
                      to: { field: pipeline_id, kind: Pipeline }
                      label: IN_PIPELINE
                    - from: { field: id, kind: TestNode }
                      to: { field: pipeline_id, kind: Pipeline }
                      label: IN_PIPELINE
            "#,
        );
        let err = result
            .expect_err("duplicate emission should error")
            .to_string();
        assert!(err.contains("duplicate"), "got: {err}");
        assert!(err.contains("IN_PIPELINE"), "got: {err}");
    }

    #[test]
    fn fk_edges_flatten_via_edge_mappings_iter() {
        let etl = parse_etl_yaml(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
                  edges:
                    - from: { field: id, kind: TestNode }
                      to: { field: pipeline_id, kind: Pipeline }
                      label: IN_PIPELINE
                    - from: { field: pipeline_id, kind: Pipeline }
                      to: { field: id, kind: TestNode }
                      label: HAS_JOB
                    - from: { field: id, kind: TestNode }
                      to: { field: project_id, kind: Project }
                      label: IN_PROJECT
            "#,
        )
        .expect("mixed forms should parse");

        let flattened: Vec<(&str, &str)> = etl
            .edge_mappings()
            .map(|(col, m)| (col.as_str(), m.relationship_kind.as_str()))
            .collect();
        assert_eq!(
            flattened,
            vec![
                ("pipeline_id", "IN_PIPELINE"),
                ("pipeline_id", "HAS_JOB"),
                ("project_id", "IN_PROJECT"),
            ]
        );
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

    #[test]
    fn hardcoded_watermark_in_select_is_rejected() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "argMax(col, _siphon_watermark) AS col"
                    from: source_table
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("hardcoded watermark should be rejected");
        assert!(
            err.to_string().contains("hardcodes watermark column"),
            "got: {err}"
        );
        assert!(
            err.to_string().contains("{{watermark_column}}"),
            "error should mention the placeholder, got: {err}"
        );
    }

    #[test]
    fn watermark_placeholder_in_aliased_watermark_renders_correctly() {
        let node = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "id"
                    from: source_table AS t
                    watermark: "t.{{watermark_column}}"
                    table_alias: t
                transform:
                  type: datafusion
            "#,
        )
        .expect("placeholder should be accepted");
        let etl = node.etl.unwrap();
        assert_eq!(etl.watermark(), "t._siphon_watermark");
    }

    #[test]
    fn watermark_placeholder_in_from_renders_correctly() {
        let node = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "id"
                    from: "source_table AS t JOIN (SELECT argMax(x, {{watermark_column}}) FROM y GROUP BY id) z ON t.id = z.id"
                transform:
                  type: datafusion
            "#,
        )
        .expect("placeholder in from should be accepted");
        let EtlConfig::Query { from, .. } = node.etl.unwrap() else {
            panic!("expected Query");
        };
        assert!(
            from.contains("_siphon_watermark"),
            "placeholder should be rendered: {from}"
        );
        assert!(
            !from.contains("{{watermark_column}}"),
            "placeholder should not remain: {from}"
        );
    }

    #[test]
    fn unresolved_placeholder_is_rejected() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "argMax(col, {{typo_column}}) AS col"
                    from: source_table
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("unresolved placeholder should be rejected");
        assert!(
            err.to_string().contains("unresolved placeholder"),
            "got: {err}"
        );
    }

    #[test]
    fn binary_flag_on_non_string_field_is_rejected() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
                binary: true
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("binary on int64 should be rejected");
        assert!(
            err.to_string()
                .contains("'binary: true' is only valid for 'type: string'"),
            "got: {err}"
        );
    }

    #[test]
    fn hardcoded_deleted_in_where_is_rejected() {
        let result = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "id"
                    from: source_table
                    where: "_siphon_deleted = false"
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("hardcoded deleted should be rejected");
        assert!(
            err.to_string().contains("hardcodes deleted column"),
            "got: {err}"
        );
        assert!(
            err.to_string().contains("{{deleted_column}}"),
            "error should mention the placeholder, got: {err}"
        );
    }

    #[test]
    fn deleted_placeholder_renders_correctly() {
        let node = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "id"
                    from: source_table AS t
                    where: "t.{{deleted_column}} = false"
                    deleted: "t.{{deleted_column}}"
                transform:
                  type: datafusion
            "#,
        )
        .expect("deleted placeholder should be accepted");
        let EtlConfig::Query {
            where_clause,
            deleted,
            ..
        } = node.etl.unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(where_clause.unwrap(), "t._siphon_deleted = false");
        assert_eq!(deleted, "t._siphon_deleted");
    }

    #[test]
    fn both_placeholders_render_in_same_field() {
        let node = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "argMax({{deleted_column}}, {{watermark_column}}) AS deleted"
                    from: source_table
                transform:
                  type: datafusion
            "#,
        )
        .expect("both placeholders in one field should be accepted");
        let EtlConfig::Query { select, .. } = node.etl.unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(
            select,
            "argMax(_siphon_deleted, _siphon_watermark) AS deleted"
        );
    }

    #[test]
    fn page_join_watermark_defaults_to_etl_settings() {
        let node = parse_test_node(
            r#"
            node_type: entity
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [id]
                  query:
                    select: "id"
                    from: source_table
                    page_join:
                      table: joined_table
                      alias: jt
                      fk_column: source_id
                      select: [extra_col]
                transform:
                  type: datafusion
            "#,
        )
        .expect("page_join without watermark should use default");
        let EtlConfig::Query { page_join, .. } = node.etl.unwrap() else {
            panic!("expected Query");
        };
        let pj = page_join.expect("page_join should be present");
        assert_eq!(pj.watermark, "_siphon_watermark");
    }
}
