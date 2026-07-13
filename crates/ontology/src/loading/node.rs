use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use crate::OntologyError;
use crate::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
use crate::entities::{
    DataType, EnumType, Field, FieldSelectivity, FieldSource, NodeEntity, NodeStorage, NodeStyle,
    RedactionConfig, StorageIndex, StorageProjection, TraversalPathKind, TraversalPathLookupSpec,
    VirtualSource,
};
use crate::etl::{
    ClickHouseExtract, DEFAULT_TRANSFORM, EdgeMapping, EtlScope, Extract, ExtractQuery, NodeRef,
    NodeRefKind, PathResolution, Pipeline, ReindexSource, Transform,
};

use super::{EtlSettings, ReadOntologyFile};

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
    tables: Vec<String>,
    order_by: Vec<String>,
    query: String,
    /// Extra `_batch` WHERE predicate appended to the traversal-path scope, e.g.
    /// `state = 5`. Used by `query: generated` edges and `query: generated` nodes.
    #[serde(default)]
    filter: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExtractSourceYaml {
    ClickHouse,
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
struct TransformYaml {
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    edges: Vec<EdgeMappingYaml>,
}

#[derive(Debug, Deserialize)]
struct EdgeMappingYaml {
    from: EndpointYaml,
    to: EndpointYaml,
    label: String,
    #[serde(default)]
    array_field: Option<String>,
    #[serde(default)]
    mutable: bool,
}

#[derive(Debug, Deserialize)]
struct EndpointYaml {
    field: String,
    kind: EndpointKindYaml,
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
        yaml_path: &str,
        default_entity_sort_key: &[String],
        etl_settings: &EtlSettings,
        internal_column_prefix: &str,
        reader: &impl ReadOntologyFile,
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
        if let Some(indexer) = &self.indexer {
            indexer.validate(&name)?;
        }
        let reindex = ReindexDirective::from_indexer(self.indexer.as_ref());
        let pipelines = self
            .pipelines
            .into_iter()
            .map(|p| {
                p.into_pipeline(
                    &name,
                    etl_settings,
                    node_scope,
                    reader,
                    PipelineOptions {
                        yaml_path,
                        allow_rust_transform: false,
                        is_derived: false,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reindex_on = reindex.resolve_reindex_sources(&name, &pipelines)?;
        for pipeline in &pipelines {
            if !matches!(pipeline.transform, Transform::DataFusion { .. }) {
                return Err(OntologyError::Validation(format!(
                    "node '{name}' sets transform '{}'; custom transforms are only for derived entities",
                    pipeline.name
                )));
            }
        }
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
            pipelines,
            reindex_on,
            redaction: self.redaction,
            style: self.style,
            has_traversal_path,
            global: self.global,
            storage,
        })
    }
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

    pub(crate) fn uses_source_tables(&self) -> bool {
        matches!(self.reindex.as_deref(), None | Some("use_source_tables"))
    }

    pub(crate) fn specified_reindex(&self) -> Vec<ReindexOnYaml> {
        self.tables.clone()
    }
}

/// Entity-level reindex configuration resolved from the `indexer` block. Reindex
/// is an indexer concern declared once per entity, and the pipeline model knows
/// nothing about it: either the trigger tables are derived from the pipelines'
/// source tables (`use_source_tables`), or listed explicitly under
/// `indexer.tables` (`use_specified_tables`).
#[derive(Debug, Clone, Default)]
pub(crate) struct ReindexDirective {
    use_source_tables: bool,
    specified: Vec<ReindexOnYaml>,
}

impl ReindexDirective {
    pub(crate) fn from_indexer(indexer: Option<&IndexerYaml>) -> Self {
        Self {
            use_source_tables: indexer.is_some_and(IndexerYaml::uses_source_tables),
            specified: indexer
                .map(IndexerYaml::specified_reindex)
                .unwrap_or_default(),
        }
    }

    /// Resolves this entity's reindex trigger tables. `use_specified_tables`
    /// takes the explicit list; `use_source_tables` derives the first source
    /// table of each namespaced pipeline (global entities do not participate in
    /// namespace-change reindexing).
    pub(crate) fn resolve_reindex_sources(
        self,
        entity_name: &str,
        pipelines: &[Pipeline],
    ) -> Result<Vec<ReindexSource>, OntologyError> {
        if !self.specified.is_empty() {
            return convert_reindex_on(entity_name, self.specified);
        }
        if !self.use_source_tables {
            return Ok(Vec::new());
        }
        Ok(pipelines
            .iter()
            .filter(|p| p.scope == EtlScope::Namespaced)
            .filter_map(|p| {
                let Extract::ClickHouse(extract) = &p.extract;
                extract.tables.first()
            })
            .map(|table| ReindexSource {
                table: table.clone(),
                target: entity_name.to_string(),
                traversal_path: default_path_resolution(),
            })
            .collect())
    }
}

pub(crate) struct PipelineOptions<'a> {
    pub yaml_path: &'a str,
    pub allow_rust_transform: bool,
    /// True only for derived-entity pipelines, which must carry authored SQL
    /// (their rows are neither node properties nor edge endpoints, so there is
    /// nothing for the indexer to generate a projection from).
    pub is_derived: bool,
}

impl PipelineYaml {
    pub(crate) fn into_pipeline(
        self,
        entity_name: &str,
        etl_settings: &EtlSettings,
        scope: EtlScope,
        reader: &impl ReadOntologyFile,
        options: PipelineOptions,
    ) -> Result<Pipeline, OntologyError> {
        let PipelineYaml {
            name,
            extract,
            transform,
        } = self;
        let transform = transform.into_transform()?;
        if matches!(transform, Transform::Rust(_)) && !options.allow_rust_transform {
            return Err(OntologyError::Validation(format!(
                "entity '{entity_name}' pipeline '{name}' uses a Rust transform outside derived entities"
            )));
        }
        build_pipeline(BuildPipeline {
            name,
            extract,
            transform,
            etl_settings,
            scope,
            reader,
            yaml_path: options.yaml_path,
            is_derived: options.is_derived,
        })
    }

    pub(crate) fn into_edge_pipeline(
        self,
        relationship_kind: &str,
        yaml_path: &str,
        etl_settings: &EtlSettings,
        scope: EtlScope,
        reader: &impl ReadOntologyFile,
    ) -> Result<Pipeline, OntologyError> {
        let PipelineYaml {
            name,
            extract,
            transform,
        } = self;
        let transform = transform.into_transform()?;
        // The indexer builds an edge extract from exactly one mapping (batch +
        // enrichment CTEs); more than one has no single-table shape to generate.
        let edge_count = match &transform {
            Transform::DataFusion { edges } => edges.len(),
            Transform::Rust(_) => 0,
        };
        if edge_count != 1 {
            return Err(OntologyError::Validation(format!(
                "edge '{relationship_kind}' pipeline '{name}' must declare exactly one \
                 datafusion edge mapping, found {edge_count}"
            )));
        }
        build_pipeline(BuildPipeline {
            name,
            extract,
            transform,
            etl_settings,
            scope,
            reader,
            yaml_path,
            is_derived: false,
        })
    }
}

struct BuildPipeline<'a, R: ReadOntologyFile> {
    name: String,
    extract: ExtractYaml,
    transform: Transform,
    etl_settings: &'a EtlSettings,
    scope: EtlScope,
    reader: &'a R,
    yaml_path: &'a str,
    is_derived: bool,
}

/// Builds the declarative `Pipeline` shared by node and edge construction. The
/// extract SQL itself is not produced here — the indexer generates it from this
/// model; only authored `.sql.j2` files are resolved (verbatim) at load time.
fn build_pipeline<R: ReadOntologyFile>(b: BuildPipeline<'_, R>) -> Result<Pipeline, OntologyError> {
    if b.extract.order_by.is_empty() {
        return Err(OntologyError::Validation(format!(
            "pipeline '{}': extract.order_by cannot be empty",
            b.name
        )));
    }
    if b.extract.tables.is_empty() {
        return Err(OntologyError::Validation(format!(
            "pipeline '{}': extract.tables cannot be empty",
            b.name
        )));
    }
    let generated = b.extract.query.as_str() == GENERATED_QUERY;
    // Derived-entity rows are neither node properties nor edge endpoints, so the
    // indexer has nothing to project them from; they must carry authored SQL.
    if b.is_derived && generated {
        return Err(OntologyError::Validation(format!(
            "pipeline '{}': derived entities require an authored .sql.j2 extract, not query: generated",
            b.name
        )));
    }
    if b.extract.filter.is_some() && !generated {
        return Err(OntologyError::Validation(format!(
            "pipeline '{}': extract.filter requires query: generated (authored .sql.j2 ignores it)",
            b.name
        )));
    }
    let query =
        b.extract
            .resolve_query(b.reader, &b.name, b.yaml_path, b.extract.filter.clone())?;
    // Sql pipelines recover their effective watermark/deleted from the authored
    // `.sql.j2` in the indexer; here both are always the settings defaults.
    let watermark = b.etl_settings.watermark.clone();
    let deleted = b.etl_settings.deleted.clone();
    let extract = match b.extract.source_type {
        ExtractSourceYaml::ClickHouse => Extract::ClickHouse(ClickHouseExtract {
            tables: b.extract.tables,
            order_by: b.extract.order_by,
            watermark,
            deleted,
            query,
        }),
    };
    Ok(Pipeline {
        name: b.name,
        scope: b.scope,
        extract,
        transform: b.transform,
    })
}

pub(crate) fn convert_reindex_on(
    entity_name: &str,
    entries: Vec<ReindexOnYaml>,
) -> Result<Vec<ReindexSource>, OntologyError> {
    entries
        .into_iter()
        .map(|entry| match entry {
            ReindexOnYaml::Bare(table) => Ok(ReindexSource {
                table,
                target: entity_name.to_string(),
                traversal_path: default_path_resolution(),
            }),
            ReindexOnYaml::Detailed(detailed) => Ok(ReindexSource {
                table: detailed.table,
                target: entity_name.to_string(),
                traversal_path: convert_path_resolution(entity_name, detailed.traversal_path)?,
            }),
        })
        .collect()
}

fn default_path_resolution() -> PathResolution {
    PathResolution::Column(TRAVERSAL_PATH_COLUMN.to_string())
}

fn convert_path_resolution(
    entity_name: &str,
    resolution: Option<PathResolutionYaml>,
) -> Result<PathResolution, OntologyError> {
    let Some(resolution) = resolution else {
        return Ok(default_path_resolution());
    };

    match (
        resolution.column,
        resolution.dictionary,
        resolution.key_column,
    ) {
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
        (None, None, None) => Ok(default_path_resolution()),
    }
}

impl ExtractYaml {
    /// Classifies the `query:` field. `generated` builds `ExtractQuery::Generated`
    /// (the indexer produces the SQL); a `.sql.j2` file is read and carried verbatim.
    /// No substitution or parsing happens here — that is the indexer's concern.
    fn resolve_query(
        &self,
        reader: &impl ReadOntologyFile,
        pipeline_name: &str,
        yaml_path: &str,
        filter: Option<String>,
    ) -> Result<ExtractQuery, OntologyError> {
        match self.query.as_str() {
            GENERATED_QUERY => Ok(ExtractQuery::Generated { filter }),
            file if file.ends_with(".sql.j2") => {
                let sql_path = Path::new(yaml_path)
                    .parent()
                    .map(|dir| dir.join(file))
                    .unwrap_or_else(|| Path::new(file).to_path_buf())
                    .to_string_lossy()
                    .replace('\\', "/");
                Ok(ExtractQuery::Sql(reader.read(&sql_path)?))
            }
            other => Err(OntologyError::Validation(format!(
                "pipeline '{pipeline_name}': unsupported extract.query '{other}'"
            ))),
        }
    }
}

impl TransformYaml {
    fn into_transform(self) -> Result<Transform, OntologyError> {
        if self.type_name == DEFAULT_TRANSFORM {
            let edges = self
                .edges
                .into_iter()
                .map(EdgeMappingYaml::into_mapping)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Transform::DataFusion { edges });
        }
        if !self.edges.is_empty() {
            return Err(OntologyError::Validation(format!(
                "Rust transform '{}' cannot declare datafusion edges",
                self.type_name
            )));
        }
        Ok(Transform::Rust(self.type_name))
    }
}

impl EdgeMappingYaml {
    fn into_mapping(self) -> Result<EdgeMapping, OntologyError> {
        Ok(EdgeMapping {
            label: self.label,
            source: self.from.into_node_ref()?,
            target: self.to.into_node_ref()?,
            array_field: self.array_field,
            mutable: self.mutable,
        })
    }
}

impl EndpointYaml {
    fn into_node_ref(self) -> Result<NodeRef, OntologyError> {
        Ok(NodeRef {
            field: self.field,
            kind: match self.kind {
                EndpointKindYaml::Literal(lit) => NodeRefKind::Literal(lit),
                EndpointKindYaml::Derived { derive, mapping } => NodeRefKind::Derived {
                    column: derive,
                    mapping,
                },
            },
            enrich: self.enrich,
        })
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

    struct EmptyReader;

    impl ReadOntologyFile for EmptyReader {
        fn read(&self, path: &str) -> Result<String, OntologyError> {
            Err(OntologyError::Io {
                path: path.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, path.to_string()),
            })
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

    fn parse_test_node(yaml: &str) -> Result<NodeEntity, OntologyError> {
        let node: NodeYaml = serde_yaml::from_str(yaml).unwrap();
        node.into_entity(
            "TestNode".to_string(),
            "nodes/test/test_node.yaml",
            &["id".to_string()],
            &test_etl_settings(),
            "_gkg_",
            &EmptyReader,
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

    #[test]
    fn node_rejects_empty_extract_tables() {
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
                  tables: []
                  order_by: [id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("empty extract.tables should be rejected");
        assert!(
            err.to_string().contains("extract.tables cannot be empty"),
            "got: {err}"
        );
    }

    #[test]
    fn node_rejects_filter_on_sql_query() {
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
                  query: test_node.sql
                  filter: "state = 5"
                transform:
                  type: datafusion
            "#,
        );
        let err = result.expect_err("filter on an authored .sql query should be rejected");
        assert!(
            err.to_string()
                .contains("extract.filter requires query: generated"),
            "got: {err}"
        );
    }

    #[test]
    fn node_rejects_rust_transform_pipeline() {
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
                  query: generated
                transform:
                  type: system_notes
            "#,
        );
        let err = result.expect_err("custom transform on a node should be rejected");
        assert!(
            err.to_string()
                .contains("uses a Rust transform outside derived entities"),
            "got: {err}"
        );
    }

    #[test]
    fn node_pipeline_converts_edges_and_generated_sql() {
        let node = parse_test_node(
            r#"
            node_type: TestNode
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              project_id:
                type: int64
                source: project_id
              traversal_path:
                type: string
                source: traversal_path
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [traversal_path, id]
                  query: generated
                transform:
                  type: datafusion
                  edges:
                    - from:
                        field: id
                        kind: TestNode
                      to:
                        field: project_id
                        kind: Project
                      label: IN_PROJECT
            "#,
        )
        .expect("pipeline should parse");
        let pipeline = node.pipelines.first().expect("pipeline should exist");
        assert_eq!(pipeline.name, "TestNode");
        let Extract::ClickHouse(extract) = &pipeline.extract;
        assert!(matches!(
            extract.query,
            ExtractQuery::Generated { filter: None }
        ));
        let Transform::DataFusion { edges } = &pipeline.transform else {
            panic!("expected datafusion transform");
        };
        assert_eq!(edges[0].label, "IN_PROJECT");
        assert!(
            matches!(edges[0].target.kind, NodeRefKind::Literal(ref kind) if kind == "Project")
        );
    }

    #[test]
    fn pipeline_reindex_on_defaults_to_traversal_path_column() {
        let node = parse_test_node(
            r#"
            node_type: TestNode
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              traversal_path:
                type: string
                source: traversal_path
            indexer:
              reindex: use_specified_tables
              tables: [source_table, source_details]
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [traversal_path, id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        )
        .expect("pipeline should parse");
        let sources = &node.reindex_on;
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].table, "source_table");
        assert_eq!(
            sources[0].traversal_path,
            PathResolution::Column("traversal_path".to_string())
        );
        assert_eq!(sources[1].table, "source_details");
    }

    #[test]
    fn pipeline_reindex_on_accepts_dictionary_resolution() {
        let node = parse_test_node(
            r#"
            node_type: TestNode
            domain: test
            destination_table: gl_test
            properties:
              id:
                type: int64
                source: id
              traversal_path:
                type: string
                source: traversal_path
            indexer:
              reindex: use_specified_tables
              tables:
                - table: source_details
                  traversal_path:
                    dictionary: source_paths_dict
                    key_column: source_id
            pipelines:
              - name: TestNode
                extract:
                  type: clickhouse
                  tables: [source_table]
                  order_by: [traversal_path, id]
                  query: generated
                transform:
                  type: datafusion
            "#,
        )
        .expect("pipeline should parse");
        let [source] = node.reindex_on.as_slice() else {
            panic!("expected one reindex source");
        };
        assert_eq!(source.table, "source_details");
        assert_eq!(
            source.traversal_path,
            PathResolution::Dictionary {
                dictionary: "source_paths_dict".to_string(),
                key_column: "source_id".to_string(),
            }
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
    fn generated_edge_carries_filter_and_enrich_declaration() {
        // The edge SQL itself is generated by the indexer (covered by its golden);
        // here we assert the declarative model the indexer consumes.
        let ontology = crate::Ontology::load_embedded().expect("load");
        let edge_pipeline = |name: &str| {
            ontology
                .edge_pipelines
                .values()
                .flatten()
                .find(|p| p.name == name)
                .unwrap_or_else(|| panic!("pipeline {name} not found"))
        };

        let reopened = edge_pipeline("REOPENED_siphon_resource_state_events_MergeRequest");
        let Extract::ClickHouse(reopened_extract) = &reopened.extract;
        let ExtractQuery::Generated { filter } = &reopened_extract.query else {
            panic!("REOPENED should be a generated edge");
        };
        assert_eq!(filter.as_deref(), Some("state = 5"));

        // MEMBER_OF's `to` endpoint is polymorphic (Group/Project via source_type),
        // so it declares no enrichable literal kind.
        let member = edge_pipeline("MEMBER_OF_siphon_members");
        let mapping = &member.transform.edges()[0];
        assert!(matches!(mapping.source.kind, NodeRefKind::Literal(ref k) if k == "User"));
        assert!(!mapping.source.enrich.is_empty());
        assert!(matches!(mapping.target.kind, NodeRefKind::Derived { .. }));
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
            "#,
        );
        let err = result.expect_err("binary on int64 should be rejected");
        assert!(
            err.to_string()
                .contains("'binary: true' is only valid for 'type: string'"),
            "got: {err}"
        );
    }
}
