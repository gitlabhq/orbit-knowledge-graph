//! `export.yaml` parsed and checked against the ontology. Every table,
//! column, node kind, tag and transform is named there; the walker in
//! `mod.rs` only follows the result.

use ontology::{DataType, Ontology};
use orbit_arrow::{ColumnSpec, ColumnType};

use crate::dsl::parser::parse_pipeline;
use crate::dsl::types::{Ctx, Tf};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::EdgeKind;

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    ids: IdsYaml,
    header: Vec<ColumnYaml>,
    entities: indexmap::IndexMap<String, EntityYaml>,
    edges: EdgesYaml,
    #[serde(skip)]
    pub schemas: std::collections::HashMap<String, Vec<ColumnSpec>>,
}

impl ExportConfig {
    pub fn embedded() -> &'static Self {
        static CONFIG: std::sync::LazyLock<ExportConfig> = std::sync::LazyLock::new(|| {
            orbit_utils::yaml::from_str(include_str!("../../config/export.yaml"))
                .expect("embedded export config must parse")
        });
        &CONFIG
    }
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityYaml {
    source: String,
    #[serde(default)]
    exclude_parent: Option<String>,
    #[serde(default)]
    exclude_tag: indexmap::IndexMap<String, Vec<String>>,
    #[serde(default)]
    expand: Option<String>,
    id: IdYaml,
    columns: Vec<ColumnYaml>,
}

#[derive(Clone, serde::Deserialize)]
struct IdsYaml {
    prefix: Vec<String>,
}

#[derive(Clone, serde::Deserialize)]
struct IdYaml {
    seed: String,
    columns: Vec<String>,
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnYaml {
    name: String,
    #[serde(rename = "type")]
    dtype: String,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    r#const: Option<String>,
    #[serde(default)]
    position: Option<String>,
    #[serde(default)]
    span: Vec<String>,
    #[serde(default)]
    on: Option<String>,
    #[serde(default)]
    cap: Option<usize>,
    #[serde(default)]
    over: Option<String>,
    #[serde(default)]
    split: Option<String>,
    #[serde(default)]
    max: Option<usize>,
    #[serde(default)]
    sort: bool,
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EdgesYaml {
    columns: Vec<ColumnYaml>,
    containment: Vec<ContainmentYaml>,
    graph: Vec<GraphEdgeYaml>,
}

#[derive(Clone, serde::Deserialize)]
struct ContainmentYaml {
    from: String,
    to: String,
    kind: String,
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphEdgeYaml {
    edge: String,
    #[serde(default)]
    from: Option<String>,
    to: String,
    kind: String,
    #[serde(default)]
    fallback: bool,
    #[serde(default)]
    site: Vec<String>,
    #[serde(default)]
    site_child: Option<String>,
    #[serde(default)]
    receiver: bool,
    #[serde(default)]
    lift: Option<String>,
    #[serde(default)]
    target_tag: indexmap::IndexMap<String, Vec<String>>,
    #[serde(default)]
    reference: bool,
    #[serde(default)]
    set: indexmap::IndexMap<String, String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Position {
    StartLine,
    EndLine,
    StartByte,
    EndByte,
    StartCol,
    EndCol,
}

/// A `span:` candidate: the node itself when it carries the tag, or its
/// first child of the kind.
#[derive(Clone, Copy)]
pub(super) enum SpanCandidate {
    Tagged(u32),
    Child(u16),
}

/// Where a row column's value comes from.
pub(super) enum Source {
    Transform(Tf),
    Const(String),
    Position(Position, Vec<SpanCandidate>),
}

/// Which node a column runs on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum On {
    Node,
    Expanded,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Over {
    Cut,
    Words,
    Empty,
    Drop,
}

pub(super) struct Bounds {
    pub(super) cap: Option<usize>,
    pub(super) over: Over,
    pub(super) split: Option<String>,
    pub(super) max: Option<usize>,
    pub(super) sort: bool,
}

pub(super) struct EntityColumn {
    pub(super) name: String,
    pub(super) col_type: ColumnType,
    pub(super) nullable: bool,
    pub(super) on: On,
    pub(super) source: Source,
    pub(super) bounds: Bounds,
    pub(super) allowed: Option<Vec<String>>,
}

impl EntityColumn {
    pub(super) fn admits(&self, value: &str) -> bool {
        self.allowed
            .as_ref()
            .is_none_or(|allowed| allowed.iter().any(|v| v == value))
    }
}

pub(super) enum RowSource {
    Value(String),
    Const(String),
}

pub(super) struct Column {
    pub(super) name: String,
    pub(super) col_type: ColumnType,
    pub(super) source: RowSource,
}

/// A part of the row id: a column's value, or the node's byte range.
pub(super) enum IdPart {
    Column(usize),
    Span,
}

pub(super) struct EntityPlan {
    pub(super) name: String,
    pub(super) table: String,
    pub(super) specs: Vec<ColumnSpec>,
    pub(super) header: Vec<Column>,
    pub(super) source_kinds: Vec<u16>,
    pub(super) exclude_parent: Option<u16>,
    pub(super) exclude_tag: Vec<(u32, Vec<u32>)>,
    pub(super) expand: Option<u16>,
    pub(super) id_seed: String,
    pub(super) id_parts: Vec<IdPart>,
    pub(super) columns: Vec<EntityColumn>,
}

pub(super) struct Containment {
    pub(super) from: usize,
    pub(super) to: usize,
    pub(super) kind: String,
}

pub(super) struct GraphEdge {
    pub(super) edge: EdgeKind,
    pub(super) from: Option<usize>,
    pub(super) to: usize,
    pub(super) kind: String,
    pub(super) fallback: bool,
    pub(super) site: Vec<u16>,
    pub(super) site_child: Option<u16>,
    pub(super) receiver: bool,
    pub(super) lift: Option<usize>,
    pub(super) target_tag: Vec<(u32, Vec<u32>)>,
    pub(super) reference: bool,
    pub(super) set: Vec<(String, String)>,
}

/// `export.yaml` checked against the ontology and interned for one `Lang`.
pub struct ExportPlan {
    pub(super) id_prefix: Vec<String>,
    pub(super) entities: Vec<EntityPlan>,
    pub(super) edge_table: String,
    pub(super) edge_specs: Vec<ColumnSpec>,
    pub(super) edge_columns: Vec<Column>,
    pub(super) containment: Vec<Containment>,
    pub(super) graph: Vec<GraphEdge>,
    pub batch_bytes: usize,
}

impl ExportPlan {
    pub fn from_config(
        config: &ExportConfig,
        ontology: &Ontology,
        lang: &Lang,
    ) -> Result<Self, LoadError> {
        let yaml = config;
        let mut entities = Vec::new();
        for name in ontology.local_entity_names() {
            let entity = yaml
                .entities
                .get(name)
                .ok_or_else(|| LoadError::new(format!("export.yaml has no entity '{name}'")))?;
            let mut entity = compile_entity(name, entity, &yaml.header, ontology, lang)?;
            if let Some(specs) = config.schemas.get(&entity.table) {
                check_schema(&entity.specs, specs)?;
                entity.specs = specs.clone();
            }
            entity.header = entity
                .specs
                .iter()
                .filter(|spec| !entity.columns.iter().any(|column| column.name == spec.name))
                .map(|spec| {
                    Ok(Column {
                        name: spec.name.clone(),
                        col_type: spec.col_type,
                        source: match yaml.header.iter().find(|column| column.name == spec.name) {
                            Some(column) => row_source(column)?,
                            None => RowSource::Value(spec.name.clone()),
                        },
                    })
                })
                .collect::<Result<_, LoadError>>()?;
            entities.push(entity);
        }
        for name in yaml.entities.keys() {
            if !entities.iter().any(|e| e.name == *name) {
                return Err(LoadError::new(format!(
                    "export.yaml entity '{name}' is not a local entity of the ontology"
                )));
            }
        }
        let entity_index = |name: &str| -> Result<usize, LoadError> {
            entities.iter().position(|e| e.name == name).ok_or_else(|| {
                LoadError::new(format!("export.yaml edge names unknown entity '{name}'"))
            })
        };

        let edge_fields: Vec<(String, DataType, bool)> = ontology
            .local_edge_columns()
            .iter()
            .map(|c| (c.name.clone(), c.data_type, false))
            .collect();
        let edge_specs = compile_specs(
            "edges",
            &yaml.edges.columns.iter().collect::<Vec<_>>(),
            &edge_fields,
        )?;
        let edge_columns = yaml
            .edges
            .columns
            .iter()
            .map(|c| {
                Ok(Column {
                    name: c.name.clone(),
                    col_type: yaml_column_type(&c.dtype)?,
                    source: row_source(c)?,
                })
            })
            .collect::<Result<_, LoadError>>()?;

        let containment = yaml
            .edges
            .containment
            .iter()
            .map(|c| {
                Ok(Containment {
                    from: entity_index(&c.from)?,
                    to: entity_index(&c.to)?,
                    kind: c.kind.clone(),
                })
            })
            .collect::<Result<_, LoadError>>()?;
        let graph = yaml
            .edges
            .graph
            .iter()
            .map(|g| {
                Ok(GraphEdge {
                    edge: edge_kind(&g.edge)?,
                    from: g.from.as_deref().map(entity_index).transpose()?,
                    to: entity_index(&g.to)?,
                    kind: g.kind.clone(),
                    fallback: g.fallback,
                    site: g.site.iter().map(|k| lang.intern_kind(k)).collect(),
                    site_child: g.site_child.as_deref().map(|k| lang.intern_kind(k)),
                    receiver: g.receiver,
                    lift: g.lift.as_deref().map(entity_index).transpose()?,
                    target_tag: g
                        .target_tag
                        .iter()
                        .map(|(key, values)| {
                            let values = values.iter().map(|v| lang.syms.intern(v)).collect();
                            (lang.syms.intern(key), values)
                        })
                        .collect(),
                    reference: g.reference,
                    set: g.set.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                })
            })
            .collect::<Result<_, LoadError>>()?;

        Ok(Self {
            id_prefix: yaml.ids.prefix.clone(),
            entities,
            edge_table: ontology
                .local_edge_table_name()
                .ok_or_else(|| LoadError::new("ontology has no local edge table"))?
                .to_string(),
            edge_specs,
            edge_columns,
            containment,
            graph,
            batch_bytes: orbit_arrow::BATCH_BYTES,
        })
    }
}

fn edge_kind(name: &str) -> Result<EdgeKind, LoadError> {
    name.parse()
        .map_err(|_| LoadError::new(format!("export.yaml: unknown edge kind '{name}'")))
}

fn yaml_column_type(name: &str) -> Result<ColumnType, LoadError> {
    match name {
        "Int64" => Ok(ColumnType::Int),
        "Utf8" => Ok(ColumnType::Str),
        "DictionaryUtf8" => Ok(ColumnType::DictStr),
        "Boolean" => Ok(ColumnType::Bool),
        "ListUtf8" => Ok(ColumnType::StrList),
        "ListInt64" => Ok(ColumnType::IntList),
        "DictionaryListUtf8" => Ok(ColumnType::DictStrList),
        "TimestampMicros" => Ok(ColumnType::TimestampMicros),
        _ => Err(LoadError::new(format!(
            "unsupported export column type '{name}'"
        ))),
    }
}

fn column_spec(column: &ColumnYaml) -> Result<ColumnSpec, LoadError> {
    Ok(ColumnSpec {
        name: column.name.clone(),
        col_type: yaml_column_type(&column.dtype)?,
        nullable: column.nullable,
    })
}

fn check_schema(mapped: &[ColumnSpec], schema: &[ColumnSpec]) -> Result<(), LoadError> {
    let names: std::collections::HashSet<_> = schema.iter().map(|spec| &spec.name).collect();
    if names.len() != schema.len() {
        return Err(LoadError::new("duplicate export schema column"));
    }
    for column in mapped {
        if !schema.iter().any(|spec| {
            spec.name == column.name
                && spec.nullable == column.nullable
                && (spec.col_type == column.col_type
                    || matches!(
                        (column.col_type, spec.col_type),
                        (ColumnType::Str, ColumnType::DictStr)
                            | (ColumnType::StrList, ColumnType::DictStrList)
                    ))
        }) {
            return Err(LoadError::new(format!(
                "incompatible export schema column '{}'",
                column.name
            )));
        }
    }
    Ok(())
}

fn compile_specs(
    table: &str,
    columns: &[&ColumnYaml],
    fields: &[(String, DataType, bool)],
) -> Result<Vec<ColumnSpec>, LoadError> {
    let mut specs = columns
        .iter()
        .map(|column| column_spec(column))
        .collect::<Result<Vec<_>, _>>()?;
    let expected: Vec<_> = fields
        .iter()
        .map(|(name, data_type, nullable)| ColumnSpec {
            name: name.clone(),
            col_type: match data_type {
                DataType::Int => ColumnType::Int,
                DataType::Bool => ColumnType::Bool,
                DataType::DateTime => ColumnType::TimestampMicros,
                DataType::StringList => ColumnType::StrList,
                DataType::IntList => ColumnType::IntList,
                _ => ColumnType::Str,
            },
            nullable: *nullable,
        })
        .collect();
    check_schema(&expected, &specs)?;
    if specs.len() != expected.len() {
        return Err(LoadError::new(format!(
            "unexpected export columns in '{table}'"
        )));
    }
    specs.sort_by_key(|spec| fields.iter().position(|(name, _, _)| *name == spec.name));
    Ok(specs)
}

fn compile_entity(
    name: &str,
    yaml: &EntityYaml,
    header: &[ColumnYaml],
    ontology: &Ontology,
    lang: &Lang,
) -> Result<EntityPlan, LoadError> {
    let table = ontology
        .get_node(name)
        .ok_or_else(|| LoadError::new(format!("ontology has no node '{name}'")))?
        .destination_table
        .clone();
    let ontology_fields = ontology
        .local_entity_fields(name)
        .ok_or_else(|| LoadError::new(format!("'{name}' is not a local entity")))?;
    let fields: Vec<(String, DataType, bool)> = ontology_fields
        .iter()
        .map(|f| (f.name.clone(), f.data_type, f.nullable))
        .collect();
    let all_columns: Vec<&ColumnYaml> = header.iter().chain(&yaml.columns).collect();
    let specs = compile_specs(name, &all_columns, &fields)?;

    let columns = yaml
        .columns
        .iter()
        .map(|column| {
            let source = compile_source(name, column, lang)?;
            if (column.dtype == "Int64") != matches!(source, Source::Position(..)) {
                return Err(LoadError::new(format!(
                    "export.yaml: {name}.{}: Int64 columns are filled by positions and only those",
                    column.name
                )));
            }
            let on = match column.on.as_deref() {
                None => On::Node,
                Some("expanded") if yaml.expand.is_some() => On::Expanded,
                Some(other) => {
                    return Err(LoadError::new(format!(
                        "export.yaml: {name}.{}: `on: {other}` needs the entity to declare `expand:`",
                        column.name
                    )));
                }
            };
            let over = match column.over.as_deref() {
                None | Some("cut") => Over::Cut,
                Some("words") => Over::Words,
                Some("empty") => Over::Empty,
                Some("drop") => Over::Drop,
                Some(other) => {
                    return Err(LoadError::new(format!(
                        "export.yaml: {name}.{}: `over` is cut, words, empty or drop, not '{other}'",
                        column.name
                    )));
                }
            };
            let field = ontology_fields.iter().find(|f| f.name == column.name);
            Ok(EntityColumn {
                name: column.name.clone(),
                col_type: yaml_column_type(&column.dtype)?,
                nullable: column.nullable,
                on,
                source,
                bounds: Bounds {
                    cap: column.cap,
                    over,
                    split: column.split.clone(),
                    max: column.max,
                    sort: column.sort,
                },
                allowed: field
                    .and_then(|f| f.enum_values.as_ref())
                    .filter(|_| field.is_some_and(|f| matches!(f.enum_type, ontology::EnumType::String)))
                    .map(|values| values.values().cloned().collect()),
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;

    let id_parts = yaml
        .id
        .columns
        .iter()
        .map(|part| {
            if part == "span" {
                return Ok(IdPart::Span);
            }
            columns
                .iter()
                .position(|c| c.name == *part)
                .map(IdPart::Column)
                .ok_or_else(|| {
                    LoadError::new(format!(
                        "export.yaml: {name} id names '{part}', which is not one of its columns"
                    ))
                })
        })
        .collect::<Result<_, _>>()?;

    Ok(EntityPlan {
        name: name.to_string(),
        table,
        specs,
        header: Vec::new(),
        source_kinds: yaml
            .source
            .split('|')
            .map(|k| lang.intern_kind(k))
            .collect(),
        exclude_parent: yaml.exclude_parent.as_deref().map(|k| lang.intern_kind(k)),
        exclude_tag: yaml
            .exclude_tag
            .iter()
            .map(|(key, values)| {
                let values = values.iter().map(|v| lang.syms.intern(v)).collect();
                (lang.syms.intern(key), values)
            })
            .collect(),
        expand: yaml.expand.as_deref().map(|k| lang.intern_kind(k)),
        id_seed: yaml.id.seed.clone(),
        id_parts,
        columns,
    })
}

fn row_source(column: &ColumnYaml) -> Result<RowSource, LoadError> {
    match (&column.from, &column.r#const) {
        (Some(name), None) => Ok(RowSource::Value(name.clone())),
        (None, Some(value)) => Ok(RowSource::Const(value.clone())),
        _ => Err(LoadError::new(format!(
            "export.yaml: {} needs exactly one source",
            column.name
        ))),
    }
}

impl ExportPlan {
    /// Every envelope name the header and id prefix refer to.
    pub(super) fn envelope_names(&self) -> impl Iterator<Item = &str> {
        self.entities
            .iter()
            .flat_map(|entity| &entity.header)
            .filter_map(|c| match &c.source {
                RowSource::Value(name) if name != "id" => Some(name.as_str()),
                _ => None,
            })
            .chain(self.id_prefix.iter().map(String::as_str))
            .chain(
                self.edge_columns
                    .iter()
                    .filter_map(|column| match &column.source {
                        RowSource::Value(name)
                            if !matches!(
                                name.as_str(),
                                "source.id"
                                    | "source.entity"
                                    | "kind"
                                    | "target.id"
                                    | "target.entity"
                                    | "lines"
                                    | "resolution"
                            ) =>
                        {
                            Some(name.as_str())
                        }
                        _ => None,
                    }),
            )
    }
}

fn compile_source(entity: &str, column: &ColumnYaml, lang: &Lang) -> Result<Source, LoadError> {
    let chosen = [column.r#const.is_some(), column.position.is_some()]
        .iter()
        .filter(|b| **b)
        .count();
    if chosen > 1 || (chosen == 1 && column.from.is_some()) {
        return Err(LoadError::new(format!(
            "export.yaml: {entity}.{}: use one of from, const, position",
            column.name
        )));
    }
    if let Some(value) = &column.r#const {
        return Ok(Source::Const(value.clone()));
    }
    let Some(position) = column.position.as_deref() else {
        let tf = match &column.from {
            Some(pipe) => parse_pipeline(&mut Ctx::new(lang), pipe).map_err(|e| {
                LoadError::new(format!("export.yaml: {entity}.{}: {e}", column.name))
            })?,
            None => Tf::Id,
        };
        return Ok(Source::Transform(tf));
    };
    let span = column
        .span
        .iter()
        .map(|candidate| match candidate.as_str() {
            tag if tag.starts_with("tag:") => Ok(SpanCandidate::Tagged(lang.syms.intern(&tag[4..]))),
            kind if kind.starts_with("__") => Ok(SpanCandidate::Child(lang.intern_kind(kind))),
            other => Err(LoadError::new(format!(
                "export.yaml: {entity}.{}: span candidates are `tag:<key>` or a kind, not '{other}'",
                column.name
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let position = match position {
        "start_line" => Position::StartLine,
        "end_line" => Position::EndLine,
        "start_byte" => Position::StartByte,
        "end_byte" => Position::EndByte,
        "start_col" => Position::StartCol,
        "end_col" => Position::EndCol,
        other => {
            return Err(LoadError::new(format!(
                "export.yaml: {entity}.{}: unknown position '{other}'",
                column.name
            )));
        }
    };
    Ok(Source::Position(position, span))
}
