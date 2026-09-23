//! `export.yaml` parsed and checked against the ontology. Every table,
//! column, node kind, tag and transform is named there; the walker in
//! `mod.rs` only follows the result.

use ontology::{DataType, Ontology};
use orbit_utils::arrow::{ColumnSpec, ColumnType};

use crate::dsl::parser::parse_pipeline;
use crate::dsl::types::{Ctx, Tf};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::EdgeKind;

static EXPORT_YAML: &str = include_str!("../../../config/export.yaml");

// ── export.yaml ──

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportYaml {
    ids: IdsYaml,
    header: Vec<ColumnYaml>,
    entities: indexmap::IndexMap<String, EntityYaml>,
    edges: EdgesYaml,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityYaml {
    source: String,
    #[serde(default)]
    exclude_parent: Option<String>,
    #[serde(default)]
    expand: Option<String>,
    id: IdYaml,
    columns: Vec<ColumnYaml>,
}

#[derive(serde::Deserialize)]
struct IdsYaml {
    prefix: Vec<String>,
}

#[derive(serde::Deserialize)]
struct IdYaml {
    seed: String,
    columns: Vec<String>,
}

#[derive(serde::Deserialize)]
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
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EdgesYaml {
    columns: Vec<ColumnYaml>,
    containment: Vec<ContainmentYaml>,
    graph: Vec<GraphEdgeYaml>,
}

#[derive(serde::Deserialize)]
struct ContainmentYaml {
    from: String,
    to: String,
    kind: String,
}

#[derive(serde::Deserialize)]
struct GraphEdgeYaml {
    edge: String,
    #[serde(default)]
    from: Option<String>,
    to: String,
    kind: String,
    #[serde(default)]
    unless_source_has: Option<String>,
}

// ── compiled plan ──

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

pub(super) struct EntityColumn {
    pub(super) name: String,
    pub(super) on: On,
    pub(super) source: Source,
}

/// Where a header column's value comes from: the row's own id, a named
/// envelope value, or a literal.
#[derive(Clone)]
pub(super) enum HeaderSource {
    RowId,
    Envelope(String),
    Const(String),
}

/// Where an edge column's value comes from.
#[derive(Clone)]
pub(super) enum EdgeSource {
    SourceId,
    SourceEntity,
    Kind,
    TargetId,
    TargetEntity,
    Const(String),
}

pub(super) struct Column<S> {
    pub(super) name: String,
    pub(super) source: S,
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
    pub(super) source_kinds: Vec<u16>,
    pub(super) exclude_parent: Option<u16>,
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
    pub(super) unless_source_has: Option<EdgeKind>,
}

/// `export.yaml` checked against the ontology and interned for one `Lang`.
pub struct ExportPlan {
    pub(super) id_prefix: Vec<String>,
    pub(super) header: Vec<Column<HeaderSource>>,
    pub(super) entities: Vec<EntityPlan>,
    pub(super) edge_table: String,
    pub(super) edge_specs: Vec<ColumnSpec>,
    pub(super) edge_columns: Vec<Column<EdgeSource>>,
    pub(super) containment: Vec<Containment>,
    pub(super) graph: Vec<GraphEdge>,
}

impl ExportPlan {
    pub fn load(ontology: &Ontology, lang: &Lang) -> Result<Self, LoadError> {
        let yaml: ExportYaml = orbit_utils::yaml::from_str(EXPORT_YAML)?;
        let header: Vec<Column<HeaderSource>> = yaml
            .header
            .iter()
            .map(|c| {
                Ok(Column {
                    name: c.name.clone(),
                    source: header_source(c)?,
                })
            })
            .collect::<Result<_, LoadError>>()?;

        let mut entities = Vec::new();
        for name in ontology.local_entity_names() {
            let entity = yaml
                .entities
                .get(name)
                .ok_or_else(|| LoadError::new(format!("export.yaml has no entity '{name}'")))?;
            entities.push(compile_entity(name, entity, &yaml.header, ontology, lang)?);
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
        check_columns(
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
                    source: edge_source(c)?,
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
                    unless_source_has: g.unless_source_has.as_deref().map(edge_kind).transpose()?,
                })
            })
            .collect::<Result<_, LoadError>>()?;

        Ok(Self {
            id_prefix: yaml.ids.prefix.clone(),
            header,
            entities,
            edge_table: ontology
                .local_edge_table_name()
                .ok_or_else(|| LoadError::new("ontology has no local edge table"))?
                .to_string(),
            edge_specs: edge_fields
                .iter()
                .map(|(name, data_type, nullable)| ColumnSpec {
                    name: name.clone(),
                    col_type: column_type(data_type),
                    nullable: *nullable,
                })
                .collect(),
            edge_columns,
            containment,
            graph,
        })
    }
}

fn edge_kind(name: &str) -> Result<EdgeKind, LoadError> {
    name.parse()
        .map_err(|_| LoadError::new(format!("export.yaml: unknown edge kind '{name}'")))
}

fn column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Int => ColumnType::Int,
        _ => ColumnType::Str,
    }
}

/// The Arrow type name `export.yaml` uses for an ontology type.
fn arrow_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int => "Int64",
        _ => "Utf8",
    }
}

/// The YAML columns and the ontology's must agree exactly: same names, same
/// types, same nullability, nothing missing on either side.
fn check_columns(
    table: &str,
    columns: &[&ColumnYaml],
    fields: &[(String, DataType, bool)],
) -> Result<(), LoadError> {
    for column in columns {
        let (_, data_type, nullable) = fields
            .iter()
            .find(|(name, _, _)| *name == column.name)
            .ok_or_else(|| {
                LoadError::new(format!(
                    "export.yaml: {table}.{} is not a column of the ontology",
                    column.name
                ))
            })?;
        let expected = arrow_name(data_type);
        if column.dtype != expected {
            return Err(LoadError::new(format!(
                "export.yaml: {table}.{} is {} here but {expected} in the ontology",
                column.name, column.dtype
            )));
        }
        if column.nullable != *nullable {
            return Err(LoadError::new(format!(
                "export.yaml: {table}.{} nullable={} here but {nullable} in the ontology",
                column.name, column.nullable
            )));
        }
    }
    for (name, _, _) in fields {
        if !columns.iter().any(|c| c.name == *name) {
            return Err(LoadError::new(format!(
                "export.yaml: {table}.{name} is in the ontology but not mapped"
            )));
        }
    }
    Ok(())
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
    let fields: Vec<(String, DataType, bool)> = ontology
        .local_entity_fields(name)
        .ok_or_else(|| LoadError::new(format!("'{name}' is not a local entity")))?
        .iter()
        .map(|f| (f.name.clone(), f.data_type, f.nullable))
        .collect();
    let all_columns: Vec<&ColumnYaml> = header.iter().chain(&yaml.columns).collect();
    check_columns(name, &all_columns, &fields)?;

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
            Ok(EntityColumn {
                name: column.name.clone(),
                on,
                source,
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
        specs: fields
            .iter()
            .map(|(name, data_type, nullable)| ColumnSpec {
                name: name.clone(),
                col_type: column_type(data_type),
                nullable: *nullable,
            })
            .collect(),
        source_kinds: yaml
            .source
            .split('|')
            .map(|k| lang.intern_kind(k))
            .collect(),
        exclude_parent: yaml.exclude_parent.as_deref().map(|k| lang.intern_kind(k)),
        expand: yaml.expand.as_deref().map(|k| lang.intern_kind(k)),
        id_seed: yaml.id.seed.clone(),
        id_parts,
        columns,
    })
}

fn one_of(column: &ColumnYaml, flags: &[bool]) -> Result<(), LoadError> {
    if flags.iter().filter(|b| **b).count() == 1 {
        Ok(())
    } else {
        Err(LoadError::new(format!(
            "export.yaml: {} needs exactly one source",
            column.name
        )))
    }
}

fn header_source(column: &ColumnYaml) -> Result<HeaderSource, LoadError> {
    one_of(column, &[column.from.is_some(), column.r#const.is_some()])?;
    if let Some(value) = &column.r#const {
        return Ok(HeaderSource::Const(value.clone()));
    }
    Ok(match column.from.as_deref().unwrap_or_default() {
        "id" => HeaderSource::RowId,
        name => HeaderSource::Envelope(name.to_string()),
    })
}

impl ExportPlan {
    /// Every envelope name the header and id prefix refer to.
    pub(super) fn envelope_names(&self) -> impl Iterator<Item = &str> {
        self.header
            .iter()
            .filter_map(|c| match &c.source {
                HeaderSource::Envelope(name) => Some(name.as_str()),
                _ => None,
            })
            .chain(self.id_prefix.iter().map(String::as_str))
    }
}

fn edge_source(column: &ColumnYaml) -> Result<EdgeSource, LoadError> {
    one_of(column, &[column.from.is_some(), column.r#const.is_some()])?;
    if let Some(value) = &column.r#const {
        return Ok(EdgeSource::Const(value.clone()));
    }
    Ok(match column.from.as_deref().unwrap_or_default() {
        "source.id" => EdgeSource::SourceId,
        "source.entity" => EdgeSource::SourceEntity,
        "kind" => EdgeSource::Kind,
        "target.id" => EdgeSource::TargetId,
        "target.entity" => EdgeSource::TargetEntity,
        other => {
            return Err(LoadError::new(format!(
                "export.yaml: edges.{}: unknown source '{other}'",
                column.name
            )));
        }
    })
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
