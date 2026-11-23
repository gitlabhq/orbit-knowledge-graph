//! The rows of the `export` phase as the ontology's local tables: batched by
//! table, with the envelope's columns and ids, checked against the ontology.

use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use arrow::record_batch::RecordBatch;
use ontology::{DataType, Ontology};
use orbit_arrow::{BatchBuilder, ColumnSpec, ColumnType};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::driver::State;
use crate::engine::lang::DataSpec;
use crate::engine::{Loc, Part, Phase, Row, RowShape, Val};
use crate::env::Env;
use crate::error::{Error, LoadError};
use crate::sentinel::Sentinel;
use crate::tree::{Compact, Tree};

/// Per-table column specs that replace the ontology's, such as the indexer's
/// dictionary-encoded strings and version columns.
#[derive(Clone, Default)]
pub struct ExportConfig {
    pub schemas: std::collections::HashMap<String, Vec<ColumnSpec>>,
}

/// A value the caller hands in with every export.
#[derive(Clone, Copy)]
pub enum Scalar<'a> {
    Int(i64),
    Str(&'a str),
    Bool(bool),
}

impl Scalar<'_> {
    fn text(&self) -> Cow<'_, str> {
        match self {
            Scalar::Int(n) => Cow::Owned(n.to_string()),
            Scalar::Str(s) => Cow::Borrowed(s),
            Scalar::Bool(value) => Cow::Owned(value.to_string()),
        }
    }

    fn write(
        self,
        builder: &mut BatchBuilder,
        name: &str,
        column_type: ColumnType,
    ) -> Result<(), arrow::error::ArrowError> {
        match (self, column_type) {
            (Self::Int(value), ColumnType::TimestampMicros) => {
                builder.col(name)?.push_timestamp_micros(value)
            }
            (Self::Int(value), ColumnType::Int) => builder.col(name)?.push_int(value),
            (Self::Bool(value), ColumnType::Bool) => builder.col(name)?.push_bool(value),
            (Self::Str(value), ColumnType::Str | ColumnType::DictStr) => {
                builder.col(name)?.push_str(value)
            }
            _ => Err(arrow::error::ArrowError::SchemaError(format!(
                "invalid value for export column '{name}'"
            ))),
        }
    }
}

/// Named values every row carries, such as the project and branch being
/// indexed. A column no rule sets comes from here, and a value here wins over
/// a rule's own.
#[derive(Clone)]
pub struct Envelope<'a> {
    values: Vec<(&'a str, Scalar<'a>)>,
}

impl<'a> Envelope<'a> {
    pub fn new(values: impl IntoIterator<Item = (&'a str, Scalar<'a>)>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    fn get(&self, name: &str) -> Option<Scalar<'a>> {
        self.values
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| *v)
    }

    fn check(&self, tables: &Tables) -> Result<(), Error> {
        for name in &tables.envelope {
            if self.get(name).is_none() {
                let given: Vec<&str> = self.values.iter().map(|(n, _)| *n).collect();
                return Err(LoadError::new(format!(
                    "export refers to envelope value '{name}' but the caller gave {given:?}"
                ))
                .into());
            }
        }
        Ok(())
    }

    /// `FxHash` over `[prefix values.., seed, parts..]`, sign bit cleared: the
    /// same computation as code-graph's `compute_id`, so the same inputs give
    /// the same id in both pipelines.
    fn id(&self, prefix: &[String], seed: &str, parts: &[String]) -> i64 {
        let prefix: Vec<String> = prefix
            .iter()
            .filter_map(|name| self.get(name))
            .map(|v| v.text().into_owned())
            .collect();
        let components: Vec<&str> = prefix
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(seed))
            .chain(parts.iter().map(String::as_str))
            .collect();
        let mut hasher = FxHasher::default();
        components.hash(&mut hasher);
        (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }
}

/// Which rows an export writes: directories, the files this family owns, and
/// with `only` just the rows of those trees and the edges touching them.
pub struct Scope<'a> {
    pub include_directories: bool,
    pub owns_file: &'a dyn Fn(&str) -> bool,
    pub only: Option<&'a FxHashSet<u32>>,
}

impl Scope<'_> {
    pub fn all() -> Scope<'static> {
        Scope {
            include_directories: true,
            owns_file: &|_| true,
            only: None,
        }
    }

    fn writes(&self, entity: &str, fi: u32, label: &str) -> bool {
        let in_only = self.only.is_none_or(|only| only.contains(&fi));
        match entity {
            "Directory" => self.include_directories,
            "File" => (self.owns_file)(label) && in_only,
            _ => in_only,
        }
    }
}

struct Table {
    name: String,
    entity: String,
    specs: Vec<ColumnSpec>,
}

/// The ontology's local tables as the export rules fill them.
pub struct Tables {
    tables: Vec<Table>,
    shapes: Vec<usize>,
    id_prefix: Vec<String>,
    envelope: Vec<String>,
    pub batch_bytes: usize,
}

fn column_type(data_type: DataType) -> ColumnType {
    match data_type {
        DataType::Int => ColumnType::Int,
        DataType::Bool => ColumnType::Bool,
        DataType::DateTime => ColumnType::TimestampMicros,
        _ => ColumnType::Str,
    }
}

fn check_schema(mapped: &[ColumnSpec], schema: &[ColumnSpec]) -> Result<(), LoadError> {
    let names: FxHashSet<_> = schema.iter().map(|spec| &spec.name).collect();
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

fn check_shape(table: &Table, shape: &RowShape) -> Result<(), LoadError> {
    let has_id = table.specs.iter().any(|s| s.name == "id");
    if has_id != shape.id.is_some() {
        let need = if has_id { "need" } else { "take no" };
        return Err(LoadError::new(format!(
            "export rows of '{}' {need} an id",
            table.name
        )));
    }
    if let Some(name) = shape
        .columns
        .iter()
        .find(|c| !table.specs.iter().any(|s| s.name == **c))
    {
        return Err(LoadError::new(format!(
            "export rows of '{}' set unknown column '{name}'",
            table.name
        )));
    }
    Ok(())
}

impl Tables {
    pub fn load(env: &Env, ontology: &Ontology) -> Result<Self, LoadError> {
        let mut tables = Vec::new();
        for name in ontology.local_entity_names() {
            let table = ontology
                .get_node(name)
                .ok_or_else(|| LoadError::new(format!("ontology has no node '{name}'")))?
                .destination_table
                .clone();
            let specs = ontology
                .local_entity_fields(name)
                .ok_or_else(|| LoadError::new(format!("'{name}' is not a local entity")))?
                .iter()
                .map(|f| ColumnSpec {
                    name: f.name.clone(),
                    col_type: column_type(f.data_type),
                    nullable: f.nullable,
                })
                .collect();
            tables.push(Table {
                name: table,
                entity: name.to_string(),
                specs,
            });
        }
        tables.push(Table {
            name: ontology
                .local_edge_table_name()
                .ok_or_else(|| LoadError::new("ontology has no local edge table"))?
                .to_string(),
            entity: String::new(),
            specs: ontology
                .local_edge_columns()
                .iter()
                .map(|c| ColumnSpec {
                    name: c.name.clone(),
                    col_type: column_type(c.data_type),
                    nullable: false,
                })
                .collect(),
        });
        for table in &mut tables {
            if let Some(schema) = env.export_config.schemas.get(&table.name) {
                check_schema(&table.specs, schema)?;
                table.specs = schema.clone();
            }
        }
        let phase = env.pipeline.phase_over("graph");
        let index = |name: &str| {
            tables
                .iter()
                .position(|t| t.name == name)
                .ok_or_else(|| LoadError::new(format!("export rule names unknown table '{name}'")))
        };
        let shapes = phase
            .rows
            .iter()
            .map(|shape| {
                let at = index(&shape.table)?;
                check_shape(&tables[at], shape)?;
                for (_, table) in &shape.expect {
                    index(table)?;
                }
                Ok(at)
            })
            .collect::<Result<Vec<_>, LoadError>>()?;
        let mut envelope: Vec<String> = Vec::new();
        for (at, table) in tables.iter().enumerate() {
            let rows: Vec<&RowShape> = phase
                .rows
                .iter()
                .zip(&shapes)
                .filter_map(|(shape, &t)| (t == at).then_some(shape))
                .collect();
            if rows.is_empty() {
                return Err(LoadError::new(format!(
                    "no export rule writes table '{}'",
                    table.name
                )));
            }
            for spec in &table.specs {
                let set = rows.iter().any(|r| r.columns.contains(&spec.name));
                if !set && spec.name != "id" && !envelope.contains(&spec.name) {
                    envelope.push(spec.name.clone());
                }
            }
        }
        let id_prefix: Vec<String> = match env.pipeline.setting(0, "id_prefix") {
            Some(DataSpec::List(items)) => items
                .iter()
                .filter_map(|i| match i {
                    DataSpec::Str(s) => Some(s.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        for name in &id_prefix {
            if !envelope.contains(name) {
                envelope.push(name.clone());
            }
        }
        Ok(Self {
            tables,
            shapes,
            id_prefix,
            envelope,
            batch_bytes: orbit_arrow::BATCH_BYTES,
        })
    }
}

/// One `(table, batch)` per table, the edge table last.
pub fn export(
    env: &Env,
    state: &mut State,
    tables: &Tables,
    envelope: &Envelope,
    scope: &Scope,
    run: &Sentinel,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    envelope.check(tables)?;
    run.check()?;
    let phase = env.pipeline.phase_over("graph");
    let project = state.trees.len() as u32;
    state.trees.push(state.project_compact(env));
    let graph = crate::engine::EdgeIndex::new(&state.edges);
    let walk = state.trees.len();
    let rows = crate::engine::run_trees(
        phase,
        &state.trees,
        walk,
        Some(project),
        &state.edges,
        &graph,
        env,
        run,
    )
    .map(|(_, rows)| rows);
    let written = rows.map_err(Error::from).and_then(|rows| {
        Sink {
            env,
            trees: &state.trees,
            phase,
            tables,
            envelope,
        }
        .write(scope, rows)
    });
    state.trees.pop();
    written
}

/// The directories that hold `paths`, and the edges between them.
pub fn export_directories(
    paths: &[&str],
    ontology: &Ontology,
    envelope: &Envelope,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    let env = Env::for_lang(crate::language::SupportLang::Markdown)?;
    let tables = Tables::load(&env, ontology)?;
    let mut state = State::new(&env);
    state.trees = paths
        .iter()
        .map(|path| Tree::unparsed(&env.lang, path, 0, "").into())
        .collect();
    let none = FxHashSet::default();
    let scope = Scope {
        include_directories: true,
        owns_file: &|_| false,
        only: Some(&none),
    };
    export(
        &env,
        &mut state,
        &tables,
        envelope,
        &scope,
        &Sentinel::disabled(),
    )
}

struct Sink<'a> {
    env: &'a Env,
    trees: &'a [Tree<Compact>],
    phase: &'a Phase,
    tables: &'a Tables,
    envelope: &'a Envelope<'a>,
}

/// A row's id, its table, and whether it is written.
type Ids = FxHashMap<Loc, (i64, usize, bool)>;

impl Sink<'_> {
    fn text(&self, value: Option<Val>) -> Cow<'_, str> {
        let lang = &self.env.lang;
        Cow::Borrowed(match value {
            Some(Val::Sym(s)) => lang.syms.resolve(s),
            Some(Val::Node(l) | Val::Type(l)) => lang
                .syms
                .resolve(self.trees[l.fi as usize].sym_at(l.node, lang)),
            Some(Val::Kind(k)) => lang.kind_name(k),
            Some(Val::Bool(true)) => "true",
            Some(Val::Bool(false)) => "false",
            Some(Val::Opaque) | None => "",
        })
    }

    fn write(&self, scope: &Scope, rows: Vec<Row>) -> Result<Vec<(String, RecordBatch)>, Error> {
        let tables = &self.tables.tables;
        let mut builders = tables
            .iter()
            .map(|t| BatchBuilder::new(&t.specs, 0))
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::new();
        let (entities, edges): (Vec<&Row>, Vec<&Row>) = rows
            .iter()
            .partition(|r| self.phase.rows[usize::from(r.shape)].id.is_some());
        let mut ids = Ids::default();
        for row in entities {
            let shape = &self.phase.rows[usize::from(row.shape)];
            let at = self.tables.shapes[usize::from(row.shape)];
            let (seed, parts) = shape.id.as_ref().expect("entity rows have ids");
            let parts: Vec<String> = parts
                .iter()
                .map(|part| match *part {
                    Part::Column(i) | Part::Value(i) => self.text(row.values[i]).into_owned(),
                })
                .collect();
            let id = self.envelope.id(&self.tables.id_prefix, seed, &parts);
            let label = &self.trees[row.at.fi as usize].label;
            let written = scope.writes(&tables[at].entity, row.at.fi, label);
            ids.insert(row.at, (id, at, written));
            if written {
                self.row(&mut builders[at], &tables[at], shape, row, Some(id), &ids)?;
                if let Some(batch) = builders[at].take_if_over(self.tables.batch_bytes)? {
                    out.push((tables[at].name.clone(), batch));
                }
            }
        }
        let mut seen = FxHashSet::default();
        for row in edges {
            let shape = &self.phase.rows[usize::from(row.shape)];
            let at = self.tables.shapes[usize::from(row.shape)];
            let Some(nodes) = self.endpoints(&tables[at], shape, row, &ids) else {
                continue;
            };
            let written = match shape.follows {
                Some(column) => nodes
                    .iter()
                    .find(|(c, _)| *c == column)
                    .is_some_and(|(_, loc)| ids[loc].2),
                None => scope
                    .only
                    .is_none_or(|only| nodes.iter().any(|(_, loc)| only.contains(&loc.fi))),
            };
            if !written {
                continue;
            }
            if nodes.windows(2).any(|w| w[0].1.fi != w[1].1.fi) {
                let key: Vec<String> = (0..shape.columns.len())
                    .map(|i| match row.values[i] {
                        Some(Val::Node(loc)) => ids[&loc].0.to_string(),
                        value => self.text(value).into_owned(),
                    })
                    .collect();
                if !seen.insert(key) {
                    continue;
                }
            }
            self.row(&mut builders[at], &tables[at], shape, row, None, &ids)?;
            if let Some(batch) = builders[at].take_if_over(self.tables.batch_bytes)? {
                out.push((tables[at].name.clone(), batch));
            }
        }
        for (table, builder) in tables.iter().zip(builders) {
            out.push((table.name.clone(), builder.finish()?));
        }
        Ok(out)
    }

    /// The nodes an edge row's integer columns stand for, when each has a
    /// row of the expected table.
    fn endpoints(
        &self,
        table: &Table,
        shape: &RowShape,
        row: &Row,
        ids: &Ids,
    ) -> Option<Vec<(usize, Loc)>> {
        let mut nodes = Vec::new();
        for (i, name) in shape.columns.iter().enumerate() {
            let int = table
                .specs
                .iter()
                .any(|s| s.name == *name && s.col_type == ColumnType::Int);
            if !int {
                continue;
            }
            let loc = row.values[i].and_then(|v| v.loc())?;
            let &(_, at, _) = ids.get(&loc)?;
            let expected = shape.expect.iter().find(|(c, _)| *c == i);
            if expected.is_some_and(|(_, want)| self.tables.tables[at].name != *want) {
                return None;
            }
            nodes.push((i, loc));
        }
        Some(nodes)
    }

    fn row(
        &self,
        b: &mut BatchBuilder,
        table: &Table,
        shape: &RowShape,
        row: &Row,
        id: Option<i64>,
        ids: &Ids,
    ) -> Result<(), Error> {
        for spec in &table.specs {
            let column = shape.columns.iter().position(|c| *c == spec.name);
            let text;
            let value = match (self.envelope.get(&spec.name), column) {
                (Some(value), _) => value,
                (None, Some(i)) => match (spec.col_type, row.values[i]) {
                    (ColumnType::Int, Some(Val::Node(loc))) => Scalar::Int(ids[&loc].0),
                    (ColumnType::Int | ColumnType::TimestampMicros, value) => {
                        let found = self.text(value);
                        Scalar::Int(found.parse().map_err(|_| {
                            LoadError::new(format!(
                                "export column {}.{} is not a number: {found:?}",
                                table.name, spec.name
                            ))
                        })?)
                    }
                    (ColumnType::Bool, value) => Scalar::Bool(self.text(value) == "true"),
                    (_, value) => {
                        text = self.text(value);
                        Scalar::Str(&text)
                    }
                },
                (None, None) if spec.name == "id" => Scalar::Int(id.unwrap_or_default()),
                (None, None) => {
                    return Err(LoadError::new(format!(
                        "export column {}.{} has no value",
                        table.name, spec.name
                    ))
                    .into());
                }
            };
            value.write(b, &spec.name, spec.col_type)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python() -> Env {
        Env::for_lang(crate::language::SupportLang::Python).expect("python rules load")
    }

    #[test]
    fn a_missing_envelope_value_is_a_named_error() {
        let ontology = Ontology::load_embedded().expect("embedded ontology");
        let tables = Tables::load(&python(), &ontology).expect("export rules load");
        let envelope = Envelope::new([("project_id", Scalar::Int(1))]);
        let message = envelope.check(&tables).unwrap_err().to_string();
        assert!(message.contains("'branch'"), "{message}");
        assert!(message.contains("[\"project_id\"]"), "{message}");
    }

    #[test]
    fn export_rules_agree_with_the_ontology() {
        let ontology = Ontology::load_embedded().expect("embedded ontology");
        let tables = Tables::load(&python(), &ontology).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(tables.tables.len(), ontology.local_entity_names().len() + 1);
    }
}
