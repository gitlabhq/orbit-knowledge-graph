//! The canonical tree becomes the ontology's local tables, following
//! `config/export.yaml`. The YAML names every table, column, node kind and
//! tag; this module knows none of them. It checks the YAML against the
//! ontology when loaded, then walks the trees.

use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use arrow::record_batch::RecordBatch;
use orbit_utils::arrow::{BatchBuilder, ColumnType};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::dsl::types::Tf;
use crate::error::{Error, LoadError};
use crate::file_tree::ProjectTree;
use crate::intern::Lang;
use crate::tree::{CallResolution, Compact};
type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

mod parse;

use parse::{EntityColumn, EntityPlan, IdPart, On, Position, RowSource, Source, SpanCandidate};
pub use parse::{ExportConfig, ExportPlan};

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
/// indexed. `export.yaml` binds them to header columns and picks the ones
/// that seed row ids, so ids match code-graph's for the same inputs.
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

    /// Confirms every name the plan refers to was handed in.
    fn check(&self, plan: &ExportPlan) -> Result<(), Error> {
        for name in plan.envelope_names() {
            if self.get(name).is_none() {
                let given: Vec<&str> = self.values.iter().map(|(n, _)| *n).collect();
                return Err(LoadError::new(format!(
                    "export.yaml refers to envelope value '{name}' but the caller gave {given:?}"
                ))
                .into());
            }
        }
        Ok(())
    }

    /// `FxHash` over `[prefix values.., seed, parts..]`, sign bit cleared: the
    /// same computation as code-graph's `compute_id`, so the same inputs give
    /// the same id in both pipelines.
    fn id(&self, plan: &ExportPlan, seed: &str, parts: &[&str]) -> i64 {
        let prefix: Vec<Cow<str>> = plan
            .id_prefix
            .iter()
            .filter_map(|name| self.get(name))
            .map(|v| v.text().into_owned().into())
            .collect();
        let components: Vec<&str> = prefix
            .iter()
            .map(Cow::as_ref)
            .chain(std::iter::once(seed))
            .chain(parts.iter().copied())
            .collect();
        let mut hasher = FxHasher::default();
        components.hash(&mut hasher);
        (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }
}

/// Every tree the exporter walks: the parsed files plus the project tree,
/// with each file's root standing in for its `__file` node so ancestry
/// continues from a definition to its file to its directories.
struct Forest<'a> {
    trees: &'a [Tree],
    project: Tree,
    /// For each file tree, the project-tree node its root stands in for.
    stand_in: Vec<Option<u32>>,
}

const PROJECT: u32 = u32::MAX;

impl<'a> Forest<'a> {
    fn new(trees: &'a [Tree], lang: &Lang) -> Self {
        let labels: Vec<&str> = trees.iter().map(|t| t.label.as_str()).collect();
        let project = ProjectTree::directory_tree(lang, &labels);
        let path = Tf::TreePath("/".into());
        let mut by_path: FxHashMap<&str, u32> = FxHashMap::default();
        for node in project.root().descendants() {
            if node.children().next().is_none() {
                let sym = path.apply_sym(&project, lang, project.to_id(node.index()), None);
                by_path.insert(lang.syms.resolve(sym), node.index());
            }
        }
        let stand_in = labels.iter().map(|l| by_path.get(*l).copied()).collect();
        Self {
            trees,
            project: project.into(),
            stand_in,
        }
    }

    fn tree(&self, tree: u32) -> &Tree {
        if tree == PROJECT {
            &self.project
        } else {
            &self.trees[tree as usize]
        }
    }

    fn cursor(&self, tree: u32, node: u32) -> Cursor<'_> {
        self.tree(tree).cursor(node)
    }

    /// Ancestors of `(tree, node)`, crossing from a file root into the project tree.
    fn ancestors(&self, tree: u32, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let own = self
            .cursor(tree, node)
            .ancestors()
            .map(move |a| (tree, a.index()));
        let above = if tree == PROJECT {
            None
        } else {
            self.stand_in[tree as usize]
        };
        let project = above
            .into_iter()
            .flat_map(move |file| self.cursor(PROJECT, file).ancestors())
            .map(|a| (PROJECT, a.index()));
        own.chain(project)
    }

    fn all(&self) -> impl Iterator<Item = (u32, &Tree)> {
        self.trees
            .iter()
            .enumerate()
            .map(|(i, t)| (i as u32, t))
            .chain(std::iter::once((PROJECT, &self.project)))
    }
}

/// The node a row describes and, under `expand:`, the child it was expanded
/// from; without an expansion the node stands in for both.
struct Row<'a> {
    tree: &'a Tree,
    node: Cursor<'a>,
    expanded: Cursor<'a>,
}

impl<'a> Row<'a> {
    fn value(&self, column: &'a EntityColumn, lang: &'a Lang) -> Scalar<'a> {
        let node = match column.on {
            On::Node => self.node,
            On::Expanded => self.expanded,
        };
        match &column.source {
            Source::Const(value) => Scalar::Str(value),
            Source::Transform(tf) => {
                let sym = tf.apply_at(self.tree, lang, node.index(), None);
                Scalar::Str(lang.syms.resolve(sym))
            }
            Source::Position(p, span) => Scalar::Int(position(measured(node, span), *p)),
        }
    }

    fn span(&self) -> String {
        format!("{}:{}", self.node.start(), self.node.end())
    }
}

fn measured<'a>(node: Cursor<'a>, candidates: &[SpanCandidate]) -> Cursor<'a> {
    candidates
        .iter()
        .find_map(|c| match c {
            SpanCandidate::Tagged(tag) if node.has_tag(*tag) => Some(node),
            SpanCandidate::Tagged(_) => None,
            SpanCandidate::Child(kind) => child(node, *kind),
        })
        .unwrap_or(node)
}

fn position(c: Cursor, position: Position) -> i64 {
    match position {
        Position::StartLine => c.start_row() as i64 + 1,
        Position::EndLine => c.end_row() as i64 + 1,
        Position::StartByte => c.start() as i64,
        Position::EndByte => c.end() as i64,
        Position::StartCol => c.start_col() as i64 + 1,
        Position::EndCol => c.end_col() as i64 + 1,
    }
}

fn child(c: Cursor, kind: u16) -> Option<Cursor> {
    c.children().find(|ch| ch.kind() == kind)
}

/// One `(table, batch)` per local entity, then the edge table.
pub fn export(
    trees: &[Tree],
    edges: &[crate::tree::Edge],
    lang: &Lang,
    plan: &ExportPlan,
    envelope: &Envelope,
    include_directories: bool,
    owns_file: &dyn Fn(&str) -> bool,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    envelope.check(plan)?;
    let forest = Forest::new(trees, lang);
    let mut ids = FxHashMap::default();
    let mut tables = Vec::new();
    for (index, entity) in plan.entities.iter().enumerate() {
        write_entity(
            &mut tables,
            plan,
            entity,
            index,
            &forest,
            lang,
            envelope,
            &mut ids,
            &|tree| match entity.name.as_str() {
                "Directory" => include_directories,
                "File" => owns_file(&tree.label),
                _ => true,
            },
        )?;
    }
    for batch in write_edges(plan, edges, &forest, &ids, envelope)? {
        tables.push((plan.edge_table.clone(), batch));
    }
    Ok(tables)
}

#[allow(clippy::too_many_arguments)]
fn write_entity(
    tables: &mut Vec<(String, RecordBatch)>,
    plan: &ExportPlan,
    entity: &EntityPlan,
    index: usize,
    forest: &Forest,
    lang: &Lang,
    envelope: &Envelope,
    ids: &mut FxHashMap<(u32, u32), (i64, usize, bool)>,
    emit: &dyn Fn(&Tree) -> bool,
) -> Result<(), Error> {
    let mut builder = BatchBuilder::new(&entity.specs, 0)?;
    let b = &mut builder;
    for (ti, tree) in forest.all() {
        let root = tree.root();
        let nodes = std::iter::once(root)
            .chain(root.descendants())
            .filter(|c| entity.source_kinds.contains(&c.kind()))
            .filter(|c| {
                entity
                    .exclude_parent
                    .is_none_or(|ep| !c.parent().is_some_and(|p| p.kind() == ep))
            });
        for node in nodes {
            let mut expansions: Vec<Cursor> = match entity.expand {
                Some(kind) => node
                    .children()
                    .filter(|ch| ch.kind() == kind && ch.sym() != 0)
                    .collect(),
                None => Vec::new(),
            };
            if expansions.is_empty() {
                expansions.push(node);
            }
            for expanded in expansions {
                let row = Row {
                    tree,
                    node,
                    expanded,
                };
                let parts: Vec<String> = entity
                    .id_parts
                    .iter()
                    .map(|part| match part {
                        IdPart::Span => row.span(),
                        IdPart::Column(i) => {
                            row.value(&entity.columns[*i], lang).text().into_owned()
                        }
                    })
                    .collect();
                let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
                let id = envelope.id(plan, &entity.id_seed, &parts);
                let emitted = emit(tree);
                ids.insert((ti, expanded.index()), (id, index, emitted));
                if !emitted {
                    continue;
                }
                for column in &entity.header {
                    let value = match &column.source {
                        RowSource::Value(name) if name == "id" => Scalar::Int(id),
                        RowSource::Value(name) => envelope.get(name).expect("validated envelope"),
                        RowSource::Const(v) => envelope.get(&column.name).unwrap_or(Scalar::Str(v)),
                    };
                    value.write(b, &column.name, column.col_type)?;
                }
                for column in &entity.columns {
                    row.value(column, lang)
                        .write(b, &column.name, column.col_type)?;
                }
                if let Some(batch) = b.take_if_over(plan.batch_bytes)? {
                    tables.push((entity.table.clone(), batch));
                }
            }
        }
    }
    tables.push((entity.table.clone(), builder.finish()?));
    Ok(())
}

struct EdgeRow<'a> {
    source: (i64, &'a str),
    kind: &'a str,
    target: (i64, &'a str),
}

fn write_edges(
    plan: &ExportPlan,
    edges: &[crate::tree::Edge],
    forest: &Forest,
    ids: &FxHashMap<(u32, u32), (i64, usize, bool)>,
    envelope: &Envelope,
) -> Result<Vec<RecordBatch>, Error> {
    let entity_name = |index: usize| plan.entities[index].name.as_str();
    let mut rows: Vec<EdgeRow> = Vec::new();

    let mut located: Vec<_> = ids.iter().collect();
    located.sort_unstable();
    for (&(tree, node), &(id, entity, emitted)) in located {
        if !emitted {
            continue;
        }
        for rule in plan.containment.iter().filter(|c| c.to == entity) {
            let enclosing = forest
                .ancestors(tree, node)
                .find_map(|loc| ids.get(&loc).filter(|(_, e, _)| *e == rule.from));
            if let Some(&(from_id, _, _)) = enclosing {
                rows.push(EdgeRow {
                    source: (from_id, entity_name(rule.from)),
                    kind: &rule.kind,
                    target: (id, entity_name(entity)),
                });
            }
        }
    }

    let mut seen_cross_file = FxHashSet::default();

    for e in edges {
        let Some(&(from, from_entity, _)) = ids.get(&(e.from_tree, e.from_node)) else {
            continue;
        };
        let Some(&(to, to_entity, _)) = ids.get(&(e.to_tree, e.to_node)) else {
            continue;
        };
        let rule = plan.graph.iter().find(|g| {
            g.edge == e.kind && g.to == to_entity && g.from.is_none_or(|f| f == from_entity)
        });
        let Some(rule) = rule else {
            continue;
        };
        if rule.fallback && e.call_resolution != CallResolution::Unknown {
            continue;
        }
        let kind = rule.kind.as_str();
        if e.from_tree != e.to_tree && !seen_cross_file.insert((from, to, kind)) {
            continue;
        }
        rows.push(EdgeRow {
            source: (from, entity_name(from_entity)),
            kind,
            target: (to, entity_name(to_entity)),
        });
    }

    let mut batches = Vec::new();
    let mut b = BatchBuilder::new(&plan.edge_specs, rows.len())?;
    for row in &rows {
        for column in &plan.edge_columns {
            let value = match &column.source {
                RowSource::Const(value) => envelope.get(&column.name).unwrap_or(Scalar::Str(value)),
                RowSource::Value(name) => match name.as_str() {
                    "source.id" => Scalar::Int(row.source.0),
                    "source.entity" => Scalar::Str(row.source.1),
                    "kind" => Scalar::Str(row.kind),
                    "target.id" => Scalar::Int(row.target.0),
                    "target.entity" => Scalar::Str(row.target.1),
                    name => envelope.get(name).expect("validated envelope"),
                },
            };
            value.write(&mut b, &column.name, column.col_type)?;
        }
        if let Some(batch) = b.take_if_over(plan.batch_bytes)? {
            batches.push(batch);
        }
    }
    batches.push(b.finish()?);
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::Ontology;

    #[test]
    fn a_missing_envelope_value_is_a_named_error() {
        let ontology = Ontology::load_embedded().expect("embedded ontology");
        let lang = Lang::new();
        let plan = ExportPlan::from_config(ExportConfig::embedded(), &ontology, &lang)
            .expect("export.yaml loads");
        let envelope = Envelope::new([("project_id", Scalar::Int(1))]);
        let message = envelope.check(&plan).unwrap_err().to_string();
        assert!(message.contains("'branch'"), "{message}");
        assert!(message.contains("[\"project_id\"]"), "{message}");
    }

    #[test]
    fn export_yaml_agrees_with_the_ontology() {
        let ontology = Ontology::load_embedded().expect("embedded ontology");
        let lang = Lang::new();
        let plan = ExportPlan::from_config(ExportConfig::embedded(), &ontology, &lang)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(plan.entities.len(), ontology.local_entity_names().len());
    }
}
