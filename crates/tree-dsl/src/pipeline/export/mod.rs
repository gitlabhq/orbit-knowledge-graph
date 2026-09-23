//! The canonical tree becomes the ontology's local tables, following
//! `config/export.yaml`. The YAML names every table, column, node kind and
//! tag; this module knows none of them. It checks the YAML against the
//! ontology when loaded, then walks the trees.

use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use orbit_utils::arrow::BatchBuilder;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::dsl::types::Tf;
use crate::error::Error;
use crate::file_tree::ProjectTree;
use crate::intern::Lang;
use crate::pipeline::State;
use crate::tree::{Cursor, EdgeKind, Tree};

mod plan;

pub use plan::ExportPlan;
use plan::{
    EdgeSource, EntityColumn, EntityPlan, HeaderSource, IdPart, On, Position, Source, SpanCandidate,
};

/// Values every row carries and the seed for stable ids; the same envelope
/// code-graph writes, so both pipelines land in the same tables with the
/// same ids.
pub struct Envelope<'a> {
    pub project_id: i64,
    pub branch: &'a str,
    pub commit_sha: &'a str,
}

impl Envelope<'_> {
    fn id(&self, seed: &str, parts: &[&str]) -> i64 {
        let mut hasher = FxHasher::default();
        self.project_id.to_string().hash(&mut hasher);
        self.branch.hash(&mut hasher);
        seed.hash(&mut hasher);
        parts.hash(&mut hasher);
        (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }
}

// ── the forest ──

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
            project,
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

// ── rows ──

enum Value<'a> {
    Str(Cow<'a, str>),
    Int(i64),
}

impl Value<'_> {
    fn id_part(&self) -> String {
        match self {
            Value::Str(s) => s.to_string(),
            Value::Int(n) => n.to_string(),
        }
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
    fn value(&self, column: &'a EntityColumn, lang: &'a Lang) -> Value<'a> {
        let node = match column.on {
            On::Node => self.node,
            On::Expanded => self.expanded,
        };
        match &column.source {
            Source::Const(value) => Value::Str(Cow::Borrowed(value)),
            Source::Transform(tf) => {
                let sym = tf.apply_sym(self.tree, lang, self.tree.to_id(node.index()), None);
                Value::Str(Cow::Borrowed(lang.syms.resolve(sym)))
            }
            Source::Position(p, span) => Value::Int(position(measured(node, span), *p)),
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

// ── export ──

/// One `(table, batch)` per local entity, then the edge table.
pub fn export(
    state: &State,
    lang: &Lang,
    ontology: &Ontology,
    envelope: &Envelope,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    let plan = ExportPlan::load(ontology, lang)?;
    let forest = Forest::new(&state.trees, lang);
    let mut ids: FxHashMap<(u32, u32), (i64, usize)> = FxHashMap::default();
    let mut tables = Vec::new();
    for (index, entity) in plan.entities.iter().enumerate() {
        let mut b = BatchBuilder::new(&entity.specs, 0)?;
        write_entity(
            &mut b, &plan, entity, index, &forest, lang, envelope, &mut ids,
        )?;
        tables.push((entity.table.clone(), b.finish()?));
    }
    tables.push((
        plan.edge_table.clone(),
        write_edges(&plan, state, &forest, &ids)?,
    ));
    Ok(tables)
}

#[allow(clippy::too_many_arguments)]
fn write_entity(
    b: &mut BatchBuilder,
    plan: &ExportPlan,
    entity: &EntityPlan,
    index: usize,
    forest: &Forest,
    lang: &Lang,
    envelope: &Envelope,
    ids: &mut FxHashMap<(u32, u32), (i64, usize)>,
) -> Result<(), Error> {
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
                        IdPart::Column(i) => row.value(&entity.columns[*i], lang).id_part(),
                    })
                    .collect();
                let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
                let id = envelope.id(&entity.id_seed, &parts);
                for column in &plan.header {
                    match &column.source {
                        HeaderSource::Id => b.col(&column.name)?.push_int(id)?,
                        HeaderSource::ProjectId => {
                            b.col(&column.name)?.push_int(envelope.project_id)?
                        }
                        HeaderSource::Branch => b.col(&column.name)?.push_str(envelope.branch)?,
                        HeaderSource::CommitSha => {
                            b.col(&column.name)?.push_str(envelope.commit_sha)?
                        }
                        HeaderSource::Const(v) => b.col(&column.name)?.push_str(v)?,
                    }
                }
                for column in &entity.columns {
                    match row.value(column, lang) {
                        Value::Int(n) => b.col(&column.name)?.push_int(n)?,
                        Value::Str(s) => b.col(&column.name)?.push_str(s)?,
                    }
                }
                ids.insert((ti, expanded.index()), (id, index));
            }
        }
    }
    Ok(())
}

struct EdgeRow<'a> {
    source: (i64, &'a str),
    kind: &'a str,
    target: (i64, &'a str),
}

fn write_edges(
    plan: &ExportPlan,
    state: &State,
    forest: &Forest,
    ids: &FxHashMap<(u32, u32), (i64, usize)>,
) -> Result<RecordBatch, Error> {
    let entity_name = |index: usize| plan.entities[index].name.as_str();
    let mut rows: Vec<EdgeRow> = Vec::new();

    let mut located: Vec<_> = ids.iter().collect();
    located.sort_unstable();
    for (&(tree, node), &(id, entity)) in located {
        for rule in plan.containment.iter().filter(|c| c.to == entity) {
            let enclosing = forest
                .ancestors(tree, node)
                .find_map(|loc| ids.get(&loc).filter(|(_, e)| *e == rule.from));
            if let Some(&(from_id, _)) = enclosing {
                rows.push(EdgeRow {
                    source: (from_id, entity_name(rule.from)),
                    kind: &rule.kind,
                    target: (id, entity_name(entity)),
                });
            }
        }
    }

    let sources_with: FxHashMap<EdgeKind, FxHashSet<(u32, u32)>> = plan
        .graph
        .iter()
        .filter_map(|g| g.unless_source_has)
        .map(|kind| {
            let set = state
                .edges
                .iter()
                .filter(|e| e.kind == kind)
                .map(|e| (e.from_tree, e.from_node))
                .collect();
            (kind, set)
        })
        .collect();
    let mut seen_cross_file = FxHashSet::default();

    for e in &state.edges {
        let Some(&(from, from_entity)) = ids.get(&(e.from_tree, e.from_node)) else {
            continue;
        };
        let Some(&(to, to_entity)) = ids.get(&(e.to_tree, e.to_node)) else {
            continue;
        };
        let rule = plan.graph.iter().find(|g| {
            g.edge == e.kind && g.to == to_entity && g.from.is_none_or(|f| f == from_entity)
        });
        let Some(rule) = rule else {
            continue;
        };
        if rule
            .unless_source_has
            .is_some_and(|k| sources_with[&k].contains(&(e.from_tree, e.from_node)))
        {
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

    Ok(
        BatchBuilder::new(&plan.edge_specs, rows.len())?.build(&rows, |row, b| {
            for column in &plan.edge_columns {
                match &column.source {
                    EdgeSource::SourceId => b.col(&column.name)?.push_int(row.source.0)?,
                    EdgeSource::SourceEntity => b.col(&column.name)?.push_str(row.source.1)?,
                    EdgeSource::Kind => b.col(&column.name)?.push_str(row.kind)?,
                    EdgeSource::TargetId => b.col(&column.name)?.push_int(row.target.0)?,
                    EdgeSource::TargetEntity => b.col(&column.name)?.push_str(row.target.1)?,
                    EdgeSource::Const(v) => b.col(&column.name)?.push_str(v)?,
                }
            }
            Ok(())
        })?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_yaml_agrees_with_the_ontology() {
        let ontology = Ontology::load_embedded().expect("embedded ontology");
        let lang = Lang::new();
        let plan = ExportPlan::load(&ontology, &lang).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(plan.entities.len(), ontology.local_entity_names().len());
    }
}
