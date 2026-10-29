//! The canonical tree becomes the ontology's local tables, following
//! `config/export.yaml`. The YAML names every table, column, node kind and
//! tag; this module knows none of them. It checks the YAML against the
//! ontology when loaded, then walks the trees.

use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use arrow::record_batch::RecordBatch;
use orbit_arrow::{BatchBuilder, ColumnType};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::canonical::Canonical as C;
use crate::dsl::types::{EdgeCtx, EdgeIndex, Tf};
use crate::error::{Error, LoadError};
use crate::file_tree::ProjectTree;
use crate::intern::Lang;
use crate::sentinel::Sentinel;
use crate::tree::{CallResolution, Compact, Resolution};
type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

mod parse;

use parse::{
    EntityColumn, EntityPlan, GraphEdge, IdPart, On, Over, Position, RowSource, Source,
    SpanCandidate,
};
pub use parse::{ExportConfig, ExportPlan};

/// A value the caller hands in with every export.
#[derive(Clone)]
pub enum Scalar<'a> {
    Int(i64),
    Str(&'a str),
    Bool(bool),
    Text(Cow<'a, str>),
    List(Vec<Cow<'a, str>>),
    Ints(Vec<i64>),
    Null,
}

impl Scalar<'_> {
    fn text(&self) -> Cow<'_, str> {
        match self {
            Scalar::Int(n) => Cow::Owned(n.to_string()),
            Scalar::Str(s) => Cow::Borrowed(s),
            Scalar::Bool(value) => Cow::Owned(value.to_string()),
            Scalar::Text(s) => Cow::Borrowed(s),
            Scalar::List(values) => Cow::Owned(values.join("\u{1f}")),
            Scalar::Ints(values) => Cow::Owned(format!("{values:?}")),
            Scalar::Null => Cow::Borrowed(""),
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
            (Self::Str(value @ ("true" | "false")), ColumnType::Bool) => {
                builder.col(name)?.push_bool(value == "true")
            }
            (Self::Str(""), ColumnType::StrList) => builder.col(name)?.push_str_list(&[]),
            (Self::Str(""), ColumnType::IntList) => builder.col(name)?.push_int_list(&[]),
            (Self::Str(value), ColumnType::Str | ColumnType::DictStr) => {
                builder.col(name)?.push_str(value)
            }
            (Self::Text(value), ColumnType::Str | ColumnType::DictStr) => {
                builder.col(name)?.push_str(value)
            }
            (Self::Str(value), ColumnType::Bool) if matches!(value, "true" | "false") => {
                builder.col(name)?.push_bool(value == "true")
            }
            (Self::List(values), ColumnType::StrList | ColumnType::DictStrList) => {
                let values: Vec<&str> = values.iter().map(Cow::as_ref).collect();
                builder.col(name)?.push_str_list(&values)
            }
            (Self::Ints(values), ColumnType::IntList) => builder.col(name)?.push_int_list(&values),
            (Self::Null, ColumnType::Str | ColumnType::DictStr) => {
                builder.col(name)?.push_opt_str(None::<&str>)
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
            .map(|(_, v)| v.clone())
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
    edges: Option<&'a EdgeCtx<'a>>,
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
                let sym = tf.apply_at(self.tree, lang, node.index(), self.edges);
                Scalar::Str(lang.syms.resolve(sym))
            }
            Source::Position(p, span) => Scalar::Int(position(measured(node, span), *p)),
        }
    }

    fn finish(
        &self,
        column: &'a EntityColumn,
        value: Scalar<'a>,
    ) -> Result<Option<Scalar<'a>>, Error> {
        let Scalar::Str(text) = value else {
            return Ok(Some(value));
        };
        let bounds = &column.bounds;
        let capped = |text: &'a str| match bounds.cap {
            Some(cap) if text.len() > cap => match bounds.over {
                Over::Cut | Over::Words => Some(orbit_utils::strings::truncate_bytes(
                    text,
                    cap,
                    "…",
                    bounds.over == Over::Words,
                )),
                Over::Empty => Some(Cow::Borrowed("")),
                Over::Drop => None,
            },
            _ => Some(Cow::Borrowed(text)),
        };
        if let Some(sep) = &bounds.split {
            let mut values: Vec<Cow<str>> = text
                .split(sep.as_str())
                .filter(|part| !part.is_empty())
                .filter_map(capped)
                .collect();
            if bounds.sort {
                values.sort_unstable();
                values.dedup();
            }
            values.truncate(bounds.max.unwrap_or(usize::MAX));
            if let Some(bad) = values.iter().find(|v| !column.admits(v)) {
                return Err(self.not_allowed(column, bad));
            }
            return Ok(Some(Scalar::List(values)));
        }
        let Some(text) = capped(text) else {
            return Ok(None);
        };
        if column.nullable && text.is_empty() {
            return Ok(Some(Scalar::Null));
        }
        if !column.admits(&text) {
            return Err(self.not_allowed(column, &text));
        }
        Ok(Some(match text {
            Cow::Borrowed(text) => Scalar::Str(text),
            owned => Scalar::Text(owned),
        }))
    }

    fn not_allowed(&self, column: &EntityColumn, value: &str) -> Error {
        LoadError::new(format!(
            "export: column '{}' has {value:?} in {}, which is not one of {:?}",
            column.name,
            self.tree.label,
            column.allowed.as_deref().unwrap_or_default()
        ))
        .into()
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
#[allow(clippy::too_many_arguments)]
pub fn export(
    trees: &[Tree],
    edges: &[crate::tree::Edge],
    lang: &Lang,
    plan: &ExportPlan,
    envelope: &Envelope,
    include_directories: bool,
    owns_file: &dyn Fn(&str) -> bool,
    run: &Sentinel,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    export_trees(
        trees,
        edges,
        lang,
        plan,
        envelope,
        include_directories,
        owns_file,
        None,
        run,
    )
}

/// The directories that hold `paths`, and the edges between them.
pub fn export_directories(
    paths: &[&str],
    export_config: &ExportConfig,
    ontology: &ontology::Ontology,
    envelope: &Envelope,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    let lang = Lang::new();
    let plan = ExportPlan::from_config(export_config, ontology, &lang)?;
    let trees: Vec<Tree> = paths
        .iter()
        .map(|path| crate::tree::Tree::unparsed(&lang, path, 0, "").into())
        .collect();
    export_trees(
        &trees,
        &[],
        &lang,
        &plan,
        envelope,
        true,
        &|_| false,
        Some(&FxHashSet::default()),
        &Sentinel::disabled(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn export_trees(
    trees: &[Tree],
    edges: &[crate::tree::Edge],
    lang: &Lang,
    plan: &ExportPlan,
    envelope: &Envelope,
    include_directories: bool,
    owns_file: &dyn Fn(&str) -> bool,
    only: Option<&FxHashSet<u32>>,
    run: &Sentinel,
) -> Result<Vec<(String, RecordBatch)>, Error> {
    envelope.check(plan)?;
    run.check()?;
    let forest = Forest::new(trees, lang);
    let needed: Option<FxHashSet<u32>> = only.map(|only| {
        let endpoints = edges
            .iter()
            .filter(|e| only.contains(&e.from_tree) || only.contains(&e.to_tree))
            .flat_map(|e| [e.from_tree, e.to_tree]);
        only.iter()
            .copied()
            .chain(endpoints)
            .chain([PROJECT])
            .collect()
    });
    let wanted = |tree: u32| needed.as_ref().is_none_or(|needed| needed.contains(&tree));
    let mut ids = FxHashMap::default();
    let mut fanout = Fanout::default();
    let mut tables = Vec::new();
    let edge_index = EdgeIndex::new(edges);
    for (index, entity) in plan.entities.iter().enumerate() {
        write_entity(
            &mut tables,
            plan,
            entity,
            index,
            &forest,
            &edge_index,
            lang,
            envelope,
            &mut ids,
            &mut fanout,
            &wanted,
            &|index, tree| match entity.name.as_str() {
                "Directory" => include_directories,
                "File" => owns_file(&tree.label) && only.is_none_or(|only| only.contains(&index)),
                _ => only.is_none_or(|only| only.contains(&index)),
            },
            run,
        )?;
    }
    run.check()?;
    for batch in write_edges(plan, edges, &forest, &ids, &fanout, envelope, only)? {
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
    edge_index: &EdgeIndex,
    lang: &Lang,
    envelope: &Envelope,
    ids: &mut FxHashMap<(u32, u32), (i64, usize, bool)>,
    fanout: &mut Fanout,
    wanted: &dyn Fn(u32) -> bool,
    emit: &dyn Fn(u32, &Tree) -> bool,
    run: &Sentinel,
) -> Result<(), Error> {
    let mut builder = BatchBuilder::new(&entity.specs, 0)?;
    let b = &mut builder;
    let mut written = FxHashSet::default();
    for (ti, tree) in forest.all().filter(|(ti, _)| wanted(*ti)) {
        run.check()?;
        let ctx = EdgeCtx {
            tree_index: ti,
            edges: edge_index,
            source: None,
        };
        let root = tree.root();
        let nodes = std::iter::once(root)
            .chain(root.descendants())
            .filter(|c| entity.source_kinds.contains(&c.kind()))
            .filter(|c| {
                entity
                    .exclude_parent
                    .is_none_or(|ep| !c.parent().is_some_and(|p| p.kind() == ep))
            })
            .filter(|c| {
                !entity
                    .exclude_tag
                    .iter()
                    .any(|(key, values)| c.tag(*key).is_some_and(|v| values.contains(&v)))
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
                    edges: Some(&ctx),
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
                let values = entity
                    .columns
                    .iter()
                    .map(|column| row.finish(column, row.value(column, lang)))
                    .collect::<Result<Option<Vec<_>>, Error>>()?;
                let Some(values) = values else {
                    continue;
                };
                let id = envelope.id(plan, &entity.id_seed, &parts);
                let emitted = emit(ti, tree);
                ids.insert((ti, expanded.index()), (id, index, emitted));
                if expanded.index() != node.index() {
                    fanout
                        .entry((ti, node.index()))
                        .or_default()
                        .push((id, index));
                }
                if !emitted || !written.insert(id) {
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
                for (column, value) in entity.columns.iter().zip(values) {
                    value.write(b, &column.name, column.col_type)?;
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

type Enclosing = FxHashMap<(usize, (u32, u32)), Option<(u32, u32)>>;
type Fanout = FxHashMap<(u32, u32), Vec<(i64, usize)>>;

const MAX_LINES: usize = 64;

struct EdgeRow<'a> {
    source: (i64, &'a str),
    kind: &'a str,
    target: (i64, &'a str),
    lines: Vec<i64>,
    resolution: Resolution,
    set: &'a [(String, String)],
}

impl GraphEdge {
    fn admits(&self, e: &crate::tree::Edge, forest: &Forest) -> bool {
        let site = e.site.map(|s| forest.cursor(e.from_tree, s));
        let target = forest.cursor(e.to_tree, e.to_node);
        (self.site.is_empty() || site.is_some_and(|c| self.site.contains(&c.kind())))
            && self.site_child.is_none_or(|kind| {
                site.is_some_and(|c| c.children().any(|child| child.kind() == kind))
            })
            && (!self.receiver
                || site
                    .and_then(|c| c.child(C::Callee))
                    .and_then(|callee| callee.child_sym(C::Member))
                    .is_some_and(|member| Some(member) != target.child_sym(C::DefName)))
            && self
                .target_tag
                .iter()
                .all(|(key, values)| target.tag(*key).is_some_and(|v| values.contains(&v)))
            && (!self.reference || e.call_resolution == CallResolution::Reference)
    }
}

fn write_edges(
    plan: &ExportPlan,
    edges: &[crate::tree::Edge],
    forest: &Forest,
    ids: &FxHashMap<(u32, u32), (i64, usize, bool)>,
    fanout: &Fanout,
    envelope: &Envelope,
    only: Option<&FxHashSet<u32>>,
) -> Result<Vec<RecordBatch>, Error> {
    let entity_name = |index: usize| plan.entities[index].name.as_str();
    let mut rows: Vec<EdgeRow> = Vec::new();

    let mut located: Vec<_> = ids.iter().collect();
    located.sort_unstable();
    let mut enclosing = Enclosing::default();
    let mut path = Vec::new();
    let mut current_tree = None;
    for (&(tree, node), &(id, entity, emitted)) in located {
        if !emitted {
            continue;
        }
        if current_tree != Some(tree) {
            current_tree = Some(tree);
            enclosing.clear();
        }
        for rule in plan.containment.iter().filter(|c| c.to == entity) {
            path.clear();
            path.push((tree, node));
            let found = loop {
                let (tree, node) = *path.last().expect("the start location");
                let Some(above) = forest
                    .ancestors(tree, node)
                    .find(|loc| ids.contains_key(loc))
                else {
                    break None;
                };
                if ids[&above].1 == rule.from {
                    break Some(above);
                }
                if let Some(&known) = enclosing.get(&(rule.from, above)) {
                    break known;
                }
                path.push(above);
            };
            for &loc in &path {
                enclosing.insert((rule.from, loc), found);
            }
            if let Some(&(from_id, _, _)) = found.and_then(|loc| ids.get(&loc)) {
                rows.push(EdgeRow {
                    source: (from_id, entity_name(rule.from)),
                    kind: &rule.kind,
                    target: (id, entity_name(entity)),
                    lines: Vec::new(),
                    resolution: Resolution::None,
                    set: &[],
                });
            }
        }
    }

    let mut merged: FxHashMap<(i64, &str, i64), usize> = FxHashMap::default();
    for e in edges {
        if only.is_some_and(|only| !only.contains(&e.from_tree) && !only.contains(&e.to_tree)) {
            continue;
        }
        let Some(&(to, to_entity, _)) = ids.get(&(e.to_tree, e.to_node)) else {
            continue;
        };
        let sources = match ids.get(&(e.from_tree, e.from_node)) {
            Some(&(from, entity, _)) => vec![(from, entity)],
            None => fanout
                .get(&(e.from_tree, e.from_node))
                .cloned()
                .unwrap_or_default(),
        };
        let resolution = match e.resolution {
            Resolution::None if e.kind != crate::tree::EdgeKind::Defines => {
                match e.from_tree == e.to_tree {
                    true => Resolution::Local,
                    false => Resolution::Import,
                }
            }
            resolution => resolution,
        };
        let line = e
            .site
            .map(|site| forest.cursor(e.from_tree, site).start_row() as i64 + 1);
        let enclosing = |tree: u32, node: u32, entity: usize| {
            std::iter::once((tree, node))
                .chain(forest.ancestors(tree, node))
                .find_map(|loc| ids.get(&loc).filter(|found| found.1 == entity))
                .map(|found| found.0)
        };
        let mut matched = Vec::new();
        for (from, from_entity) in sources {
            let admits = |g: &&GraphEdge| {
                g.edge == e.kind
                    && g.to == to_entity
                    && g.from.is_none_or(|f| f == from_entity)
                    && g.admits(e, forest)
            };
            let rule = plan.graph.iter().filter(|g| g.lift.is_none()).find(admits);
            if let Some(rule) = rule
                && !(rule.fallback && e.call_resolution != CallResolution::Unknown)
            {
                matched.push((rule, (from, from_entity), (to, to_entity), resolution, line));
            }
            for rule in plan
                .graph
                .iter()
                .filter(|g| g.lift.is_some())
                .filter(admits)
            {
                let entity = rule.lift.expect("a lifted rule");
                let ends = enclosing(e.from_tree, e.from_node, entity)
                    .zip(enclosing(e.to_tree, e.to_node, entity))
                    .filter(|(a, b)| a != b);
                if let Some((a, b)) = ends {
                    matched.push((rule, (a, entity), (b, entity), Resolution::None, None));
                }
            }
        }
        for (rule, (from, from_entity), (to, to_entity), resolution, line) in matched {
            let kind = rule.kind.as_str();
            let at = *merged.entry((from, kind, to)).or_insert_with(|| {
                rows.push(EdgeRow {
                    source: (from, entity_name(from_entity)),
                    kind,
                    target: (to, entity_name(to_entity)),
                    lines: Vec::new(),
                    resolution,
                    set: &rule.set,
                });
                rows.len() - 1
            });
            let row = &mut rows[at];
            row.resolution = row.resolution.min(resolution);
            row.lines.extend(line);
        }
    }
    for row in &mut rows {
        row.lines.sort_unstable();
        row.lines.dedup();
        row.lines.truncate(MAX_LINES);
    }

    let mut batches = Vec::new();
    let mut b = BatchBuilder::new(&plan.edge_specs, rows.len())?;
    for row in &rows {
        for column in &plan.edge_columns {
            let set = row
                .set
                .iter()
                .find(|(name, _)| *name == column.name)
                .map(|(_, value)| Scalar::Str(value));
            let value = match &column.source {
                RowSource::Const(value) => set
                    .or_else(|| envelope.get(&column.name))
                    .unwrap_or(Scalar::Str(value)),
                RowSource::Value(name) => match name.as_str() {
                    "source.id" => Scalar::Int(row.source.0),
                    "source.entity" => Scalar::Str(row.source.1),
                    "kind" => Scalar::Str(row.kind),
                    "target.id" => Scalar::Int(row.target.0),
                    "target.entity" => Scalar::Str(row.target.1),
                    "lines" => Scalar::Ints(row.lines.clone()),
                    "resolution" => Scalar::Str(row.resolution.into()),
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
