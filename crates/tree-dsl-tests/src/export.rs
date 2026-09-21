use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_56::array::{ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;

use tree_dsl::canonical::Canonical as C;
use tree_dsl::intern::Lang;
use tree_dsl::tree::{Cursor, Edge, EdgeKind, Tree};

pub type Datasets = HashMap<String, RecordBatch>;

// ── Config types ──

#[derive(serde::Deserialize)]
struct ExportConfig {
    entities: indexmap::IndexMap<String, EntityConfig>,
    edges: indexmap::IndexMap<String, EdgeConfig>,
}

#[derive(serde::Deserialize)]
struct EntityConfig {
    source: String,
    #[serde(default)]
    exclude_parent: Option<String>,
    #[serde(default)]
    expand: Option<String>,
    columns: Vec<ColumnConfig>,
}

#[derive(serde::Deserialize)]
struct ColumnConfig {
    name: String,
    #[serde(rename = "type")]
    dtype: String,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    compute: Option<String>,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    expand_sym: bool,
}

#[derive(serde::Deserialize)]
struct EdgeConfig {
    source: String,
    target: String,
    #[serde(default)]
    tree_containment: bool,
    #[serde(default)]
    from_edges: bool,
    #[serde(default)]
    only_kinds: Vec<String>,
    #[serde(default)]
    exclude_kinds: Vec<String>,
    #[serde(default)]
    dedup_cross: bool,
    #[serde(default)]
    skip_if_has_calls: bool,
    #[serde(default)]
    label: Option<String>,
}

// ── Table builder ──

enum Val<'a> {
    I(i64),
    S(&'a str),
    B(bool),
    Null,
}

struct Table {
    schema: Vec<(String, DataType, bool)>,
    cols: Vec<Box<dyn ArrayBuilder>>,
}

impl Table {
    fn from_columns(cols: &[ColumnConfig]) -> Self {
        let schema: Vec<(String, DataType, bool)> = cols
            .iter()
            .map(|c| {
                let dt = match c.dtype.as_str() {
                    "Int64" => DataType::Int64,
                    "Utf8" => DataType::Utf8,
                    "Boolean" => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                (c.name.clone(), dt, c.nullable)
            })
            .collect();
        let builders = schema
            .iter()
            .map(|(_, dt, _)| -> Box<dyn ArrayBuilder> {
                match dt {
                    DataType::Int64 => Box::new(Int64Builder::new()),
                    DataType::Utf8 => Box::new(StringBuilder::new()),
                    DataType::Boolean => Box::new(BooleanBuilder::new()),
                    _ => Box::new(StringBuilder::new()),
                }
            })
            .collect();
        Self {
            schema,
            cols: builders,
        }
    }

    fn edge() -> Self {
        Self::from_columns(&[
            ColumnConfig {
                name: "source_id".into(),
                dtype: "Int64".into(),
                from: None,
                compute: None,
                nullable: false,
                expand_sym: false,
            },
            ColumnConfig {
                name: "target_id".into(),
                dtype: "Int64".into(),
                from: None,
                compute: None,
                nullable: false,
                expand_sym: false,
            },
            ColumnConfig {
                name: "edge_kind".into(),
                dtype: "Utf8".into(),
                from: None,
                compute: None,
                nullable: false,
                expand_sym: false,
            },
        ])
    }

    fn row(&mut self, vals: &[Val]) {
        for (i, v) in vals.iter().enumerate() {
            match v {
                Val::I(n) => self.cols[i]
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .unwrap()
                    .append_value(*n),
                Val::S(s) => self.cols[i]
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(s),
                Val::B(b) => self.cols[i]
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap()
                    .append_value(*b),
                Val::Null => match self.schema[i].1 {
                    DataType::Utf8 => self.cols[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_null(),
                    DataType::Int64 => self.cols[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .unwrap()
                        .append_null(),
                    DataType::Boolean => self.cols[i]
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap()
                        .append_null(),
                    _ => {}
                },
            }
        }
    }

    fn finish(self) -> anyhow::Result<RecordBatch> {
        let schema = Arc::new(Schema::new(
            self.schema
                .iter()
                .map(|(n, dt, null)| Field::new(n, dt.clone(), *null))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<Arc<dyn arrow_56::array::Array>> =
            self.cols.into_iter().map(|mut b| b.finish()).collect();
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

// ── Helpers ──

type IdMap = HashMap<(usize, u32), i64>;

fn child_sym_by_kind(c: Cursor, kind_name: &str, lang: &Lang) -> u32 {
    let kind = lang.intern_kind(kind_name);
    c.children()
        .find(|ch| ch.kind() == kind)
        .map(|ch| ch.sym())
        .unwrap_or(0)
}

fn find_tag(tree: &Tree, c: Cursor, tag_key: &str, lang: &Lang) -> u32 {
    let key_sym = lang.syms.intern(tag_key);
    if let Some(v) = tree.get_tag(c.index(), key_sym) {
        return v;
    }
    for a in c.ancestors() {
        if let Some(v) = tree.get_tag(a.index(), key_sym) {
            return v;
        }
    }
    0
}

fn find_display_sym(tree: &Tree, c: Cursor, kind_name: &str, lang: &Lang) -> u32 {
    if let Some(tag_key) = kind_name.strip_prefix("tag:") {
        return find_tag(tree, c, tag_key, lang);
    }
    let s = child_sym_by_kind(c, kind_name, lang);
    if s != 0 {
        return s;
    }
    for a in c.ancestors() {
        let s = child_sym_by_kind(a, kind_name, lang);
        if s != 0 {
            return s;
        }
    }
    0
}

fn resolve_column<'a>(
    tree: &'a Tree,
    c: Cursor<'a>,
    col: &ColumnConfig,
    lang: &'a Lang,
    id: i64,
    expand_node: Option<Cursor<'a>>,
) -> Val<'a> {
    if col.dtype == "Int64" && col.compute.is_none() && col.from.is_none() {
        return Val::I(id);
    }
    if let Some(ref compute) = col.compute {
        return compute_val(tree, c, compute, lang, expand_node);
    }
    if col.expand_sym {
        if let Some(en) = expand_node {
            let s = en.sym();
            return if s != 0 {
                Val::S(lang.syms.resolve(s))
            } else {
                Val::Null
            };
        }
        return Val::Null;
    }
    if let Some(ref from) = col.from {
        let raw = if from == "sym" {
            c.sym()
        } else {
            let mut v = find_display_sym(tree, c, from, lang);
            if v == 0 {
                if let Some(en) = expand_node {
                    v = find_display_sym(tree, en, from, lang);
                }
            }
            v
        };
        if raw == 0 {
            return if col.nullable {
                Val::Null
            } else if col.dtype == "Boolean" {
                Val::B(false)
            } else {
                Val::S("")
            };
        }
        let resolved = lang.syms.resolve(raw);
        return match col.dtype.as_str() {
            "Boolean" => Val::B(resolved == "true"),
            "Int64" => Val::I(resolved.parse().unwrap_or(0)),
            _ => Val::S(resolved),
        };
    }
    Val::Null
}

fn compute_val<'a>(
    tree: &'a Tree,
    c: Cursor<'a>,
    compute: &str,
    lang: &'a Lang,
    expand: Option<Cursor<'a>>,
) -> Val<'a> {
    let import_type_key = lang.syms.intern("import_type");
    match compute {
        "import_type" => {
            if let Some(en) = expand {
                if let Some(v) = tree.get_tag(en.index(), import_type_key) {
                    return Val::S(lang.syms.resolve(v));
                }
            }
            if let Some(v) = tree.get_tag(c.index(), import_type_key) {
                return Val::S(lang.syms.resolve(v));
            }
            let source_sym = c.child_sym(C::Source).unwrap_or(0);
            if let Some(en) = expand {
                if en.sym() == source_sym {
                    return Val::S("Import");
                }
            }
            Val::S("NamedImport")
        }
        "filename" => {
            let path = lang.syms.resolve(c.sym());
            Val::S(path.rsplit('/').next().unwrap_or(path))
        }
        "extension" => {
            let path = lang.syms.resolve(c.sym());
            let filename = path.rsplit('/').next().unwrap_or(path);
            Val::S(filename.rsplit('.').next().unwrap_or(""))
        }
        "is_type_only" => Val::B(c.is(C::ImportType)),
        "start_line" => Val::I(c.start() as i64),
        "end_line" => Val::I(c.end() as i64),
        "start_byte" => Val::I(c.start() as i64),
        "end_byte" => Val::I(c.end() as i64),
        "start_col" => Val::I(0),
        "end_col" => Val::I(0),
        "defname_start_line" => {
            let loc = c.child(C::DefName).unwrap_or(c);
            Val::I(loc.start_row() as i64 + 1)
        }
        "defname_end_line" => {
            let loc = c.child(C::DefName).unwrap_or(c);
            Val::I(loc.end_row() as i64 + 1)
        }
        "defname_start_byte" => {
            let loc = c.child(C::DefName).unwrap_or(c);
            Val::I(loc.start() as i64)
        }
        "defname_end_byte" => {
            let loc = c.child(C::DefName).unwrap_or(c);
            Val::I(loc.end() as i64)
        }
        "defname_start_col" => {
            let loc = c.child(C::DefName).unwrap_or(c);
            Val::I(loc.start_col() as i64 + 1)
        }
        "defname_end_col" => {
            let loc = c.child(C::DefName).unwrap_or(c);
            Val::I(loc.end_col() as i64 + 1)
        }
        _ => Val::Null,
    }
}

// ── Export ──

static EXPORT_YAML: &str = include_str!("../../tree-dsl/config/export.yaml");

pub fn export(trees: &[Tree], edges: &[Edge], lang: &Lang) -> anyhow::Result<Datasets> {
    let config: ExportConfig = serde_yaml::from_str(EXPORT_YAML)?;
    let mut ds = Datasets::new();

    let mut entity_ids: HashMap<String, IdMap> = HashMap::new();
    let mut next_id: i64 = 1;

    // Pass 1: scan trees, assign IDs, build entity tables
    for (entity_name, entity_config) in &config.entities {
        let source_kinds: Vec<u16> = entity_config
            .source
            .split('|')
            .map(|k| lang.intern_kind(k))
            .collect();
        let exclude_parent: Option<u16> = entity_config
            .exclude_parent
            .as_deref()
            .map(|k| lang.intern_kind(k));
        let expand_kind: Option<u16> = entity_config.expand.as_deref().map(|k| lang.intern_kind(k));

        let mut ids = IdMap::new();
        let mut t = Table::from_columns(&entity_config.columns);

        for (fi, tree) in trees.iter().enumerate() {
            let root = tree.root();
            for c in std::iter::once(root).chain(root.descendants()) {
                if !source_kinds.contains(&c.kind()) {
                    continue;
                }
                if let Some(ep) = exclude_parent {
                    if c.parent().is_some_and(|p| p.kind() == ep) {
                        continue;
                    }
                }

                if let Some(ek) = expand_kind {
                    let names: Vec<Cursor> = c
                        .children()
                        .filter(|ch| ch.kind() == ek && ch.sym() != 0)
                        .collect();
                    if names.is_empty() {
                        ids.insert((fi, c.index()), next_id);
                        let row: Vec<Val> = entity_config
                            .columns
                            .iter()
                            .map(|col| resolve_column(tree, c, col, lang, next_id, None))
                            .collect();
                        t.row(&row);
                        next_id += 1;
                    } else {
                        for name in &names {
                            ids.insert((fi, name.index()), next_id);
                            let row: Vec<Val> = entity_config
                                .columns
                                .iter()
                                .map(|col| resolve_column(tree, c, col, lang, next_id, Some(*name)))
                                .collect();
                            t.row(&row);
                            next_id += 1;
                        }
                    }
                } else {
                    ids.insert((fi, c.index()), next_id);
                    let row: Vec<Val> = entity_config
                        .columns
                        .iter()
                        .map(|col| resolve_column(tree, c, col, lang, next_id, None))
                        .collect();
                    t.row(&row);
                    next_id += 1;
                }
            }
        }

        ds.insert(entity_name.clone(), t.finish()?);
        entity_ids.insert(entity_name.clone(), ids);
    }

    // Pass 2: build edge tables
    let resolved: HashSet<(usize, u32)> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .map(|e| (e.from_tree as usize, e.from_node))
        .collect();

    for (edge_name, edge_config) in &config.edges {
        let src_ids = &entity_ids[&edge_config.source];
        let tgt_ids = &entity_ids[&edge_config.target];

        let only_kinds: Vec<EdgeKind> = edge_config
            .only_kinds
            .iter()
            .filter_map(|k| k.parse().ok())
            .collect();
        let exclude_kinds: Vec<EdgeKind> = edge_config
            .exclude_kinds
            .iter()
            .filter_map(|k| k.parse().ok())
            .collect();

        let mut t = Table::edge();

        if edge_config.tree_containment {
            for (fi, tree) in trees.iter().enumerate() {
                let root_id = src_ids
                    .get(&(fi, tree.root().index()))
                    .copied()
                    .unwrap_or(0);
                if root_id == 0 {
                    continue;
                }
                for c in tree.root().descendants() {
                    if let Some(&tid) = tgt_ids.get(&(fi, c.index())) {
                        let label = if let Some(ref l) = edge_config.label {
                            l.as_str()
                        } else {
                            "Defines"
                        };
                        t.row(&[Val::I(root_id), Val::I(tid), Val::S(label)]);
                    }
                }
                for edge in edges.iter().filter(|e| e.from_tree == fi as u32) {
                    if edge.from_node == 0 && edge.kind == EdgeKind::Calls {
                        if let Some(&tid) = tgt_ids.get(&(fi, edge.to_node)) {
                            t.row(&[Val::I(root_id), Val::I(tid), Val::S("Calls")]);
                        }
                    }
                }
            }
        }

        if edge_config.from_edges {
            let mut seen = HashSet::new();
            for e in edges {
                if !only_kinds.is_empty() && !only_kinds.contains(&e.kind) {
                    continue;
                }
                if exclude_kinds.contains(&e.kind) {
                    continue;
                }
                if edge_config.skip_if_has_calls
                    && resolved.contains(&(e.from_tree as usize, e.from_node))
                {
                    continue;
                }

                let Some(&from) = src_ids.get(&(e.from_tree as usize, e.from_node)) else {
                    continue;
                };
                let Some(&to) = tgt_ids.get(&(e.to_tree as usize, e.to_node)) else {
                    continue;
                };
                let label = edge_config.label.as_deref().unwrap_or(e.kind.name());

                if edge_config.dedup_cross
                    && e.from_tree != e.to_tree
                    && !seen.insert((from, to, label))
                {
                    continue;
                }
                t.row(&[Val::I(from), Val::I(to), Val::S(label)]);
            }
        }

        ds.insert(edge_name.clone(), t.finish()?);
    }

    Ok(ds)
}
