use std::collections::HashMap;
use std::sync::Arc;

use arrow_56::array::{ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;

use tree_dsl::canonical::{self, Canonical as C};
use tree_dsl::intern::Lang;
use tree_dsl::tree::{Cursor, Edge, EdgeKind, Tree};

pub type Datasets = HashMap<String, RecordBatch>;

enum Val<'a> {
    I(i64),
    S(&'a str),
    B(bool),
    Null,
}

struct Table {
    schema: Vec<(&'static str, DataType, bool)>,
    cols: Vec<Box<dyn ArrayBuilder>>,
}

impl Table {
    fn new(schema: &[(&'static str, DataType, bool)]) -> Self {
        let cols = schema
            .iter()
            .map(|(_, dt, _)| -> Box<dyn ArrayBuilder> {
                match dt {
                    DataType::Int64 => Box::new(Int64Builder::new()),
                    DataType::Utf8 => Box::new(StringBuilder::new()),
                    DataType::Boolean => Box::new(BooleanBuilder::new()),
                    _ => panic!("unsupported type"),
                }
            })
            .collect();
        Self {
            schema: schema.to_vec(),
            cols,
        }
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
                .map(|(n, dt, null)| Field::new(*n, dt.clone(), *null))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<Arc<dyn arrow_56::array::Array>> =
            self.cols.into_iter().map(|mut b| b.finish()).collect();
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

type IdMap = HashMap<(usize, u32), i64>;

fn child_display_sym(c: Cursor, kind: C) -> u32 {
    c.children()
        .find(|ch| ch.kind() == kind as u16)
        .map(|ch| ch.sym())
        .unwrap_or(0)
}

fn find_display_sym(c: Cursor, kind: C) -> u32 {
    let d = child_display_sym(c, kind);
    if d != 0 {
        return d;
    }
    for a in c.ancestors() {
        let s = child_display_sym(a, kind);
        if s != 0 {
            return s;
        }
    }
    0
}

fn sym(lang: &Lang, s: u32) -> &str {
    lang.syms.resolve(s)
}

pub fn export(trees: &[Tree], edges: &[Edge], lang: &Lang) -> anyhow::Result<Datasets> {
    let mut ds = Datasets::new();
    let (file_ids, def_ids, imp_ids, imp_name_ids) = assign_ids(trees);
    let all_imp_ids: IdMap = imp_ids
        .iter()
        .chain(imp_name_ids.iter())
        .map(|(&k, &v)| (k, v))
        .collect();

    ds.insert("File".into(), build_files(trees, lang, &file_ids)?);
    ds.insert("Definition".into(), build_defs(trees, lang, &def_ids)?);
    ds.insert(
        "ImportedSymbol".into(),
        build_imports(trees, lang, &imp_ids, &imp_name_ids)?,
    );

    let (f2d, f2i) = build_file_edges(trees, edges, &file_ids, &def_ids, &all_imp_ids);
    ds.insert("FileToDefinition".into(), f2d?);
    ds.insert("FileToImportedSymbol".into(), f2i?);
    let resolved: std::collections::HashSet<(usize, u32)> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .map(|e| (e.from_tree as usize, e.from_node))
        .collect();
    ds.insert(
        "DefinitionToDefinition".into(),
        join_edges(
            edges,
            &def_ids,
            &def_ids,
            |e| e.kind != EdgeKind::Imports,
            |e| e.kind.name(),
            true,
        )?,
    );
    ds.insert(
        "DefinitionToImportedSymbol".into(),
        join_edges(
            edges,
            &def_ids,
            &all_imp_ids,
            |e| {
                e.kind == EdgeKind::Imports
                    && !resolved.contains(&(e.from_tree as usize, e.from_node))
            },
            |_| "Calls",
            false,
        )?,
    );
    ds.insert(
        "ImportedSymbolToDefinition".into(),
        join_edges(
            edges,
            &all_imp_ids,
            &def_ids,
            |e| e.kind == EdgeKind::Imports,
            |_| "Resolves",
            false,
        )?,
    );
    Ok(ds)
}

fn assign_ids(trees: &[Tree]) -> (IdMap, IdMap, IdMap, IdMap) {
    let mut file_ids = IdMap::new();
    let mut def_ids = IdMap::new();
    let mut imp_ids = IdMap::new();
    let mut imp_name_ids = IdMap::new();
    let mut next: i64 = 1;

    for (fi, tree) in trees.iter().enumerate() {
        file_ids.insert((fi, 0), next);
        next += 1;
        for c in tree.root().descendants() {
            let i = c.index();
            if canonical::has_def_type(c) {
                def_ids.insert((fi, i), next);
                next += 1;
            } else if c.is(C::ModuleExport) {
                for imp in c.children().filter(|ch| ch.is(C::Import)) {
                    for name in imp.names() {
                        def_ids.insert((fi, name.index()), next);
                        next += 1;
                    }
                }
            } else if (c.is(C::Import) || c.is(C::ImportType))
                && !c.parent().is_some_and(|p| p.is(C::ModuleExport))
            {
                imp_ids.insert((fi, i), next);
                next += 1;
                for name in c.children().filter(|ch| ch.is(C::Name) && ch.sym() != 0) {
                    imp_name_ids.insert((fi, name.index()), next);
                    next += 1;
                }
            }
        }
    }
    (file_ids, def_ids, imp_ids, imp_name_ids)
}

fn build_files(trees: &[Tree], lang: &Lang, ids: &IdMap) -> anyhow::Result<RecordBatch> {
    let mut t = Table::new(&[
        ("id", DataType::Int64, false),
        ("path", DataType::Utf8, false),
        ("name", DataType::Utf8, false),
        ("extension", DataType::Utf8, false),
        ("language", DataType::Utf8, false),
    ]);
    for (fi, tree) in trees.iter().enumerate() {
        let root = tree.root();
        let path = sym(lang, root.sym());
        let filename = path.rsplit('/').next().unwrap_or(path);
        let ext = filename.rsplit('.').next().unwrap_or("");
        let lang_sym = child_display_sym(root, C::DisplayLanguage);
        let language = if lang_sym != 0 {
            sym(lang, lang_sym)
        } else {
            "unknown"
        };
        t.row(&[
            Val::I(ids[&(fi, 0)]),
            Val::S(path),
            Val::S(filename),
            Val::S(ext),
            Val::S(language),
        ]);
    }
    t.finish()
}

fn build_defs(trees: &[Tree], lang: &Lang, ids: &IdMap) -> anyhow::Result<RecordBatch> {
    let mut t = Table::new(&[
        ("id", DataType::Int64, false),
        ("file_path", DataType::Utf8, false),
        ("fqn", DataType::Utf8, false),
        ("name", DataType::Utf8, false),
        ("definition_type", DataType::Utf8, false),
        ("start_line", DataType::Int64, false),
        ("end_line", DataType::Int64, false),
        ("start_byte", DataType::Int64, false),
        ("end_byte", DataType::Int64, false),
        ("start_char", DataType::Int64, false),
        ("end_char", DataType::Int64, false),
    ]);
    for (fi, tree) in trees.iter().enumerate() {
        for c in tree.root().descendants() {
            let Some(&did) = ids.get(&(fi, c.index())) else {
                continue;
            };
            let fp = sym(lang, find_display_sym(c, C::DisplayFilePath));

            if canonical::has_def_type(c) {
                let fqn = sym(lang, child_display_sym(c, C::DisplayFqn));
                let name = sym(lang, c.child_sym(C::DefName).unwrap_or(0));
                let dt = sym(lang, child_display_sym(c, C::DisplayDefType));
                let loc = c.child(C::DefName).unwrap_or(c);
                t.row(&[
                    Val::I(did),
                    Val::S(fp),
                    Val::S(fqn),
                    Val::S(name),
                    Val::S(dt),
                    Val::I(loc.start_row() as i64 + 1),
                    Val::I(loc.end_row() as i64 + 1),
                    Val::I(loc.start() as i64),
                    Val::I(loc.end() as i64),
                    Val::I(loc.start_col() as i64 + 1),
                    Val::I(loc.end_col() as i64 + 1),
                ]);
            } else if c.is(C::Name) {
                let display = c.child_sym(C::Alias).filter(|&a| a != 0).unwrap_or(c.sym());
                let ds = sym(lang, display);
                t.row(&[
                    Val::I(did),
                    Val::S(fp),
                    Val::S(ds),
                    Val::S(ds),
                    Val::S("ModuleExport"),
                    Val::I(c.start() as i64),
                    Val::I(c.end() as i64),
                    Val::I(c.start() as i64),
                    Val::I(c.end() as i64),
                    Val::I(0),
                    Val::I(0),
                ]);
            }
        }
    }
    t.finish()
}

fn build_imports(
    trees: &[Tree],
    lang: &Lang,
    imp_ids: &IdMap,
    name_ids: &IdMap,
) -> anyhow::Result<RecordBatch> {
    let mut t = Table::new(&[
        ("id", DataType::Int64, false),
        ("file_path", DataType::Utf8, false),
        ("import_type", DataType::Utf8, false),
        ("path", DataType::Utf8, false),
        ("name", DataType::Utf8, true),
        ("alias", DataType::Utf8, true),
        ("is_type_only", DataType::Boolean, false),
        ("has_target", DataType::Boolean, false),
        ("start_line", DataType::Int64, false),
        ("end_line", DataType::Int64, false),
        ("start_byte", DataType::Int64, false),
        ("end_byte", DataType::Int64, false),
        ("start_char", DataType::Int64, false),
        ("end_char", DataType::Int64, false),
    ]);
    for (fi, tree) in trees.iter().enumerate() {
        for c in tree.root().descendants() {
            if !(c.is(C::Import) || c.is(C::ImportType))
                || c.parent().is_some_and(|p| p.is(C::ModuleExport))
            {
                continue;
            }
            let Some(&iid) = imp_ids.get(&(fi, c.index())) else {
                continue;
            };
            let fp = sym(lang, child_display_sym(c, C::DisplayFilePath));
            let sp = child_display_sym(c, C::DisplaySourcePath);
            let source = sym(
                lang,
                if sp != 0 {
                    sp
                } else {
                    c.child_sym(C::SourcePath)
                        .or(c.child_sym(C::Source))
                        .unwrap_or(0)
                },
            );
            let is_type = c.is(C::ImportType);
            let resolved = sym(lang, child_display_sym(c, C::DisplayResolved)) == "true";
            let parent_it = child_display_sym(c, C::DisplayImportType);

            let names: Vec<Cursor> = c
                .children()
                .filter(|ch| ch.is(C::Name) && ch.sym() != 0)
                .collect();
            let source_sym = c.child_sym(C::Source).unwrap_or(0);

            if names.is_empty() {
                t.row(&[
                    Val::I(iid),
                    Val::S(fp),
                    Val::S("Import"),
                    Val::S(source),
                    Val::Null,
                    Val::Null,
                    Val::B(is_type),
                    Val::B(resolved),
                    Val::I(c.start() as i64),
                    Val::I(c.end() as i64),
                    Val::I(c.start() as i64),
                    Val::I(c.end() as i64),
                    Val::I(0),
                    Val::I(0),
                ]);
            } else {
                for name in &names {
                    let ns = name.sym();
                    let als = name.child_sym(C::Alias).unwrap_or(0);
                    let nt = sym(lang, ns);
                    let it_sym = child_display_sym(*name, C::DisplayImportType);
                    let label = if it_sym != 0 {
                        sym(lang, it_sym)
                    } else if parent_it != 0 {
                        sym(lang, parent_it)
                    } else if names.len() == 1 && ns == source_sym {
                        "Import"
                    } else {
                        "NamedImport"
                    };
                    let nid = name_ids.get(&(fi, name.index())).copied().unwrap_or(iid);
                    let name_resolved = name
                        .children()
                        .find(|ch| ch.kind() == C::DisplayResolved as u16)
                        .map(|ch| sym(lang, ch.sym()) == "true")
                        .unwrap_or(resolved);
                    let show_name = ns != 0
                        && !(nt == "*" && als != 0)
                        && !(names.len() == 1 && ns == source_sym);
                    let name_val = if show_name {
                        Val::S(nt)
                    } else if nt == "*" && als == 0 {
                        Val::S("*")
                    } else {
                        Val::Null
                    };
                    let alias_val = if als != 0 {
                        Val::S(sym(lang, als))
                    } else {
                        Val::Null
                    };
                    t.row(&[
                        Val::I(nid),
                        Val::S(fp),
                        Val::S(label),
                        Val::S(source),
                        name_val,
                        alias_val,
                        Val::B(is_type),
                        Val::B(name_resolved),
                        Val::I(c.start() as i64),
                        Val::I(c.end() as i64),
                        Val::I(c.start() as i64),
                        Val::I(c.end() as i64),
                        Val::I(0),
                        Val::I(0),
                    ]);
                }
            }
        }
    }
    t.finish()
}

fn build_file_edges(
    trees: &[Tree],
    edges: &[Edge],
    file_ids: &IdMap,
    def_ids: &IdMap,
    imp_ids: &IdMap,
) -> (anyhow::Result<RecordBatch>, anyhow::Result<RecordBatch>) {
    let mut d = edge_table();
    let mut i = edge_table();
    for (fi, tree) in trees.iter().enumerate() {
        let fid = file_ids[&(fi, 0)];
        for c in tree.root().descendants() {
            if let Some(&did) = def_ids.get(&(fi, c.index())) {
                if canonical::has_def_type(c) {
                    d.row(&[Val::I(fid), Val::I(did), Val::S("Defines")]);
                }
            }
            if let Some(&iid) = imp_ids.get(&(fi, c.index())) {
                i.row(&[Val::I(fid), Val::I(iid), Val::S("Imports")]);
            }
        }
        for edge in edges.iter().filter(|e| e.from_tree == fi as u32) {
            if edge.from_node == 0 && edge.kind == EdgeKind::Calls {
                if let Some(&tid) = def_ids.get(&(fi, edge.to_node)) {
                    d.row(&[Val::I(fid), Val::I(tid), Val::S("Calls")]);
                }
            }
        }
    }
    (d.finish(), i.finish())
}

fn join_edges(
    edges: &[Edge],
    src_ids: &IdMap,
    tgt_ids: &IdMap,
    filter: impl Fn(&Edge) -> bool,
    label: impl Fn(&Edge) -> &str,
    dedup_cross: bool,
) -> anyhow::Result<RecordBatch> {
    let mut t = edge_table();
    let mut seen = std::collections::HashSet::new();
    for e in edges {
        if !filter(e) {
            continue;
        }
        let Some(&from) = src_ids.get(&(e.from_tree as usize, e.from_node)) else {
            continue;
        };
        let Some(&to) = tgt_ids.get(&(e.to_tree as usize, e.to_node)) else {
            continue;
        };
        if dedup_cross && e.from_tree != e.to_tree && !seen.insert((from, to, label(e))) {
            continue;
        }
        t.row(&[Val::I(from), Val::I(to), Val::S(label(e))]);
    }
    t.finish()
}

fn edge_table() -> Table {
    Table::new(&[
        ("source_id", DataType::Int64, false),
        ("target_id", DataType::Int64, false),
        ("edge_kind", DataType::Utf8, false),
    ])
}
