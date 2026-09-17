use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashSet;

use crate::canonical::{self as canonical, Canonical as C};
use crate::intern::Lang;
use crate::tree::{Edge, EdgeKind, Tree};
use crate::treesitter::SupportLang;

pub type Datasets = HashMap<String, RecordBatch>;

pub fn export(
    trees: &[Tree],
    cross_edges: &[Edge],
    lang: &Lang,
    _support_lang: SupportLang,
) -> anyhow::Result<Datasets> {
    let mut ds = Datasets::new();

    let mut id_map: HashMap<(usize, u32), i64> = HashMap::new();
    let mut next_id: i64 = 1;

    ds.insert(
        "File".into(),
        build_files(trees, lang, &mut id_map, &mut next_id)?,
    );
    ds.insert(
        "Definition".into(),
        build_defs(trees, lang, &mut id_map, &mut next_id)?,
    );

    ds.insert(
        "ImportedSymbol".into(),
        build_imports(trees, lang, &mut id_map, &mut next_id)?,
    );

    let (f2d, f2i) = build_file_edges(trees, &id_map);
    ds.insert("FileToDefinition".into(), f2d?);
    ds.insert("FileToImportedSymbol".into(), f2i?);
    ds.insert(
        "DefinitionToDefinition".into(),
        build_def2def(trees, cross_edges, &id_map)?,
    );
    ds.insert(
        "DefinitionToImportedSymbol".into(),
        build_def2imp(trees, cross_edges, &id_map)?,
    );
    ds.insert(
        "ImportedSymbolToDefinition".into(),
        build_imp2def(cross_edges, &id_map)?,
    );

    Ok(ds)
}

fn display_child_sym(tree: &Tree, node: u32, kind: C) -> u32 {
    tree.cursor(node)
        .children()
        .find(|c| c.kind() == kind as u16)
        .map_or(0, |c| c.sym())
}

fn display_child_str<'a>(tree: &Tree, node: u32, kind: C, lang: &'a Lang) -> &'a str {
    let sym = display_child_sym(tree, node, kind);
    if sym == 0 { "" } else { lang.syms.resolve(sym) }
}

fn make_batch(
    fields: &[(&str, DataType, bool)],
    columns: Vec<Box<dyn ArrayBuilder>>,
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, dt, null)| Field::new(*n, dt.clone(), *null))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<Arc<dyn arrow::array::Array>> =
        columns.into_iter().map(|mut b| b.finish()).collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn edge_batch(s: Int64Builder, t: Int64Builder, k: StringBuilder) -> anyhow::Result<RecordBatch> {
    make_batch(
        &[
            ("source_id", DataType::Int64, false),
            ("target_id", DataType::Int64, false),
            ("edge_kind", DataType::Utf8, false),
        ],
        vec![Box::new(s), Box::new(t), Box::new(k)],
    )
}

fn build_files(
    trees: &[Tree],
    lang: &Lang,
    id_map: &mut HashMap<(usize, u32), i64>,
    next_id: &mut i64,
) -> anyhow::Result<RecordBatch> {
    let n = trees.len();
    let (mut id_b, mut path_b, mut name_b, mut ext_b, mut lang_b) = (
        Int64Builder::with_capacity(n),
        StringBuilder::with_capacity(n, n * 32),
        StringBuilder::with_capacity(n, n * 16),
        StringBuilder::with_capacity(n, n * 4),
        StringBuilder::with_capacity(n, n * 8),
    );
    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.root().sym());
        let fid = *next_id;
        *next_id += 1;
        id_map.insert((fi, 0), fid);
        id_b.append_value(fid);
        path_b.append_value(path);
        let filename = path.rsplit('/').next().unwrap_or(path);
        name_b.append_value(filename);
        let ext = filename.rsplit('.').next().unwrap_or("");
        ext_b.append_value(ext);
        lang_b.append_value(match ext {
            "py" | "pyi" => "python",
            "ts" => "typescript",
            "tsx" => "tsx",
            "js" | "jsx" | "mjs" | "cjs" => "javascript",
            "rs" => "rust",
            _ => "unknown",
        });
    }
    make_batch(
        &[
            ("id", DataType::Int64, false),
            ("path", DataType::Utf8, false),
            ("name", DataType::Utf8, false),
            ("extension", DataType::Utf8, false),
            ("language", DataType::Utf8, false),
        ],
        vec![
            Box::new(id_b),
            Box::new(path_b),
            Box::new(name_b),
            Box::new(ext_b),
            Box::new(lang_b),
        ],
    )
}

fn build_defs(
    trees: &[Tree],
    lang: &Lang,
    id_map: &mut HashMap<(usize, u32), i64>,
    next_id: &mut i64,
) -> anyhow::Result<RecordBatch> {
    let (mut id_b, mut fp_b, mut fqn_b, mut name_b, mut dt_b) = (
        Int64Builder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let (mut sl_b, mut el_b, mut sb_b, mut eb_b, mut sc_b, mut ec_b) = (
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
    );

    for (fi, tree) in trees.iter().enumerate() {
        for nr in tree.root().descendants() {
            if !canonical::has_def_type(nr) {
                continue;
            }
            let idx = nr.index();
            let did = *next_id;
            *next_id += 1;
            id_map.insert((fi, idx), did);

            let fqn = display_child_str(tree, idx, C::DisplayFqn, lang);
            let def_type = display_child_str(tree, idx, C::DisplayDefType, lang);
            let file_path = display_child_str(tree, idx, C::DisplayFilePath, lang);
            let name_sym = nr.child_sym(C::DefName).unwrap_or(0);
            let dn = nr.child(C::DefName);
            let name_cur = dn.unwrap_or(nr);

            id_b.append_value(did);
            fp_b.append_value(if file_path.is_empty() {
                lang.syms.resolve(tree.root().sym())
            } else {
                file_path
            });
            fqn_b.append_value(fqn);
            name_b.append_value(lang.syms.resolve(name_sym));
            dt_b.append_value(def_type);
            sl_b.append_value(name_cur.start_row() as i64 + 1);
            el_b.append_value(name_cur.end_row() as i64 + 1);
            sb_b.append_value(name_cur.start() as i64);
            eb_b.append_value(name_cur.end() as i64);
            sc_b.append_value(name_cur.start_col() as i64 + 1);
            ec_b.append_value(name_cur.end_col() as i64 + 1);
        }
    }
    make_batch(
        &[
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
        ],
        vec![
            Box::new(id_b),
            Box::new(fp_b),
            Box::new(fqn_b),
            Box::new(name_b),
            Box::new(dt_b),
            Box::new(sl_b),
            Box::new(el_b),
            Box::new(sb_b),
            Box::new(eb_b),
            Box::new(sc_b),
            Box::new(ec_b),
        ],
    )
}

fn build_imports(
    trees: &[Tree],
    lang: &Lang,
    id_map: &mut HashMap<(usize, u32), i64>,
    next_id: &mut i64,
) -> anyhow::Result<RecordBatch> {
    let (mut id_b, mut fp_b, mut it_b, mut path_b, mut name_b, mut alias_b) = (
        Int64Builder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let (mut to_b, mut ht_b) = (BooleanBuilder::new(), BooleanBuilder::new());
    let (mut sl_b, mut el_b, mut sb_b, mut eb_b, mut sc_b, mut ec_b) = (
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
        Int64Builder::new(),
    );

    for (fi, tree) in trees.iter().enumerate() {
        for nr in tree.root().descendants() {
            if !(nr.is(C::Import) || nr.is(C::ImportType))
                || nr.parent().is_some_and(|p| p.is(C::ModuleExport))
            {
                continue;
            }
            let idx = nr.index();
            let import_type = display_child_str(tree, idx, C::DisplayImportType, lang);
            let file_path = display_child_str(tree, idx, C::DisplayFilePath, lang);
            let has_target_str = display_child_str(tree, idx, C::DisplayHasTarget, lang);
            let is_type_only_str = display_child_str(tree, idx, C::DisplayIsTypeOnly, lang);
            let has_target = has_target_str == "true";
            let is_type_only = is_type_only_str == "true";

            let source_sym = nr.child_sym(C::Source).unwrap_or(0);
            let source_path_sym = nr.child_sym(C::SourcePath);
            let source_str = source_path_sym
                .map(|sp| lang.syms.resolve(sp).to_string())
                .unwrap_or_else(|| lang.syms.resolve(source_sym).to_string());

            let names: Vec<_> = nr.names().collect();

            if names.is_empty() {
                let iid = *next_id;
                *next_id += 1;
                id_map.insert((fi, idx), iid);
                id_b.append_value(iid);
                fp_b.append_value(if file_path.is_empty() {
                    lang.syms.resolve(tree.root().sym())
                } else {
                    file_path
                });
                it_b.append_value(import_type);
                path_b.append_value(&source_str);
                name_b.append_null();
                alias_b.append_null();
                to_b.append_value(is_type_only);
                ht_b.append_value(has_target);
                sl_b.append_value(nr.start() as i64);
                el_b.append_value(nr.end() as i64);
                sb_b.append_value(nr.start() as i64);
                eb_b.append_value(nr.end() as i64);
                sc_b.append_value(0);
                ec_b.append_value(0);
            } else {
                for n in &names {
                    let iid = *next_id;
                    *next_id += 1;
                    id_map.insert((fi, n.index()), iid);

                    let ns = n.sym();
                    let als = n.child_sym(C::Alias).unwrap_or(0);
                    let name_text = lang.syms.resolve(ns);
                    let source_eq_name = names.len() == 1 && ns == source_sym;

                    id_b.append_value(iid);
                    fp_b.append_value(if file_path.is_empty() {
                        lang.syms.resolve(tree.root().sym())
                    } else {
                        file_path
                    });
                    it_b.append_value(import_type);
                    path_b.append_value(&source_str);
                    if ns != 0 && !(name_text == "*" && als != 0) && !source_eq_name {
                        name_b.append_value(name_text);
                    } else if name_text == "*" && als == 0 {
                        name_b.append_value("*");
                    } else {
                        name_b.append_null();
                    }
                    if als != 0 {
                        alias_b.append_value(lang.syms.resolve(als));
                    } else {
                        alias_b.append_null();
                    }
                    to_b.append_value(is_type_only);
                    ht_b.append_value(has_target);
                    sl_b.append_value(nr.start() as i64);
                    el_b.append_value(nr.end() as i64);
                    sb_b.append_value(nr.start() as i64);
                    eb_b.append_value(nr.end() as i64);
                    sc_b.append_value(0);
                    ec_b.append_value(0);
                }
            }
        }
    }
    make_batch(
        &[
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
        ],
        vec![
            Box::new(id_b),
            Box::new(fp_b),
            Box::new(it_b),
            Box::new(path_b),
            Box::new(name_b),
            Box::new(alias_b),
            Box::new(to_b),
            Box::new(ht_b),
            Box::new(sl_b),
            Box::new(el_b),
            Box::new(sb_b),
            Box::new(eb_b),
            Box::new(sc_b),
            Box::new(ec_b),
        ],
    )
}

fn build_file_edges(
    trees: &[Tree],
    id_map: &HashMap<(usize, u32), i64>,
) -> (anyhow::Result<RecordBatch>, anyhow::Result<RecordBatch>) {
    let (mut ds, mut dt, mut dk) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    let (mut is, mut it, mut ik) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    for (fi, tree) in trees.iter().enumerate() {
        let Some(&fid) = id_map.get(&(fi, 0)) else {
            continue;
        };
        for nr in tree.root().descendants() {
            let i = nr.index();
            if canonical::has_def_type(nr) {
                if let Some(&did) = id_map.get(&(fi, i)) {
                    ds.append_value(fid);
                    dt.append_value(did);
                    dk.append_value("Defines");
                }
            } else if (nr.is(C::Import) || nr.is(C::ImportType))
                && !nr.parent().is_some_and(|p| p.is(C::ModuleExport))
            {
                for name in nr.names() {
                    if let Some(&iid) = id_map.get(&(fi, name.index())) {
                        is.append_value(fid);
                        it.append_value(iid);
                        ik.append_value("Imports");
                    }
                }
                if nr.names().count() == 0 {
                    if let Some(&iid) = id_map.get(&(fi, nr.index())) {
                        is.append_value(fid);
                        it.append_value(iid);
                        ik.append_value("Imports");
                    }
                }
            }
        }
        for edge in tree.edges().iter() {
            if edge.from.node == 0
                && edge.kind == EdgeKind::Calls
                && let Some(&tid) = id_map.get(&(fi, edge.to.node))
            {
                ds.append_value(fid);
                dt.append_value(tid);
                dk.append_value("Calls");
            }
        }
    }
    (edge_batch(ds, dt, dk), edge_batch(is, it, ik))
}

fn build_def2def(
    trees: &[Tree],
    cross_edges: &[Edge],
    id_map: &HashMap<(usize, u32), i64>,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    for (fi, tree) in trees.iter().enumerate() {
        for edge in tree.edges().iter() {
            if edge.kind == EdgeKind::Imports {
                continue;
            }
            let label = edge.kind.name();
            if let (Some(&from), Some(&to)) = (
                id_map.get(&(fi, edge.from.node)),
                id_map.get(&(fi, edge.to.node)),
            ) {
                s.append_value(from);
                t.append_value(to);
                k.append_value(label);
            }
        }
    }
    let mut cross_seen = std::collections::HashSet::new();
    for ce in cross_edges {
        if ce.kind == EdgeKind::Imports {
            continue;
        }
        let label = ce.kind.name();
        if let (Some(&from), Some(&to)) = (
            id_map.get(&(ce.from.tree as usize, ce.from.node)),
            id_map.get(&(ce.to.tree as usize, ce.to.node)),
        ) {
            if !cross_seen.insert((from, to, label)) {
                continue;
            }
            s.append_value(from);
            t.append_value(to);
            k.append_value(label);
        }
    }
    edge_batch(s, t, k)
}

fn build_def2imp(
    trees: &[Tree],
    cross_edges: &[Edge],
    id_map: &HashMap<(usize, u32), i64>,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    let resolved: FxHashSet<(usize, u32)> = cross_edges
        .iter()
        .filter(|ce| ce.kind == EdgeKind::Calls)
        .map(|ce| (ce.from.tree as usize, ce.from.node))
        .collect();
    for (fi, tree) in trees.iter().enumerate() {
        for edge in tree.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            if resolved.contains(&(fi, edge.from.node)) {
                continue;
            }
            let Some(&caller_id) = id_map.get(&(fi, edge.from.node)) else {
                continue;
            };
            if let Some(&iid) = id_map.get(&(fi, edge.to.node)) {
                s.append_value(caller_id);
                t.append_value(iid);
                k.append_value("Calls");
            }
        }
    }
    edge_batch(s, t, k)
}

fn build_imp2def(
    cross_edges: &[Edge],
    id_map: &HashMap<(usize, u32), i64>,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    for ce in cross_edges {
        if ce.kind != EdgeKind::Imports {
            continue;
        }
        let Some(&target_id) = id_map.get(&(ce.to.tree as usize, ce.to.node)) else {
            continue;
        };
        if let Some(&iid) = id_map.get(&(ce.from.tree as usize, ce.from.node)) {
            s.append_value(iid);
            t.append_value(target_id);
            k.append_value("Resolves");
        }
    }
    edge_batch(s, t, k)
}
