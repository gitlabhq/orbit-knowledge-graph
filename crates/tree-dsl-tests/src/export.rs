use std::collections::HashMap;
use std::sync::Arc;

use arrow_56::array::{ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;

use tree_dsl::canonical::{self, Canonical as C};
use tree_dsl::intern::Lang;
use tree_dsl::tree::{Cursor, Edge, EdgeKind, Tree};

pub type Datasets = HashMap<String, RecordBatch>;

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
    ds.insert(
        "DefinitionToDefinition".into(),
        build_def2def(edges, &def_ids)?,
    );
    ds.insert(
        "DefinitionToImportedSymbol".into(),
        build_def2imp(edges, &def_ids, &all_imp_ids)?,
    );
    ds.insert(
        "ImportedSymbolToDefinition".into(),
        build_imp2def(edges, &def_ids, &all_imp_ids)?,
    );

    Ok(ds)
}

type IdMap = HashMap<(usize, u32), i64>;

fn assign_ids(trees: &[Tree]) -> (IdMap, IdMap, IdMap, IdMap) {
    let mut file_ids = IdMap::new();
    let mut def_ids = IdMap::new();
    let mut imp_ids = IdMap::new();
    let mut imp_name_ids = IdMap::new();
    let mut next_id: i64 = 1;

    for (fi, tree) in trees.iter().enumerate() {
        file_ids.insert((fi, 0), next_id);
        next_id += 1;

        for c in tree.root().descendants() {
            let i = c.index();
            if canonical::has_def_type(c) {
                def_ids.insert((fi, i), next_id);
                next_id += 1;
            } else if c.is(C::ModuleExport) {
                for imp in c.children().filter(|ch| ch.is(C::Import)) {
                    for name in imp.names() {
                        def_ids.insert((fi, name.index()), next_id);
                        next_id += 1;
                    }
                }
            } else if (c.is(C::Import) || c.is(C::ImportType))
                && !c.parent().is_some_and(|p| p.is(C::ModuleExport))
            {
                imp_ids.insert((fi, i), next_id);
                next_id += 1;
                for name in c.children().filter(|ch| ch.is(C::Name) && ch.sym() != 0) {
                    imp_name_ids.insert((fi, name.index()), next_id);
                    next_id += 1;
                }
            }
        }
    }

    (file_ids, def_ids, imp_ids, imp_name_ids)
}

fn child_display_sym(c: Cursor, kind: C) -> u32 {
    c.children()
        .find(|ch| ch.kind() == kind as u16)
        .map(|ch| ch.sym())
        .unwrap_or(0)
}

fn find_display_sym(c: Cursor, kind: C) -> u32 {
    let direct = child_display_sym(c, kind);
    if direct != 0 {
        return direct;
    }
    for a in c.ancestors() {
        let sym = child_display_sym(a, kind);
        if sym != 0 {
            return sym;
        }
    }
    0
}

fn build_files(trees: &[Tree], lang: &Lang, file_ids: &IdMap) -> anyhow::Result<RecordBatch> {
    let n = trees.len();
    let (mut id_b, mut path_b, mut name_b, mut ext_b, mut lang_b) = (
        Int64Builder::with_capacity(n),
        StringBuilder::with_capacity(n, n * 32),
        StringBuilder::with_capacity(n, n * 16),
        StringBuilder::with_capacity(n, n * 4),
        StringBuilder::with_capacity(n, n * 8),
    );
    for (fi, tree) in trees.iter().enumerate() {
        let root = tree.root();
        let path = lang.syms.resolve(root.sym());
        id_b.append_value(file_ids[&(fi, 0)]);
        path_b.append_value(path);
        let filename = path.rsplit('/').next().unwrap_or(path);
        name_b.append_value(filename);
        let ext = filename.rsplit('.').next().unwrap_or("");
        ext_b.append_value(ext);
        let lang_sym = child_display_sym(root, C::DisplayLanguage);
        lang_b.append_value(if lang_sym != 0 {
            lang.syms.resolve(lang_sym)
        } else {
            match ext {
                "py" | "pyi" => "python",
                "ts" => "typescript",
                "tsx" => "tsx",
                "js" | "jsx" | "mjs" | "cjs" => "javascript",
                "rs" => "rust",
                _ => "unknown",
            }
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

fn build_defs(trees: &[Tree], lang: &Lang, def_ids: &IdMap) -> anyhow::Result<RecordBatch> {
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
        for c in tree.root().descendants() {
            let i = c.index();
            let Some(&did) = def_ids.get(&(fi, i)) else {
                continue;
            };

            let fqn_sym = child_display_sym(c, C::DisplayFqn);
            let fp_sym = find_display_sym(c, C::DisplayFilePath);
            let dt_sym = child_display_sym(c, C::DisplayDefType);
            let name_sym = c.child_sym(C::DefName).unwrap_or(0);

            if canonical::has_def_type(c) {
                let dn = c.child(C::DefName);
                let loc = dn.unwrap_or(c);
                id_b.append_value(did);
                fp_b.append_value(lang.syms.resolve(fp_sym));
                fqn_b.append_value(lang.syms.resolve(fqn_sym));
                name_b.append_value(lang.syms.resolve(name_sym));
                dt_b.append_value(lang.syms.resolve(dt_sym));
                sl_b.append_value(loc.start_row() as i64 + 1);
                el_b.append_value(loc.end_row() as i64 + 1);
                sb_b.append_value(loc.start() as i64);
                eb_b.append_value(loc.end() as i64);
                sc_b.append_value(loc.start_col() as i64 + 1);
                ec_b.append_value(loc.end_col() as i64 + 1);
            } else if c.is(C::Name) {
                let display = if c.child_sym(C::Alias).unwrap_or(0) != 0 {
                    c.child_sym(C::Alias).unwrap_or(0)
                } else {
                    c.sym()
                };
                id_b.append_value(did);
                fp_b.append_value(lang.syms.resolve(fp_sym));
                fqn_b.append_value(lang.syms.resolve(display));
                name_b.append_value(lang.syms.resolve(display));
                dt_b.append_value("ModuleExport");
                sl_b.append_value(c.start() as i64);
                el_b.append_value(c.end() as i64);
                sb_b.append_value(c.start() as i64);
                eb_b.append_value(c.end() as i64);
                sc_b.append_value(0);
                ec_b.append_value(0);
            }
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
    imp_ids: &IdMap,
    imp_name_ids: &IdMap,
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
        for c in tree.root().descendants() {
            if !(c.is(C::Import) || c.is(C::ImportType))
                || c.parent().is_some_and(|p| p.is(C::ModuleExport))
            {
                continue;
            }
            let Some(&iid) = imp_ids.get(&(fi, c.index())) else {
                continue;
            };

            let fp_sym = child_display_sym(c, C::DisplayFilePath);
            let fp = lang.syms.resolve(fp_sym);
            let display_source = child_display_sym(c, C::DisplaySourcePath);
            let source_sym = if display_source != 0 {
                display_source
            } else {
                c.child_sym(C::SourcePath)
                    .or(c.child_sym(C::Source))
                    .unwrap_or(0)
            };
            let source = lang.syms.resolve(source_sym);
            let is_type_only = c.is(C::ImportType);
            let resolved_sym = child_display_sym(c, C::DisplayResolved);
            let has_target = lang.syms.resolve(resolved_sym) == "true";

            let names: Vec<Cursor> = c
                .children()
                .filter(|ch| ch.is(C::Name) && ch.sym() != 0)
                .collect();

            if names.is_empty() {
                id_b.append_value(iid);
                fp_b.append_value(fp);
                it_b.append_value("Import");
                path_b.append_value(source);
                name_b.append_null();
                alias_b.append_null();
                to_b.append_value(is_type_only);
                ht_b.append_value(has_target);
                append_loc(
                    &mut sl_b, &mut el_b, &mut sb_b, &mut eb_b, &mut sc_b, &mut ec_b, c,
                );
            } else {
                for name in &names {
                    let ns = name.sym();
                    let als = name.child_sym(C::Alias).unwrap_or(0);
                    let name_text = lang.syms.resolve(ns);
                    let import_type_sym = child_display_sym(*name, C::DisplayImportType);
                    let parent_type_sym = child_display_sym(c, C::DisplayImportType);
                    let label = if import_type_sym != 0 {
                        lang.syms.resolve(import_type_sym)
                    } else if parent_type_sym != 0 {
                        lang.syms.resolve(parent_type_sym)
                    } else if names.len() == 1 && ns == source_sym {
                        "Import"
                    } else {
                        "NamedImport"
                    };

                    let name_id = imp_name_ids
                        .get(&(fi, name.index()))
                        .copied()
                        .unwrap_or(iid);
                    id_b.append_value(name_id);
                    fp_b.append_value(fp);
                    it_b.append_value(label);
                    path_b.append_value(source);
                    if ns != 0
                        && !(name_text == "*" && als != 0)
                        && !(names.len() == 1 && ns == source_sym)
                    {
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
                    let name_resolved = name
                        .children()
                        .find(|ch| ch.kind() == C::DisplayResolved as u16)
                        .map(|ch| lang.syms.resolve(ch.sym()) == "true")
                        .unwrap_or(has_target);
                    ht_b.append_value(name_resolved);
                    append_loc(
                        &mut sl_b, &mut el_b, &mut sb_b, &mut eb_b, &mut sc_b, &mut ec_b, c,
                    );
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

fn append_loc(
    sl: &mut Int64Builder,
    el: &mut Int64Builder,
    sb: &mut Int64Builder,
    eb: &mut Int64Builder,
    sc: &mut Int64Builder,
    ec: &mut Int64Builder,
    c: Cursor,
) {
    sl.append_value(c.start() as i64);
    el.append_value(c.end() as i64);
    sb.append_value(c.start() as i64);
    eb.append_value(c.end() as i64);
    sc.append_value(0);
    ec.append_value(0);
}

fn build_file_edges(
    trees: &[Tree],
    edges: &[Edge],
    file_ids: &IdMap,
    def_ids: &IdMap,
    imp_ids: &IdMap,
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
        let fid = file_ids[&(fi, 0)];
        for c in tree.root().descendants() {
            let i = c.index();
            if let Some(&did) = def_ids.get(&(fi, i)) {
                if canonical::has_def_type(c) {
                    ds.append_value(fid);
                    dt.append_value(did);
                    dk.append_value("Defines");
                }
            }
            if let Some(&iid) = imp_ids.get(&(fi, i)) {
                is.append_value(fid);
                it.append_value(iid);
                ik.append_value("Imports");
            }
        }
        for edge in edges.iter().filter(|e| e.from_tree == fi as u32) {
            if edge.from_node == 0
                && edge.kind == EdgeKind::Calls
                && let Some(&tid) = def_ids.get(&(fi, edge.to_node))
            {
                ds.append_value(fid);
                dt.append_value(tid);
                dk.append_value("Calls");
            }
        }
    }

    (edge_batch(ds, dt, dk), edge_batch(is, it, ik))
}

fn build_def2def(edges: &[Edge], def_ids: &IdMap) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    let mut cross_seen = std::collections::HashSet::new();
    for edge in edges {
        if edge.kind == EdgeKind::Imports {
            continue;
        }
        let label = edge.kind.name();
        if let (Some(&from), Some(&to)) = (
            def_ids.get(&(edge.from_tree as usize, edge.from_node)),
            def_ids.get(&(edge.to_tree as usize, edge.to_node)),
        ) {
            if edge.from_tree != edge.to_tree && !cross_seen.insert((from, to, label)) {
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
    edges: &[Edge],
    def_ids: &IdMap,
    imp_name_ids: &IdMap,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    let resolved: std::collections::HashSet<(usize, u32)> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .map(|e| (e.from_tree as usize, e.from_node))
        .collect();
    for edge in edges {
        if edge.kind != EdgeKind::Imports {
            continue;
        }
        let fi = edge.from_tree as usize;
        if resolved.contains(&(fi, edge.from_node)) {
            continue;
        }
        let Some(&caller_id) = def_ids.get(&(fi, edge.from_node)) else {
            continue;
        };
        if let Some(&iid) = imp_name_ids.get(&(fi, edge.to_node)) {
            s.append_value(caller_id);
            t.append_value(iid);
            k.append_value("Calls");
        }
    }
    edge_batch(s, t, k)
}

fn build_imp2def(
    edges: &[Edge],
    def_ids: &IdMap,
    imp_name_ids: &IdMap,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    for edge in edges {
        if edge.kind != EdgeKind::Imports {
            continue;
        }
        let Some(&target_id) = def_ids.get(&(edge.to_tree as usize, edge.to_node)) else {
            continue;
        };
        let key = (edge.from_tree as usize, edge.from_node);
        if let Some(&iid) = imp_name_ids.get(&key) {
            s.append_value(iid);
            t.append_value(target_id);
            k.append_value("Resolves");
        }
    }
    edge_batch(s, t, k)
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
    let arrays: Vec<Arc<dyn arrow_56::array::Array>> =
        columns.into_iter().map(|mut b| b.finish()).collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}
