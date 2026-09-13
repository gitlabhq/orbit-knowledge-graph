use std::collections::HashMap;
use std::sync::Arc;

use arrow_56::array::{ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;

use tree_dsl::canonical::Canonical as C;
use tree_dsl::grammar::SupportLang;
use tree_dsl::lang::Lang;
use tree_dsl::tree::Tree;

pub type LanceDatasets = HashMap<String, RecordBatch>;

// ── ID assignment ──

pub struct IdMaps {
    pub defs: HashMap<(usize, u32), i64>,
    pub imports: HashMap<(usize, u32), Vec<i64>>,
    /// Maps (file_index, __name_node) -> single import ID
    pub import_by_name: HashMap<(usize, u32), i64>,
    pub modules: HashMap<usize, i64>,
}

fn assign_ids(trees: &[Tree], lang: &Lang) -> IdMaps {
    let mut defs = HashMap::new();
    let mut imports = HashMap::new();
    let mut import_by_name = HashMap::new();
    let mut modules = HashMap::new();
    let mut next_def: i64 = 1000;
    let mut next_imp: i64 = 5000;
    let mut next_mod: i64 = 900_000;

    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.root().sym());
        if matches!(
            SupportLang::from_path(path),
            Some(SupportLang::JavaScript | SupportLang::TypeScript | SupportLang::Tsx)
        ) {
            next_mod += 1;
            modules.insert(fi, next_mod);
        }
        for i in 0..tree.len() {
            let nr = tree.cursor(i);
            if nr.is_dead() {
                continue;
            }
            if nr.has(C::DefType) {
                next_def += 1;
                defs.insert((fi, i), next_def);
            } else if nr.is(C::Import) || nr.is(C::ImportType) {
                let name_nodes: Vec<u32> = nr
                    .children()
                    .filter(|c| c.is(C::Name) && c.sym() != 0)
                    .map(|c| c.index())
                    .collect();
                let count = name_nodes.len().max(1);
                let ids: Vec<i64> = (0..count)
                    .map(|_| {
                        next_imp += 1;
                        next_imp
                    })
                    .collect();
                for (ni, &name_node) in name_nodes.iter().enumerate() {
                    import_by_name.insert((fi, name_node), ids[ni]);
                }
                imports.insert((fi, i), ids);
            }
        }
    }
    IdMaps {
        defs,
        imports,
        import_by_name,
        modules,
    }
}

// ── Public API ──

pub fn to_datasets(
    trees: &[Tree],
    cross_edges: &[tree_dsl::tree::Edge],
    lang: &mut Lang,
    support_lang: SupportLang,
    resolve_config: &tree_dsl::file_tree::ResolveConfig,
) -> anyhow::Result<LanceDatasets> {
    let ids = assign_ids(trees, lang);
    let mut ds = HashMap::new();
    ds.insert("File".into(), build_files(trees, lang)?);
    ds.insert("Definition".into(), build_defs(trees, lang, &ids)?);
    ds.insert(
        "ImportedSymbol".into(),
        build_imports(trees, lang, &ids, support_lang, resolve_config)?,
    );
    let (f2d, f2i) = build_file_edges(trees, &ids);
    ds.insert("FileToDefinition".into(), f2d?);
    ds.insert("FileToImportedSymbol".into(), f2i?);
    ds.insert(
        "DefinitionToDefinition".into(),
        build_def2def(trees, cross_edges, &ids)?,
    );
    ds.insert(
        "DefinitionToImportedSymbol".into(),
        build_def2imp(trees, cross_edges, &ids)?,
    );
    ds.insert(
        "ImportedSymbolToDefinition".into(),
        build_imp2def(trees, cross_edges, &ids)?,
    );
    Ok(ds)
}

// ── FQN builder ──

fn def_fqn(tree: &Tree, node: u32, lang: &Lang) -> String {
    let path_str = lang.syms.resolve(tree.root().sym()).to_string();
    let lang_id = SupportLang::from_path(&path_str);
    let sep = lang_id.map(|l| l.fqn_separator()).unwrap_or(".");
    let skip_root = matches!(
        lang_id,
        Some(
            SupportLang::JavaScript
                | SupportLang::TypeScript
                | SupportLang::Tsx
                | SupportLang::Rust
        )
    );

    let mut parts = Vec::new();
    for a in std::iter::once(tree.cursor(node)).chain(tree.cursor(node).ancestors()) {
        if a.has(C::DefType) || a.index() == 0 {
            let name = if a.index() == 0 && skip_root {
                String::new()
            } else if a.index() == 0 {
                let path = lang.syms.resolve(a.sym()).to_string();
                let stem = lang_id.map(|l| l.strip_extension(&path)).unwrap_or(&path);
                let collapsed = if stem.ends_with("/__init__") || stem == "__init__" {
                    stem.strip_suffix("/__init__").unwrap_or("").to_string()
                } else if stem.ends_with("/index") || stem == "index" {
                    stem.strip_suffix("/index").unwrap_or("").to_string()
                } else if matches!(lang_id, Some(SupportLang::Rust))
                    && (stem.ends_with("/mod")
                        || stem == "mod"
                        || stem.ends_with("/lib")
                        || stem == "lib"
                        || stem.ends_with("/main")
                        || stem == "main")
                {
                    stem.rsplit_once('/')
                        .map(|(p, _)| p.to_string())
                        .unwrap_or_default()
                } else {
                    stem.to_string()
                };
                collapsed.replace('/', sep)
            } else {
                let sym = a.child_sym(C::DefName).unwrap_or(0);
                lang.syms.resolve(sym).to_string()
            };
            if !name.is_empty() {
                parts.push(name);
            }
        }
    }
    parts.reverse();
    parts.join(sep)
}

// ── Batch helpers ──

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

// ── Table builders ──

fn build_files(trees: &[Tree], lang: &Lang) -> anyhow::Result<RecordBatch> {
    let n = trees.len();
    let (mut id_b, mut path_b, mut name_b, mut ext_b, mut lang_b) = (
        Int64Builder::with_capacity(n),
        StringBuilder::with_capacity(n, n * 32),
        StringBuilder::with_capacity(n, n * 16),
        StringBuilder::with_capacity(n, n * 4),
        StringBuilder::with_capacity(n, n * 8),
    );
    for (i, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.root().sym());
        id_b.append_value(i as i64 + 1);
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

fn build_defs(trees: &[Tree], lang: &Lang, ids: &IdMaps) -> anyhow::Result<RecordBatch> {
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
        let path = lang.syms.resolve(tree.root().sym()).to_string();
        for i in 0..tree.len() {
            let nr = tree.cursor(i);
            if !nr.has(C::DefType) {
                continue;
            }
            let did = ids.defs[&(fi, i)];
            let name_sym = nr.child_sym(C::DefName).unwrap_or(0);
            let deftype_sym = nr.child_sym(C::DefType).unwrap_or(0);
            id_b.append_value(did);
            fp_b.append_value(&path);
            fqn_b.append_value(def_fqn(tree, i, lang));
            name_b.append_value(lang.syms.resolve(name_sym));
            dt_b.append_value(lang.syms.resolve(deftype_sym));
            sl_b.append_value(nr.start() as i64);
            el_b.append_value(nr.end() as i64);
            sb_b.append_value(nr.start() as i64);
            eb_b.append_value(nr.end() as i64);
            sc_b.append_value(0);
            ec_b.append_value(0);
        }
    }
    for (fi, tree) in trees.iter().enumerate() {
        if let Some(&mid) = ids.modules.get(&fi) {
            let path = lang.syms.resolve(tree.root().sym()).to_string();
            id_b.append_value(mid);
            fp_b.append_value(&path);
            fqn_b.append_value(&path);
            name_b.append_value(&path);
            dt_b.append_value("Module");
            sl_b.append_value(0);
            el_b.append_value(0);
            sb_b.append_value(0);
            eb_b.append_value(0);
            sc_b.append_value(0);
            ec_b.append_value(0);
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
    ids: &IdMaps,
    support_lang: SupportLang,
    resolve_config: &tree_dsl::file_tree::ResolveConfig,
) -> anyhow::Result<RecordBatch> {
    let use_resolved =
        resolve_config.display_source == tree_dsl::file_tree::DisplaySource::Resolved;
    let fqn_sep = support_lang.fqn_separator();
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
        let fp = lang.syms.resolve(tree.root().sym()).to_string();
        for i in 0..tree.len() {
            let nr = tree.cursor(i);
            if !(nr.is(C::Import) || nr.is(C::ImportType)) {
                continue;
            }
            let Some(imp_ids) = ids.imports.get(&(fi, i)) else {
                continue;
            };

            let source_sym = nr.child_sym(C::Source).unwrap_or(0);
            let source_str = if use_resolved {
                nr.child_sym(C::SourcePath)
                    .map(|sp| lang.syms.resolve(sp).replace('/', fqn_sep))
                    .unwrap_or_else(|| lang.syms.resolve(source_sym).to_string())
            } else {
                lang.syms.resolve(source_sym).to_string()
            };
            let source_str = source_str.as_str();
            let is_type_only = nr.is(C::ImportType);

            let names: Vec<(u32, u32)> = nr
                .children()
                .filter(|c| c.is(C::Name) && c.sym() != 0)
                .map(|c| {
                    let alias = c.child_sym(C::Alias).unwrap_or(0);
                    (c.sym(), alias)
                })
                .collect();

            let has_alias = names.iter().any(|(_, a)| *a != 0);
            let is_wildcard = names.len() == 1 && lang.syms.resolve(names[0].0) == "*";
            let source_eq_name = names.len() == 1 && names[0].0 == source_sym;
            let label = if is_wildcard {
                "WildcardImport"
            } else if has_alias {
                "AliasedImport"
            } else if names.is_empty() || source_eq_name {
                "Import"
            } else {
                "FromImport"
            };

            if names.is_empty() {
                let iid = imp_ids[0];
                id_b.append_value(iid);
                fp_b.append_value(&fp);
                it_b.append_value(label);
                path_b.append_value(source_str);
                name_b.append_null();
                alias_b.append_null();
                to_b.append_value(is_type_only);
                ht_b.append_value(false);
                sl_b.append_value(nr.start() as i64);
                el_b.append_value(nr.end() as i64);
                sb_b.append_value(nr.start() as i64);
                eb_b.append_value(nr.end() as i64);
                sc_b.append_value(0);
                ec_b.append_value(0);
            } else {
                for (ni, &(ns, als)) in names.iter().enumerate() {
                    let iid = imp_ids[ni];
                    id_b.append_value(iid);
                    fp_b.append_value(&fp);
                    it_b.append_value(label);
                    path_b.append_value(source_str);
                    if ns != 0 {
                        name_b.append_value(lang.syms.resolve(ns));
                    } else {
                        name_b.append_null();
                    }
                    if als != 0 {
                        alias_b.append_value(lang.syms.resolve(als));
                    } else {
                        alias_b.append_null();
                    }
                    to_b.append_value(is_type_only);
                    ht_b.append_value(false);
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
    ids: &IdMaps,
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
        let fid = fi as i64 + 1;
        for i in 0..tree.len() {
            let nr = tree.cursor(i);
            if nr.has(C::DefType) {
                if let Some(&did) = ids.defs.get(&(fi, i)) {
                    ds.append_value(fid);
                    dt.append_value(did);
                    dk.append_value("Defines");
                }
            } else if (nr.is(C::Import) || nr.is(C::ImportType))
                && let Some(iids) = ids.imports.get(&(fi, i))
            {
                for &iid in iids {
                    is.append_value(fid);
                    it.append_value(iid);
                    ik.append_value("Imports");
                }
            }
        }
        for edge in tree.edges().iter() {
            if edge.from.node == 0
                && edge.kind == tree_dsl::tree::EdgeKind::Calls
                && let Some(&tid) = ids.defs.get(&(fi, edge.to.node))
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
    cross_edges: &[tree_dsl::tree::Edge],
    ids: &IdMaps,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    for (fi, tree) in trees.iter().enumerate() {
        for edge in tree.edges().iter() {
            let label = match edge.kind {
                tree_dsl::tree::EdgeKind::Calls => "Calls",
                tree_dsl::tree::EdgeKind::Defines => "Defines",
                _ => continue,
            };
            if let (Some(&from), Some(&to)) = (
                ids.defs.get(&(fi, edge.from.node)),
                ids.defs.get(&(fi, edge.to.node)),
            ) {
                s.append_value(from);
                t.append_value(to);
                k.append_value(label);
            }
        }
    }
    let mut cross_seen = std::collections::HashSet::new();
    for ce in cross_edges {
        let label = match ce.kind {
            tree_dsl::tree::EdgeKind::Calls => "Calls",
            tree_dsl::tree::EdgeKind::Defines => "Defines",
            _ => continue,
        };
        if let (Some(&from), Some(&to)) = (
            ids.defs.get(&(ce.from.tree as usize, ce.from.node)),
            ids.defs.get(&(ce.to.tree as usize, ce.to.node)),
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
    cross_edges: &[tree_dsl::tree::Edge],
    ids: &IdMaps,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    let resolved: std::collections::HashSet<(usize, u32)> = cross_edges
        .iter()
        .filter(|ce| ce.kind == tree_dsl::tree::EdgeKind::Calls)
        .map(|ce| (ce.from.tree as usize, ce.from.node))
        .collect();
    for (fi, tree) in trees.iter().enumerate() {
        for edge in tree.edges().iter() {
            if edge.kind != tree_dsl::tree::EdgeKind::Imports {
                continue;
            }
            if resolved.contains(&(fi, edge.from.node)) {
                continue;
            }
            let Some(&caller_id) = ids.defs.get(&(fi, edge.from.node)) else {
                continue;
            };
            if let Some(&iid) = ids.import_by_name.get(&(fi, edge.to.node)) {
                s.append_value(caller_id);
                t.append_value(iid);
                k.append_value("Calls");
            } else if let Some(iids) = ids.imports.get(&(fi, edge.to.node)) {
                for &iid in iids {
                    s.append_value(caller_id);
                    t.append_value(iid);
                    k.append_value("Calls");
                }
            }
        }
    }
    edge_batch(s, t, k)
}

fn build_imp2def(
    trees: &[Tree],
    cross_edges: &[tree_dsl::tree::Edge],
    ids: &IdMaps,
) -> anyhow::Result<RecordBatch> {
    let (mut s, mut t, mut k) = (
        Int64Builder::new(),
        Int64Builder::new(),
        StringBuilder::new(),
    );
    for ce in cross_edges {
        if ce.kind != tree_dsl::tree::EdgeKind::Imports {
            continue;
        }
        let Some(&target_id) = ids.defs.get(&(ce.to.tree as usize, ce.to.node)) else {
            continue;
        };
        let key = (ce.from.tree as usize, ce.from.node);
        if let Some(&iid) = ids.import_by_name.get(&key) {
            s.append_value(iid);
            t.append_value(target_id);
            k.append_value("Resolves");
        } else if let Some(iids) = ids.imports.get(&key) {
            for &iid in iids {
                s.append_value(iid);
                t.append_value(target_id);
                k.append_value("Resolves");
            }
        }
    }
    edge_batch(s, t, k)
}
