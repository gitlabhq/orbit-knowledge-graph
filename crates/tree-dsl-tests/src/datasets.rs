use std::collections::HashMap;
use std::sync::Arc;

use arrow_56::array::{ArrayBuilder, BooleanBuilder, Int64Builder, StringBuilder};
use arrow_56::datatypes::{DataType, Field, Schema};
use arrow_56::record_batch::RecordBatch;

use tree_dsl::grammar::SupportLang;
use tree_dsl::lang::Lang;
use tree_dsl::tree::Tree;

pub type LanceDatasets = HashMap<String, RecordBatch>;

// ── Synthetic helpers ──

fn synth_sym(tree: &Tree, node: u32, kind: u16) -> u32 {
    tree.children(node)
        .find(|&c| tree.kind(c) == kind)
        .map(|c| tree.sym(c))
        .unwrap_or(0)
}

fn has_synth(tree: &Tree, node: u32, kind: u16) -> bool {
    tree.children(node).any(|c| tree.kind(c) == kind)
}

// ── Synthetic kind cache ──

struct Sk {
    deftype: u16,
    import: u16,
    import_type: u16,
    source: u16,
    name: u16,
    alias: u16,
    type_only: u16,
}

impl Sk {
    fn new(lang: &Lang) -> Self {
        let s = |n: &str| lang.kinds.lookup(n) as u16;
        Self {
            deftype: s("__deftype"),
            import: s("__import"),
            import_type: s("__import_type"),
            source: s("__source"),
            name: s("__name"),
            alias: s("__alias"),
            type_only: s("__type_only"),
        }
    }

    fn is_def(&self, tree: &Tree, node: u32) -> bool {
        has_synth(tree, node, self.deftype)
    }

    fn is_import(&self, tree: &Tree, node: u32) -> bool {
        let k = tree.nodes[node as usize].kind;
        k == self.import || k == self.import_type
    }
}

// ── ID assignment ──

pub struct IdMaps {
    pub defs: HashMap<(usize, u32), i64>,
    pub imports: HashMap<(usize, u32), Vec<i64>>,
    /// Maps (file_index, __name_node) → single import ID
    pub import_by_name: HashMap<(usize, u32), i64>,
    pub modules: HashMap<usize, i64>,
}

fn assign_ids(trees: &[Tree], lang: &Lang, sk: &Sk) -> IdMaps {
    let mut defs = HashMap::new();
    let mut imports = HashMap::new();
    let mut import_by_name = HashMap::new();
    let mut modules = HashMap::new();
    let mut next_def: i64 = 1000;
    let mut next_imp: i64 = 5000;
    let mut next_mod: i64 = 900_000;

    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.nodes[0].sym);
        if !matches!(SupportLang::from_path(path), Some(SupportLang::Python)) {
            next_mod += 1;
            modules.insert(fi, next_mod);
        }
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.dead {
                continue;
            }
            let node = i as u32;
            if sk.is_def(tree, node) {
                next_def += 1;
                defs.insert((fi, node), next_def);
            } else if sk.is_import(tree, node) {
                let name_nodes: Vec<u32> = tree
                    .children(node)
                    .filter(|&c| tree.kind(c) == sk.name && tree.sym(c) != 0)
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
                imports.insert((fi, node), ids);
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
) -> anyhow::Result<LanceDatasets> {
    let sk = Sk::new(lang);
    let ids = assign_ids(trees, lang, &sk);
    let mut ds = HashMap::new();
    ds.insert("File".into(), build_files(trees, lang)?);
    ds.insert("Definition".into(), build_defs(trees, lang, &ids, &sk)?);
    ds.insert(
        "ImportedSymbol".into(),
        build_imports(trees, lang, &ids, &sk)?,
    );
    let (f2d, f2i) = build_file_edges(trees, &ids, &sk);
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

fn def_name_sym(tree: &Tree, node: u32, name_f: u16, left_f: u16) -> u32 {
    tree.child_by_field(node, name_f)
        .or_else(|| {
            if left_f != 0 {
                tree.child_by_field(node, left_f)
            } else {
                None
            }
        })
        .map(|c| tree.sym(c))
        .unwrap_or(0)
}

fn def_fqn(tree: &Tree, node: u32, lang: &Lang, sk: &Sk) -> String {
    let path_str = lang.syms.resolve(tree.nodes[0].sym).to_string();
    let lang_id = SupportLang::from_path(&path_str);
    let sep = lang_id.map(|l| l.fqn_separator()).unwrap_or(".");
    let name_f = lang.fields.lookup("name") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let skip_root = matches!(
        lang_id,
        Some(SupportLang::JavaScript | SupportLang::TypeScript | SupportLang::Tsx)
    );

    let mut parts = Vec::new();
    let mut n = node;
    loop {
        let nd = &tree.nodes[n as usize];
        if sk.is_def(tree, n) || n == 0 {
            let name = if n == 0 && skip_root {
                String::new()
            } else if n == 0 {
                let path = lang.syms.resolve(nd.sym).to_string();
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
                let sym = def_name_sym(tree, n, name_f, left_f);
                lang.syms.resolve(sym).to_string()
            };
            if !name.is_empty() {
                parts.push(name);
            }
        }
        if n == 0 {
            break;
        }
        n = nd.parent;
        if n == u32::MAX {
            break;
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
        let path = lang.syms.resolve(tree.nodes[0].sym);
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

fn build_defs(trees: &[Tree], lang: &Lang, ids: &IdMaps, sk: &Sk) -> anyhow::Result<RecordBatch> {
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
    let name_f = lang.fields.lookup("name") as u16;

    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.nodes[0].sym).to_string();
        for (i, n) in tree.nodes.iter().enumerate() {
            let node = i as u32;
            if !sk.is_def(tree, node) {
                continue;
            }
            let did = ids.defs[&(fi, node)];
            let left_f = lang.fields.lookup("left") as u16;
            let name_sym = def_name_sym(tree, node, name_f, left_f);
            let deftype_sym = synth_sym(tree, node, sk.deftype);
            id_b.append_value(did);
            fp_b.append_value(&path);
            fqn_b.append_value(def_fqn(tree, node, lang, sk));
            name_b.append_value(lang.syms.resolve(name_sym));
            dt_b.append_value(lang.syms.resolve(deftype_sym));
            sl_b.append_value(n.start as i64);
            el_b.append_value(n.end as i64);
            sb_b.append_value(n.start as i64);
            eb_b.append_value(n.end as i64);
            sc_b.append_value(0);
            ec_b.append_value(0);
        }
    }
    // Module defs (non-Python)
    for (fi, tree) in trees.iter().enumerate() {
        if let Some(&mid) = ids.modules.get(&fi) {
            let path = lang.syms.resolve(tree.nodes[0].sym).to_string();
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
    sk: &Sk,
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
        let fp = lang.syms.resolve(tree.nodes[0].sym).to_string();
        for (i, n) in tree.nodes.iter().enumerate() {
            let node = i as u32;
            if !sk.is_import(tree, node) {
                continue;
            }
            let Some(imp_ids) = ids.imports.get(&(fi, node)) else {
                continue;
            };

            let source_sym = synth_sym(tree, node, sk.source);
            let source_str = lang.syms.resolve(source_sym);
            let is_type_only = tree.nodes[node as usize].kind == sk.import_type;

            // Collect __name children with optional __alias (skip empty syms)
            let names: Vec<(u32, u32)> = tree
                .children(node)
                .filter(|&c| tree.kind(c) == sk.name && tree.sym(c) != 0)
                .map(|c| {
                    let alias = tree
                        .children(c)
                        .find(|&gc| tree.kind(gc) == sk.alias)
                        .map(|gc| tree.sym(gc))
                        .unwrap_or(0);
                    (tree.sym(c), alias)
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
                sl_b.append_value(n.start as i64);
                el_b.append_value(n.end as i64);
                sb_b.append_value(n.start as i64);
                eb_b.append_value(n.end as i64);
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
                    sl_b.append_value(n.start as i64);
                    el_b.append_value(n.end as i64);
                    sb_b.append_value(n.start as i64);
                    eb_b.append_value(n.end as i64);
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
    sk: &Sk,
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
        for (i, _n) in tree.nodes.iter().enumerate() {
            let node = i as u32;
            if sk.is_def(tree, node) {
                if let Some(&did) = ids.defs.get(&(fi, node)) {
                    ds.append_value(fid);
                    dt.append_value(did);
                    dk.append_value("Defines");
                }
            } else if sk.is_import(tree, node)
                && let Some(iids) = ids.imports.get(&(fi, node))
            {
                for &iid in iids {
                    is.append_value(fid);
                    it.append_value(iid);
                    ik.append_value("Imports");
                }
            }
        }
        for edge in &tree.edges {
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
        for edge in &tree.edges {
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
        for edge in &tree.edges {
            if edge.kind != tree_dsl::tree::EdgeKind::Imports {
                continue;
            }
            if resolved.contains(&(fi, edge.from.node)) {
                continue;
            }
            let Some(&caller_id) = ids.defs.get(&(fi, edge.from.node)) else {
                continue;
            };
            // edge.to is now a __name node — look up its specific import ID
            if let Some(&iid) = ids.import_by_name.get(&(fi, edge.to.node)) {
                s.append_value(caller_id);
                t.append_value(iid);
                k.append_value("Calls");
            } else if let Some(iids) = ids.imports.get(&(fi, edge.to.node)) {
                // Fallback: edge.to is __import node (wildcard case)
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
        // The cross-edge goes from import_node → def_node.
        // Find the import ID(s) for this import node.
        if let Some(iids) = ids.imports.get(&(ce.from.tree as usize, ce.from.node)) {
            for &iid in iids {
                s.append_value(iid);
                t.append_value(target_id);
                k.append_value("Resolves");
            }
        }
        // Also check if any intra-file E_IMPORTS edges point to this import
        for edge in &trees[ce.from.tree as usize].edges {
            if edge.kind != tree_dsl::tree::EdgeKind::Imports || edge.to.node != ce.from.node {
                continue;
            };
            if let Some(iids) = ids.imports.get(&(ce.from.tree as usize, edge.to.node)) {
                for &iid in iids {
                    s.append_value(iid);
                    t.append_value(target_id);
                    k.append_value("Resolves");
                }
            }
        }
    }
    edge_batch(s, t, k)
}
