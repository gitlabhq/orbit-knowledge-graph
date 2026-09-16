//! Cross-file resolver. Reads canonical __import/__source/__name nodes.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::grammar::SupportLang;
use crate::lang::Lang;
use crate::tree::{Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type};

type VisibleMap = Vec<FxHashMap<u32, (usize, u32)>>;

pub struct ResolveResult {
    pub cross_edges: Vec<Edge>,
}

pub fn resolve(
    trees: &mut [Tree],
    lang: &mut Lang,
    support_lang: SupportLang,
    lookup_prefixes: &[String],
    external: &[String],
) -> ResolveResult {
    let index_names = support_lang.index_names();
    let file_index = build_file_index(trees, lang, support_lang, index_names);
    let mut visible = build_visible_names(trees);
    let (reqs, mut cross_edges) =
        gather_imports(trees, lang, &file_index, lookup_prefixes, external);

    let ambiguous =
        propagate_reexports(trees, lang, &reqs, &mut visible, support_lang, index_names);

    for req in &reqs {
        let resolved_sym = lang.syms.intern(&req.target_path);
        let sp_idx = trees[req.fi]
            .cursor(req.node)
            .child(C::SourcePath)
            .map(|n| n.index());
        if let Some(sn) = sp_idx {
            let nid = trees[req.fi].to_id(sn);
            trees[req.fi].node_mut(nid).sym = resolved_sym;
        }
    }

    let root = Cursor::new(trees, 0, 0);

    let import_edges = build_import_edges(
        root,
        lang,
        &reqs,
        &visible,
        &ambiguous,
        support_lang,
        index_names,
        &file_index,
    );
    cross_edges.extend(import_edges);
    let (module_call_edges, call_edges) =
        build_call_edges(root, lang, &cross_edges, &reqs, &visible);
    let type_edges = build_type_edges(root, &call_edges, &cross_edges, &visible);
    let field_edges = build_typed_field_edges(root, &cross_edges);
    cross_edges.extend(module_call_edges);
    cross_edges.extend(call_edges);
    cross_edges.extend(type_edges);
    cross_edges.extend(field_edges);
    ResolveResult { cross_edges }
}

fn build_file_index(
    trees: &[Tree],
    lang: &Lang,
    support_lang: SupportLang,
    index_names: &[String],
) -> FxHashMap<String, usize> {
    let mut idx: FxHashMap<String, usize> =
        FxHashMap::with_capacity_and_hasher(trees.len() * 3, Default::default());
    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.root().sym()).to_string();
        let file_lang = SupportLang::from_path(&path).unwrap_or(support_lang);
        let stem = file_lang.strip_extension(&path);
        idx.insert(path.clone(), fi);
        idx.insert(stem.to_string(), fi);
        for name in index_names {
            let suffix = format!("/{name}");
            if stem.ends_with(&suffix) {
                let pkg = &stem[..stem.len() - suffix.len()];
                if !pkg.is_empty() {
                    idx.insert(pkg.to_string(), fi);
                }
            } else if stem == name.as_str() {
                idx.insert(String::new(), fi);
            }
        }
    }
    idx
}

fn build_visible_names(trees: &[Tree]) -> VisibleMap {
    trees
        .iter()
        .enumerate()
        .map(|(fi, tree)| {
            let mut names = FxHashMap::with_capacity_and_hasher(16, Default::default());
            for c in tree.root().descendants() {
                if crate::canonical::has_def_type(c) {
                    if let Some(ns) = c.child_sym(C::DefName) {
                        names.insert(ns, (fi, c.index()));
                    }
                    if let Some(ds) = c.child_sym(C::DefaultExport) {
                        names.insert(ds, (fi, c.index()));
                    }
                }
            }
            names
        })
        .collect()
}

fn gather_imports(
    trees: &[Tree],
    lang: &Lang,
    file_index: &FxHashMap<String, usize>,
    lookup_prefixes: &[String],
    external: &[String],
) -> (Vec<ImportReq>, Vec<Edge>) {
    let import_estimate = trees.iter().map(|t| t.len() as usize).sum::<usize>() / 20;
    let mut reqs = Vec::with_capacity(import_estimate);
    let mut cross_edges = Vec::with_capacity(import_estimate);
    for (fi, tree) in trees.iter().enumerate() {
        for cur in tree.root().descendants() {
            if cur.kind() != C::Import && cur.kind() != C::ImportType {
                continue;
            }
            let Some(source_sym) = cur.child_sym(C::SourcePath) else {
                continue;
            };
            let source_str = lang.syms.resolve(source_sym).to_string();
            if external
                .iter()
                .any(|e| e == source_str.split('/').next().unwrap_or(&source_str))
            {
                continue;
            }
            let target_path = if source_str.starts_with("./") || source_str.starts_with("../") {
                resolve_relative(lang.syms.resolve(tree.root().sym()), &source_str)
            } else {
                source_str.clone()
            };
            let node_idx = cur.index();
            if let Some(tfi) = resolve_path(&target_path, file_index, lookup_prefixes) {
                reqs.push(ImportReq {
                    fi,
                    node: node_idx,
                    target_fi: tfi,
                    target_path,
                });
            } else {
                for c in cur.names() {
                    let submod = format!("{target_path}/{}", lang.syms.resolve(c.sym()));
                    if let Some(sub_fi) = resolve_path(&submod, file_index, lookup_prefixes) {
                        cross_edges.push(Edge::new(fi, node_idx, sub_fi, 0, EdgeKind::Imports));
                        reqs.push(ImportReq {
                            fi,
                            node: node_idx,
                            target_fi: sub_fi,
                            target_path: submod,
                        });
                    }
                }
            }
        }
    }
    (reqs, cross_edges)
}

fn resolve_path(
    target: &str,
    file_index: &FxHashMap<String, usize>,
    prefixes: &[String],
) -> Option<usize> {
    file_index.get(target).copied().or_else(|| {
        prefixes.iter().find_map(|p| {
            let c = if p.is_empty() {
                target.to_string()
            } else {
                format!("{p}/{target}")
            };
            file_index.get(&c).copied()
        })
    })
}

fn propagate_reexports(
    trees: &[Tree],
    lang: &Lang,
    reqs: &[ImportReq],
    visible: &mut VisibleMap,
    support_lang: SupportLang,
    index_names: &[String],
) -> FxHashSet<(usize, u32)> {
    let mut ambiguous: FxHashSet<(usize, u32)> = FxHashSet::default();
    for _round in 0..3 {
        let mut new_exports: Vec<(usize, u32, usize, u32)> = Vec::new();
        for req in reqs {
            let path = lang.syms.resolve(trees[req.fi].root().sym());
            let file_lang = SupportLang::from_path(path).unwrap_or(support_lang);
            let stem = file_lang.strip_extension(path);
            if !index_names
                .iter()
                .any(|idx| stem.ends_with(&format!("/{idx}")) || stem == idx.as_str())
            {
                continue;
            }
            for c in trees[req.fi]
                .cursor(req.node)
                .children()
                .filter(|c| c.is(C::Name) && c.sym() != 0)
            {
                let ns = c.sym();

                if lang.syms.resolve(ns) == "*" {
                    let target_entries: Vec<_> = visible[req.target_fi]
                        .iter()
                        .map(|(&s, &v)| (s, v))
                        .collect();
                    for (ds, (tfi, tn)) in target_entries {
                        if !visible[req.fi].contains_key(&ds) {
                            new_exports.push((req.fi, ds, tfi, tn));
                        }
                    }
                } else if let Some(&(tfi, tn)) = visible[req.target_fi].get(&ns)
                    && !visible[req.fi].contains_key(&ns)
                {
                    new_exports.push((req.fi, ns, tfi, tn));
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        for (fi, ns, tfi, tn) in new_exports {
            if let Some(&(efi, en)) = visible[fi].get(&ns) {
                if efi != tfi || en != tn {
                    ambiguous.insert((fi, ns));
                }
                continue;
            }
            visible[fi].insert(ns, (tfi, tn));
        }
    }
    ambiguous
}

// ── Edge-building functions: receive a corpus Cursor, no raw &[Tree] ──

fn build_import_edges(
    corpus: Cursor,
    lang: &Lang,
    reqs: &[ImportReq],
    visible: &VisibleMap,
    ambiguous: &FxHashSet<(usize, u32)>,
    support_lang: SupportLang,
    index_names: &[String],
    file_index: &FxHashMap<String, usize>,
) -> Vec<Edge> {
    let mut edges = Vec::with_capacity(reqs.len());
    for req in reqs {
        let (fi, tfi) = (req.fi, req.target_fi);
        let import = corpus.jump(fi as u32, req.node);
        for c in import.names() {
            let ns = c.sym();
            let name_str = lang.syms.resolve(ns);
            if name_str == "*" {
                if c.child_sym(C::Alias).is_some() {
                    edges.push(c.edge_to(c.jump(tfi as u32, 0), EdgeKind::Imports));
                } else {
                    for (&dn, &(rfi, rn)) in &visible[tfi] {
                        if !ambiguous.contains(&(tfi, dn)) {
                            edges.push(c.edge_to(c.jump(rfi as u32, rn), EdgeKind::Imports));
                        }
                    }
                }
                continue;
            }
            if ambiguous.contains(&(tfi, ns)) {
                continue;
            }
            if let Some(&(rfi, rn)) = visible[tfi].get(&ns) {
                edges.push(c.edge_to(c.jump(rfi as u32, rn), EdgeKind::Imports));
            } else {
                let results = follow_import_chain(corpus, reqs, visible, ns, tfi);
                if results.len() == 1 {
                    edges.push(
                        c.edge_to(c.jump(results[0].0 as u32, results[0].1), EdgeKind::Imports),
                    );
                } else {
                    let tgt = corpus.jump(tfi as u32, 0);
                    let target_stem = support_lang.strip_extension(lang.syms.resolve(tgt.sym()));
                    if let Some(dir) = index_names
                        .iter()
                        .find_map(|idx| target_stem.strip_suffix(&format!("/{idx}")))
                        && let Some(&sub_fi) = file_index.get(&format!("{dir}/{name_str}"))
                    {
                        edges.push(c.edge_to(c.jump(sub_fi as u32, 0), EdgeKind::Imports));
                    }
                }
            }
        }
    }
    edges
}

fn build_call_edges(
    corpus: Cursor,
    lang: &Lang,
    cross_edges: &[Edge],
    reqs: &[ImportReq],
    visible: &VisibleMap,
) -> (Vec<Edge>, Vec<Edge>) {
    let mut import_targets: FxHashMap<(usize, u32), Vec<usize>> = FxHashMap::default();
    for ce in cross_edges.iter().filter(|e| e.kind == EdgeKind::Imports) {
        import_targets
            .entry((ce.from.tree as usize, ce.from.node))
            .or_default()
            .push(ce.to.tree as usize);
    }

    let mut module_calls = Vec::with_capacity(reqs.len());
    for req in reqs {
        let (fi, import_node) = (req.fi, req.node);
        let nodes = &corpus.trees_ref()[fi];
        let mut target_files = vec![req.target_fi];
        if let Some(targets) = import_targets.get(&(fi, import_node)) {
            for &tfi in targets {
                if !target_files.contains(&tfi) {
                    target_files.push(tfi);
                }
            }
        }
        for child in nodes.cursor(import_node).children() {
            if let Some(targets) = import_targets.get(&(fi, child.index())) {
                for &tfi in targets {
                    if !target_files.contains(&tfi) {
                        target_files.push(tfi);
                    }
                }
            }
        }
        for edge in nodes.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let et = edge.to.node;
            if et != import_node
                && nodes
                    .cursor(et)
                    .parent()
                    .is_none_or(|p| p.index() != import_node)
            {
                continue;
            }
            let caller = corpus.jump(fi as u32, edge.from.node);
            for d in caller.descendants().filter(|d| d.is(C::Call)) {
                if let Some(mn) = d.child(C::Callee).and_then(|cn| cn.child(C::Member)) {
                    let ms = mn.sym();
                    if ms != 0 {
                        for &tfi in &target_files {
                            if let Some(&(dfi, dn)) = visible[tfi].get(&ms) {
                                let tgt = caller.jump(dfi as u32, dn);
                                if is_callable(tgt) {
                                    module_calls.push(caller.edge_to(tgt, EdgeKind::Calls));
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    let mut reverse_visible: FxHashMap<(usize, u32), u32> = FxHashMap::default();
    for names in visible.iter() {
        for (&sym, &(vfi, vn)) in names {
            reverse_visible.insert((vfi, vn), sym);
        }
    }

    let mut call_edges = Vec::with_capacity(cross_edges.len());
    for ce in cross_edges.iter().filter(|e| e.kind == EdgeKind::Imports) {
        let target_name = reverse_visible
            .get(&(ce.to.tree as usize, ce.to.node))
            .copied()
            .unwrap_or(0);
        let ft = &corpus.trees_ref()[ce.from.tree as usize];
        let import_parent = ft.cursor(ce.from.node).parent().map(|p| p.index());
        for edge in ft.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let ei = edge.to.node;
            let direct = ei == ce.from.node
                || ft.cursor(ei).parent().map(|p| p.index()) == Some(ce.from.node)
                || import_parent == Some(ei);
            if !direct {
                continue;
            }
            if lang.syms.resolve(ft.cursor(ce.from.node).sym()) == "*" && target_name != 0 {
                let found = corpus
                    .jump(ce.from.tree, edge.from.node)
                    .descendants()
                    .any(|d| d.is(C::Call) && d.child_sym(C::Callee) == Some(target_name));
                if !found {
                    continue;
                }
            }
            let target = corpus.follow(ce);
            if !is_callable(target) {
                continue;
            }
            let from = corpus.jump(ce.from.tree, edge.from.node);
            call_edges.push(from.edge_to(target, EdgeKind::Calls));
        }
    }
    (module_calls, call_edges)
}

fn is_callable(def: Cursor) -> bool {
    crate::canonical::is_callable_def(def)
}

fn build_type_edges(
    corpus: Cursor,
    call_edges: &[Edge],
    cross_edges: &[Edge],
    visible: &VisibleMap,
) -> Vec<Edge> {
    let mut type_edges = Vec::new();
    for ce in call_edges.iter().filter(|e| e.kind == EdgeKind::Calls) {
        let target = corpus.follow(ce);
        let caller = corpus.jump(ce.from.tree, ce.from.node);

        let Some(ret_sym) = infer_return_type(target) else {
            continue;
        };
        let Some((type_fi, type_node)) =
            resolve_type(ret_sym, ce.to.tree as usize, corpus, visible, cross_edges)
        else {
            continue;
        };

        let target_name = target.child_sym(C::DefName).unwrap_or(0);
        let bound: Vec<u32> = caller
            .descendants()
            .filter(|d| d.is(C::Binding) && d.sym() != 0)
            .filter_map(|d| {
                let callee = d.child(C::Rhs)?.child(C::Call)?.child_sym(C::Callee)?;
                (callee == target_name).then_some(d.sym())
            })
            .collect();

        let class = caller.jump(type_fi as u32, type_node);
        for d in caller.descendants().filter(|d| d.is(C::Call)) {
            if let Some(mn) = d.child(C::Callee).and_then(|cn| cn.child(C::Member)) {
                let obj = mn.child_sym(C::Object).unwrap_or(0);
                if !bound.contains(&obj) {
                    continue;
                }
                let method = mn.sym();
                if method != 0
                    && let Some(m) = find_method_in(class, method)
                {
                    type_edges.push(caller.edge_to(m, EdgeKind::Calls));
                }
            }
        }
    }
    type_edges
}

fn resolve_type(
    ret_sym: u32,
    target_fi: usize,
    corpus: Cursor,
    visible: &VisibleMap,
    cross_edges: &[Edge],
) -> Option<(usize, u32)> {
    if let Some(&loc) = visible[target_fi].get(&ret_sym) {
        return Some(loc);
    }
    for ce in cross_edges {
        if ce.from.tree as usize == target_fi
            && ce.kind == EdgeKind::Imports
            && corpus.follow(ce).child_sym(C::DefName) == Some(ret_sym)
        {
            return Some((ce.to.tree as usize, ce.to.node));
        }
    }
    None
}

fn build_typed_field_edges(corpus: Cursor, cross_edges: &[Edge]) -> Vec<Edge> {
    let mut edges = Vec::new();
    for ce in cross_edges.iter().filter(|e| e.kind == EdgeKind::Imports) {
        let target = corpus.follow(ce);
        let target_dt = crate::canonical::def_type_of(target);
        if !target_dt.is_some_and(|k| matches!(k, C::Class | C::Struct)) {
            continue;
        }
        let target_name = target.child_sym(C::DefName).unwrap_or(0);
        if target_name == 0 {
            continue;
        }
        let ft = &corpus.trees_ref()[ce.from.tree as usize];
        for nr in ft.root().descendants() {
            if !nr.is(C::Binding) {
                continue;
            }
            let Some(ivar) = nr.child(C::Ivar) else {
                continue;
            };
            let callee_sym = nr
                .child(C::Rhs)
                .and_then(|r| r.child(C::Call))
                .and_then(|c| c.child_sym(C::Callee));
            if callee_sym != Some(target_name) {
                continue;
            }
            let ivar_sym = ivar.sym();
            if ivar_sym == 0 {
                continue;
            }
            let class = nr.enclosing(|a| {
                crate::canonical::def_type_of(a)
                    .is_some_and(|k| matches!(k, C::Class | C::Struct | C::ImplBlock))
            });
            let Some(cls) = class else {
                continue;
            };
            for d in cls.descendants().filter(|d| d.is(C::Call)) {
                let Some(callee) = d.child(C::Callee) else {
                    continue;
                };
                let Some(member) = callee.child(C::Member) else {
                    continue;
                };
                let obj_ivar = member.child(C::Object).and_then(|o| o.child(C::Ivar));
                if obj_ivar.is_none_or(|iv| iv.sym() != ivar_sym) {
                    continue;
                }
                let method_sym = member.sym();
                if method_sym == 0 {
                    continue;
                }
                let caller = d.enclosing(|a| crate::canonical::has_def_type(a));
                let Some(caller_def) = caller else {
                    continue;
                };
                if let Some(m) = find_method_in(target, method_sym) {
                    edges.push(
                        corpus
                            .jump(ce.from.tree, caller_def.index())
                            .edge_to(m, EdgeKind::Calls),
                    );
                }
            }
        }
    }
    edges
}

fn follow_import_chain(
    corpus: Cursor,
    reqs: &[ImportReq],
    visible: &VisibleMap,
    wanted: u32,
    start_fi: usize,
) -> Vec<(usize, u32)> {
    let mut results = Vec::new();
    let mut visited = Vec::new();
    let mut stack = vec![(start_fi, wanted)];
    while let Some((fi, ws)) = stack.pop() {
        if visited.contains(&(fi, ws)) {
            continue;
        }
        visited.push((fi, ws));
        if let Some(&loc) = visible[fi].get(&ws) {
            if !results.contains(&loc) {
                results.push(loc);
            }
            continue;
        }
        let tree = &corpus.trees_ref()[fi];
        for nc in tree.root().descendants() {
            if nc.kind() != C::Import {
                continue;
            }
            for c in nc.children().filter(|c| c.is(C::Name)) {
                let import_name = c.sym();
                let alias = c.child_sym(C::Alias).unwrap_or(0);
                if import_name != ws && alias != ws {
                    continue;
                }
                for req in reqs {
                    if req.fi == fi && req.node == nc.index() {
                        stack.push((req.target_fi, import_name));
                    }
                }
            }
        }
        if visited.len() > 10 {
            break;
        }
    }
    results
}

struct ImportReq {
    fi: usize,
    node: u32,
    target_fi: usize,
    target_path: String,
}

fn resolve_relative(current_file: &str, source: &str) -> String {
    let dir = current_file.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    let mut parts: Vec<&str> = if dir.is_empty() {
        Vec::new()
    } else {
        dir.split('/').collect()
    };
    let mut rest = source;
    loop {
        if let Some(r) = rest.strip_prefix("../") {
            parts.pop();
            rest = r;
        } else if let Some(r) = rest.strip_prefix("./") {
            rest = r;
        } else {
            break;
        }
    }
    if rest == ".." {
        parts.pop();
        rest = "";
    } else if rest == "." {
        rest = "";
    }
    if rest.is_empty() {
        parts.join("/")
    } else if parts.is_empty() {
        rest.to_string()
    } else {
        format!("{}/{rest}", parts.join("/"))
    }
}
