//! Cross-file resolver. Reads canonical __import/__source/__name nodes.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::grammar::SupportLang;
use crate::lang::Lang;
use crate::tree::{Cursor, Edge, EdgeKind, NodeRef, Step, Tree};

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
    let (reexports, ambiguous) =
        propagate_reexports(trees, lang, &reqs, &mut visible, support_lang, index_names);

    for req in &reqs {
        let resolved_sym = lang.syms.intern(&req.target_path);
        let sp_idx = trees[req.fi]
            .cursor(req.node)
            .child(C::SourcePath)
            .map(|n| n.index());
        if let Some(sn) = sp_idx {
            trees[req.fi].nodes[sn as usize].sym = resolved_sym;
        }
    }

    let import_edges = build_import_edges(
        trees,
        lang,
        &reqs,
        &visible,
        &reexports,
        &ambiguous,
        support_lang,
        index_names,
        &file_index,
    );
    cross_edges.extend(import_edges);
    let (module_call_edges, call_edges) =
        build_call_edges(trees, lang, &cross_edges, &reqs, &visible);
    let type_edges = build_type_edges(trees, &call_edges, &cross_edges, &visible);
    cross_edges.extend(module_call_edges);
    cross_edges.extend(call_edges);
    cross_edges.extend(type_edges);
    ResolveResult { cross_edges }
}

fn build_file_index(
    trees: &[Tree],
    lang: &Lang,
    support_lang: SupportLang,
    index_names: &[String],
) -> FxHashMap<String, usize> {
    let mut idx: FxHashMap<String, usize> = FxHashMap::default();
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

fn build_visible_names(trees: &[Tree]) -> Vec<FxHashMap<u32, u32>> {
    trees
        .iter()
        .map(|tree| {
            let mut names = FxHashMap::default();
            for i in 0..tree.len() {
                if tree.nodes[i as usize].dead {
                    continue;
                }
                let c = tree.cursor(i);
                if c.has(C::DefType) {
                    if let Some(ns) = c.child_sym(C::DefName) {
                        names.insert(ns, i);
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
    let mut reqs = Vec::new();
    let mut cross_edges = Vec::new();

    for (fi, tree) in trees.iter().enumerate() {
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.kind != C::Import && n.kind != C::ImportType {
                continue;
            }
            let cur = tree.cursor(i as u32);
            let source_sym = match cur.child_sym(C::SourcePath) {
                Some(s) => s,
                None => continue,
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

            if let Some(tfi) = resolve_path(&target_path, file_index, lookup_prefixes) {
                reqs.push(ImportReq {
                    fi,
                    node: i as u32,
                    target_fi: tfi,
                    target_path,
                });
            } else {
                for c in cur.children().filter(|c| c.is(C::Name) && c.sym() != 0) {
                    let submod = format!("{target_path}/{}", lang.syms.resolve(c.sym()));
                    if let Some(sub_fi) = resolve_path(&submod, file_index, lookup_prefixes) {
                        cross_edges.push(Edge::new(fi, i as u32, sub_fi, 0, EdgeKind::Imports));
                        reqs.push(ImportReq {
                            fi,
                            node: i as u32,
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
            let candidate = if p.is_empty() {
                target.to_string()
            } else {
                format!("{p}/{target}")
            };
            file_index.get(&candidate).copied()
        })
    })
}

fn propagate_reexports(
    trees: &[Tree],
    lang: &Lang,
    reqs: &[ImportReq],
    visible: &mut [FxHashMap<u32, u32>],
    support_lang: SupportLang,
    index_names: &[String],
) -> (
    FxHashMap<(usize, u32), (usize, u32)>,
    FxHashSet<(usize, u32)>,
) {
    let mut reexports: FxHashMap<(usize, u32), (usize, u32)> = FxHashMap::default();
    let mut ambiguous: FxHashSet<(usize, u32)> = FxHashSet::default();
    for _round in 0..3 {
        let mut new_exports = Vec::new();
        for req in reqs {
            let path = lang.syms.resolve(trees[req.fi].root().sym());
            let stem = support_lang.strip_extension(path);
            let is_index = index_names
                .iter()
                .any(|idx| stem.ends_with(&format!("/{idx}")) || stem == idx.as_str());
            if !is_index {
                continue;
            }
            for c in trees[req.fi]
                .cursor(req.node)
                .children()
                .filter(|c| c.is(C::Name))
            {
                let ns = c.sym();
                if ns == 0 {
                    continue;
                }
                if lang.syms.resolve(ns) == "*" {
                    for (&ds, &dn) in &visible[req.target_fi] {
                        if !visible[req.fi].contains_key(&ds) {
                            new_exports.push((req.fi, ds, req.target_fi, dn));
                        }
                    }
                    let target_re: Vec<_> = reexports
                        .iter()
                        .filter(|((fi, _), _)| *fi == req.target_fi)
                        .map(|((_, s), (tfi, tn))| (*s, *tfi, *tn))
                        .collect();
                    for (s, tfi, tn) in target_re {
                        if !visible[req.fi].contains_key(&s) {
                            new_exports.push((req.fi, s, tfi, tn));
                        }
                    }
                } else if let Some(&dn) = visible[req.target_fi].get(&ns) {
                    if !visible[req.fi].contains_key(&ns) {
                        new_exports.push((req.fi, ns, req.target_fi, dn));
                    }
                } else if let Some(&(tfi, tn)) = reexports.get(&(req.target_fi, ns))
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
            if let Some(&existing) = visible[fi].get(&ns) {
                let (efi, en) = reexports.get(&(fi, ns)).copied().unwrap_or((fi, existing));
                if efi != tfi || en != tn {
                    ambiguous.insert((fi, ns));
                }
                continue;
            }
            visible[fi].insert(ns, tn);
            reexports.insert((fi, ns), (tfi, tn));
        }
    }
    (reexports, ambiguous)
}

fn build_import_edges(
    trees: &[Tree],
    lang: &Lang,
    reqs: &[ImportReq],
    visible: &[FxHashMap<u32, u32>],
    reexports: &FxHashMap<(usize, u32), (usize, u32)>,
    ambiguous: &FxHashSet<(usize, u32)>,
    support_lang: SupportLang,
    index_names: &[String],
    file_index: &FxHashMap<String, usize>,
) -> Vec<Edge> {
    let mut edges = Vec::new();
    for req in reqs {
        let (fi, tfi) = (req.fi, req.target_fi);
        for c in trees[fi]
            .cursor(req.node)
            .children()
            .filter(|c| c.is(C::Name) && c.sym() != 0)
        {
            let ns = c.sym();
            let name_str = lang.syms.resolve(ns);

            if name_str == "*" {
                for (&dn, &dnode) in &visible[tfi] {
                    if !ambiguous.contains(&(tfi, dn)) {
                        let (rfi, rn) = reexports.get(&(tfi, dn)).copied().unwrap_or((tfi, dnode));
                        edges.push(Edge::new(fi, c.index(), rfi, rn, EdgeKind::Imports));
                    }
                }
                continue;
            }
            if ambiguous.contains(&(tfi, ns)) {
                continue;
            }
            if let Some(&(rfi, rn)) = reexports.get(&(tfi, ns)) {
                edges.push(Edge::new(fi, c.index(), rfi, rn, EdgeKind::Imports));
            } else if let Some(&dn) = visible[tfi].get(&ns) {
                edges.push(Edge::new(fi, c.index(), tfi, dn, EdgeKind::Imports));
            } else {
                let results = follow_import_chain(trees, reqs, visible, ns, tfi);
                if results.len() == 1 {
                    edges.push(Edge::new(
                        fi,
                        c.index(),
                        results[0].0,
                        results[0].1,
                        EdgeKind::Imports,
                    ));
                } else {
                    let target_stem =
                        support_lang.strip_extension(lang.syms.resolve(trees[tfi].root().sym()));
                    if let Some(dir) = index_names
                        .iter()
                        .find_map(|idx| target_stem.strip_suffix(&format!("/{idx}")))
                    {
                        if let Some(&sub_fi) = file_index.get(&format!("{dir}/{name_str}")) {
                            edges.push(Edge::new(fi, c.index(), sub_fi, 0, EdgeKind::Imports));
                        }
                    }
                }
            }
        }
    }
    edges
}

fn build_call_edges(
    trees: &[Tree],
    lang: &Lang,
    cross_edges: &[Edge],
    reqs: &[ImportReq],
    visible: &[FxHashMap<u32, u32>],
) -> (Vec<Edge>, Vec<Edge>) {
    let mut module_calls = Vec::new();
    for req in reqs {
        let (fi, import_node) = (req.fi, req.node);
        let tree = &trees[fi];
        let mut target_files = vec![req.target_fi];
        for ce in cross_edges {
            if ce.from.tree as usize == fi
                && (ce.from.node == import_node
                    || tree.nodes[ce.from.node as usize].parent == import_node)
                && ce.kind == EdgeKind::Imports
                && !target_files.contains(&(ce.to.tree as usize))
            {
                target_files.push(ce.to.tree as usize);
            }
        }
        for edge in tree.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let et = edge.to.node;
            if et != import_node && tree.nodes[et as usize].parent != import_node {
                continue;
            }
            let caller = tree.cursor(edge.from.node);
            for d in caller.descendants().filter(|d| d.is(C::Call)) {
                if let Some(mn) = d.child(C::Callee).and_then(|cn| cn.child(C::Member)) {
                    let ms = mn.sym();
                    if ms != 0 {
                        for &tfi in &target_files {
                            if let Some(&dn) = visible[tfi].get(&ms) {
                                module_calls.push(Edge::new(
                                    fi,
                                    edge.from.node,
                                    tfi,
                                    dn,
                                    EdgeKind::Calls,
                                ));
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    let mut call_edges = Vec::new();
    for ce in cross_edges.iter().filter(|e| e.kind == EdgeKind::Imports) {
        let target_name = visible[ce.to.tree as usize]
            .iter()
            .find(|(_, n)| **n == ce.to.node)
            .map(|(s, _)| *s)
            .unwrap_or(0);
        let ft = &trees[ce.from.tree as usize];
        let import_parent = ft.nodes[ce.from.node as usize].parent;
        for edge in ft.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let ei = edge.to.node;
            if ei != ce.from.node
                && ei != import_parent
                && ft.nodes[ei as usize].parent != import_parent
            {
                continue;
            }
            let is_wildcard = lang.syms.resolve(ft.sym(ce.from.node)) == "*";
            if is_wildcard && target_name != 0 {
                let found = ft
                    .cursor(edge.from.node)
                    .descendants()
                    .any(|d| d.is(C::Call) && d.child_sym(C::Callee) == Some(target_name));
                if !found {
                    continue;
                }
            }
            call_edges.push(Edge {
                from: NodeRef::new(ce.from.tree as usize, edge.from.node),
                to: ce.to,
                kind: EdgeKind::Calls,
            });
        }
    }
    (module_calls, call_edges)
}

fn build_type_edges(
    trees: &[Tree],
    call_edges: &[Edge],
    cross_edges: &[Edge],
    visible: &[FxHashMap<u32, u32>],
) -> Vec<Edge> {
    let mut type_edges = Vec::new();
    for ce in call_edges.iter().filter(|e| e.kind == EdgeKind::Calls) {
        let target = Cursor::new(trees, ce.to.tree, ce.to.node);
        let caller = Cursor::new(trees, ce.from.tree, ce.from.node);

        let ret_sym = match infer_return_type(target) {
            Some(s) => s,
            None => continue,
        };

        let (type_fi, type_node) =
            match resolve_type(ret_sym, ce.to.tree as usize, trees, visible, cross_edges) {
                Some(r) => r,
                None => continue,
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

        let type_cur = caller.jump(type_fi as u32, type_node);
        for d in caller.descendants().filter(|d| d.is(C::Call)) {
            if let Some(mn) = d.child(C::Callee).and_then(|cn| cn.child(C::Member)) {
                let obj = mn.child_sym(C::Object).unwrap_or(0);
                if !bound.contains(&obj) {
                    continue;
                }
                let method = mn.sym();
                if method != 0 {
                    if let Some(m) = find_method_in(type_cur, method) {
                        type_edges.push(caller.edge_to(m, EdgeKind::Calls));
                    }
                }
            }
        }
    }
    type_edges
}

fn infer_return_type(def: Cursor) -> Option<u32> {
    def.child_sym(C::ReturnType).or_else(|| {
        def.descend(|n| {
            if n.is(C::Return) {
                if let Some(s) = n.child(C::Call).and_then(|c| c.child_sym(C::Callee)) {
                    return Step::Out(s);
                }
            }
            if n.is(C::Def) && n.index() != def.index() {
                Step::Over
            } else {
                Step::Into
            }
        })
    })
}

fn find_method_in(class: Cursor, name: u32) -> Option<Cursor> {
    class.descend(|n| {
        if n.is(C::DefType) {
            if let Some(p) = n.parent() {
                if p.index() != class.index() && p.child_sym(C::DefName) == Some(name) {
                    return Step::Out(p);
                }
            }
        }
        Step::Into
    })
}

fn resolve_type(
    ret_sym: u32,
    target_fi: usize,
    trees: &[Tree],
    visible: &[FxHashMap<u32, u32>],
    cross_edges: &[Edge],
) -> Option<(usize, u32)> {
    if let Some(&cn) = visible[target_fi].get(&ret_sym) {
        return Some((target_fi, cn));
    }
    for ce in cross_edges {
        if ce.from.tree as usize == target_fi && ce.kind == EdgeKind::Imports {
            let dn = trees[ce.to.tree as usize]
                .cursor(ce.to.node)
                .child_sym(C::DefName)
                .unwrap_or(0);
            if dn == ret_sym {
                return Some((ce.to.tree as usize, ce.to.node));
            }
        }
    }
    None
}

fn follow_import_chain(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &[FxHashMap<u32, u32>],
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
        if let Some(&dn) = visible[fi].get(&ws) {
            if !results.contains(&(fi, dn)) {
                results.push((fi, dn));
            }
            continue;
        }
        for (ni, n) in trees[fi].nodes.iter().enumerate() {
            if n.kind != C::Import {
                continue;
            }
            for c in trees[fi]
                .cursor(ni as u32)
                .children()
                .filter(|c| c.is(C::Name))
            {
                let import_name = c.sym();
                let alias = c.child_sym(C::Alias).unwrap_or(0);
                if import_name != ws && alias != ws {
                    continue;
                }
                for req in reqs {
                    if req.fi == fi && req.node == ni as u32 {
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
