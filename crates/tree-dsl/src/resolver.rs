//! Cross-file resolver. Reads canonical __import/__source/__name nodes.
//! Language-agnostic: source paths are already canonical /-separated
//! after YAML pipe transforms. Extension stripping and index-file
//! collapsing are driven by languages.yaml via SupportLang.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::grammar::SupportLang;
use crate::lang::Lang;
use crate::tree::{Edge, EdgeKind, NodeRef, Tree};

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
        let sp = trees[req.fi]
            .nr(req.node)
            .child(C::SourcePath)
            .map(|n| n.index());
        if let Some(sn) = sp {
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
    let mut file_index: FxHashMap<String, usize> = FxHashMap::default();
    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.nodes[0].sym).to_string();
        let file_lang = SupportLang::from_path(&path).unwrap_or(support_lang);
        let stem = file_lang.strip_extension(&path);
        file_index.insert(path.clone(), fi);
        file_index.insert(stem.to_string(), fi);
        for idx_name in index_names {
            let suffix = format!("/{idx_name}");
            if stem.ends_with(&suffix) {
                let pkg = &stem[..stem.len() - suffix.len()];
                if !pkg.is_empty() {
                    file_index.insert(pkg.to_string(), fi);
                }
            } else if stem == idx_name.as_str() {
                file_index.insert(String::new(), fi);
            }
        }
    }
    file_index
}

fn build_visible_names(trees: &[Tree]) -> Vec<FxHashMap<u32, u32>> {
    let mut visible: Vec<FxHashMap<u32, u32>> = Vec::with_capacity(trees.len());
    for tree in trees.iter() {
        let mut names: FxHashMap<u32, u32> = FxHashMap::default();
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.dead {
                continue;
            }
            let nr = tree.nr(i as u32);
            if !nr.has(C::DefType) {
                continue;
            }
            if let Some(ns) = nr.child_sym(C::DefName) {
                names.insert(ns, i as u32);
            }
        }
        visible.push(names);
    }
    visible
}

fn gather_imports(
    trees: &[Tree],
    lang: &Lang,
    file_index: &FxHashMap<String, usize>,
    lookup_prefixes: &[String],
    external: &[String],
) -> (Vec<ImportReq>, Vec<Edge>) {
    let mut reqs: Vec<ImportReq> = Vec::new();
    let mut cross_edges = Vec::new();

    for (fi, tree) in trees.iter().enumerate() {
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.kind != C::Import && n.kind != C::ImportType {
                continue;
            }
            let source_sym = tree.nr(i as u32).child_sym(C::SourcePath).unwrap_or(0);
            if source_sym == 0 {
                continue;
            }
            let source_str = lang.syms.resolve(source_sym).to_string();

            let root_module = source_str.split('/').next().unwrap_or(&source_str);
            if external.iter().any(|e| e == root_module) {
                continue;
            }

            let target_path = if source_str.starts_with("./") || source_str.starts_with("../") {
                let current = lang.syms.resolve(trees[fi].nodes[0].sym);
                resolve_relative(current, &source_str)
            } else {
                source_str.clone()
            };

            let tfi = file_index.get(&target_path).copied().or_else(|| {
                for prefix in lookup_prefixes {
                    let candidate = if prefix.is_empty() {
                        target_path.clone()
                    } else {
                        format!("{prefix}/{target_path}")
                    };
                    if let Some(&tfi) = file_index.get(&candidate) {
                        return Some(tfi);
                    }
                }
                None
            });

            if let Some(tfi) = tfi {
                reqs.push(ImportReq {
                    fi,
                    node: i as u32,
                    target_fi: tfi,
                    target_path,
                });
            } else {
                for c in tree.nr(i as u32).children() {
                    if !c.is(C::Name) || c.sym() == 0 {
                        continue;
                    }
                    let name_str = lang.syms.resolve(c.sym());
                    let submod = format!("{target_path}/{name_str}");
                    let sub_fi = file_index.get(&submod).copied().or_else(|| {
                        for prefix in lookup_prefixes.iter() {
                            let candidate = if prefix.is_empty() {
                                submod.clone()
                            } else {
                                format!("{prefix}/{submod}")
                            };
                            if let Some(&fi) = file_index.get(&candidate) {
                                return Some(fi);
                            }
                        }
                        None
                    });
                    if let Some(sub_fi) = sub_fi {
                        cross_edges.push(Edge::new(fi, i as u32, sub_fi, 0, EdgeKind::Imports));
                        reqs.push(ImportReq {
                            fi,
                            node: i as u32,
                            target_fi: sub_fi,
                            target_path: submod.clone(),
                        });
                    }
                }
            }
        }
    }

    (reqs, cross_edges)
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
            let path = lang.syms.resolve(trees[req.fi].nodes[0].sym);
            let stem = support_lang.strip_extension(path);
            let is_index = index_names
                .iter()
                .any(|idx| stem.ends_with(&format!("/{idx}")) || stem == idx.as_str());
            if !is_index {
                continue;
            }
            let tree = &trees[req.fi];
            for c in tree.nr(req.node).children() {
                if !c.is(C::Name) {
                    continue;
                }
                let name_sym = c.sym();
                if name_sym == 0 {
                    continue;
                }
                let name_str = lang.syms.resolve(name_sym);
                if name_str == "*" {
                    for (&def_sym, &def_node) in &visible[req.target_fi] {
                        if !visible[req.fi].contains_key(&def_sym) {
                            new_exports.push((req.fi, def_sym, req.target_fi, def_node));
                        }
                    }
                    let target_reexports: Vec<_> = reexports
                        .iter()
                        .filter(|((fi, _), _)| *fi == req.target_fi)
                        .map(|((_, sym), (tfi, tn))| (*sym, *tfi, *tn))
                        .collect();
                    for (sym, tfi, tn) in target_reexports {
                        if !visible[req.fi].contains_key(&sym) {
                            new_exports.push((req.fi, sym, tfi, tn));
                        }
                    }
                } else if let Some(&def_node) = visible[req.target_fi].get(&name_sym) {
                    if !visible[req.fi].contains_key(&name_sym) {
                        new_exports.push((req.fi, name_sym, req.target_fi, def_node));
                    }
                } else if let Some(&(tfi, tn)) = reexports.get(&(req.target_fi, name_sym))
                    && !visible[req.fi].contains_key(&name_sym)
                {
                    new_exports.push((req.fi, name_sym, tfi, tn));
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        for (fi, name_sym, tfi, tn) in new_exports {
            if let Some(&existing) = visible[fi].get(&name_sym) {
                let (existing_fi, existing_node) = reexports
                    .get(&(fi, name_sym))
                    .copied()
                    .unwrap_or((fi, existing));
                if existing_fi != tfi || existing_node != tn {
                    ambiguous.insert((fi, name_sym));
                }
                continue;
            }
            visible[fi].insert(name_sym, tn);
            reexports.insert((fi, name_sym), (tfi, tn));
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
        let fi = req.fi;
        let i = req.node;
        let tfi = req.target_fi;
        let tree = &trees[fi];
        for c in tree.nr(i).children() {
            if !c.is(C::Name) {
                continue;
            }
            let name_sym = c.sym();
            if name_sym == 0 {
                continue;
            }
            let name_str = lang.syms.resolve(name_sym);

            if name_str == "*" {
                for (&def_name, &def_node) in &visible[tfi] {
                    if ambiguous.contains(&(tfi, def_name)) {
                        continue;
                    }
                    let (real_fi, real_node) = reexports
                        .get(&(tfi, def_name))
                        .copied()
                        .unwrap_or((tfi, def_node));
                    edges.push(Edge::new(
                        fi,
                        c.index(),
                        real_fi,
                        real_node,
                        EdgeKind::Imports,
                    ));
                }
                continue;
            }

            if ambiguous.contains(&(tfi, name_sym)) {
                continue;
            }

            if let Some(&(re_fi, re_node)) = reexports.get(&(tfi, name_sym)) {
                edges.push(Edge::new(fi, c.index(), re_fi, re_node, EdgeKind::Imports));
                continue;
            }
            if let Some(&def_node) = visible[tfi].get(&name_sym) {
                edges.push(Edge::new(fi, c.index(), tfi, def_node, EdgeKind::Imports));
                continue;
            }

            let results = follow_import_chain(trees, reqs, visible, name_sym, tfi);
            if results.len() == 1 {
                let (def_fi, def_node) = results[0];
                edges.push(Edge::new(
                    fi,
                    c.index(),
                    def_fi,
                    def_node,
                    EdgeKind::Imports,
                ));
                continue;
            }

            let target_stem =
                support_lang.strip_extension(lang.syms.resolve(trees[tfi].nodes[0].sym));
            if let Some(target_dir) = index_names
                .iter()
                .find_map(|idx| target_stem.strip_suffix(&format!("/{idx}")))
            {
                let submod_path = format!("{target_dir}/{name_str}");
                if let Some(&sub_fi) = file_index.get(&submod_path) {
                    edges.push(Edge::new(fi, c.index(), sub_fi, 0, EdgeKind::Imports));
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
    let mut module_call_edges = Vec::new();
    for req in reqs {
        let fi = req.fi;
        let import_node = req.node;
        let tree = &trees[fi];

        let mut target_files = vec![req.target_fi];
        for ce in cross_edges {
            if ce.from.tree as usize == fi
                && (ce.from.node == import_node
                    || tree.nodes[ce.from.node as usize].parent == import_node)
                && ce.kind == EdgeKind::Imports
            {
                if !target_files.contains(&(ce.to.tree as usize)) {
                    target_files.push(ce.to.tree as usize);
                }
            }
        }

        for edge in tree.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let edge_target = edge.to.node;
            if edge_target != import_node && tree.nodes[edge_target as usize].parent != import_node
            {
                continue;
            }

            let caller = edge.from.node;
            for d in tree.nr(caller).descendants() {
                if !d.is(C::Call) {
                    continue;
                }
                if let Some(cn) = d.child(C::Callee)
                    && let Some(mn) = cn.child(C::Member)
                {
                    let member_sym = mn.sym();
                    if member_sym == 0 {
                        continue;
                    }
                    for &tfi in &target_files {
                        if let Some(&def_node) = visible[tfi].get(&member_sym) {
                            module_call_edges.push(Edge::new(
                                fi,
                                caller,
                                tfi,
                                def_node,
                                EdgeKind::Calls,
                            ));
                            break;
                        }
                    }
                }
            }
        }
    }

    let mut call_edges = Vec::new();
    for ce in cross_edges {
        if ce.kind != EdgeKind::Imports {
            continue;
        }
        let target_name = visible[ce.to.tree as usize]
            .iter()
            .find(|(_, node)| **node == ce.to.node)
            .map(|(sym, _)| *sym)
            .unwrap_or(0);

        let from_tree = &trees[ce.from.tree as usize];
        for edge in from_tree.edges().iter() {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let edge_import = edge.to.node;
            let import_parent = from_tree.nodes[ce.from.node as usize].parent;
            let matches_import = edge_import == ce.from.node
                || edge_import == import_parent
                || from_tree.nodes[edge_import as usize].parent == import_parent;
            if !matches_import {
                continue;
            }

            let is_wildcard = lang.syms.resolve(from_tree.sym(ce.from.node)) == "*";

            if is_wildcard && target_name != 0 {
                let caller_node = edge.from.node;
                let mut matched = false;
                for d in from_tree.nr(caller_node).descendants() {
                    if d.is(C::Call) {
                        let callee = d.child_sym(C::Callee).unwrap_or(0);
                        if callee == target_name {
                            matched = true;
                            break;
                        }
                    }
                }
                if !matched {
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

    (module_call_edges, call_edges)
}

fn build_type_edges(
    trees: &[Tree],
    call_edges: &[Edge],
    cross_edges: &[Edge],
    visible: &[FxHashMap<u32, u32>],
) -> Vec<Edge> {
    let mut type_edges = Vec::new();
    for ce in call_edges {
        if ce.kind != EdgeKind::Calls {
            continue;
        }
        let caller_fi = ce.from.tree as usize;
        let caller_node = ce.from.node;
        let target_fi = ce.to.tree as usize;
        let target_node = ce.to.node;

        let target_nr = trees[target_fi].nr(target_node);
        let return_type_sym = target_nr.child_sym(C::ReturnType).or_else(|| {
            for d in target_nr.descendants() {
                if d.is(C::Return) {
                    if let Some(call) = d.child(C::Call) {
                        return call.child_sym(C::Callee);
                    }
                }
            }
            None
        });

        let Some(ret_sym) = return_type_sym else {
            continue;
        };

        let mut resolved_fi = None;
        let mut resolved_node = None;
        if let Some(&cn) = visible[target_fi].get(&ret_sym) {
            resolved_fi = Some(target_fi);
            resolved_node = Some(cn);
        }
        if resolved_fi.is_none() {
            for ce2 in cross_edges {
                if ce2.from.tree as usize == target_fi && ce2.kind == EdgeKind::Imports {
                    let dn = trees[ce2.to.tree as usize]
                        .nr(ce2.to.node)
                        .child_sym(C::DefName)
                        .unwrap_or(0);
                    if dn == ret_sym {
                        resolved_fi = Some(ce2.to.tree as usize);
                        resolved_node = Some(ce2.to.node);
                        break;
                    }
                }
            }
        }

        let (Some(type_fi), Some(type_node)) = (resolved_fi, resolved_node) else {
            continue;
        };

        let tree = &trees[caller_fi];
        let target_name_sym = trees[target_fi]
            .nr(target_node)
            .child_sym(C::DefName)
            .unwrap_or(0);

        let mut bound_vars: Vec<u32> = Vec::new();
        for d in tree.nr(caller_node).descendants() {
            if !d.is(C::Binding) {
                continue;
            }
            let lhs = d.sym();
            let rhs_call = d.child(C::Rhs).and_then(|rhs| rhs.child(C::Call));
            if let Some(rn) = rhs_call {
                let callee = rn.child_sym(C::Callee).unwrap_or(0);
                if callee == target_name_sym && lhs != 0 {
                    bound_vars.push(lhs);
                }
            }
        }

        for d in tree.nr(caller_node).descendants() {
            if !d.is(C::Call) {
                continue;
            }
            let member_n = d.child(C::Callee).and_then(|cn| cn.child(C::Member));
            if let Some(mn) = member_n {
                let obj_sym = mn.child_sym(C::Object).unwrap_or(0);
                if !bound_vars.contains(&obj_sym) {
                    continue;
                }
                let mem_sym = mn.sym();
                if mem_sym != 0 {
                    let mut found = false;
                    for cd in trees[type_fi].nr(type_node).descendants() {
                        if cd.is(C::DefType) {
                            if let Some(p) = cd.parent() {
                                if p.index() != type_node {
                                    let mname = p.child_sym(C::DefName).unwrap_or(0);
                                    if mname == mem_sym && !found {
                                        type_edges.push(Edge::new(
                                            caller_fi,
                                            caller_node,
                                            type_fi,
                                            p.index(),
                                            EdgeKind::Calls,
                                        ));
                                        found = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    type_edges
}

fn follow_import_chain(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &[FxHashMap<u32, u32>],
    wanted: u32,
    start_fi: usize,
) -> Vec<(usize, u32)> {
    let mut results: Vec<(usize, u32)> = Vec::new();
    let mut visited: Vec<(usize, u32)> = Vec::new();
    let mut stack: Vec<(usize, u32)> = vec![(start_fi, wanted)];

    while let Some((fi, wanted_sym)) = stack.pop() {
        if visited.contains(&(fi, wanted_sym)) {
            continue;
        }
        visited.push((fi, wanted_sym));

        if let Some(&def_node) = visible[fi].get(&wanted_sym) {
            if !results.contains(&(fi, def_node)) {
                results.push((fi, def_node));
            }
            continue;
        }

        let tree = &trees[fi];
        for (ni, n) in tree.nodes.iter().enumerate() {
            if n.kind != C::Import {
                continue;
            }
            for c in tree.nr(ni as u32).children() {
                if !c.is(C::Name) {
                    continue;
                }
                let import_name = c.sym();
                let alias_sym = c.child_sym(C::Alias).unwrap_or(0);

                let matches = import_name == wanted_sym || alias_sym == wanted_sym;
                if !matches {
                    continue;
                }

                let original_name = import_name;
                for req in reqs {
                    if req.fi == fi && req.node == ni as u32 {
                        stack.push((req.target_fi, original_name));
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
