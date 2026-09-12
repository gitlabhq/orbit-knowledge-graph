//! Cross-file resolver. Reads __import/__source/__name synthetics.
//! Language-agnostic: source paths are already canonical /-separated
//! after YAML pipe transforms. Extension stripping and index-file
//! collapsing are driven by languages.yaml via SupportLang.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::grammar::SupportLang;
use crate::lang::Lang;
use crate::tree::{Edge, EdgeKind, NONE, NodeRef, Tree};

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
    let k_import = lang.lookup_kind("__import");
    let k_import_type = lang.lookup_kind("__import_type");
    let k_source_path = lang.lookup_kind("__source_path");
    let k_name = lang.lookup_kind("__name");
    let k_alias = lang.lookup_kind("__alias");
    let k_deftype = lang.lookup_kind("__deftype");
    let k_call = lang.lookup_kind("__call");
    let k_callee = lang.lookup_kind("__callee");
    let k_member = lang.lookup_kind("__member");
    let k_object = lang.lookup_kind("__object");
    let k_binding = lang.lookup_kind("__binding");
    let k_rhs = lang.lookup_kind("__rhs");
    let k_defname = lang.lookup_kind("__defname");
    let k_return_type = lang.lookup_kind("__return_type");
    let name_f = lang.fields.lookup("name") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let right_f = lang.fields.lookup("right") as u16;
    let _callee_f = lang.fields.lookup("callee") as u16;
    let _member_f = lang.fields.lookup("member") as u16;
    let _object_f = lang.fields.lookup("object") as u16;
    let ret_type_f = lang.fields.lookup("return_type") as u16;
    let return_k = lang.lookup_kind("__return");
    let return_k_legacy = lang.kinds.lookup("return_statement") as u16;
    let index_names = support_lang.index_names();

    let file_index = build_file_index(trees, lang, support_lang, index_names);
    let mut visible = build_visible_names(trees, k_deftype, name_f, left_f, k_defname);
    let (reqs, mut cross_edges) = gather_imports(
        trees,
        lang,
        k_import,
        k_import_type,
        k_source_path,
        k_name,
        &file_index,
        lookup_prefixes,
        external,
    );
    let (reexports, ambiguous) = propagate_reexports(
        trees,
        lang,
        &reqs,
        &mut visible,
        support_lang,
        index_names,
        k_name,
    );

    // Write resolved target paths back to __source_path so downstream
    // consumers (datasets) can convert to display format.
    for req in &reqs {
        let resolved_sym = lang.syms.intern(&req.target_path);
        let sp_node = trees[req.fi]
            .children(req.node)
            .find(|&c| trees[req.fi].kind(c) == k_source_path);
        if let Some(sn) = sp_node {
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
        k_import,
        k_name,
        k_alias,
    );
    cross_edges.extend(import_edges);
    let (module_call_edges, call_edges) = build_call_edges(
        trees,
        lang,
        &cross_edges,
        &reqs,
        &visible,
        k_call,
        k_callee,
        k_member,
        k_name,
    );
    let type_edges = build_type_edges(
        trees,
        &call_edges,
        &cross_edges,
        &visible,
        name_f,
        left_f,
        right_f,
        k_deftype,
        k_call,
        k_callee,
        k_member,
        k_object,
        k_binding,
        k_rhs,
        ret_type_f,
        return_k,
        k_defname,
        k_return_type,
        return_k_legacy,
    );
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

fn build_visible_names(
    trees: &[Tree],
    k_deftype: u16,
    name_f: u16,
    left_f: u16,
    defname_k: u16,
) -> Vec<FxHashMap<u32, u32>> {
    let mut visible: Vec<FxHashMap<u32, u32>> = Vec::with_capacity(trees.len());
    for tree in trees.iter() {
        let mut names: FxHashMap<u32, u32> = FxHashMap::default();
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.dead {
                continue;
            }
            if !tree.children(i as u32).any(|c| tree.kind(c) == k_deftype) {
                continue;
            }
            let ns = name_sym(tree, i as u32, name_f, left_f, defname_k);
            if ns != 0 {
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
    k_import: u16,
    k_import_type: u16,
    k_source_path: u16,
    k_name: u16,
    file_index: &FxHashMap<String, usize>,
    lookup_prefixes: &[String],
    external: &[String],
) -> (Vec<ImportReq>, Vec<Edge>) {
    let mut reqs: Vec<ImportReq> = Vec::new();
    let mut cross_edges = Vec::new();

    for (fi, tree) in trees.iter().enumerate() {
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.kind != k_import && n.kind != k_import_type {
                continue;
            }
            let source_sym = tree
                .children(i as u32)
                .find(|&c| tree.kind(c) == k_source_path)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
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
                // Submodule resolution: for each __name child, check if
                // {source_path}/{name} exists as a file (implicit namespace packages).
                let tree = &trees[fi];
                for c in tree.children(i as u32) {
                    if tree.kind(c) != k_name || tree.sym(c) == 0 {
                        continue;
                    }
                    let name_str = lang.syms.resolve(tree.sym(c));
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
    k_name: u16,
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
            for c in tree.children(req.node) {
                if tree.kind(c) != k_name {
                    continue;
                }
                let name_sym = tree.sym(c);
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
    k_import: u16,
    k_name: u16,
    k_alias: u16,
) -> Vec<Edge> {
    let mut edges = Vec::new();
    for req in reqs {
        let fi = req.fi;
        let i = req.node;
        let tfi = req.target_fi;
        let tree = &trees[fi];
        for c in tree.children(i) {
            if tree.kind(c) != k_name {
                continue;
            }
            let name_sym = tree.sym(c);
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
                    edges.push(Edge::new(fi, c, real_fi, real_node, EdgeKind::Imports));
                }
                continue;
            }

            if ambiguous.contains(&(tfi, name_sym)) {
                continue;
            }

            if let Some(&(re_fi, re_node)) = reexports.get(&(tfi, name_sym)) {
                edges.push(Edge::new(fi, c, re_fi, re_node, EdgeKind::Imports));
                continue;
            }
            if let Some(&def_node) = visible[tfi].get(&name_sym) {
                edges.push(Edge::new(fi, c, tfi, def_node, EdgeKind::Imports));
                continue;
            }

            let results = follow_import_chain(
                trees, reqs, visible, name_sym, tfi, k_import, k_name, k_alias,
            );
            if results.len() == 1 {
                let (def_fi, def_node) = results[0];
                edges.push(Edge::new(fi, c, def_fi, def_node, EdgeKind::Imports));
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
                    edges.push(Edge::new(fi, c, sub_fi, 0, EdgeKind::Imports));
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
    k_call: u16,
    k_callee: u16,
    k_member: u16,
    _k_name: u16,
) -> (Vec<Edge>, Vec<Edge>) {
    let mut module_call_edges = Vec::new();
    for req in reqs {
        let fi = req.fi;
        let import_node = req.node;

        let mut target_files = vec![req.target_fi];
        for ce in cross_edges {
            if ce.from.tree as usize == fi
                && (ce.from.node == import_node
                    || trees[fi].nodes[ce.from.node as usize].parent == import_node)
                && ce.kind == EdgeKind::Imports
            {
                if !target_files.contains(&(ce.to.tree as usize)) {
                    target_files.push(ce.to.tree as usize);
                }
            }
        }

        for edge in &trees[fi].edges {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let edge_target = edge.to.node;
            if edge_target != import_node
                && trees[fi].nodes[edge_target as usize].parent != import_node
            {
                continue;
            }

            let caller = edge.from.node;
            for d in trees[fi].descendants(caller) {
                if trees[fi].kind(d) != k_call {
                    continue;
                }
                let callee_node = trees[fi]
                    .children(d)
                    .find(|&c| trees[fi].kind(c) == k_callee);
                if let Some(cn) = callee_node
                    && let Some(mn) = trees[fi]
                        .children(cn)
                        .find(|&c| trees[fi].kind(c) == k_member)
                {
                    let member_sym = trees[fi].sym(mn);
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

        for edge in &trees[ce.from.tree as usize].edges {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let edge_import = edge.to.node;
            let import_parent = trees[ce.from.tree as usize].nodes[ce.from.node as usize].parent;
            let matches_import = edge_import == ce.from.node
                || edge_import == import_parent
                || trees[ce.from.tree as usize].nodes[edge_import as usize].parent == import_parent;
            if !matches_import {
                continue;
            }

            let is_wildcard = lang
                .syms
                .resolve(trees[ce.from.tree as usize].sym(ce.from.node))
                == "*";

            if is_wildcard && target_name != 0 {
                let caller_node = edge.from.node;
                let mut matched = false;
                for d in trees[ce.from.tree as usize].descendants(caller_node) {
                    if trees[ce.from.tree as usize].kind(d) == k_call {
                        let callee = trees[ce.from.tree as usize]
                            .children(d)
                            .find(|&c| trees[ce.from.tree as usize].kind(c) == k_callee)
                            .map(|c| trees[ce.from.tree as usize].sym(c))
                            .unwrap_or(0);
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

#[allow(clippy::too_many_arguments)]
fn build_type_edges(
    trees: &[Tree],
    call_edges: &[Edge],
    cross_edges: &[Edge],
    visible: &[FxHashMap<u32, u32>],
    name_f: u16,
    left_f: u16,
    right_f: u16,
    k_deftype: u16,
    k_call: u16,
    k_callee: u16,
    k_member: u16,
    k_object: u16,
    k_binding: u16,
    k_rhs: u16,
    ret_type_f: u16,
    return_k: u16,
    k_defname: u16,
    k_return_type: u16,
    return_k_legacy: u16,
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

        let return_type_sym = if k_return_type != 0 {
            trees[target_fi]
                .children(target_node)
                .find(|&c| trees[target_fi].kind(c) == k_return_type)
                .map(|c| trees[target_fi].sym(c))
                .filter(|&s| s != 0)
        } else {
            None
        }
        .or_else(|| {
            if ret_type_f != 0 {
                trees[target_fi]
                    .child_by_field(target_node, ret_type_f)
                    .map(|r| trees[target_fi].sym(r))
                    .filter(|&s| s != 0)
            } else {
                None
            }
        })
        .or_else(|| {
            if return_k == 0 && return_k_legacy == 0 {
                return None;
            }
            for d in trees[target_fi].descendants(target_node) {
                let dk = trees[target_fi].nodes[d as usize].kind;
                if (return_k != 0 && dk == return_k)
                    || (return_k_legacy != 0 && dk == return_k_legacy)
                {
                    for c in trees[target_fi].children(d) {
                        if trees[target_fi].kind(c) == k_call {
                            return trees[target_fi]
                                .children(c)
                                .find(|&c2| trees[target_fi].kind(c2) == k_callee)
                                .map(|c2| trees[target_fi].sym(c2))
                                .filter(|&s| s != 0);
                        }
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
                    let dn = name_sym(
                        &trees[ce2.to.tree as usize],
                        ce2.to.node,
                        name_f,
                        left_f,
                        k_defname,
                    );
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
        let target_name_sym = name_sym(&trees[target_fi], target_node, name_f, left_f, k_defname);

        let mut bound_vars: Vec<u32> = Vec::new();
        for d in tree.descendants(caller_node) {
            if tree.kind(d) != k_binding && !tree.children(d).any(|c| tree.kind(c) == k_binding) {
                continue;
            }
            let lhs = if tree.kind(d) == k_binding && tree.sym(d) != 0 {
                tree.sym(d)
            } else {
                tree.child_by_field(d, left_f)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0)
            };
            let rhs_call = tree
                .children(d)
                .find(|&c| tree.kind(c) == k_rhs)
                .and_then(|rhs| tree.children(rhs).find(|&c| tree.kind(c) == k_call))
                .or_else(|| {
                    tree.child_by_field(d, right_f)
                        .filter(|&r| tree.kind(r) == k_call)
                });
            if let Some(rn) = rhs_call {
                let callee = tree
                    .children(rn)
                    .find(|&c| tree.kind(c) == k_callee)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                if callee == target_name_sym && lhs != 0 {
                    bound_vars.push(lhs);
                }
            }
        }

        for d in tree.descendants(caller_node) {
            if tree.kind(d) == k_call {
                let callee_n = tree.children(d).find(|&c| tree.kind(c) == k_callee);
                let member_n =
                    callee_n.and_then(|cn| tree.children(cn).find(|&c| tree.kind(c) == k_member));
                if let Some(mn) = member_n {
                    let obj_sym = tree
                        .children(mn)
                        .find(|&c| tree.kind(c) == k_object)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if !bound_vars.contains(&obj_sym) {
                        continue;
                    }
                    let mem_sym = tree.sym(mn);
                    if mem_sym != 0 {
                        let mut found = false;
                        for cd in trees[type_fi].descendants(type_node) {
                            if trees[type_fi].kind(cd) == k_deftype {
                                let mn = trees[type_fi].nodes[cd as usize].parent;
                                if mn != NONE && mn != type_node {
                                    let mname =
                                        name_sym(&trees[type_fi], mn, name_f, left_f, k_defname);
                                    if mname == mem_sym && !found {
                                        type_edges.push(Edge::new(
                                            caller_fi,
                                            caller_node,
                                            type_fi,
                                            mn,
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

/// Follow import chains to find the defining file for a name.
/// Returns a list of (file_index, node) matches. If >1, the name is ambiguous.
fn follow_import_chain(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &[FxHashMap<u32, u32>],
    name_sym: u32,
    start_fi: usize,
    k_import: u16,
    k_name: u16,
    k_alias: u16,
) -> Vec<(usize, u32)> {
    let mut results: Vec<(usize, u32)> = Vec::new();
    let mut visited: Vec<(usize, u32)> = Vec::new();
    let mut stack: Vec<(usize, u32)> = vec![(start_fi, name_sym)];

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
            if n.kind != k_import {
                continue;
            }
            for c in tree.children(ni as u32) {
                if tree.kind(c) != k_name {
                    continue;
                }
                let import_name = tree.sym(c);
                let alias_sym = tree
                    .children(c)
                    .find(|&gc| tree.kind(gc) == k_alias)
                    .map(|gc| tree.sym(gc))
                    .unwrap_or(0);

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

fn name_sym(tree: &Tree, node: u32, name_f: u16, left_f: u16, defname_k: u16) -> u32 {
    let s = tree
        .child_by_field(node, name_f)
        .or_else(|| tree.child_by_field(node, left_f))
        .map(|c| tree.sym(c))
        .unwrap_or(0);
    if s != 0 {
        return s;
    }
    if defname_k != 0 {
        tree.children(node)
            .find(|&c| tree.kind(c) == defname_k)
            .map(|c| tree.sym(c))
            .unwrap_or(0)
    } else {
        0
    }
}

/// Resolve a relative path ("./foo" or "../bar") against the current file's directory.
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
    } else {
        if parts.is_empty() {
            rest.to_string()
        } else {
            format!("{}/{rest}", parts.join("/"))
        }
    }
}
