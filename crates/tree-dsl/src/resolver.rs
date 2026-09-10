//! Cross-file resolver. Reads __import/__source/__name synthetics.
//! Language-agnostic: source paths are already canonical /-separated
//! after YAML pipe transforms. Extension stripping and index-file
//! collapsing are driven by languages.yaml via SupportLang.

use rustc_hash::FxHashMap;

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
    let k_import = lang.kind_id("__import");
    let k_source = lang.kind_id("__source");
    let k_source_path = lang.kind_id("__source_path");
    let k_name = lang.kind_id("__name");
    let k_alias = lang.kind_id("__alias");
    let k_deftype = lang.kind_id("__deftype");
    let name_f = lang.fields.lookup("name") as u16;
    let left_f = lang.fields.lookup("left") as u16;

    let index_names = support_lang.index_names();

    // Build file index: stem → file_index
    let mut file_index: FxHashMap<String, usize> = FxHashMap::default();
    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.nodes[0].sym).to_string();
        let stem = support_lang.strip_extension(&path);
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

    // Build visible-names per file: name_sym → node_index
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
            let name_sym = tree
                .child_by_field(i as u32, name_f)
                .or_else(|| tree.child_by_field(i as u32, left_f))
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if name_sym != 0 {
                names.insert(name_sym, i as u32);
            }
        }
        visible.push(names);
    }

    let mut reqs: Vec<ImportReq> = Vec::new();
    let mut cross_edges = Vec::new();

    for (fi, tree) in trees.iter().enumerate() {
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.kind != k_import {
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
                // No file matches the source path. Try submodule resolution:
                // for each __name child, check if {source_path}/{name} exists
                // as a file (handles implicit namespace packages).
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
                        cross_edges.push(Edge {
                            from: NodeRef {
                                tree: fi as u32,
                                node: i as u32,
                            },
                            to: NodeRef {
                                tree: sub_fi as u32,
                                node: 0,
                            },
                            kind: EdgeKind::Imports,
                        });
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

    let mut reexports: FxHashMap<(usize, u32), (usize, u32)> = FxHashMap::default();
    let mut ambiguous: rustc_hash::FxHashSet<(usize, u32)> = rustc_hash::FxHashSet::default();
    for _round in 0..3 {
        let mut new_exports = Vec::new();
        for req in &reqs {
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

    // Rewrite __source to the resolved module path in the language's native separator
    let fqn_sep = support_lang.fqn_separator();
    for req in &reqs {
        let resolved = req.target_path.replace('/', fqn_sep);
        let resolved_sym = lang.syms.get(&resolved);
        let src_node = trees[req.fi]
            .children(req.node)
            .find(|&c| trees[req.fi].kind(c) == k_source);
        if let Some(sn) = src_node {
            trees[req.fi].nodes[sn as usize].sym = resolved_sym;
        }
    }

    // Build E_IMPORTS cross-edges with import-chain following.
    for req in &reqs {
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
                    cross_edges.push(Edge {
                        from: NodeRef {
                            tree: fi as u32,
                            node: i,
                        },
                        to: NodeRef {
                            tree: real_fi as u32,
                            node: real_node,
                        },
                        kind: EdgeKind::Imports,
                    });
                }
                continue;
            }

            if ambiguous.contains(&(tfi, name_sym)) {
                continue;
            }

            if let Some(&(re_fi, re_node)) = reexports.get(&(tfi, name_sym)) {
                cross_edges.push(Edge {
                    from: NodeRef {
                        tree: fi as u32,
                        node: i,
                    },
                    to: NodeRef {
                        tree: re_fi as u32,
                        node: re_node,
                    },
                    kind: EdgeKind::Imports,
                });
                continue;
            }
            if let Some(&def_node) = visible[tfi].get(&name_sym) {
                cross_edges.push(Edge {
                    from: NodeRef {
                        tree: fi as u32,
                        node: i,
                    },
                    to: NodeRef {
                        tree: tfi as u32,
                        node: def_node,
                    },
                    kind: EdgeKind::Imports,
                });
                continue;
            }

            // Import-chain following: search target file's imports for one
            // that re-exports this name, then follow the chain.
            let results = follow_import_chain(
                trees, lang, &reqs, &visible, name_sym, tfi, k_import, k_name, k_alias, name_f,
                left_f,
            );
            if results.len() == 1 {
                let (def_fi, def_node) = results[0];
                cross_edges.push(Edge {
                    from: NodeRef {
                        tree: fi as u32,
                        node: i,
                    },
                    to: NodeRef {
                        tree: def_fi as u32,
                        node: def_node,
                    },
                    kind: EdgeKind::Imports,
                });
                continue;
            }

            // Submodule fallback (index-file target or namespace package)
            let target_stem =
                support_lang.strip_extension(lang.syms.resolve(trees[tfi].nodes[0].sym));
            if let Some(target_dir) = index_names
                .iter()
                .find_map(|idx| target_stem.strip_suffix(&format!("/{idx}")))
            {
                let submod_path = format!("{target_dir}/{name_str}");
                if let Some(&sub_fi) = file_index.get(&submod_path) {
                    cross_edges.push(Edge {
                        from: NodeRef {
                            tree: fi as u32,
                            node: i,
                        },
                        to: NodeRef {
                            tree: sub_fi as u32,
                            node: 0,
                        },
                        kind: EdgeKind::Imports,
                    });
                }
            }
        }
    }

    // E_CALLS cross-edges: follow intra-file E_IMPORTS → cross-file target
    let k_call = lang.kind_id("__call");
    let k_member = lang.kind_id("__member");
    let callee_f = lang.fields.lookup("callee") as u16;
    let member_f = lang.fields.lookup("member") as u16;
    let object_f = lang.fields.lookup("object") as u16;

    // Module-level import member access: import X; X.func()
    // Also handles submodule imports: from pkg import mod; mod.func()
    let mut module_call_edges = Vec::new();
    for req in &reqs {
        let fi = req.fi;
        let import_node = req.node;

        // Collect all target files for this import: the primary target
        // plus any submodule files resolved via cross-edges.
        let mut target_files = vec![req.target_fi];
        for ce in &cross_edges {
            if ce.from.tree as usize == fi
                && ce.from.node == import_node
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
                let callee_node = trees[fi].child_by_field(d, callee_f);
                if let Some(cn) = callee_node
                    && trees[fi].kind(cn) == k_member
                {
                    let member_sym = trees[fi]
                        .child_by_field(cn, member_f)
                        .map(|c| trees[fi].sym(c))
                        .unwrap_or(0);
                    if member_sym == 0 {
                        continue;
                    }
                    for &tfi in &target_files {
                        if let Some(&def_node) = visible[tfi].get(&member_sym) {
                            module_call_edges.push(Edge {
                                from: NodeRef {
                                    tree: fi as u32,
                                    node: caller,
                                },
                                to: NodeRef {
                                    tree: tfi as u32,
                                    node: def_node,
                                },
                                kind: EdgeKind::Calls,
                            });
                            break;
                        }
                    }
                }
            }
        }
    }

    let mut call_edges = Vec::new();
    for ce in &cross_edges {
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
            let matches_import = edge_import == ce.from.node
                || trees[ce.from.tree as usize].nodes[edge_import as usize].parent == ce.from.node;
            if !matches_import {
                continue;
            }

            let is_wildcard = trees[ce.from.tree as usize]
                .children(ce.from.node)
                .any(|c| {
                    trees[ce.from.tree as usize].kind(c) == k_name
                        && lang.syms.resolve(trees[ce.from.tree as usize].sym(c)) == "*"
                });

            if is_wildcard && target_name != 0 {
                let caller_node = edge.from.node;
                let mut matched = false;
                for d in trees[ce.from.tree as usize].descendants(caller_node) {
                    if trees[ce.from.tree as usize].kind(d) == k_call {
                        let callee = trees[ce.from.tree as usize]
                            .child_by_field(d, callee_f)
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
                from: NodeRef {
                    tree: ce.from.tree,
                    node: edge.from.node,
                },
                to: ce.to,
                kind: EdgeKind::Calls,
            });
        }
    }

    // Cross-file return type resolution
    let k_binding = lang.kind_id("__binding");
    let right_f = lang.fields.lookup("right") as u16;
    let ret_type_f = lang.fields.lookup("return_type") as u16;

    let mut type_edges = Vec::new();
    for ce in &call_edges {
        if ce.kind != EdgeKind::Calls {
            continue;
        }
        let caller_fi = ce.from.tree as usize;
        let caller_node = ce.from.node;
        let target_fi = ce.to.tree as usize;
        let target_node = ce.to.node;

        let return_type_sym = if ret_type_f != 0 {
            trees[target_fi]
                .child_by_field(target_node, ret_type_f)
                .map(|r| trees[target_fi].sym(r))
                .filter(|&s| s != 0)
        } else {
            None
        }
        .or_else(|| {
            let return_k = lang.kinds.lookup("return_statement") as u16;
            if return_k == 0 {
                return None;
            }
            for d in trees[target_fi].descendants(target_node) {
                if trees[target_fi].nodes[d as usize].kind == return_k {
                    for c in trees[target_fi].children(d) {
                        if trees[target_fi].kind(c) == k_call {
                            return trees[target_fi]
                                .child_by_field(c, callee_f)
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
            for ce2 in &cross_edges {
                if ce2.from.tree as usize == target_fi && ce2.kind == EdgeKind::Imports {
                    let def_name = trees[ce2.to.tree as usize]
                        .child_by_field(ce2.to.node, name_f)
                        .or_else(|| trees[ce2.to.tree as usize].child_by_field(ce2.to.node, left_f))
                        .map(|c| trees[ce2.to.tree as usize].sym(c))
                        .unwrap_or(0);
                    if def_name == ret_sym {
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
            .child_by_field(target_node, name_f)
            .or_else(|| trees[target_fi].child_by_field(target_node, left_f))
            .map(|c| trees[target_fi].sym(c))
            .unwrap_or(0);

        let mut bound_vars: Vec<u32> = Vec::new();
        for d in tree.descendants(caller_node) {
            if !tree.children(d).any(|c| tree.kind(c) == k_binding) {
                continue;
            }
            let rhs = tree.child_by_field(d, right_f);
            if let Some(rn) = rhs
                && tree.kind(rn) == k_call
            {
                let callee = tree
                    .child_by_field(rn, callee_f)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                if callee == target_name_sym {
                    let lhs = tree
                        .child_by_field(d, left_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if lhs != 0 {
                        bound_vars.push(lhs);
                    }
                }
            }
        }

        for d in tree.descendants(caller_node) {
            if tree.kind(d) == k_call {
                let callee_n = tree.child_by_field(d, callee_f);
                if let Some(cn) = callee_n
                    && tree.kind(cn) == k_member
                {
                    let obj_sym = tree
                        .child_by_field(cn, object_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if !bound_vars.contains(&obj_sym) {
                        continue;
                    }
                    let mem_sym = tree
                        .child_by_field(cn, member_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if mem_sym != 0 {
                        let mut found = false;
                        for cd in trees[type_fi].descendants(type_node) {
                            if trees[type_fi].kind(cd) == k_deftype {
                                let mn = trees[type_fi].nodes[cd as usize].parent;
                                if mn != NONE && mn != type_node {
                                    let mname = trees[type_fi]
                                        .child_by_field(mn, name_f)
                                        .or_else(|| trees[type_fi].child_by_field(mn, left_f))
                                        .map(|c| trees[type_fi].sym(c))
                                        .unwrap_or(0);
                                    if mname == mem_sym && !found {
                                        type_edges.push(Edge {
                                            from: NodeRef {
                                                tree: caller_fi as u32,
                                                node: caller_node,
                                            },
                                            to: NodeRef {
                                                tree: type_fi as u32,
                                                node: mn,
                                            },
                                            kind: EdgeKind::Calls,
                                        });
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

    cross_edges.extend(module_call_edges);
    cross_edges.extend(call_edges);
    cross_edges.extend(type_edges);
    ResolveResult { cross_edges }
}

/// Follow import chains to find the defining file for a name.
/// Returns a list of (file_index, node) matches. If >1, the name is ambiguous.
fn follow_import_chain(
    trees: &[Tree],
    lang: &Lang,
    reqs: &[ImportReq],
    visible: &[FxHashMap<u32, u32>],
    name_sym: u32,
    start_fi: usize,
    k_import: u16,
    k_name: u16,
    k_alias: u16,
    name_f: u16,
    left_f: u16,
) -> Vec<(usize, u32)> {
    let mut results: Vec<(usize, u32)> = Vec::new();
    let mut visited: Vec<(usize, u32)> = Vec::new();
    let mut stack: Vec<(usize, u32)> = vec![(start_fi, name_sym)];

    while let Some((fi, wanted_sym)) = stack.pop() {
        if visited.contains(&(fi, wanted_sym)) {
            continue;
        }
        visited.push((fi, wanted_sym));

        // Check if the wanted name is defined locally
        if let Some(&def_node) = visible[fi].get(&wanted_sym) {
            if !results.contains(&(fi, def_node)) {
                results.push((fi, def_node));
            }
            continue;
        }

        // Scan this file's imports for one that brings in the wanted name
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
                // Check alias: the import's __alias child matches the wanted name
                let alias_sym = tree
                    .children(c)
                    .find(|&gc| tree.kind(gc) == k_alias)
                    .map(|gc| tree.sym(gc))
                    .unwrap_or(0);

                let matches = import_name == wanted_sym || alias_sym == wanted_sym;
                if !matches {
                    continue;
                }

                // Find the resolved target of this import via reqs
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
    // Handle trailing ".." or "." without slash
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
