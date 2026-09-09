//! Cross-file resolver. Reads __import/__source/__name synthetics.

use rustc_hash::FxHashMap;

use crate::lang::{E_IMPORTS, Lang, SYNTH};
use crate::tree::Tree;

#[derive(Clone, Copy)]
pub enum SourceRoot {
    ProjectRoot,
    PackageMarkerClimb,
}

pub struct CrossEdge {
    pub from_file: usize,
    pub from_node: u32,
    pub to_file: usize,
    pub to_node: u32,
    pub kind: u16,
}

pub struct ResolveResult {
    pub cross_edges: Vec<CrossEdge>,
}

/// Resolve cross-file imports by reading __import/__source/__name synthetics.
pub fn resolve(trees: &mut [Tree], lang: &mut Lang) -> ResolveResult {
    let k_import = lang.kinds.lookup("__import") as u16 | SYNTH;
    let k_source = lang.kinds.lookup("__source") as u16 | SYNTH;
    let k_name = lang.kinds.lookup("__name") as u16 | SYNTH;
    let k_deftype = lang.kinds.lookup("__deftype") as u16 | SYNTH;
    let name_f = lang.fields.lookup("name") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let right_f = lang.fields.lookup("right") as u16;

    // Build file index: path_stem → file_index
    let mut file_index: FxHashMap<String, usize> = FxHashMap::default();
    for (fi, tree) in trees.iter().enumerate() {
        let path = lang.syms.resolve(tree.nodes[0].sym).to_string();
        // Index by: full path, stem (no extension), and stem with __init__ collapsed
        let stem = strip_py_ext(&path);
        file_index.insert(path.clone(), fi);
        file_index.insert(stem.to_string(), fi);
        // Collapse __init__: mypackage/__init__ → mypackage
        if stem.ends_with("/__init__") || stem == "__init__" {
            let pkg = stem.strip_suffix("/__init__").unwrap_or("");
            if !pkg.is_empty() {
                file_index.insert(pkg.to_string(), fi);
            }
        }
    }

    // Build visible-names per file: file_index → (name_sym → node_index)
    let mut visible: Vec<FxHashMap<u32, u32>> = Vec::with_capacity(trees.len());
    for tree in trees.iter() {
        let mut names: FxHashMap<u32, u32> = FxHashMap::default();
        for (i, n) in tree.nodes.iter().enumerate() {
            if n.flags & crate::lang::DEAD != 0 {
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

    // Collect import resolution requests first, then mutate
    struct ImportReq {
        fi: usize,
        node: u32,
        source_str: String,
        target_fi: usize,
        target_path: String,
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
                .find(|&c| tree.kind(c) == k_source)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if source_sym == 0 {
                continue;
            }
            let source_str = lang.syms.resolve(source_sym).to_string();
            let (depth, module_path) = parse_python_source(&source_str);
            let target_path = if depth > 0 {
                let current = lang.syms.resolve(trees[fi].nodes[0].sym);
                resolve_relative(current, depth, module_path)
            } else {
                module_path.replace('.', "/")
            };
            let tfi = file_index.get(&target_path).copied().or_else(|| {
                // Try with source root prefixes (directories of existing files)
                let current = lang.syms.resolve(trees[fi].nodes[0].sym);
                let dir = current.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
                // Try current file's directory and ancestors
                let mut prefix = dir;
                loop {
                    let candidate = if prefix.is_empty() {
                        target_path.clone()
                    } else {
                        format!("{prefix}/{target_path}")
                    };
                    if let Some(&tfi) = file_index.get(&candidate) {
                        return Some(tfi);
                    }
                    if let Some((parent, _)) = prefix.rsplit_once('/') {
                        prefix = parent;
                    } else {
                        break;
                    }
                }
                None
            });
            if let Some(tfi) = tfi {
                reqs.push(ImportReq {
                    fi,
                    node: i as u32,
                    source_str,
                    target_fi: tfi,
                    target_path,
                });
            }
        }
    }

    // Propagate re-exports: for __init__ files, resolved imports become visible names.
    // Store re-export targets separately: (init_fi, name_sym) → (target_fi, target_node)
    let mut reexports: FxHashMap<(usize, u32), (usize, u32)> = FxHashMap::default();
    for _round in 0..3 {
        let mut new_exports = Vec::new();
        for req in &reqs {
            let path = lang.syms.resolve(trees[req.fi].nodes[0].sym);
            let is_init = path.ends_with("/__init__.py") || path == "__init__.py";
            if !is_init {
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
                    // Also follow re-exports from target
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
                } else if let Some(&(tfi, tn)) = reexports.get(&(req.target_fi, name_sym)) {
                    if !visible[req.fi].contains_key(&name_sym) {
                        new_exports.push((req.fi, name_sym, tfi, tn));
                    }
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        for (fi, name_sym, tfi, tn) in new_exports {
            visible[fi].insert(name_sym, tn);
            reexports.insert((fi, name_sym), (tfi, tn));
        }
    }

    // Update __source syms to resolved paths
    for req in &reqs {
        let resolved_module = req.target_path.replace('/', ".");
        let resolved_sym = lang.syms.get(&resolved_module);
        let src_node = trees[req.fi]
            .children(req.node)
            .find(|&c| trees[req.fi].kind(c) == k_source);
        if let Some(sn) = src_node {
            trees[req.fi].nodes[sn as usize].sym = resolved_sym;
        }
    }

    // Build cross-edges
    for req in &reqs {
        let fi = req.fi;
        let i = req.node;
        let tfi = req.target_fi;
        {
            // Read __name children and match to defs in target
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
                        // Follow re-export if the visible entry points to another file
                        let (real_fi, real_node) = reexports
                            .get(&(tfi, def_name))
                            .copied()
                            .unwrap_or((tfi, def_node));
                        cross_edges.push(CrossEdge {
                            from_file: fi,
                            from_node: i,
                            to_file: real_fi,
                            to_node: real_node,
                            kind: E_IMPORTS,
                        });
                    }
                } else if let Some(&(re_fi, re_node)) = reexports.get(&(tfi, name_sym)) {
                    // Re-exported name — follow through to the actual def
                    cross_edges.push(CrossEdge {
                        from_file: fi,
                        from_node: i,
                        to_file: re_fi,
                        to_node: re_node,
                        kind: E_IMPORTS,
                    });
                } else if let Some(&def_node) = visible[tfi].get(&name_sym) {
                    cross_edges.push(CrossEdge {
                        from_file: fi,
                        from_node: i,
                        to_file: tfi,
                        to_node: def_node,
                        kind: E_IMPORTS,
                    });
                }
            }
        }
    }

    // Emit E_CALLS cross-edges: follow intra-file E_IMPORTS from callers to import nodes,
    // then cross-file from import to target def.
    // For wildcard imports, match the caller's ref name against the target def name.
    let k_call = lang.kinds.lookup("__call") as u16 | SYNTH;
    let k_member = lang.kinds.lookup("__member") as u16 | SYNTH;
    let callee_f = lang.fields.lookup("callee") as u16;
    let member_f = lang.fields.lookup("member") as u16;
    let object_f = lang.fields.lookup("object") as u16;

    // For module-level imports (import X / import X as Y), resolve member accesses.
    // Find callers that do module.func() and resolve func in the target module.
    for req in &reqs {
        let fi = req.fi;
        let import_node = req.node;
        let tfi = req.target_fi;

        // Check if this is a module-level import (name == module stem or has alias)
        for edge in &trees[fi].edges {
            if edge.kind != E_IMPORTS {
                continue;
            }
            let edge_target = edge.to;
            // Check if edge points to this import's __name or the import itself
            if edge_target != import_node
                && trees[fi].nodes[edge_target as usize].parent != import_node
            {
                continue;
            }

            let caller = edge.from;
            // Search caller's descendants for __call(callee: __member(object: X, member: Y))
            for d in trees[fi].descendants(caller) {
                if trees[fi].kind(d) != k_call {
                    continue;
                }
                let callee_node = trees[fi].child_by_field(d, callee_f);
                if let Some(cn) = callee_node {
                    if trees[fi].kind(cn) == k_member {
                        let member_sym = trees[fi]
                            .child_by_field(cn, member_f)
                            .map(|c| trees[fi].sym(c))
                            .unwrap_or(0);
                        if member_sym != 0 {
                            if let Some(&def_node) = visible[tfi].get(&member_sym) {
                                cross_edges.push(CrossEdge {
                                    from_file: fi,
                                    from_node: caller,
                                    to_file: tfi,
                                    to_node: def_node,
                                    kind: crate::lang::E_CALLS,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    let mut call_edges = Vec::new();
    for ce in &cross_edges {
        if ce.kind != E_IMPORTS {
            continue;
        }
        let target_name = visible[ce.to_file]
            .iter()
            .find(|(_, node)| **node == ce.to_node)
            .map(|(sym, _)| *sym)
            .unwrap_or(0);

        for edge in &trees[ce.from_file].edges {
            if edge.kind != E_IMPORTS {
                continue;
            }
            // edge.to could be __import node or __name node (child of __import)
            let edge_import = edge.to;
            let matches_import = edge_import == ce.from_node
                || trees[ce.from_file].nodes[edge_import as usize].parent == ce.from_node;
            if !matches_import {
                continue;
            }

            // Check if this is a wildcard import — if so, match the caller's ref name
            let is_wildcard = trees[ce.from_file].children(ce.from_node).any(|c| {
                trees[ce.from_file].kind(c) == k_name
                    && lang.syms.resolve(trees[ce.from_file].sym(c)) == "*"
            });

            if is_wildcard && target_name != 0 {
                // Find what name the caller is referencing
                let caller_node = edge.from;
                // Walk caller's descendants for __call nodes that reference this import
                let mut matched = false;
                for d in trees[ce.from_file].descendants(caller_node) {
                    if trees[ce.from_file].kind(d) == k_call {
                        let callee = trees[ce.from_file]
                            .child_by_field(d, callee_f)
                            .map(|c| trees[ce.from_file].sym(c))
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

            call_edges.push(CrossEdge {
                from_file: ce.from_file,
                from_node: edge.from,
                to_file: ce.to_file,
                to_node: ce.to_node,
                kind: crate::lang::E_CALLS,
            });
        }
    }

    // Cross-file return type resolution:
    // When caller calls imported_func() and binds the result (x = imported_func()),
    // then accesses x.method(), resolve method on the imported function's return type.
    let k_binding = lang.kinds.lookup("__binding") as u16 | SYNTH;
    let k_ivar = lang.kinds.lookup("__ivar") as u16 | SYNTH;
    let ret_type_f = lang.fields.lookup("return_type") as u16;

    let mut type_edges = Vec::new();
    for ce in &call_edges {
        if ce.kind != crate::lang::E_CALLS {
            continue;
        }
        let caller_fi = ce.from_file;
        let caller_node = ce.from_node;
        let target_fi = ce.to_file;
        let target_node = ce.to_node;

        // Find the return type of the target function
        let return_type_sym = if ret_type_f != 0 {
            trees[target_fi]
                .child_by_field(target_node, ret_type_f)
                .map(|r| trees[target_fi].sym(r))
                .filter(|&s| s != 0)
        } else {
            None
        }
        .or_else(|| {
            // Infer from body: return X() → X
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

        // Find the class def for the return type — could be in any file
        let mut class_fi = None;
        let mut class_node = None;
        // Check target file's visible names first
        if let Some(&cn) = visible[target_fi].get(&ret_sym) {
            class_fi = Some(target_fi);
            class_node = Some(cn);
        }
        // Check if it was imported in the target file (re-exported or directly)
        if class_fi.is_none() {
            for ce2 in &cross_edges {
                if ce2.from_file == target_fi && ce2.kind == E_IMPORTS {
                    // Check if the target def name matches ret_sym
                    let def_name = trees[ce2.to_file]
                        .child_by_field(ce2.to_node, name_f)
                        .or_else(|| trees[ce2.to_file].child_by_field(ce2.to_node, left_f))
                        .map(|c| trees[ce2.to_file].sym(c))
                        .unwrap_or(0);
                    if def_name == ret_sym {
                        class_fi = Some(ce2.to_file);
                        class_node = Some(ce2.to_node);
                        break;
                    }
                }
            }
        }

        let (Some(cls_fi), Some(cls_node)) = (class_fi, class_node) else {
            continue;
        };

        // Find which variable was bound to the import call result.
        // Look for `x = imported_func()` bindings in the caller.
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
            if let Some(rn) = rhs {
                if tree.kind(rn) == k_call {
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
        }

        // Find member accesses on those bound variables
        for d in tree.descendants(caller_node) {
            if tree.kind(d) == k_call {
                let callee_n = tree.child_by_field(d, callee_f);
                if let Some(cn) = callee_n {
                    if tree.kind(cn) == k_member {
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
                            // Search class descendants for the method
                            let mut found = false;
                            for cd in trees[cls_fi].descendants(cls_node) {
                                if trees[cls_fi].kind(cd) == k_deftype {
                                    let mn = trees[cls_fi].nodes[cd as usize].parent;
                                    if mn != crate::lang::NONE && mn != cls_node {
                                        let mname = trees[cls_fi]
                                            .child_by_field(mn, name_f)
                                            .or_else(|| trees[cls_fi].child_by_field(mn, left_f))
                                            .map(|c| trees[cls_fi].sym(c))
                                            .unwrap_or(0);
                                        if mname == mem_sym && !found {
                                            type_edges.push(CrossEdge {
                                                from_file: caller_fi,
                                                from_node: caller_node,
                                                to_file: cls_fi,
                                                to_node: mn,
                                                kind: crate::lang::E_CALLS,
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
    }

    // Return all cross-edges
    cross_edges.extend(call_edges);
    cross_edges.extend(type_edges);
    ResolveResult { cross_edges }
}

fn strip_py_ext(path: &str) -> &str {
    path.strip_suffix(".py")
        .or_else(|| path.strip_suffix(".pyi"))
        .unwrap_or(path)
}

fn parse_python_source(source: &str) -> (usize, &str) {
    let mut depth = 0;
    let mut rest = source;
    while let Some(r) = rest.strip_prefix('.') {
        depth += 1;
        rest = r;
    }
    (depth, rest)
}

fn resolve_relative(current_file: &str, depth: usize, module_path: &str) -> String {
    let stem = strip_py_ext(current_file);
    let parts: Vec<&str> = stem.split('/').collect();
    // Go up `depth` directories from the current file's directory
    let dir_parts = if parts.len() > 1 {
        &parts[..parts.len() - 1]
    } else {
        &[]
    };
    let up = depth.min(dir_parts.len());
    let base: Vec<&str> = dir_parts[..dir_parts.len() - up + 1].to_vec();

    if module_path.is_empty() {
        base.join("/")
    } else {
        let module_parts: Vec<&str> = module_path.split('.').collect();
        let mut result = base;
        result.extend_from_slice(&module_parts);
        result.join("/")
    }
}
