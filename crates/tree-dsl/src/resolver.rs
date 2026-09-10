//! Cross-file resolver. Reads __import/__source/__name synthetics.
//! Language-agnostic: source paths are already canonical /-separated
//! after YAML pipe transforms. Extension stripping and index-file
//! collapsing are driven by languages.yaml via SupportLang.

use rustc_hash::FxHashMap;

use crate::grammar::SupportLang;
use crate::lang::{E_IMPORTS, Lang, SYNTH};
use crate::tree::Tree;

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

pub fn resolve(
    trees: &mut [Tree],
    lang: &mut Lang,
    support_lang: SupportLang,
    source_roots: &[String],
) -> ResolveResult {
    let k_import = lang.kinds.lookup("__import") as u16 | SYNTH;
    let k_source = lang.kinds.lookup("__source") as u16 | SYNTH;
    let k_source_path = lang.kinds.lookup("__source_path") as u16 | SYNTH;
    let k_name = lang.kinds.lookup("__name") as u16 | SYNTH;
    let k_deftype = lang.kinds.lookup("__deftype") as u16 | SYNTH;
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

    // Collect import resolution requests
    struct ImportReq {
        fi: usize,
        node: u32,
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
                .find(|&c| tree.kind(c) == k_source_path)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if source_sym == 0 {
                continue;
            }
            let source_str = lang.syms.resolve(source_sym).to_string();

            let target_path = if source_str.starts_with("./") || source_str.starts_with("../") {
                let current = lang.syms.resolve(trees[fi].nodes[0].sym);
                resolve_relative(current, &source_str)
            } else {
                source_str.clone()
            };

            let tfi = file_index.get(&target_path).copied().or_else(|| {
                // Walk up the current file's directory ancestors.
                let current = lang.syms.resolve(trees[fi].nodes[0].sym);
                let dir = current.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
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
                // Try each source root detected by the file-tree walker.
                for root in source_roots {
                    let candidate = format!("{root}/{target_path}");
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
            }
        }
    }

    // Re-export propagation for index files
    let mut reexports: FxHashMap<(usize, u32), (usize, u32)> = FxHashMap::default();
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

    // Build E_IMPORTS cross-edges
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

    // E_CALLS cross-edges: follow intra-file E_IMPORTS → cross-file target
    let k_call = lang.kinds.lookup("__call") as u16 | SYNTH;
    let k_member = lang.kinds.lookup("__member") as u16 | SYNTH;
    let callee_f = lang.fields.lookup("callee") as u16;
    let member_f = lang.fields.lookup("member") as u16;
    let object_f = lang.fields.lookup("object") as u16;

    // Module-level import member access: import X; X.func()
    for req in &reqs {
        let fi = req.fi;
        let import_node = req.node;
        let tfi = req.target_fi;

        for edge in &trees[fi].edges {
            if edge.kind != E_IMPORTS {
                continue;
            }
            let edge_target = edge.to;
            if edge_target != import_node
                && trees[fi].nodes[edge_target as usize].parent != import_node
            {
                continue;
            }

            let caller = edge.from;
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
                    if member_sym != 0
                        && let Some(&def_node) = visible[tfi].get(&member_sym)
                    {
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
            let edge_import = edge.to;
            let matches_import = edge_import == ce.from_node
                || trees[ce.from_file].nodes[edge_import as usize].parent == ce.from_node;
            if !matches_import {
                continue;
            }

            let is_wildcard = trees[ce.from_file].children(ce.from_node).any(|c| {
                trees[ce.from_file].kind(c) == k_name
                    && lang.syms.resolve(trees[ce.from_file].sym(c)) == "*"
            });

            if is_wildcard && target_name != 0 {
                let caller_node = edge.from;
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

    // Cross-file return type resolution
    let k_binding = lang.kinds.lookup("__binding") as u16 | SYNTH;
    let right_f = lang.fields.lookup("right") as u16;
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

        let mut class_fi = None;
        let mut class_node = None;
        if let Some(&cn) = visible[target_fi].get(&ret_sym) {
            class_fi = Some(target_fi);
            class_node = Some(cn);
        }
        if class_fi.is_none() {
            for ce2 in &cross_edges {
                if ce2.from_file == target_fi && ce2.kind == E_IMPORTS {
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

    cross_edges.extend(call_edges);
    cross_edges.extend(type_edges);
    ResolveResult { cross_edges }
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
