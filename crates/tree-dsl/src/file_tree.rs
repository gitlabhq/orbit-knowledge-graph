//! File-tree walker. Builds a Tree from file paths, runs S-expression
//! resolve rules on it, and extracts source roots via the `climb` operation.

use rustc_hash::FxHashMap;

use crate::lang::Lang;
use crate::pattern::{self, Rewrite};
use crate::tree::{NONE, Node, Tree};

/// Result of walking the file tree.
pub struct WalkResult {
    /// Paths to try as prefixes when resolving absolute imports.
    pub lookup_prefixes: Vec<String>,
}

/// Resolve-stage config parsed from a language YAML file.
pub enum ResolveStage {
    Rules(Vec<Rewrite>),
    Climb { while_kind: u16, mark_kind: u16 },
}

pub struct ResolveConfig {
    pub stages: Vec<ResolveStage>,
    /// Synthetic kinds whose marked nodes provide resolution prefixes.
    pub lookup_from: Vec<u16>,
    /// Module names that should never resolve to local files (e.g. stdlib).
    pub external: Vec<String>,
}

impl Default for ResolveConfig {
    fn default() -> Self {
        Self {
            stages: vec![],
            lookup_from: vec![],
            external: vec![],
        }
    }
}

/// Build a file tree from paths, run resolve stages, return results.
pub fn walk(paths: &[String], lang: &mut Lang, config: &ResolveConfig) -> WalkResult {
    if config.stages.is_empty() && config.lookup_from.is_empty() {
        return WalkResult {
            lookup_prefixes: vec![],
        };
    }

    let mut tree = build_file_tree(paths, lang);

    for stage in &config.stages {
        match stage {
            ResolveStage::Rules(rules) => {
                pattern::apply_rewrites(&mut tree, lang, rules);
            }
            ResolveStage::Climb {
                while_kind,
                mark_kind,
            } => {
                climb(&mut tree, lang, *while_kind, *mark_kind);
                tree.compact();
            }
        }
    }

    let mut prefixes = collect_marked_paths(&tree, lang, &config.lookup_from);
    let packages = collect_packages(&tree, lang);
    let detected = prefixes.clone();
    add_fallback_roots(paths, &detected, &packages, &mut prefixes);
    WalkResult {
        lookup_prefixes: prefixes,
    }
}

fn build_file_tree(paths: &[String], lang: &mut Lang) -> Tree {
    let root_kind = lang.kind("__root");
    let dir_kind = lang.kind("__dir");
    let file_kind = lang.kind("__file");

    // Collect unique directory segments and files into a trie-like structure.
    // Key: parent path (empty = root), Value: (segment_name, is_file)
    let mut children: FxHashMap<String, Vec<(String, bool)>> = FxHashMap::default();

    for path in paths {
        let parts: Vec<&str> = path.split('/').collect();
        for i in 0..parts.len() {
            let parent = if i == 0 {
                String::new()
            } else {
                parts[..i].join("/")
            };
            let segment = parts[i].to_string();
            let is_file = i == parts.len() - 1;
            let entry = children.entry(parent).or_default();
            if !entry.iter().any(|(s, f)| s == &segment && *f == is_file) {
                entry.push((segment, is_file));
            }
        }
    }

    // Sort children for deterministic output.
    for v in children.values_mut() {
        v.sort();
    }

    // Build nodes via DFS from root.
    let mut nodes: Vec<Node> = Vec::new();
    let root_idx = nodes.len() as u32;
    nodes.push(Node {
        kind: root_kind,
        named: true,
        parent: NONE,
        sym: 0,
        size: 0,
        ..Default::default()
    });

    fn add_children(
        parent_path: &str,
        parent_idx: u32,
        children: &FxHashMap<String, Vec<(String, bool)>>,
        nodes: &mut Vec<Node>,
        lang: &mut Lang,
        dir_kind: u16,
        file_kind: u16,
    ) {
        let Some(kids) = children.get(parent_path) else {
            return;
        };
        for (segment, is_file) in kids {
            let idx = nodes.len() as u32;
            let kind = if *is_file { file_kind } else { dir_kind };
            let sym = lang.syms.get(segment);
            nodes.push(Node {
                kind,
                named: true,
                parent: parent_idx,
                sym,
                size: 0,
                ..Default::default()
            });
            if !is_file {
                let child_path = if parent_path.is_empty() {
                    segment.clone()
                } else {
                    format!("{parent_path}/{segment}")
                };
                add_children(&child_path, idx, children, nodes, lang, dir_kind, file_kind);
            }
            nodes[idx as usize].size = (nodes.len() as u32) - idx;
        }
    }

    add_children(
        "", root_idx, &children, &mut nodes, lang, dir_kind, file_kind,
    );
    nodes[root_idx as usize].size = nodes.len() as u32;

    Tree::from_nodes(nodes)
}

/// Walk up from each node with `while_kind`, mark the first ancestor without it.
fn climb(tree: &mut Tree, lang: &mut Lang, while_kind: u16, mark_kind: u16) {
    let mut marked: Vec<u32> = Vec::new();

    for i in 0..tree.nodes.len() as u32 {
        if !tree.children(i).any(|c| tree.kind(c) == while_kind) {
            continue;
        }
        let mut node = tree.nodes[i as usize].parent;
        while node != NONE {
            if tree.children(node).any(|c| tree.kind(c) == while_kind) {
                node = tree.nodes[node as usize].parent;
            } else {
                if !marked.contains(&node) {
                    marked.push(node);
                }
                break;
            }
        }
    }

    for node in marked {
        tree.append(
            node,
            Node {
                kind: mark_kind,
                named: true,
                sym: 0,
                size: 1,
                parent: node,
                ..Default::default()
            },
        );
    }
}

/// Collect paths of nodes carrying any of the given synthetic markers.
fn collect_marked_paths(tree: &Tree, lang: &Lang, markers: &[u16]) -> Vec<String> {
    if markers.is_empty() {
        return vec![];
    }
    let root_node_kind = lang.kind_id("__root");
    let mut paths = Vec::new();
    for i in 0..tree.nodes.len() as u32 {
        if tree.nodes[i as usize].kind == root_node_kind {
            continue;
        }
        let has_marker = markers
            .iter()
            .any(|&mk| tree.children(i).any(|c| tree.kind(c) == mk));
        if has_marker {
            let path = node_path(tree, i, lang);
            if !paths.contains(&path) {
                paths.push(path);
            }
        }
    }
    paths
}

/// Reconstruct the full path of a directory node by walking up parent pointers.
fn node_path(tree: &Tree, mut node: u32, lang: &Lang) -> String {
    let dir_kind = lang.kind_id("__dir");
    let root_kind = lang.kind_id("__root");
    let mut parts = Vec::new();
    while node != NONE {
        let n = &tree.nodes[node as usize];
        if n.kind == root_kind {
            break;
        }
        if n.kind == dir_kind && n.sym != 0 {
            parts.push(lang.syms.resolve(n.sym).to_string());
        }
        node = n.parent;
    }
    parts.reverse();
    parts.join("/")
}

/// Collect paths of directories marked `__package` by the resolve rules.
fn collect_packages(tree: &Tree, lang: &Lang) -> Vec<String> {
    let pkg_kind = lang.kind_id("__package");
    if pkg_kind == 0 {
        return vec![];
    }
    let mut pkgs = Vec::new();
    for i in 0..tree.nodes.len() as u32 {
        if tree.children(i).any(|c| tree.kind(c) == pkg_kind) {
            pkgs.push(node_path(tree, i, lang));
        }
    }
    pkgs
}

/// Add top-level directories that aren't descendants of any detected root
/// and weren't marked as packages by the resolve rules.
fn add_fallback_roots(
    paths: &[String],
    existing: &[String],
    packages: &[String],
    out: &mut Vec<String>,
) {
    let mut candidates: Vec<String> = Vec::new();
    for path in paths {
        let top = match path.split_once('/') {
            Some((dir, _)) => dir.to_string(),
            None => continue,
        };
        if !candidates.contains(&top) && !existing.contains(&top) {
            candidates.push(top);
        }
    }
    for c in candidates {
        if !packages.contains(&c) && !out.contains(&c) {
            out.push(c);
        }
    }
}
