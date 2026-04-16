//! Import resolution strategies.
//!
//! Used by the fused walker to resolve import-based references.
//! All definition/import lookups go through `CodeGraph`'s indexes.

use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;

use super::graph::CodeGraph;
use super::rules::ImportStrategy;

/// Verify FQN lookup results against actual def FQNs (hash collision guard).
/// Verify FQN lookup results against actual def FQNs (hash collision guard).
fn verify_fqn(graph: &CodeGraph, candidates: &[NodeIndex], expected_fqn: &str) -> Vec<NodeIndex> {
    candidates
        .iter()
        .copied()
        .filter(|&idx| graph.def(idx).fqn.to_string() == expected_fqn)
        .collect()
}

// ── ResolveSettings ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ResolveSettings {
    pub per_file_timeout: Option<std::time::Duration>,
    pub max_chain_depth: usize,
    pub slow_ref_threshold: Option<std::time::Duration>,
    pub chain_fallback: bool,
    pub compound_key_recovery: bool,
    pub implicit_scope_on_base: bool,
}

impl Default for ResolveSettings {
    fn default() -> Self {
        Self {
            per_file_timeout: None,
            max_chain_depth: 10,
            slow_ref_threshold: Some(std::time::Duration::from_millis(100)),
            chain_fallback: true,
            compound_key_recovery: true,
            implicit_scope_on_base: true,
        }
    }
}

// ── Import strategies ───────────────────────────────────────────

pub(crate) fn apply_import_strategies(
    strategies: &[ImportStrategy],
    graph: &CodeGraph,
    file_node: NodeIndex,
    name: &str,
    sep: &str,
    import_map: &FxHashMap<String, Vec<NodeIndex>>,
) -> Vec<NodeIndex> {
    for strategy in strategies {
        let candidates = match strategy {
            ImportStrategy::ScopeFqnWalk => scope_fqn_walk(graph, file_node, name, sep),
            ImportStrategy::ExplicitImport => explicit_import(import_map, name),
            ImportStrategy::WildcardImport => wildcard_import(graph, file_node, name, sep),
            ImportStrategy::SamePackage => same_package(graph, file_node, name, sep),
            ImportStrategy::SameFile => same_file(graph, file_node, name),
            ImportStrategy::FilePath => vec![],
        };
        if !candidates.is_empty() {
            return candidates;
        }
    }
    vec![]
}

pub(crate) fn resolve_import(
    graph: &CodeGraph,
    import_idx: NodeIndex,
    sep: &str,
) -> Vec<NodeIndex> {
    let import = graph.import(import_idx);
    let symbol_name = import
        .alias
        .as_deref()
        .or(import.name.as_deref())
        .unwrap_or("");
    if symbol_name.is_empty() || import.wildcard {
        return vec![];
    }

    let full_fqn = if import.path.is_empty() {
        symbol_name.to_string()
    } else {
        format!("{}{}{}", import.path, sep, symbol_name)
    };

    let by_fqn = graph.lookup_fqn(&full_fqn);
    if !by_fqn.is_empty() {
        return verify_fqn(graph, by_fqn, &full_fqn);
    }

    if !import.path.is_empty() {
        let by_path = graph.lookup_fqn(&import.path);
        if !by_path.is_empty() {
            return verify_fqn(graph, by_path, &import.path);
        }
    }
    vec![]
}

fn scope_fqn_walk(
    graph: &CodeGraph,
    file_node: NodeIndex,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    let defs: Vec<_> = graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
        .filter_map(|idx| {
            graph.graph[idx]
                .def_id()
                .map(|id| &graph.defs[id.0 as usize])
        })
        .collect();

    for def in &defs {
        if def.is_top_level {
            let candidate = format!("{}{}{}", def.fqn, sep, name);
            let matches = graph.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return verify_fqn(graph, matches, &candidate);
            }
        }
    }
    for def in &defs {
        let fqn_str = def.fqn.to_string();
        let mut current = fqn_str.as_str();
        loop {
            let candidate = format!("{}{}{}", current, sep, name);
            let matches = graph.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return verify_fqn(graph, matches, &candidate);
            }
            match current.rfind(sep) {
                Some(pos) => current = &current[..pos],
                None => break,
            }
        }
    }
    vec![]
}

fn explicit_import(import_map: &FxHashMap<String, Vec<NodeIndex>>, name: &str) -> Vec<NodeIndex> {
    import_map.get(name).cloned().unwrap_or_default()
}

fn wildcard_import(
    graph: &CodeGraph,
    file_node: NodeIndex,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    for neighbor in graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
    {
        if let Some(import_id) = graph.graph[neighbor].import_id()
            && let imp = &graph.imports[import_id.0 as usize]
            && imp.wildcard
        {
            let candidate = format!("{}{}{}", imp.path, sep, name);
            let matches = graph.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return verify_fqn(graph, matches, &candidate);
            }
        }
    }
    vec![]
}

fn same_package(graph: &CodeGraph, file_node: NodeIndex, name: &str, sep: &str) -> Vec<NodeIndex> {
    for neighbor in graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
    {
        if let Some(def_id) = graph.graph[neighbor].def_id()
            && let def = &graph.defs[def_id.0 as usize]
            && def.is_top_level
        {
            let fqn_str = def.fqn.to_string();
            if let Some(sep_pos) = fqn_str.rfind(sep) {
                let candidate = format!("{}{}{}", &fqn_str[..sep_pos], sep, name);
                let matches = graph.lookup_fqn(&candidate);
                if !matches.is_empty() {
                    return verify_fqn(graph, matches, &candidate);
                }
            }
        }
    }
    vec![]
}

fn same_file(graph: &CodeGraph, file_node: NodeIndex, name: &str) -> Vec<NodeIndex> {
    let file_path = graph.graph[file_node].path();

    let by_fqn: Vec<NodeIndex> = graph
        .lookup_fqn(name)
        .iter()
        .filter(|&&idx| graph.def(idx).fqn.to_string() == name) // verify hash
        .filter(|&&idx| graph.def_in_file(idx, file_path))
        .copied()
        .collect();
    if !by_fqn.is_empty() {
        return by_fqn;
    }

    graph
        .lookup_name(name)
        .iter()
        .filter(|&&idx| graph.def(idx).name == name) // verify hash
        .filter(|&&idx| graph.def_in_file(idx, file_path))
        .copied()
        .collect()
}
