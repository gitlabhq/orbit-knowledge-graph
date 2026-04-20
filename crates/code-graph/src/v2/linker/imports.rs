//! Import resolution strategies.
//!
//! All lookups go through `CodeGraph.indexes` (VerifiedMap).
//! String access goes through `CodeGraph.str(id)` (StringPool).

use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use super::graph::CodeGraph;
use super::rules::ImportStrategy;
use super::state::ScratchBuf;
use crate::v2::types::ImportBindingKind;

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
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    for strategy in strategies {
        let candidates = match strategy {
            ImportStrategy::ScopeFqnWalk => scope_fqn_walk(graph, file_node, name, sep, scratch),
            ImportStrategy::ExplicitImport => explicit_import(import_map, name),
            ImportStrategy::WildcardImport => wildcard_import(graph, file_node, name, sep, scratch),
            ImportStrategy::SamePackage => same_package(graph, file_node, name, sep, scratch),
            ImportStrategy::SameFile => same_file(graph, file_node, name),
            ImportStrategy::FilePath => vec![],
            ImportStrategy::GlobalName => global_name(graph, file_node, name),
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
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    let import = graph.import(import_idx);
    if matches!(import.binding_kind, ImportBindingKind::SideEffect) || import.wildcard {
        return vec![];
    }

    let symbol_name = import
        .alias
        .or(import.name)
        .map(|id| graph.str(id))
        .unwrap_or("");
    if symbol_name.is_empty() {
        return vec![];
    }

    let imp_path = graph.str(import.path);
    let key = if imp_path.is_empty() {
        scratch.clear();
        scratch.push_str(symbol_name);
        scratch.as_str()
    } else {
        scratch.set_fmt(format_args!("{imp_path}{sep}{symbol_name}"))
    };
    let by_fqn = graph
        .indexes
        .by_fqn
        .lookup(key, |idx| graph.def_fqn(idx) == key);
    if !by_fqn.is_empty() {
        return by_fqn.to_vec();
    }

    if !imp_path.is_empty() {
        let by_path = graph
            .indexes
            .by_fqn
            .lookup(imp_path, |idx| graph.def_fqn(idx) == imp_path);
        if !by_path.is_empty() {
            return by_path.to_vec();
        }
    }
    vec![]
}

fn scope_fqn_walk(
    graph: &CodeGraph,
    file_node: NodeIndex,
    name: &str,
    sep: &str,
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    let def_ids: Vec<_> = graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
        .filter_map(|idx| graph.graph[idx].def_id())
        .collect();

    for &did in &def_ids {
        let def = &graph.defs[did.0 as usize];
        if def.is_top_level {
            let fqn = graph.str(def.fqn);
            let key = scratch.set_fmt(format_args!("{fqn}{sep}{name}"));
            let matches = graph
                .indexes
                .by_fqn
                .lookup(key, |idx| graph.def_fqn(idx) == key);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }
    for &did in &def_ids {
        let def = &graph.defs[did.0 as usize];
        let fqn_str = graph.str(def.fqn);
        let mut current = fqn_str;
        loop {
            let key = scratch.set_fmt(format_args!("{current}{sep}{name}"));
            let matches = graph
                .indexes
                .by_fqn
                .lookup(key, |idx| graph.def_fqn(idx) == key);
            if !matches.is_empty() {
                return matches.to_vec();
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
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    for neighbor in graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
    {
        if let Some(import_id) = graph.graph[neighbor].import_id()
            && let imp = &graph.imports[import_id.0 as usize]
            && imp.wildcard
        {
            let path = graph.str(imp.path);
            let key = scratch.set_fmt(format_args!("{path}{sep}{name}"));
            let matches = graph
                .indexes
                .by_fqn
                .lookup(key, |idx| graph.def_fqn(idx) == key);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }
    vec![]
}

fn same_package(
    graph: &CodeGraph,
    file_node: NodeIndex,
    name: &str,
    sep: &str,
    scratch: &mut ScratchBuf,
) -> Vec<NodeIndex> {
    for neighbor in graph
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
    {
        if let Some(def_id) = graph.graph[neighbor].def_id()
            && let def = &graph.defs[def_id.0 as usize]
            && def.is_top_level
        {
            let fqn_str = graph.str(def.fqn);
            if let Some(sep_pos) = fqn_str.rfind(sep) {
                let key = scratch.set_fmt(format_args!("{}{sep}{name}", &fqn_str[..sep_pos]));
                let matches = graph
                    .indexes
                    .by_fqn
                    .lookup(key, |idx| graph.def_fqn(idx) == key);
                if !matches.is_empty() {
                    return matches.to_vec();
                }
            }
        }
    }
    vec![]
}

fn global_name(graph: &CodeGraph, _file_node: NodeIndex, name: &str) -> Vec<NodeIndex> {
    graph
        .indexes
        .by_name
        .lookup(name, |idx| {
            graph.def_name(idx) == name
                && graph.graph[idx]
                    .def_id()
                    .is_some_and(|d| graph.defs[d.0 as usize].is_top_level)
        })
        .to_vec()
}

fn same_file(graph: &CodeGraph, file_node: NodeIndex, name: &str) -> Vec<NodeIndex> {
    let file_path = graph.graph[file_node].path();

    let by_fqn: Vec<NodeIndex> = graph
        .indexes
        .by_fqn
        .lookup(name, |idx| graph.def_fqn(idx) == name)
        .into_iter()
        .filter(|&idx| graph.def_in_file(idx, file_path))
        .collect();
    if !by_fqn.is_empty() {
        return by_fqn;
    }

    graph
        .indexes
        .by_name
        .lookup(name, |idx| graph.def_name(idx) == name)
        .into_iter()
        .filter(|&idx| graph.def_in_file(idx, file_path))
        .collect()
}
