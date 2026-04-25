//! Import resolution strategies.
//!
//! All lookups go through `CodeGraph.indexes` (VerifiedMap).
//! String access goes through `CodeGraph.str(id)` (StringPool).

use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;

use super::graph::CodeGraph;
use super::rules::ImportStrategy;
use super::state::ScratchBuf;
use crate::v2::types::ImportBindingKind;

// ── ResolveSettings ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ResolveSettings {
    pub per_file_timeout: Option<std::time::Duration>,
    pub max_chain_depth: usize,
    pub chain_fallback: bool,
    pub compound_key_recovery: bool,
    pub implicit_scope_on_base: bool,
    /// Maximum number of results from `global_name` before discarding
    /// as too ambiguous. Prevents fan-out on common names.
    pub global_name_max_results: usize,
}

impl Default for ResolveSettings {
    fn default() -> Self {
        Self {
            per_file_timeout: None,
            max_chain_depth: 10,
            chain_fallback: true,
            compound_key_recovery: true,
            implicit_scope_on_base: true,
            global_name_max_results: 5,
        }
    }
}

// ── ImportResolver ──────────────────────────────────────────────

/// Per-file import resolver. Holds shared state so individual
/// strategy methods don't need to thread graph/sep/scratch/etc.
pub(crate) struct ImportResolver<'a> {
    pub graph: &'a CodeGraph,
    pub file_node: NodeIndex,
    pub import_map: &'a FxHashMap<String, Vec<NodeIndex>>,
    pub scratch: &'a mut ScratchBuf,
    pub settings: &'a ResolveSettings,
}

impl<'a> ImportResolver<'a> {
    /// FQN separator. Returns `&'static str` so it doesn't borrow self.
    #[inline]
    fn sep(&self) -> &'static str {
        self.graph
            .rules
            .as_ref()
            .map(|r| r.fqn_separator)
            .unwrap_or(".")
    }

    /// Run import strategies in order, returning the first non-empty result.
    pub fn apply_strategies(
        &mut self,
        strategies: &[ImportStrategy],
        name: &str,
    ) -> Vec<NodeIndex> {
        for strategy in strategies {
            let candidates = match strategy {
                ImportStrategy::ScopeFqnWalk => self.scope_fqn_walk(name),
                ImportStrategy::ExplicitImport => self.explicit_import(name),
                ImportStrategy::WildcardImport => self.wildcard_import(name),
                ImportStrategy::SamePackage => self.same_package(name),
                ImportStrategy::SameFile => self.same_file(name),
                ImportStrategy::FilePath => vec![],
                ImportStrategy::GlobalName => self.global_name(name),
                ImportStrategy::IncludeGraph => self.include_graph(name),
            };
            if !candidates.is_empty() {
                return candidates;
            }
        }
        vec![]
    }

    /// Resolve a single import node to its target definitions.
    pub fn resolve_import(&mut self, import_idx: NodeIndex) -> Vec<NodeIndex> {
        let import = self.graph.import(import_idx);
        if matches!(import.binding_kind, ImportBindingKind::SideEffect) || import.wildcard {
            return vec![];
        }

        let symbol_name = import
            .alias
            .or(import.name)
            .map(|id| self.graph.str(id))
            .unwrap_or("");
        if symbol_name.is_empty() {
            return vec![];
        }

        let sep = self.sep();
        let imp_path = self.graph.str(import.path);
        let key = if imp_path.is_empty() {
            self.scratch.clear();
            self.scratch.push_str(symbol_name);
            self.scratch.as_str()
        } else {
            self.scratch
                .set_fmt(format_args!("{imp_path}{sep}{symbol_name}"))
        };
        let by_fqn = self
            .graph
            .indexes
            .by_fqn
            .lookup(key, |idx| self.graph.def_fqn(idx) == key);
        if !by_fqn.is_empty() {
            return by_fqn.to_vec();
        }

        if !imp_path.is_empty() {
            let by_path = self
                .graph
                .indexes
                .by_fqn
                .lookup(imp_path, |idx| self.graph.def_fqn(idx) == imp_path);
            if !by_path.is_empty() {
                return by_path.to_vec();
            }
        }
        vec![]
    }

    // ── Individual strategies ───────────────────────────────────

    fn scope_fqn_walk(&mut self, name: &str) -> Vec<NodeIndex> {
        let sep = self.sep();
        let def_ids: Vec<_> = self
            .graph
            .graph
            .neighbors_directed(self.file_node, petgraph::Direction::Outgoing)
            .filter_map(|idx| self.graph.graph[idx].def_id())
            .collect();

        for &did in &def_ids {
            let def = &self.graph.defs[did.0 as usize];
            if def.is_top_level {
                let fqn = self.graph.str(def.fqn);
                let key = self.scratch.set_fmt(format_args!("{fqn}{sep}{name}"));
                let matches = self
                    .graph
                    .indexes
                    .by_fqn
                    .lookup(key, |idx| self.graph.def_fqn(idx) == key);
                if !matches.is_empty() {
                    return matches.to_vec();
                }
            }
        }
        for &did in &def_ids {
            let def = &self.graph.defs[did.0 as usize];
            let fqn_str = self.graph.str(def.fqn);
            let mut current = fqn_str;
            loop {
                let key = self.scratch.set_fmt(format_args!("{current}{sep}{name}"));
                let matches = self
                    .graph
                    .indexes
                    .by_fqn
                    .lookup(key, |idx| self.graph.def_fqn(idx) == key);
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

    fn explicit_import(&self, name: &str) -> Vec<NodeIndex> {
        self.import_map.get(name).cloned().unwrap_or_default()
    }

    fn wildcard_import(&mut self, name: &str) -> Vec<NodeIndex> {
        let sep = self.sep();
        for neighbor in self
            .graph
            .graph
            .neighbors_directed(self.file_node, petgraph::Direction::Outgoing)
        {
            if let Some(import_id) = self.graph.graph[neighbor].import_id()
                && let imp = &self.graph.imports[import_id.0 as usize]
                && imp.wildcard
            {
                let path = self.graph.str(imp.path);
                let key = self.scratch.set_fmt(format_args!("{path}{sep}{name}"));
                let matches = self
                    .graph
                    .indexes
                    .by_fqn
                    .lookup(key, |idx| self.graph.def_fqn(idx) == key);
                if !matches.is_empty() {
                    return matches.to_vec();
                }
            }
        }
        vec![]
    }

    fn same_package(&mut self, name: &str) -> Vec<NodeIndex> {
        let sep = self.sep();
        for neighbor in self
            .graph
            .graph
            .neighbors_directed(self.file_node, petgraph::Direction::Outgoing)
        {
            if let Some(def_id) = self.graph.graph[neighbor].def_id()
                && let def = &self.graph.defs[def_id.0 as usize]
                && def.is_top_level
            {
                let fqn_str = self.graph.str(def.fqn);
                if let Some(sep_pos) = fqn_str.rfind(sep) {
                    let key = self
                        .scratch
                        .set_fmt(format_args!("{}{sep}{name}", &fqn_str[..sep_pos]));
                    let matches = self
                        .graph
                        .indexes
                        .by_fqn
                        .lookup(key, |idx| self.graph.def_fqn(idx) == key);
                    if !matches.is_empty() {
                        return matches.to_vec();
                    }
                }
            }
        }
        vec![]
    }

    /// Resolve a bare name against top-level definitions across all files.
    /// Returns empty if the name is too ambiguous (more than `max_results`
    /// matches) to avoid O(candidates) fan-out on common names.
    pub fn global_name(&self, name: &str) -> Vec<NodeIndex> {
        let max_results = self.settings.global_name_max_results;
        let results = self
            .graph
            .indexes
            .by_name
            .lookup(name, |idx| {
                self.graph.def_name(idx) == name
                    && self.graph.graph[idx].def_id().is_some_and(|d| {
                        let def = &self.graph.defs[d.0 as usize];
                        if !def.is_top_level {
                            return false;
                        }
                        if !def.kind.is_type_container() {
                            self.graph.str(def.fqn) != name
                        } else {
                            true
                        }
                    })
            })
            .to_vec();
        if results.len() > max_results {
            return vec![];
        }
        results
    }

    /// Resolve a bare name via `#include` graph traversal.
    ///
    /// Collects all files reachable through this file's include imports,
    /// then searches for top-level definitions matching `name` in those
    /// files. Used for C/C++/Objective-C where `#include` makes
    /// declarations visible across translation units.
    fn include_graph(&self, name: &str) -> Vec<NodeIndex> {
        // Collect include paths by scanning all imports in the graph
        // that belong to the current file (matched by file_path).
        let file_path = self.graph.graph[self.file_node].path();
        let mut included_paths: Vec<String> = Vec::new();
        for (idx, fp, imp) in self.graph.imports_iter() {
            let _ = idx;
            if fp.as_ref() != file_path {
                continue;
            }
            let raw = self.graph.str(imp.path);
            let cleaned = raw
                .trim_matches('"')
                .trim_matches('<')
                .trim_matches('>')
                .to_string();
            included_paths.push(cleaned);
        }

        if included_paths.is_empty() {
            return Vec::new();
        }

        // For each included header, also try the corresponding source
        // file (e.g. math.h → math.c, math.cc, math.cpp, math.m).
        // Header declarations are prototypes; definitions live in the
        // paired source file.
        const SOURCE_EXTENSIONS: &[&str] = &[".c", ".cc", ".cpp", ".m"];
        let mut paired_stems: Vec<String> = Vec::new();
        for p in &included_paths {
            if let Some(stem) = p.strip_suffix(".h").or_else(|| p.strip_suffix(".hpp")) {
                paired_stems.push(stem.to_string());
            }
        }

        // Find file nodes matching include paths or paired source files,
        // then search for the bare name in those files' definitions
        let mut results = Vec::new();
        for (file_idx, file) in self.graph.files() {
            if file_idx == self.file_node {
                continue;
            }
            let is_included = included_paths
                .iter()
                .any(|p| file.path.ends_with(p.as_str()));
            let is_paired = !is_included
                && paired_stems.iter().any(|stem| {
                    SOURCE_EXTENSIONS
                        .iter()
                        .any(|ext| file.path.ends_with(&format!("{stem}{ext}")))
                });
            if !is_included && !is_paired {
                continue;
            }
            for &idx in self
                .graph
                .indexes
                .by_name
                .lookup(name, |idx| self.graph.def_name(idx) == name)
                .iter()
                .filter(|&&idx| self.graph.def_in_file(idx, &file.path))
            {
                results.push(idx);
            }
        }
        results
    }

    fn same_file(&self, name: &str) -> Vec<NodeIndex> {
        let file_path = self.graph.graph[self.file_node].path();

        let by_fqn: Vec<NodeIndex> = self
            .graph
            .indexes
            .by_fqn
            .lookup(name, |idx| self.graph.def_fqn(idx) == name)
            .into_iter()
            .filter(|&idx| self.graph.def_in_file(idx, file_path))
            .collect();
        if !by_fqn.is_empty() {
            return by_fqn;
        }

        self.graph
            .indexes
            .by_name
            .lookup(name, |idx| self.graph.def_name(idx) == name)
            .into_iter()
            .filter(|&idx| self.graph.def_in_file(idx, file_path))
            .collect()
    }
}
