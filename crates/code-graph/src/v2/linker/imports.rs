//! All lookups go through `CodeGraph.indexes` (VerifiedMap).
//! String access goes through `CodeGraph.str(id)` (StringPool).

use petgraph::graph::NodeIndex;
use rustc_hash::FxHashMap;

use super::graph::CodeGraph;
use super::rules::ImportStrategy;
use super::state::ScratchBuf;
use crate::v2::types::ImportBindingKind;

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
    /// When set, restrict the qualified-name fast path (resolver.rs) to
    /// definitions in the SAME DIRECTORY as the referencing file. For
    /// languages whose module scope is a directory (Terraform/HCL), this
    /// stops a `local.x` / `type.name` reference from resolving to a
    /// same-named definition in an unrelated module directory.
    pub same_directory_scope: bool,
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
            same_directory_scope: false,
        }
    }
}

/// The directory portion of a file path (everything before the last `/`),
/// or `""` for a repo-root file. Used for directory-scoped resolution.
pub(crate) fn dir_of(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[..i],
        None => "",
    }
}

/// Byte lengths of every prefix the scalar `rfind(sep)` scope climb probes, in
/// the same order it visits them: the full string, then each separator start
/// found right-to-left. Reproduces `rfind`'s non-overlapping backward match so
/// the probe-key sequence is byte-identical to the `current.rfind(sep)` loop.
///
/// Lazy on purpose: a def that resolves at the outermost scope stops the caller
/// after the full-FQN probe, so no backward scan runs — restoring the old
/// `rfind`-loop early-exit while still scanning each level at most once.
fn climb_prefix_ends<'a>(fqn: &'a str, sep: &'a str) -> impl Iterator<Item = usize> + 'a {
    let hay = fqn.as_bytes();
    let starts = SeparatorStarts {
        hay,
        sep: sep.as_bytes(),
        limit: hay.len(),
    };
    std::iter::once(fqn.len()).chain(starts)
}

/// Rightmost start of `sep` whose match fits within `hay[..end]`, or `None`.
/// Scans first-byte candidates right-to-left and verifies the full separator,
/// so a lone first byte (e.g. a single `:` for `::`) is skipped. `width == 1`
/// degenerates to `memrchr`: `pos + 1 <= end` always holds and the one-byte
/// equality is trivially true, so the dot separator matches `memrchr_iter`.
fn rightmost_fit(hay: &[u8], sep: &[u8], end: usize) -> Option<usize> {
    let width = sep.len();
    let mut search_end = end;
    loop {
        let pos = memchr::memrchr(sep[0], &hay[..search_end])?;
        if pos + width <= end && &hay[pos..pos + width] == sep {
            return Some(pos);
        }
        if pos == 0 {
            return None;
        }
        search_end = pos;
    }
}

/// Separator starts right-to-left, non-overlapping (a match consumes its own
/// width), matching repeated `rfind(sep)` + truncate-to-match-start. `limit` is
/// the truncation bound: advancing it to each confirmed `pos` prevents the next
/// match from overlapping, so `a::::b` yields `3, 1` (prefixes `a::`, `a`), not
/// `3, 2, 1`.
struct SeparatorStarts<'a> {
    hay: &'a [u8],
    sep: &'a [u8],
    limit: usize,
}

impl Iterator for SeparatorStarts<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        // Guards `rightmost_fit`'s `sep[0]`; an empty separator has no starts.
        if self.sep.is_empty() {
            return None;
        }
        let pos = rightmost_fit(self.hay, self.sep, self.limit)?;
        self.limit = pos;
        Some(pos)
    }
}

pub(crate) struct ImportResolver<'a> {
    pub graph: &'a CodeGraph,
    pub file_node: NodeIndex,
    pub import_map: &'a FxHashMap<String, Vec<NodeIndex>>,
    pub scratch: &'a mut ScratchBuf,
    pub settings: &'a ResolveSettings,
    pub include_index: Option<&'a super::graph::IncludeIndex>,
    pub include_reachable: &'a mut Option<rustc_hash::FxHashSet<String>>,
    pub reexport_index: Option<&'a super::graph::ReexportIndex>,
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

    pub fn resolve_import(&mut self, import_idx: NodeIndex) -> Vec<NodeIndex> {
        let import = self.graph.import(import_idx);
        if matches!(import.binding_kind, ImportBindingKind::SideEffect) || import.wildcard {
            return vec![];
        }

        // Rebuild the imported symbol's FQN from `name`; the alias is only the local handle.
        let symbol_name = import
            .name
            .or(import.alias)
            .map(|id| self.graph.str(id))
            .unwrap_or("");

        let sep = self.sep();
        let imp_path = self.graph.str(import.path);
        if symbol_name.is_empty() {
            if imp_path.is_empty() {
                return vec![];
            }
            let by_path = self
                .graph
                .indexes
                .by_fqn
                .lookup(imp_path, |idx| self.graph.def_fqn(idx) == imp_path);
            return by_path.to_vec();
        }

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

        // `from pkg import Foo` where `pkg` only re-exports `Foo`: follow the
        // re-export chain to the defining module.
        if let Some(reexport) = self.reexport_index
            && !imp_path.is_empty()
        {
            let resolved = self.follow_reexport(reexport, imp_path, symbol_name);
            if !resolved.is_empty() {
                return resolved;
            }
        }
        vec![]
    }

    fn follow_reexport(
        &self,
        reexport: &super::graph::ReexportIndex,
        module: &str,
        name: &str,
    ) -> Vec<NodeIndex> {
        const MAX_REEXPORT_DEPTH: usize = 16;
        let sep = self.sep();
        let mut visited: rustc_hash::FxHashSet<(String, String)> = rustc_hash::FxHashSet::default();
        let mut stack = vec![(module.to_string(), name.to_string(), 0usize)];
        let mut out: Vec<NodeIndex> = Vec::new();
        while let Some((module, name, depth)) = stack.pop() {
            if depth > MAX_REEXPORT_DEPTH || !visited.insert((module.clone(), name.clone())) {
                continue;
            }
            // An explicit `from M import name` wins; otherwise the name may come
            // from any `from M import *` source.
            let next: Vec<(String, String)> = match reexport.named(&module, &name) {
                Some((m, n)) => vec![(m.to_string(), n.to_string())],
                None => reexport
                    .wildcard_sources(&module)
                    .iter()
                    .map(|m| (m.clone(), name.clone()))
                    .collect(),
            };
            for (m, n) in next {
                let key = format!("{m}{sep}{n}");
                let found = self
                    .graph
                    .indexes
                    .by_fqn
                    .lookup(&key, |idx| self.graph.def_fqn(idx) == key);
                if found.is_empty() {
                    stack.push((m, n, depth + 1));
                } else {
                    out.extend(found.to_vec());
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        // Bind only on a unique definition. No match (not re-exported here) and
        // several distinct matches (ambiguous wildcard) both fall through to the
        // ImportedSymbol fallback.
        if out.len() == 1 { out } else { Vec::new() }
    }

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
            // The `{sep}{name}` suffix is invariant across the climb, so append
            // it to the shrinking prefix rather than reformatting the whole key.
            for prefix_len in climb_prefix_ends(fqn_str, sep) {
                let prefix = &fqn_str[..prefix_len];
                let key = {
                    self.scratch.clear();
                    self.scratch.push_str(prefix);
                    self.scratch.push_str(sep);
                    self.scratch.push_str(name);
                    self.scratch.as_str()
                };
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

    /// BFS through the include DAG: starting from this file's includes,
    /// recursively follow each included header's includes. For each
    /// reachable header, also search the paired source file (.h -> .c/.cpp).
    fn include_graph(&mut self, name: &str) -> Vec<NodeIndex> {
        let Some(idx) = self.include_index else {
            return Vec::new();
        };

        if self.include_reachable.is_none() {
            const SOURCE_EXTENSIONS: &[&str] = &[".c", ".cc", ".cpp", ".cxx", ".m"];
            const MAX_REACHABLE: usize = 512;
            let self_path = self.graph.graph[self.file_node].path().to_string();
            let mut visited: rustc_hash::FxHashSet<String> = rustc_hash::FxHashSet::default();
            visited.insert(self_path.clone());
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(self_path);
            let mut reachable = Vec::new();
            let empty = Vec::new();

            while let Some(current) = queue.pop_front() {
                if reachable.len() >= MAX_REACHABLE {
                    break;
                }
                for inc in idx.include_map.get(&current).unwrap_or(&empty) {
                    if let Some(matched) = idx.suffix_map.get(inc.as_str()) {
                        for &file_idx in matched {
                            let path = &idx.path_by_idx[&file_idx];
                            if visited.insert(path.clone()) {
                                reachable.push(file_idx);
                                queue.push_back(path.clone());
                            }
                        }
                    }
                    if let Some(stem) = inc
                        .strip_suffix(".h")
                        .or_else(|| inc.strip_suffix(".hpp"))
                        .or_else(|| inc.strip_suffix(".hh"))
                        .or_else(|| inc.strip_suffix(".hxx"))
                    {
                        for ext in SOURCE_EXTENSIONS {
                            let paired = format!("{stem}{ext}");
                            if let Some(src_idxs) = idx.suffix_map.get(&paired) {
                                for &src_idx in src_idxs {
                                    let src_path = &idx.path_by_idx[&src_idx];
                                    if visited.insert(src_path.clone()) {
                                        reachable.push(src_idx);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            *self.include_reachable = Some(
                reachable
                    .iter()
                    .map(|&fi| idx.path_by_idx[&fi].clone())
                    .collect(),
            );
        }

        let reachable = self.include_reachable.as_ref().unwrap();
        self.graph
            .indexes
            .by_name
            .lookup(name, |i| self.graph.def_name(i) == name)
            .into_iter()
            .filter(|&i| reachable.contains(self.graph.graph[i].path()))
            .collect()
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

#[cfg(test)]
mod tests {
    use super::{climb_prefix_ends, dir_of};

    /// The exact prefixes the pre-rewrite `current.rfind(sep)` climb probed.
    fn rfind_climb_prefixes<'a>(fqn: &'a str, sep: &str) -> Vec<&'a str> {
        let mut out = Vec::new();
        let mut current = fqn;
        loop {
            out.push(current);
            match current.rfind(sep) {
                Some(pos) => current = &current[..pos],
                None => break,
            }
        }
        out
    }

    fn new_climb_prefixes<'a>(fqn: &'a str, sep: &str) -> Vec<&'a str> {
        climb_prefix_ends(fqn, sep).map(|end| &fqn[..end]).collect()
    }

    fn assert_climb_matches(fqn: &str, sep: &str) {
        assert_eq!(
            new_climb_prefixes(fqn, sep),
            rfind_climb_prefixes(fqn, sep),
            "climb mismatch for fqn={fqn:?} sep={sep:?}"
        );
    }

    #[test]
    fn climb_matches_rfind_dot_separator() {
        for fqn in [
            "",
            "pkg",
            "pkg.Type",
            "a.b.c.d",
            "a..b",
            ".leading",
            "trailing.",
            "..",
            "net.http.Client.Do",
        ] {
            assert_climb_matches(fqn, ".");
        }
    }

    #[test]
    fn climb_matches_rfind_colon_separator() {
        for fqn in [
            "",
            "crate",
            "crate::mod::Type",
            "a::b::c::d",
            "std::collections::HashMap",
            "lone:colon",
            "trailing::",
            "::leading",
        ] {
            assert_climb_matches(fqn, "::");
        }
    }

    #[test]
    fn climb_matches_rfind_adjacent_colon_separators() {
        // rfind climbs `a::::b` -> `a::` -> `a` (starts 3 then 1), never 3,2,1.
        assert_eq!(
            new_climb_prefixes("a::::b", "::"),
            vec!["a::::b", "a::", "a"]
        );
        for fqn in ["a::::b", "a:::b", "::::", ":::", "a::::::b", "x::::y::::z"] {
            assert_climb_matches(fqn, "::");
        }
    }

    /// Mirrors the production probe-key build in `scope_fqn_walk`:
    /// `push_str(prefix)` then `push_str(sep)` then `push_str(name)` per level.
    fn composed_probe_keys(fqn: &str, sep: &str, name: &str) -> Vec<String> {
        climb_prefix_ends(fqn, sep)
            .map(|end| {
                let mut key = String::new();
                key.push_str(&fqn[..end]);
                key.push_str(sep);
                key.push_str(name);
                key
            })
            .collect()
    }

    #[test]
    fn composed_probe_keys_dot_separator() {
        assert_eq!(
            composed_probe_keys("net.http.Client", ".", "Do"),
            ["net.http.Client.Do", "net.http.Do", "net.Do"]
        );
    }

    #[test]
    fn composed_probe_keys_colon_separator() {
        assert_eq!(
            composed_probe_keys("std::collections::HashMap", "::", "new"),
            [
                "std::collections::HashMap::new",
                "std::collections::new",
                "std::new",
            ]
        );
    }

    #[test]
    fn climb_matches_rfind_exhaustive_small_alphabet() {
        // Brute force every string over {a, :, .} up to length 8 against both
        // separators; the `:`-dense inputs stress overlapping `::` matches.
        fn recurse(buf: &mut String, remaining: usize) {
            if !buf.is_empty() {
                assert_climb_matches(buf, ".");
                assert_climb_matches(buf, "::");
            }
            if remaining == 0 {
                return;
            }
            for c in ['a', ':', '.'] {
                buf.push(c);
                recurse(buf, remaining - 1);
                buf.pop();
            }
        }
        recurse(&mut String::new(), 8);
    }

    #[test]
    fn dir_of_nested_path() {
        assert_eq!(dir_of("modules/vpc/main.tf"), "modules/vpc");
    }

    #[test]
    fn dir_of_root_file() {
        assert_eq!(dir_of("main.tf"), "");
    }

    #[test]
    fn dir_of_empty() {
        assert_eq!(dir_of(""), "");
    }

    #[test]
    fn dir_of_trailing_slash() {
        assert_eq!(dir_of("a/b/"), "a/b");
    }
}
