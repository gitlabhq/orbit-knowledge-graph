//! Resolution engine: resolves references to graph edges.
//!
//! Operates on `&CodeGraph` (the single source of truth) plus
//! `&[CanonicalResult]` (for reference data that isn't in the graph).
//! All definition/import lookups go through `CodeGraph`'s indexes.

use code_graph_types::{
    CanonicalImport, CanonicalResult, EdgeKind, ExpressionStep, IStr, NodeKind, Relationship,
};
use indicatif::{ProgressBar, ProgressStyle};
use petgraph::graph::NodeIndex;
use rayon::prelude::*;
use smallvec::{SmallVec, smallvec};

use super::graph::{CodeGraph, GraphEdge};
use super::rules::{ImportStrategy, ResolutionRules};
use super::ssa::{SsaResolver, SsaStats, Value};
use super::walker::{FileWalkResult, RecordedRead};

// ── ResolveSettings ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ResolveSettings {
    pub per_file_timeout: Option<std::time::Duration>,
    pub max_chain_depth: usize,
    pub slow_ref_threshold: Option<std::time::Duration>,
    pub chain_fallback: bool,
    pub compound_key_recovery: bool,
    pub implicit_this_on_base: bool,
}

impl Default for ResolveSettings {
    fn default() -> Self {
        Self {
            per_file_timeout: None,
            max_chain_depth: 10,
            slow_ref_threshold: Some(std::time::Duration::from_millis(100)),
            chain_fallback: true,
            compound_key_recovery: true,
            implicit_this_on_base: true,
        }
    }
}

// ── Stats ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct ResolveStats {
    pub bare_refs: u64,
    pub chain_refs: u64,
    pub bare_ssa_resolved: u64,
    pub bare_ssa_def: u64,
    pub bare_ssa_import: u64,
    pub bare_ssa_type: u64,
    pub bare_early_exit_unknown: u64,
    pub bare_import_resolved: u64,
    pub bare_implicit_this_resolved: u64,
    pub bare_unresolved: u64,
    pub chain_resolved: u64,
    pub chain_fallback_fired: u64,
    pub chain_fallback_resolved: u64,
    pub chain_mid_break: u64,
    pub chain_compound_key_recovered: u64,
    pub chain_base_ident: u64,
    pub chain_base_this: u64,
    pub chain_base_super: u64,
    pub chain_base_new: u64,
    pub chain_base_other: u64,
    pub edges_from_bare_ssa: u64,
    pub edges_from_bare_import: u64,
    pub edges_from_bare_implicit: u64,
    pub edges_from_chain: u64,
    pub edges_from_chain_fallback: u64,
    pub timed_out_files: u64,
    pub timed_out_refs: u64,
    pub ssa: SsaStats,
}

impl ResolveStats {
    pub fn merge(&mut self, other: &ResolveStats) {
        self.bare_refs += other.bare_refs;
        self.chain_refs += other.chain_refs;
        self.bare_ssa_resolved += other.bare_ssa_resolved;
        self.bare_ssa_def += other.bare_ssa_def;
        self.bare_ssa_import += other.bare_ssa_import;
        self.bare_ssa_type += other.bare_ssa_type;
        self.bare_early_exit_unknown += other.bare_early_exit_unknown;
        self.bare_import_resolved += other.bare_import_resolved;
        self.bare_implicit_this_resolved += other.bare_implicit_this_resolved;
        self.bare_unresolved += other.bare_unresolved;
        self.chain_resolved += other.chain_resolved;
        self.chain_fallback_fired += other.chain_fallback_fired;
        self.chain_fallback_resolved += other.chain_fallback_resolved;
        self.chain_mid_break += other.chain_mid_break;
        self.chain_compound_key_recovered += other.chain_compound_key_recovered;
        self.chain_base_ident += other.chain_base_ident;
        self.chain_base_this += other.chain_base_this;
        self.chain_base_super += other.chain_base_super;
        self.chain_base_new += other.chain_base_new;
        self.chain_base_other += other.chain_base_other;
        self.edges_from_bare_ssa += other.edges_from_bare_ssa;
        self.edges_from_bare_import += other.edges_from_bare_import;
        self.edges_from_bare_implicit += other.edges_from_bare_implicit;
        self.edges_from_chain += other.edges_from_chain;
        self.edges_from_chain_fallback += other.edges_from_chain_fallback;
        self.timed_out_files += other.timed_out_files;
        self.timed_out_refs += other.timed_out_refs;
        self.ssa.merge(&other.ssa);
    }

    pub fn print(&self) {
        let total_refs = self.bare_refs + self.chain_refs;
        let total_edges = self.edges_from_bare_ssa
            + self.edges_from_bare_import
            + self.edges_from_bare_implicit
            + self.edges_from_chain
            + self.edges_from_chain_fallback;

        eprintln!("\n[v2] Resolution stats:");
        eprintln!(
            "  References: {} total ({} bare, {} chain)",
            total_refs, self.bare_refs, self.chain_refs
        );

        if self.bare_refs > 0 {
            eprintln!("  Bare resolution:");
            eprintln!(
                "    SSA resolved:       {:>8} ({:.1}%)",
                self.bare_ssa_resolved,
                pct(self.bare_ssa_resolved, self.bare_refs)
            );
            eprintln!("      via Def:          {:>8}", self.bare_ssa_def);
            eprintln!("      via Import:       {:>8}", self.bare_ssa_import);
            eprintln!("      via Type:         {:>8}", self.bare_ssa_type);
            eprintln!(
                "    Early exit unknown: {:>8} ({:.1}%)",
                self.bare_early_exit_unknown,
                pct(self.bare_early_exit_unknown, self.bare_refs)
            );
            eprintln!(
                "    Import fallback:    {:>8} ({:.1}%)",
                self.bare_import_resolved,
                pct(self.bare_import_resolved, self.bare_refs)
            );
            eprintln!(
                "    Implicit this:      {:>8} ({:.1}%)",
                self.bare_implicit_this_resolved,
                pct(self.bare_implicit_this_resolved, self.bare_refs)
            );
            eprintln!(
                "    Unresolved:         {:>8} ({:.1}%)",
                self.bare_unresolved,
                pct(self.bare_unresolved, self.bare_refs)
            );
        }

        if self.chain_refs > 0 {
            eprintln!("  Chain resolution:");
            eprintln!(
                "    Resolved (walk):    {:>8} ({:.1}%)",
                self.chain_resolved,
                pct(self.chain_resolved, self.chain_refs)
            );
            eprintln!(
                "    Fallback fired:     {:>8} ({:.1}%)",
                self.chain_fallback_fired,
                pct(self.chain_fallback_fired, self.chain_refs)
            );
            eprintln!(
                "    Fallback resolved:  {:>8} ({:.1}%)",
                self.chain_fallback_resolved,
                pct(self.chain_fallback_resolved, self.chain_refs)
            );
            eprintln!(
                "    Mid-chain break:    {:>8} ({:.1}%)",
                self.chain_mid_break,
                pct(self.chain_mid_break, self.chain_refs)
            );
            eprintln!(
                "    Compound key saves: {:>8}",
                self.chain_compound_key_recovered
            );
            eprintln!(
                "    Base type: ident={} this={} super={} new={} other={}",
                self.chain_base_ident,
                self.chain_base_this,
                self.chain_base_super,
                self.chain_base_new,
                self.chain_base_other
            );
        }

        eprintln!("  Edges: {} total", total_edges);
        if total_edges > 0 {
            eprintln!(
                "    from bare SSA:      {:>8} ({:.1}%)",
                self.edges_from_bare_ssa,
                pct(self.edges_from_bare_ssa, total_edges)
            );
            eprintln!(
                "    from bare import:   {:>8} ({:.1}%)",
                self.edges_from_bare_import,
                pct(self.edges_from_bare_import, total_edges)
            );
            eprintln!(
                "    from bare implicit: {:>8} ({:.1}%)",
                self.edges_from_bare_implicit,
                pct(self.edges_from_bare_implicit, total_edges)
            );
            eprintln!(
                "    from chain walk:    {:>8} ({:.1}%)",
                self.edges_from_chain,
                pct(self.edges_from_chain, total_edges)
            );
            eprintln!(
                "    from chain fallback:{:>8} ({:.1}%)",
                self.edges_from_chain_fallback,
                pct(self.edges_from_chain_fallback, total_edges)
            );
        }

        if self.timed_out_files > 0 {
            eprintln!(
                "  Timeouts: {} files, {} refs skipped",
                self.timed_out_files, self.timed_out_refs
            );
        }

        eprintln!("  SSA:");
        eprintln!("    reads:              {:>8}", self.ssa.reads);
        eprintln!("    writes:             {:>8}", self.ssa.writes);
        eprintln!(
            "    local hits:         {:>8} ({:.1}%)",
            self.ssa.local_hits,
            pct(self.ssa.local_hits, self.ssa.reads + self.ssa.local_hits)
        );
        eprintln!("    recursive lookups:  {:>8}", self.ssa.recursive_lookups);
        eprintln!("    dead ends:          {:>8}", self.ssa.dead_end_hits);
        eprintln!("    unsealed hits:      {:>8}", self.ssa.unsealed_hits);
        eprintln!("    phis created:       {:>8}", self.ssa.phis_created);
        eprintln!(
            "    phis trivial:       {:>8} ({:.1}%)",
            self.ssa.phis_trivial,
            pct(self.ssa.phis_trivial, self.ssa.phis_created)
        );
        eprintln!("    blocks created:     {:>8}", self.ssa.blocks_created);
    }
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 / d as f64 * 100.0
    }
}

pub trait HasRules {
    fn rules() -> ResolutionRules;
}

// ── Import strategies ───────────────────────────────────────────

fn apply_import_strategies(
    strategies: &[ImportStrategy],
    graph: &CodeGraph,
    results: &[CanonicalResult],
    file_idx: usize,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    let result = &results[file_idx];
    for strategy in strategies {
        let candidates = match strategy {
            ImportStrategy::ScopeFqnWalk => scope_fqn_walk(graph, result, name, sep),
            ImportStrategy::ExplicitImport => explicit_import(graph, results, file_idx, name, sep),
            ImportStrategy::WildcardImport => wildcard_import(graph, results, file_idx, name, sep),
            ImportStrategy::SamePackage => same_package(graph, result, name, sep),
            ImportStrategy::SameFile => same_file(graph, results, file_idx, name),
            ImportStrategy::FilePath => vec![],
        };
        if !candidates.is_empty() {
            return candidates;
        }
    }
    vec![]
}

fn resolve_import(graph: &CodeGraph, import: &CanonicalImport, sep: &str) -> Vec<NodeIndex> {
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
        return by_fqn.to_vec();
    }

    if !import.path.is_empty() {
        let by_path = graph.lookup_fqn(&import.path);
        if !by_path.is_empty() {
            return by_path.to_vec();
        }
    }
    vec![]
}

fn scope_fqn_walk(
    graph: &CodeGraph,
    result: &CanonicalResult,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    for def in &result.definitions {
        if def.is_top_level {
            let candidate = format!("{}{}{}", def.fqn, sep, name);
            let matches = graph.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }
    for def in &result.definitions {
        let fqn_str = def.fqn.to_string();
        let mut current = fqn_str.as_str();
        loop {
            let candidate = format!("{}{}{}", current, sep, name);
            let matches = graph.lookup_fqn(&candidate);
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

fn explicit_import(
    graph: &CodeGraph,
    results: &[CanonicalResult],
    file_idx: usize,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    for imp in &results[file_idx].imports {
        let imp_name = imp.alias.as_deref().or(imp.name.as_deref()).unwrap_or("");
        if imp_name == name {
            let defs = resolve_import(graph, imp, sep);
            if !defs.is_empty() {
                return defs;
            }
        }
    }
    vec![]
}

fn wildcard_import(
    graph: &CodeGraph,
    results: &[CanonicalResult],
    file_idx: usize,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    for imp in &results[file_idx].imports {
        if imp.wildcard {
            let candidate = format!("{}{}{}", imp.path, sep, name);
            let matches = graph.lookup_fqn(&candidate);
            if !matches.is_empty() {
                return matches.to_vec();
            }
        }
    }
    vec![]
}

fn same_package(
    graph: &CodeGraph,
    result: &CanonicalResult,
    name: &str,
    sep: &str,
) -> Vec<NodeIndex> {
    for def in &result.definitions {
        if def.is_top_level {
            let fqn_str = def.fqn.to_string();
            if let Some(sep_pos) = fqn_str.rfind(sep) {
                let candidate = format!("{}{}{}", &fqn_str[..sep_pos], sep, name);
                let matches = graph.lookup_fqn(&candidate);
                if !matches.is_empty() {
                    return matches.to_vec();
                }
            }
        }
    }
    vec![]
}

fn same_file(
    graph: &CodeGraph,
    results: &[CanonicalResult],
    file_idx: usize,
    name: &str,
) -> Vec<NodeIndex> {
    let file_path = graph.relative_path(&results[file_idx].file_path);
    let file_defs = graph.file_defs(&file_path);

    let by_fqn: Vec<NodeIndex> = graph
        .lookup_fqn(name)
        .iter()
        .filter(|idx| file_defs.contains(idx))
        .copied()
        .collect();
    if !by_fqn.is_empty() {
        return by_fqn;
    }

    graph
        .lookup_name(name)
        .iter()
        .filter(|idx| file_defs.contains(idx))
        .copied()
        .collect()
}

// ── build_edges ─────────────────────────────────────────────────

struct FileTimingEntry {
    file_idx: usize,
    num_reads: usize,
    duration: std::time::Duration,
    thread_id: usize,
}

pub struct BuildEdgesResult {
    pub edges: Vec<(NodeIndex, NodeIndex, GraphEdge)>,
    pub stats: ResolveStats,
}

pub fn build_edges(
    rules: &ResolutionRules,
    graph: &CodeGraph,
    results: &[CanonicalResult],
    walks: &mut [FileWalkResult],
    settings: &ResolveSettings,
) -> BuildEdgesResult {
    let total_reads: u64 = walks.iter().map(|w| w.reads.len() as u64).sum();
    let pb = ProgressBar::new(total_reads);
    pb.set_style(
        ProgressStyle::with_template("Resolving [{bar:40}] {pos}/{len} ({per_sec}, {eta})")
            .unwrap()
            .progress_chars("█▓░"),
    );

    #[allow(clippy::type_complexity)]
    let per_file: Vec<(Vec<(NodeIndex, NodeIndex, GraphEdge)>, ResolveStats, FileTimingEntry)> = walks
        .par_iter_mut()
        .map(|walk| {
            let file_start = std::time::Instant::now();
            let deadline = settings.per_file_timeout.map(|d| file_start + d);
            let reads = std::mem::take(&mut walk.reads);
            let num_reads = reads.len();
            let file_idx = reads.first().map(|r| r.file_idx).unwrap_or(0);
            let file_path = graph.relative_path(&results[file_idx].file_path);
            let file_node = graph.file_index.get(&file_path).copied().unwrap_or(NodeIndex::new(0));
            let thread_id = rayon::current_thread_index().unwrap_or(0);
            let mut resolver = Resolver::new(rules, graph, results, settings, &mut walk.ssa);
            let mut file_edges: Vec<(NodeIndex, NodeIndex, GraphEdge)> = Vec::new();

            for (resolved_count, read) in reads.iter().enumerate() {
                if let Some(dl) = deadline
                    && std::time::Instant::now() > dl
                {
                    let skipped = (num_reads - resolved_count) as u64;
                    resolver.stats.timed_out_files = 1;
                    resolver.stats.timed_out_refs = skipped;
                    pb.suspend(|| {
                        eprintln!("\x1b[33m[TIMEOUT] {} after {:.2?} ({} refs resolved, {} skipped)\x1b[0m",
                            file_path, file_start.elapsed(), resolved_count, skipped);
                    });
                    pb.inc(skipped);
                    break;
                }

                let result = &results[read.file_idx];
                let reference = &result.references[read.ref_idx];

                let t = std::time::Instant::now();
                let (resolved_defs, path) = if let Some(ref chain) = reference.expression {
                    resolver.stats.chain_refs += 1;
                    let defs = resolver.resolve_chain(read, chain);
                    (defs, resolver.last_chain_path)
                } else {
                    resolver.stats.bare_refs += 1;
                    let defs = resolver.resolve_bare(read);
                    (defs, resolver.last_bare_path)
                };
                let elapsed = t.elapsed();
                if let Some(threshold) = settings.slow_ref_threshold
                    && elapsed >= threshold
                {
                    pb.suspend(|| {
                        eprintln!("\x1b[31m[SLOW] {:.2?} resolving '{}' in {} (chain: {})\x1b[0m",
                            elapsed, reference.name, result.file_path, reference.expression.is_some());
                    });
                }

                let (source_idx, source_node_kind, source_def_kind) = match read.enclosing_def {
                    Some(def_node) => {
                        let def = graph.def(def_node);
                        (def_node, NodeKind::Definition, Some(def.kind))
                    }
                    None => (file_node, NodeKind::File, None),
                };

                let edge_count = resolved_defs.len() as u64;
                for target_idx in resolved_defs {
                    let target_def = graph.def(target_idx);
                    file_edges.push((
                        source_idx, target_idx,
                        GraphEdge {
                            relationship: Relationship {
                                edge_kind: EdgeKind::Calls,
                                source_node: source_node_kind,
                                target_node: NodeKind::Definition,
                                source_def_kind,
                                target_def_kind: Some(target_def.kind),
                            },
                            source_definition_range: None,
                            target_definition_range: None,
                        },
                    ));
                }

                match path {
                    ResolvePath::BareSsa => resolver.stats.edges_from_bare_ssa += edge_count,
                    ResolvePath::BareImport => resolver.stats.edges_from_bare_import += edge_count,
                    ResolvePath::BareImplicit => resolver.stats.edges_from_bare_implicit += edge_count,
                    ResolvePath::Chain => resolver.stats.edges_from_chain += edge_count,
                    ResolvePath::ChainFallback => resolver.stats.edges_from_chain_fallback += edge_count,
                    ResolvePath::None => {}
                }
                pb.inc(1);
            }

            resolver.stats.ssa.merge(&resolver.ssa.stats);
            let stats = std::mem::take(&mut resolver.stats);
            let timing = FileTimingEntry { file_idx, num_reads, duration: file_start.elapsed(), thread_id };
            (file_edges, stats, timing)
        })
        .collect();

    pb.finish_and_clear();

    let mut all_edges = Vec::new();
    let mut combined = ResolveStats::default();
    let mut timings = Vec::with_capacity(per_file.len());
    for (edges, stats, timing) in per_file {
        all_edges.extend(edges);
        combined.merge(&stats);
        timings.push(timing);
    }

    print_long_tail_analysis(results, &timings);
    BuildEdgesResult {
        edges: all_edges,
        stats: combined,
    }
}

fn print_long_tail_analysis(results: &[CanonicalResult], timings: &[FileTimingEntry]) {
    if timings.is_empty() {
        return;
    }

    let mut ref_counts: Vec<usize> = timings.iter().map(|t| t.num_reads).collect();
    ref_counts.sort_unstable();
    let total_files = ref_counts.len();
    let total_refs: usize = ref_counts.iter().sum();
    let p50 = ref_counts[total_files / 2];
    let p95 = ref_counts[total_files * 95 / 100];
    let p99 = ref_counts[total_files * 99 / 100];
    let max = *ref_counts.last().unwrap();
    let mean = total_refs / total_files;
    let files_over_1k = ref_counts.iter().filter(|&&c| c > 1000).count();

    eprintln!("  Ref distribution ({total_files} files):");
    eprintln!("    mean={mean} p50={p50} p95={p95} p99={p99} max={max} >1k={files_over_1k}");

    let mut by_duration: Vec<&FileTimingEntry> = timings.iter().collect();
    by_duration.sort_by(|a, b| b.duration.cmp(&a.duration));

    eprintln!("  Top 10 slowest files:");
    for entry in by_duration.iter().take(10) {
        let path = &results[entry.file_idx].file_path;
        let display = if path.len() > 60 {
            &path[path.len() - 60..]
        } else {
            path
        };
        eprintln!(
            "    {:>7.1?} {:>5} refs  t{:<2}  {}",
            entry.duration, entry.num_reads, entry.thread_id, display
        );
    }

    let num_threads = timings.iter().map(|t| t.thread_id).max().unwrap_or(0) + 1;
    let mut per_thread_total = vec![std::time::Duration::ZERO; num_threads];
    let mut per_thread_files = vec![0u32; num_threads];
    for t in timings {
        per_thread_total[t.thread_id] += t.duration;
        per_thread_files[t.thread_id] += 1;
    }
    let wall_clock = by_duration.first().map(|e| e.duration).unwrap_or_default();
    let total_cpu: std::time::Duration = per_thread_total.iter().sum();
    eprintln!(
        "  Thread utilization ({} threads, {:.2?} wall, {:.2?} CPU):",
        num_threads, wall_clock, total_cpu
    );
    for (tid, (total, files)) in per_thread_total
        .iter()
        .zip(per_thread_files.iter())
        .enumerate()
    {
        let util = if wall_clock.as_nanos() > 0 {
            total.as_nanos() as f64 / wall_clock.as_nanos() as f64 * 100.0
        } else {
            0.0
        };
        eprintln!(
            "    t{:<2}: {:>7.1?} ({:>5.1}%) {:>5} files",
            tid, total, util, files
        );
    }
}

// ── Resolver ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum ResolvePath {
    None,
    BareSsa,
    BareImport,
    BareImplicit,
    Chain,
    ChainFallback,
}

struct Resolver<'a> {
    rules: &'a ResolutionRules,
    graph: &'a CodeGraph,
    results: &'a [CanonicalResult],
    settings: &'a ResolveSettings,
    ssa: &'a mut SsaResolver,
    sep: &'a str,
    buf: String,
    pub stats: ResolveStats,
    pub last_bare_path: ResolvePath,
    pub last_chain_path: ResolvePath,
}

impl<'a> Resolver<'a> {
    fn new(
        rules: &'a ResolutionRules,
        graph: &'a CodeGraph,
        results: &'a [CanonicalResult],
        settings: &'a ResolveSettings,
        ssa: &'a mut SsaResolver,
    ) -> Self {
        Self {
            sep: rules.fqn_separator,
            rules,
            graph,
            results,
            settings,
            ssa,
            buf: String::with_capacity(128),
            stats: ResolveStats::default(),
            last_bare_path: ResolvePath::None,
            last_chain_path: ResolvePath::None,
        }
    }

    fn def_to_types(&self, def: &code_graph_types::CanonicalDefinition) -> SmallVec<[IStr; 2]> {
        if def.kind.is_type_container() {
            smallvec![def.fqn.as_istr()]
        } else if let Some(meta) = &def.metadata
            && let Some(rt) = &meta.return_type
        {
            smallvec![IStr::from(rt.as_str())]
        } else {
            SmallVec::new()
        }
    }

    fn resolve_aliases(
        &mut self,
        values: &[Value],
        block: super::ssa::BlockId,
    ) -> SmallVec<[Value; 4]> {
        let mut out = SmallVec::new();
        for v in values {
            match v {
                Value::Alias(name) => {
                    let reaching = self.ssa.read_variable_stateless(name, block);
                    for av in &reaching.values {
                        out.push(av.clone());
                    }
                }
                other => out.push(other.clone()),
            }
        }
        out
    }

    fn value_to_types(&mut self, value: &Value) -> SmallVec<[IStr; 2]> {
        match value {
            Value::Type(t) => smallvec![*t],
            Value::Def(idx) => {
                let def = self.graph.def(*idx);
                self.def_to_types(def)
            }
            Value::Import(idx) => {
                let import = self.graph.import(*idx);
                let defs = resolve_import(self.graph, import, self.sep);
                defs.iter()
                    .flat_map(|&di| {
                        let def = self.graph.def(di);
                        self.def_to_types(def)
                    })
                    .collect()
            }
            _ => SmallVec::new(),
        }
    }

    // ── Bare resolution ─────────────────────────────────────

    fn resolve_bare(&mut self, read: &RecordedRead) -> Vec<NodeIndex> {
        use super::rules::ResolveStage;
        self.last_bare_path = ResolvePath::None;

        for stage in &self.rules.bare_stages {
            let result = match stage {
                ResolveStage::SSA => self.resolve_bare_ssa(read),
                ResolveStage::ImportStrategies => {
                    if self.graph.lookup_name(&read.name).is_empty() {
                        self.stats.bare_early_exit_unknown += 1;
                        continue;
                    }
                    let r = apply_import_strategies(
                        &self.rules.import_strategies,
                        self.graph,
                        self.results,
                        read.file_idx,
                        &read.name,
                        self.sep,
                    );
                    if !r.is_empty() {
                        self.stats.bare_import_resolved += 1;
                        self.last_bare_path = ResolvePath::BareImport;
                    }
                    r
                }
                ResolveStage::ImplicitMember => {
                    let mut r = Vec::new();
                    if let Some(type_fqn) = &read.enclosing_type_fqn
                        && self
                            .graph
                            .lookup_member_with_supers(type_fqn, &read.name, &mut r)
                    {
                        self.stats.bare_implicit_this_resolved += 1;
                        self.last_bare_path = ResolvePath::BareImplicit;
                    }
                    r
                }
            };

            if !result.is_empty() {
                let mut result = result;
                dedup(&mut result);
                return result;
            }
        }

        self.stats.bare_unresolved += 1;
        vec![]
    }

    fn resolve_bare_ssa(&mut self, read: &RecordedRead) -> Vec<NodeIndex> {
        let reaching = self.ssa.read_variable_stateless(&read.name, read.block);
        let mut result = Vec::new();

        for value in &reaching.values {
            match value {
                Value::Def(idx) => {
                    self.stats.bare_ssa_def += 1;
                    result.push(*idx);
                }
                Value::Import(idx) => {
                    self.stats.bare_ssa_import += 1;
                    let import = self.graph.import(*idx);
                    result.extend(resolve_import(self.graph, import, self.sep));
                }
                Value::Type(type_name) => {
                    self.stats.bare_ssa_type += 1;
                    self.graph
                        .lookup_member_with_supers(type_name, &read.name, &mut result);
                }
                Value::Alias(name) => {
                    let alias_reaching = self.ssa.read_variable_stateless(name, read.block);
                    for av in &alias_reaching.values {
                        match av {
                            Value::Def(idx) => {
                                result.push(*idx);
                            }
                            Value::Import(idx) => {
                                let import = self.graph.import(*idx);
                                result.extend(resolve_import(self.graph, import, self.sep));
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if !result.is_empty() {
            self.stats.bare_ssa_resolved += 1;
            self.last_bare_path = ResolvePath::BareSsa;
        }
        result
    }

    // ── Chain resolution ────────────────────────────────────

    fn resolve_chain(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<NodeIndex> {
        self.last_chain_path = ResolvePath::None;
        if chain.is_empty() {
            return vec![];
        }

        let max_depth = self.settings.max_chain_depth;
        let effective_chain = if chain.len() > max_depth {
            &chain[chain.len() - max_depth..]
        } else {
            chain
        };

        match &effective_chain[0] {
            ExpressionStep::Ident(_) => self.stats.chain_base_ident += 1,
            ExpressionStep::This => self.stats.chain_base_this += 1,
            ExpressionStep::Super => self.stats.chain_base_super += 1,
            ExpressionStep::New(_) => self.stats.chain_base_new += 1,
            _ => self.stats.chain_base_other += 1,
        }

        let enclosing_str = read.enclosing_type_fqn.as_ref().map(|s| s.as_ref());
        let mut current_types = self.resolve_base(&effective_chain[0], read.block, enclosing_str);

        if current_types.is_empty() {
            if self.settings.chain_fallback {
                self.stats.chain_fallback_fired += 1;
                let result = self.chain_fallback(read, chain);
                if !result.is_empty() {
                    self.stats.chain_fallback_resolved += 1;
                    self.last_chain_path = ResolvePath::ChainFallback;
                }
                return result;
            }
            return vec![];
        }

        let mut compound_key = if self.settings.compound_key_recovery {
            self.compound_key_base(&effective_chain[0])
        } else {
            String::new()
        };

        for (i, step) in effective_chain[1..].iter().enumerate() {
            let is_last = i == effective_chain.len() - 2;
            let member_name = match step {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => n,
                _ => continue,
            };

            let (mut next_types, found_members) = self.walk_step(&current_types, step, member_name);

            if is_last && !found_members.is_empty() {
                self.stats.chain_resolved += 1;
                self.last_chain_path = ResolvePath::Chain;
                let mut result = found_members;
                dedup(&mut result);
                return result;
            }

            if next_types.is_empty() && found_members.is_empty() {
                let recovered = self.compound_key_step(&mut compound_key, member_name, read.block);
                if !recovered.is_empty() {
                    self.stats.chain_compound_key_recovered += 1;
                    current_types = recovered;
                    continue;
                }
            } else {
                compound_key.clear();
            }

            {
                let mut seen = rustc_hash::FxHashSet::default();
                next_types.retain(|t| seen.insert(*t));
            }
            current_types = next_types;
            if current_types.is_empty() {
                self.stats.chain_mid_break += 1;
                break;
            }
        }
        vec![]
    }

    fn resolve_base(
        &mut self,
        step: &ExpressionStep,
        block: super::ssa::BlockId,
        enclosing: Option<&str>,
    ) -> SmallVec<[IStr; 2]> {
        match step {
            ExpressionStep::Ident(name) | ExpressionStep::Call(name) => {
                let reaching = self.ssa.read_variable_stateless(name, block);
                let values = self.resolve_aliases(&reaching.values, block);
                let mut types: SmallVec<[IStr; 2]> =
                    values.iter().flat_map(|v| self.value_to_types(v)).collect();

                if types.is_empty()
                    && self.settings.implicit_this_on_base
                    && self
                        .rules
                        .bare_stages
                        .contains(&super::rules::ResolveStage::ImplicitMember)
                    && let Some(fqn) = enclosing
                {
                    let mut members = Vec::new();
                    self.graph
                        .lookup_member_with_supers(fqn, name, &mut members);
                    for &def_idx in &members {
                        let def = self.graph.def(def_idx);
                        types.extend(self.def_to_types(def));
                    }
                }
                types
            }
            ExpressionStep::This => enclosing
                .map(|fqn| smallvec![IStr::from(fqn)])
                .unwrap_or_default(),
            ExpressionStep::Super => self
                .rules
                .super_name
                .map(|name| {
                    let reaching = self.ssa.read_variable_stateless(name, block);
                    reaching
                        .values
                        .iter()
                        .filter_map(|v| match v {
                            Value::Type(t) => Some(*t),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            ExpressionStep::New(type_name) => smallvec![IStr::from(type_name.as_ref())],
            _ => SmallVec::new(),
        }
    }

    fn walk_step(
        &mut self,
        current_types: &[IStr],
        step: &ExpressionStep,
        member_name: &str,
    ) -> (SmallVec<[IStr; 2]>, Vec<NodeIndex>) {
        let mut next_types = SmallVec::new();
        let mut found_members = Vec::new();

        for type_name in current_types {
            let before = found_members.len();
            self.graph
                .lookup_member_with_supers(type_name, member_name, &mut found_members);
            for &def_idx in &found_members[before..] {
                let def = self.graph.def(def_idx);
                if matches!(step, ExpressionStep::Call(_)) {
                    if let Some(meta) = &def.metadata
                        && let Some(rt) = &meta.return_type
                    {
                        next_types.push(IStr::from(rt.as_str()));
                    }
                    if matches!(
                        def.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Constructor
                    ) {
                        next_types.push(def.fqn.as_istr());
                    }
                }
                if matches!(step, ExpressionStep::Field(_))
                    && let Some(meta) = &def.metadata
                    && let Some(ta) = &meta.type_annotation
                {
                    next_types.push(IStr::from(ta.as_str()));
                }
            }
        }
        (next_types, found_members)
    }

    fn compound_key_base(&mut self, step: &ExpressionStep) -> String {
        match step {
            ExpressionStep::Ident(n) => n.clone(),
            ExpressionStep::This => self
                .rules
                .self_names
                .first()
                .map(|s| s.to_string())
                .unwrap_or_default(),
            ExpressionStep::Super => self
                .rules
                .super_name
                .map(|s| s.to_string())
                .unwrap_or_default(),
            _ => String::new(),
        }
    }

    fn compound_key_step(
        &mut self,
        compound_key: &mut String,
        member_name: &str,
        block: super::ssa::BlockId,
    ) -> SmallVec<[IStr; 2]> {
        if compound_key.is_empty() {
            return SmallVec::new();
        }
        self.buf.clear();
        self.buf.push_str(compound_key);
        self.buf.push_str(self.sep);
        self.buf.push_str(member_name);
        std::mem::swap(compound_key, &mut self.buf);
        let reaching = self.ssa.read_variable_stateless(compound_key, block);
        reaching
            .values
            .iter()
            .flat_map(|v| self.value_to_types(v))
            .collect()
    }

    fn chain_fallback(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<NodeIndex> {
        let last = match chain.last() {
            Some(ExpressionStep::Call(n) | ExpressionStep::Field(n)) => n,
            _ => return vec![],
        };
        let bare_read = RecordedRead {
            file_idx: read.file_idx,
            ref_idx: read.ref_idx,
            block: read.block,
            name: IStr::from(last.as_str()),
            enclosing_def: read.enclosing_def,
            enclosing_type_fqn: read.enclosing_type_fqn,
        };
        self.resolve_bare(&bare_read)
    }
}

fn dedup(result: &mut Vec<NodeIndex>) {
    if result.len() <= 4 {
        let mut i = 0;
        while i < result.len() {
            let mut j = i + 1;
            while j < result.len() {
                if result[j] == result[i] {
                    result.swap_remove(j);
                } else {
                    j += 1;
                }
            }
            i += 1;
        }
    } else {
        let mut seen = rustc_hash::FxHashSet::default();
        result.retain(|r| seen.insert(*r));
    }
}
