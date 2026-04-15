//! Edge builder: converts per-file SSA walk results into resolved call edges.
//!
//! Takes per-file `FileWalkResult`s (SSA graph + recorded reads) and the
//! `ResolutionContext` (indexes), resolves each reference read to concrete
//! definitions, and produces `ResolvedEdge`s for the graph.

use code_graph_types::{EdgeKind, ExpressionStep, IStr, NodeKind, Relationship};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use smallvec::{SmallVec, smallvec};

use super::context::{DefRef, ResolutionContext};

/// Maximum chain depth before we stop walking and resolve only the last step.
/// Fluent builder chains (e.g. `builder.startObject().field().endObject()`)
/// can be 30+ steps. Beyond this depth, the call graph value is minimal
/// and the cost is linear in chain length.
const MAX_CHAIN_DEPTH: usize = 10;
use super::edges::{EdgeSource, ResolvedEdge};
use super::imports;
use super::rules::ResolutionRules;
use super::ssa::{SsaResolver, SsaStats, Value};
use super::walker::{FileWalkResult, RecordedRead};

// ── Resolution statistics ───────────────────────────────────────

/// Per-file resolution counters, aggregated after parallel resolution.
#[derive(Debug, Clone, Default)]
pub struct ResolveStats {
    // ── Top-level reference classification ──
    pub bare_refs: u64,
    pub chain_refs: u64,

    // ── Bare resolution tiers ──
    /// SSA produced at least one result (Def, Import, or Type).
    pub bare_ssa_resolved: u64,
    /// SSA resolved via Value::Def.
    pub bare_ssa_def: u64,
    /// SSA resolved via Value::Import.
    pub bare_ssa_import: u64,
    /// SSA resolved via Value::Type (member lookup on typed variable).
    pub bare_ssa_type: u64,
    /// Name not in DefinitionIndex — skipped all fallbacks.
    pub bare_early_exit_unknown: u64,
    /// Tier 2: import strategies produced results.
    pub bare_import_resolved: u64,
    /// Tier 3: implicit member lookup produced results.
    pub bare_implicit_this_resolved: u64,
    /// All tiers failed — zero results.
    pub bare_unresolved: u64,

    // ── Chain resolution paths ──
    /// Chain walk resolved at the last step (normal success).
    pub chain_resolved: u64,
    /// Chain base couldn't resolve → fell back to resolve_bare on last step.
    pub chain_fallback_fired: u64,
    /// chain_fallback produced results.
    pub chain_fallback_resolved: u64,
    /// Chain broke mid-walk (types went empty before last step).
    pub chain_mid_break: u64,
    /// Compound key SSA fallback recovered types mid-chain.
    pub chain_compound_key_recovered: u64,

    // ── Chain base classification ──
    pub chain_base_ident: u64,
    pub chain_base_this: u64,
    pub chain_base_super: u64,
    pub chain_base_new: u64,
    pub chain_base_other: u64,

    // ── Edge counts by source path ──
    pub edges_from_bare_ssa: u64,
    pub edges_from_bare_import: u64,
    pub edges_from_bare_implicit: u64,
    pub edges_from_chain: u64,
    pub edges_from_chain_fallback: u64,

    // ── SSA stats (aggregated from per-file SsaResolvers) ──
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

/// Trait to get rules from the type parameter.
pub trait HasRules {
    fn rules() -> ResolutionRules;
}

/// Per-file timing for long-tail analysis.
struct FileTimingEntry {
    file_idx: usize,
    num_reads: usize,
    duration: std::time::Duration,
    thread_id: usize,
}

/// Result of `build_edges`: resolved edges + aggregated stats.
pub struct BuildEdgesResult {
    pub edges: Vec<ResolvedEdge>,
    pub stats: ResolveStats,
}

/// Build edges from per-file walk results. This is the pipeline's resolve stage.
///
/// For each file's recorded reads, resolves references to concrete definitions
/// via SSA values, import strategies, and expression chain walking.
pub fn build_edges(
    rules: &ResolutionRules,
    ctx: &ResolutionContext,
    walks: &mut [FileWalkResult],
) -> BuildEdgesResult {
    let total_reads: u64 = walks.iter().map(|w| w.reads.len() as u64).sum();
    let pb = ProgressBar::new(total_reads);
    pb.set_style(
        ProgressStyle::with_template("Resolving [{bar:40}] {pos}/{len} ({per_sec}, {eta})")
            .unwrap()
            .progress_chars("█▓░"),
    );

    let per_file: Vec<(Vec<ResolvedEdge>, ResolveStats, FileTimingEntry)> = walks
        .par_iter_mut()
        .map(|walk| {
            let file_start = std::time::Instant::now();
            let reads = std::mem::take(&mut walk.reads);
            let num_reads = reads.len();
            let file_idx = reads.first().map(|r| r.file_idx).unwrap_or(0);
            let thread_id = rayon::current_thread_index().unwrap_or(0);
            let mut resolver = Resolver::new(rules, ctx, &mut walk.ssa);
            let mut file_edges = Vec::new();

            for read in &reads {
                let result = &ctx.results[read.file_idx];
                let reference = &result.references[read.ref_idx];

                let t = std::time::Instant::now();
                let (resolved_defs, path) = if let Some(ref chain) = reference.expression {
                    resolver.stats.chain_refs += 1;
                    let defs = resolver.resolve_chain(read, chain);
                    let path = resolver.last_chain_path;
                    (defs, path)
                } else {
                    resolver.stats.bare_refs += 1;
                    let defs = resolver.resolve_bare(read);
                    let path = resolver.last_bare_path;
                    (defs, path)
                };
                let elapsed = t.elapsed();
                if elapsed.as_millis() >= 100 {
                    pb.suspend(|| {
                        eprintln!(
                            "\x1b[31m[SLOW] {:.2?} resolving '{}' in {} (chain: {})\x1b[0m",
                            elapsed,
                            reference.name,
                            result.file_path,
                            reference.expression.is_some(),
                        );
                    });
                }

                let (source, source_node, source_def_kind) = match read.enclosing_def {
                    Some(def_ref) => {
                        let (def, _) = ctx.resolve_def(def_ref);
                        (
                            EdgeSource::Definition(def_ref),
                            NodeKind::Definition,
                            Some(def.kind),
                        )
                    }
                    None => (EdgeSource::File(read.file_idx), NodeKind::File, None),
                };

                let edge_count = resolved_defs.len() as u64;
                for target in resolved_defs {
                    let (target_def, _) = ctx.resolve_def(target);
                    file_edges.push(ResolvedEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Calls,
                            source_node,
                            target_node: NodeKind::Definition,
                            source_def_kind,
                            target_def_kind: Some(target_def.kind),
                        },
                        source,
                        target,
                        reference_range: reference.range,
                    });
                }

                // Attribute edges to the resolution path that produced them.
                match path {
                    ResolvePath::BareSsa => resolver.stats.edges_from_bare_ssa += edge_count,
                    ResolvePath::BareImport => resolver.stats.edges_from_bare_import += edge_count,
                    ResolvePath::BareImplicit => {
                        resolver.stats.edges_from_bare_implicit += edge_count
                    }
                    ResolvePath::Chain => resolver.stats.edges_from_chain += edge_count,
                    ResolvePath::ChainFallback => {
                        resolver.stats.edges_from_chain_fallback += edge_count
                    }
                    ResolvePath::None => {}
                }

                pb.inc(1);
            }

            // Collect SSA stats from this file's resolver.
            resolver.stats.ssa.merge(&resolver.ssa.stats);

            let stats = std::mem::take(&mut resolver.stats);
            let timing = FileTimingEntry {
                file_idx,
                num_reads,
                duration: file_start.elapsed(),
                thread_id,
            };
            (file_edges, stats, timing)
        })
        .collect();

    pb.finish_and_clear();

    let mut all_edges = Vec::new();
    let mut combined = ResolveStats::default();
    let mut timings: Vec<FileTimingEntry> = Vec::with_capacity(per_file.len());
    for (edges, stats, timing) in per_file {
        all_edges.extend(edges);
        combined.merge(&stats);
        timings.push(timing);
    }

    print_long_tail_analysis(ctx, &timings);

    BuildEdgesResult {
        edges: all_edges,
        stats: combined,
    }
}

fn print_long_tail_analysis(ctx: &ResolutionContext, timings: &[FileTimingEntry]) {
    if timings.is_empty() {
        return;
    }

    // Reference distribution.
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

    // Top 10 slowest files.
    let mut by_duration: Vec<&FileTimingEntry> = timings.iter().collect();
    by_duration.sort_by(|a, b| b.duration.cmp(&a.duration));

    eprintln!("  Top 10 slowest files:");
    for entry in by_duration.iter().take(10) {
        let path = &ctx.results[entry.file_idx].file_path;
        // Truncate path to last 60 chars for readability.
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

    // Thread utilization: total time per thread vs wall clock.
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

// ── Resolution path tracking ────────────────────────────────────

/// Which resolution path produced results for a single reference.
/// Used to attribute edges to the path that created them.
#[derive(Debug, Clone, Copy)]
enum ResolvePath {
    None,
    BareSsa,
    BareImport,
    BareImplicit,
    Chain,
    ChainFallback,
}

// ── Resolver ────────────────────────────────────────────────────

/// Stateful resolver that holds shared context for resolving references.
/// Eliminates parameter threading across all resolution functions.
struct Resolver<'a> {
    rules: &'a ResolutionRules,
    ctx: &'a ResolutionContext,
    ssa: &'a mut SsaResolver,
    sep: &'a str,
    /// Reusable buffer for FQN construction.
    buf: String,
    /// Per-file stats, merged into aggregate after the file is done.
    pub stats: ResolveStats,
    /// Which path the last `resolve_bare` call took.
    pub last_bare_path: ResolvePath,
    /// Which path the last `resolve_chain` call took.
    pub last_chain_path: ResolvePath,
}

impl<'a> Resolver<'a> {
    fn new(
        rules: &'a ResolutionRules,
        ctx: &'a ResolutionContext,
        ssa: &'a mut SsaResolver,
    ) -> Self {
        Self {
            sep: rules.fqn_separator,
            rules,
            ctx,
            ssa,
            buf: String::with_capacity(128),
            stats: ResolveStats::default(),
            last_bare_path: ResolvePath::None,
            last_chain_path: ResolvePath::None,
        }
    }

    // ── Shared primitive ────────────────────────────────────────

    /// Extract type name(s) from a canonical definition.
    /// Type containers return their FQN; callables return their return type.
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

    /// Convert an SSA `Value` to type name(s) for member lookup.
    /// Returns interned strings to avoid allocation during chain resolution.
    fn value_to_types(&mut self, value: &Value) -> SmallVec<[IStr; 2]> {
        match value {
            Value::Type(t) => smallvec![*t],
            Value::Def(f, d) => {
                let def = &self.ctx.results[*f].definitions[*d];
                self.def_to_types(def)
            }
            _ => SmallVec::new(),
        }
    }

    // ── Bare name resolution ────────────────────────────────────

    /// Resolve a bare name (no expression chain) via SSA + fallbacks.
    fn resolve_bare(&mut self, read: &RecordedRead) -> Vec<DefRef> {
        self.last_bare_path = ResolvePath::None;

        let reaching = self.ssa.read_variable_stateless(&read.name, read.block);

        let mut result = Vec::new();

        for value in &reaching.values {
            match value {
                Value::Def(f, d) => {
                    self.stats.bare_ssa_def += 1;
                    result.push(DefRef {
                        file_idx: *f,
                        def_idx: *d,
                    });
                }
                Value::Import(f, i) => {
                    self.stats.bare_ssa_import += 1;
                    let import = &self.ctx.results[*f].imports[*i];
                    result.extend(imports::resolve_import(self.ctx, import, self.sep));
                }
                Value::Type(type_name) => {
                    self.stats.bare_ssa_type += 1;
                    self.ctx.members.lookup_member_with_supers(
                        type_name,
                        &read.name,
                        &self.ctx.results,
                        &self.ctx.definitions,
                        &mut result,
                    );
                }
                _ => {}
            }
        }

        if !result.is_empty() {
            self.stats.bare_ssa_resolved += 1;
            self.last_bare_path = ResolvePath::BareSsa;
            dedup(&mut result);
            return result;
        }

        // Fast path: if the name doesn't exist anywhere in the definition
        // index, skip all import strategies and implicit member lookup.
        if result.is_empty() && self.ctx.definitions.lookup_name(&read.name).is_empty() {
            self.stats.bare_early_exit_unknown += 1;
            self.stats.bare_unresolved += 1;
            return result;
        }

        // Fallback 1: import strategies
        if result.is_empty() {
            result = imports::apply(
                &self.rules.import_strategies,
                self.ctx,
                read.file_idx,
                &read.name,
                self.sep,
            );
            if !result.is_empty() {
                self.stats.bare_import_resolved += 1;
                self.last_bare_path = ResolvePath::BareImport;
            }
        }

        // Fallback 2: implicit member lookup on enclosing type
        if result.is_empty()
            && self.rules.implicit_member_lookup
            && let Some(type_fqn) = &read.enclosing_type_fqn
            && self.ctx.members.lookup_member_with_supers(
                type_fqn,
                &read.name,
                &self.ctx.results,
                &self.ctx.definitions,
                &mut result,
            )
        {
            self.stats.bare_implicit_this_resolved += 1;
            self.last_bare_path = ResolvePath::BareImplicit;
        }

        if result.is_empty() {
            self.stats.bare_unresolved += 1;
        }

        dedup(&mut result);
        result
    }

    // ── Chain resolution ────────────────────────────────────────

    /// Resolve an expression chain like `[Ident("obj"), Call("method")]`.
    fn resolve_chain(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<DefRef> {
        self.last_chain_path = ResolvePath::None;

        if chain.is_empty() {
            return vec![];
        }

        let effective_chain = if chain.len() > MAX_CHAIN_DEPTH {
            &chain[chain.len() - MAX_CHAIN_DEPTH..]
        } else {
            chain
        };

        // Track base type for stats.
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
            self.stats.chain_fallback_fired += 1;
            let result = self.chain_fallback(read, chain);
            if !result.is_empty() {
                self.stats.chain_fallback_resolved += 1;
                self.last_chain_path = ResolvePath::ChainFallback;
            }
            return result;
        }

        let mut compound_key = self.compound_key_base(&effective_chain[0]);

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

            // Deduplicate types to prevent exponential growth in
            // builder chains where every method returns the same type.
            {
                let mut seen = FxHashSet::default();
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

    /// Resolve the first element of a chain to type name(s).
    fn resolve_base(
        &mut self,
        step: &ExpressionStep,
        block: super::ssa::BlockId,
        enclosing: Option<&str>,
    ) -> SmallVec<[IStr; 2]> {
        match step {
            ExpressionStep::Ident(name) | ExpressionStep::Call(name) => {
                let reaching = self.ssa.read_variable_stateless(name, block);
                let mut types: SmallVec<[IStr; 2]> = reaching
                    .values
                    .iter()
                    .flat_map(|v| self.value_to_types(v))
                    .collect();

                // Implicit-this fallback for chain bases: if SSA didn't resolve
                // the base name and we're inside a type scope, look up the name
                // as a member of the enclosing type to get its return type.
                if types.is_empty()
                    && self.rules.implicit_member_lookup
                    && let Some(fqn) = enclosing
                {
                    let mut members = Vec::new();
                    self.ctx.members.lookup_member_with_supers(
                        fqn,
                        name,
                        &self.ctx.results,
                        &self.ctx.definitions,
                        &mut members,
                    );
                    for def_ref in &members {
                        let (def, _) = self.ctx.resolve_def(*def_ref);
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

    /// Walk one step of a chain: look up member_name on each current type.
    /// Returns (next type names for further steps, found member DefRefs).
    fn walk_step(
        &mut self,
        current_types: &[IStr],
        step: &ExpressionStep,
        member_name: &str,
    ) -> (SmallVec<[IStr; 2]>, Vec<DefRef>) {
        let mut next_types = SmallVec::new();
        let mut found_members = Vec::new();

        for type_name in current_types {
            let before = found_members.len();
            self.ctx.members.lookup_member_with_supers(
                type_name,
                member_name,
                &self.ctx.results,
                &self.ctx.definitions,
                &mut found_members,
            );
            for def_ref in &found_members[before..] {
                let def = &self.ctx.results[def_ref.file_idx].definitions[def_ref.def_idx];
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

    /// Build the compound SSA key base from the first chain element.
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

    /// Try compound key fallback: read "self.db" as a single SSA variable.
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

    /// Fallback when chain base can't resolve: try bare name on last step.
    fn chain_fallback(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<DefRef> {
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

fn dedup(result: &mut Vec<DefRef>) {
    if result.len() <= 4 {
        // O(n²) but n ≤ 4, no allocation.
        let mut i = 0;
        while i < result.len() {
            let key = (result[i].file_idx, result[i].def_idx);
            if result[..i]
                .iter()
                .any(|r| r.file_idx == key.0 && r.def_idx == key.1)
            {
                result.swap_remove(i);
            } else {
                i += 1;
            }
        }
    } else {
        let mut seen = FxHashSet::default();
        result.retain(|r| seen.insert((r.file_idx, r.def_idx)));
    }
}
