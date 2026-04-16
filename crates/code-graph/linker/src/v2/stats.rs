use std::sync::Arc;

// ── SSA Stats ───────────────────────────────────────────────────

/// Per-file SSA statistics, collected during resolution reads.
#[derive(Debug, Clone, Default)]
pub struct SsaStats {
    /// Total `read_variable_stateless` calls.
    pub reads: u64,
    /// Read resolved from current block's `current_def` (no predecessor walk).
    pub local_hits: u64,
    /// Read required walking predecessors (`read_variable_recursive`).
    pub recursive_lookups: u64,
    /// Read hit an unsealed block (incomplete phi created).
    pub unsealed_hits: u64,
    /// Read hit a block with zero predecessors (returned Opaque).
    pub dead_end_hits: u64,
    /// Phi nodes created during reads.
    pub phis_created: u64,
    /// Phi nodes that were trivially eliminated (collapsed to single value).
    pub phis_trivial: u64,
    /// Total variable writes.
    pub writes: u64,
    /// Total blocks created.
    pub blocks_created: u64,
}

impl SsaStats {
    pub fn merge(&mut self, other: &SsaStats) {
        self.reads += other.reads;
        self.local_hits += other.local_hits;
        self.recursive_lookups += other.recursive_lookups;
        self.unsealed_hits += other.unsealed_hits;
        self.dead_end_hits += other.dead_end_hits;
        self.phis_created += other.phis_created;
        self.phis_trivial += other.phis_trivial;
        self.writes += other.writes;
        self.blocks_created += other.blocks_created;
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
    pub bare_implicit_scope_resolved: u64,
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
        self.bare_implicit_scope_resolved += other.bare_implicit_scope_resolved;
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
                "    Implicit scope:      {:>8} ({:.1}%)",
                self.bare_implicit_scope_resolved,
                pct(self.bare_implicit_scope_resolved, self.bare_refs)
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
pub struct FileTimingEntry {
    pub file_path: Arc<str>,
    pub num_reads: usize,
    pub duration: std::time::Duration,
    pub thread_id: usize,
}

pub fn print_long_tail_analysis(timings: &[FileTimingEntry]) {
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
        let path: &str = &entry.file_path;
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
