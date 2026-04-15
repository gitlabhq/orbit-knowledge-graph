//! Edge builder: converts per-file SSA walk results into resolved call edges.
//!
//! Takes per-file `FileWalkResult`s (SSA graph + recorded reads) and the
//! `ResolutionContext` (indexes), resolves each reference read to concrete
//! definitions, and produces `ResolvedEdge`s for the graph.

use code_graph_types::{EdgeKind, ExpressionStep, IStr, NodeKind, Relationship};
use indicatif::{ProgressBar, ProgressStyle};
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
use super::ssa::{SsaResolver, Value};
use super::walker::{FileWalkResult, RecordedRead};

/// Trait to get rules from the type parameter.
pub trait HasRules {
    fn rules() -> ResolutionRules;
}

/// Build edges from per-file walk results. This is the pipeline's resolve stage.
///
/// For each file's recorded reads, resolves references to concrete definitions
/// via SSA values, import strategies, and expression chain walking.
pub fn build_edges(
    rules: &ResolutionRules,
    ctx: &ResolutionContext,
    walks: &mut [FileWalkResult],
) -> Vec<ResolvedEdge> {
    let total_reads: u64 = walks.iter().map(|w| w.reads.len() as u64).sum();
    let pb = ProgressBar::new(total_reads);
    pb.set_style(
        ProgressStyle::with_template("Resolving [{bar:40}] {pos}/{len} ({per_sec}, {eta})")
            .unwrap()
            .progress_chars("█▓░"),
    );

    let mut edges = Vec::new();

    for walk in walks.iter_mut() {
        let reads = std::mem::take(&mut walk.reads);
        let mut resolver = Resolver::new(rules, ctx, &mut walk.ssa);

        for read in &reads {
            let result = &ctx.results[read.file_idx];
            let reference = &result.references[read.ref_idx];

            let t = std::time::Instant::now();
            let resolved_defs = if let Some(ref chain) = reference.expression {
                resolver.resolve_chain(read, chain)
            } else {
                resolver.resolve_bare(read)
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

            let source_enclosing = ctx.scopes.enclosing_scope(
                &result.file_path,
                reference.range.byte_offset.0,
                reference.range.byte_offset.1,
            );

            let (source, source_node, source_def_kind) = match source_enclosing {
                Some(s) => {
                    let def_ref = DefRef {
                        file_idx: s.file_idx,
                        def_idx: s.def_idx,
                    };
                    let (def, _) = ctx.resolve_def(def_ref);
                    (
                        EdgeSource::Definition(def_ref),
                        NodeKind::Definition,
                        Some(def.kind),
                    )
                }
                None => (EdgeSource::File(read.file_idx), NodeKind::File, None),
            };

            for target in resolved_defs {
                let (target_def, _) = ctx.resolve_def(target);
                edges.push(ResolvedEdge {
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
            pb.inc(1);
        }
    }

    pb.finish_and_clear();
    edges
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
        }
    }

    /// Build a FQN string in the reusable buffer and look it up.
    fn lookup_fqn_joined(&mut self, prefix: &str, suffix: &str) -> &[DefRef] {
        self.buf.clear();
        self.buf.push_str(prefix);
        self.buf.push_str(self.sep);
        self.buf.push_str(suffix);
        self.ctx.definitions.lookup_fqn(&self.buf)
    }

    // ── Shared primitive ────────────────────────────────────────

    /// Convert an SSA `Value` to type name(s) for member lookup.
    /// Returns interned strings to avoid allocation during chain resolution.
    fn value_to_types(&mut self, value: &Value) -> SmallVec<[IStr; 2]> {
        match value {
            Value::Type(t) => smallvec![*t],
            Value::Def(f, d) => {
                let def = &self.ctx.results[*f].definitions[*d];
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
            _ => SmallVec::new(),
        }
    }

    /// Find the enclosing type scope FQN for a reference.
    fn enclosing_type_fqn(
        &mut self,
        file_idx: usize,
        byte_start: usize,
        byte_end: usize,
    ) -> Option<String> {
        let result = &self.ctx.results[file_idx];
        let containing = self
            .ctx
            .scopes
            .containing_scopes(&result.file_path, byte_start, byte_end);
        containing
            .iter()
            .rev()
            .find(|s| result.definitions[s.def_idx].kind.is_type_container())
            .map(|s| result.definitions[s.def_idx].fqn.to_string())
    }

    // ── Bare name resolution ────────────────────────────────────

    /// Resolve a bare name (no expression chain) via SSA + fallbacks.
    fn resolve_bare(&mut self, read: &RecordedRead) -> Vec<DefRef> {
        let reaching = self.ssa.read_variable_stateless(&read.name, read.block);
        let reference = &self.ctx.results[read.file_idx].references[read.ref_idx];
        let enclosing = self.enclosing_type_fqn(
            read.file_idx,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        let mut result = Vec::new();

        for value in &reaching.values {
            match value {
                Value::Def(f, d) => {
                    result.push(DefRef {
                        file_idx: *f,
                        def_idx: *d,
                    });
                }
                Value::Import(f, i) => {
                    let import = &self.ctx.results[*f].imports[*i];
                    result.extend(imports::resolve_import(self.ctx, import, self.sep, true));
                }
                Value::Type(type_name) => {
                    if !self.ctx.members.lookup_member_with_supers(
                        type_name,
                        &read.name,
                        &self.ctx.definitions,
                        &mut result,
                    ) {
                        let fqn_matches = self.lookup_fqn_joined(type_name, &read.name);
                        result.extend_from_slice(fqn_matches);
                    }
                }
                _ => {}
            }
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
        }

        // Fallback 2: implicit member lookup on enclosing type
        if result.is_empty()
            && self.rules.implicit_member_lookup
            && let Some(type_fqn) = &enclosing
        {
            self.ctx.members.lookup_member_with_supers(
                type_fqn,
                &read.name,
                &self.ctx.definitions,
                &mut result,
            );
        }

        dedup(&mut result);
        result
    }

    // ── Chain resolution ────────────────────────────────────────

    /// Resolve an expression chain like `[Ident("obj"), Call("method")]`.
    fn resolve_chain(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<DefRef> {
        if chain.is_empty() {
            return vec![];
        }

        let reference = &self.ctx.results[read.file_idx].references[read.ref_idx];
        let enclosing = self.enclosing_type_fqn(
            read.file_idx,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        // If the chain is too long, resolve only the last few steps.
        // This handles fluent builder patterns (30+ chained calls) without
        // walking the entire chain.
        let effective_chain = if chain.len() > MAX_CHAIN_DEPTH {
            &chain[chain.len() - MAX_CHAIN_DEPTH..]
        } else {
            chain
        };

        // Step 1: resolve the base to type name(s)
        let mut current_types =
            self.resolve_base(&effective_chain[0], read.block, enclosing.as_deref());

        if current_types.is_empty() {
            return self.chain_fallback(read, chain);
        }

        // Step 2: walk remaining steps
        let mut compound_key = self.compound_key_base(&effective_chain[0]);

        for (i, step) in effective_chain[1..].iter().enumerate() {
            let is_last = i == effective_chain.len() - 2;
            let member_name = match step {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => n,
                _ => continue,
            };

            // Look up member on current type(s)
            let (next_types, found_members) = self.walk_step(&current_types, step, member_name);

            if is_last && !found_members.is_empty() {
                let mut result = found_members;
                dedup(&mut result);
                return result;
            }

            // Compound key fallback (Python's self.db)
            if next_types.is_empty() && found_members.is_empty() {
                let recovered = self.compound_key_step(&mut compound_key, member_name, read.block);
                if !recovered.is_empty() {
                    current_types = recovered;
                    continue;
                }
            } else {
                compound_key.clear();
            }

            current_types = next_types;
            if current_types.is_empty() {
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
            ExpressionStep::Ident(name) => {
                let reaching = self.ssa.read_variable_stateless(name, block);
                reaching
                    .values
                    .iter()
                    .flat_map(|v| self.value_to_types(v))
                    .collect()
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
            name: last.clone(),
        };
        self.resolve_bare(&bare_read)
    }
}

fn dedup(result: &mut Vec<DefRef>) {
    let mut seen = FxHashSet::default();
    result.retain(|r| seen.insert((r.file_idx, r.def_idx)));
}
