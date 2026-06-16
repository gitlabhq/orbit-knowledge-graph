//! Parser-level SSA engine (Braun et al., CC 2013).
//!
//! Moved from the linker so that SSA construction happens during parsing,
//! not during a second AST walk. Values are parser-local indices
//! (`LocalDef(u32)`, `ImportRef(u32)`) instead of graph `NodeIndex`.
//!
//! All variable names are `&'a str` backed by `FileArena` (bumpalo).

use crate::trace;
use crate::v2::trace::Tracer;
use crate::v2::types::ssa::ParseValue;
use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

// ── SSA types (local to the parser) ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlockId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PhiId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedSite<'a> {
    pub path: &'a str,
    pub start: u32,
    pub end: u32,
}

/// SSA value — parser-local, no graph dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SsaValue<'a> {
    /// Index into this file's definitions list.
    LocalDef(u32),
    /// Index into this file's imports list.
    ImportRef(u32),
    /// A type FQN for nested member lookup (self/this, type annotations).
    Type(&'a str),
    /// A resolved definition site, potentially outside the current file.
    ResolvedSite(ResolvedSite<'a>),
    /// Deferred name resolution — chased at write time via copy propagation.
    Alias(&'a str),
    /// Dead end — parameter, literal, or otherwise unresolvable.
    Opaque,
    /// Internal: cycle-detection sentinel for the marker algorithm.
    Marker,
    /// Internal: a phi node (resolved to concrete values).
    Phi(PhiId),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SsaStats {
    pub reads: u64,
    pub local_hits: u64,
    pub recursive_lookups: u64,
    pub phis_created: u64,
    pub phis_trivial: u64,
    pub writes: u64,
    pub blocks_created: u64,
}

/// The concrete values a variable resolves to at a given program point.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReachingDefs<'a> {
    pub values: SmallVec<[SsaValue<'a>; 2]>,
}

impl SsaValue<'_> {
    /// Convert to a ParseValue for output. Returns None for SSA-internal
    /// values (Marker, Phi) and Alias (should have been resolved).
    pub(crate) fn to_parse_value(&self) -> Option<ParseValue> {
        match self {
            SsaValue::LocalDef(i) => Some(ParseValue::LocalDef(*i)),
            SsaValue::ImportRef(i) => Some(ParseValue::ImportRef(*i)),
            SsaValue::Type(t) => Some(ParseValue::Type(smol_str::SmolStr::from(*t))),
            SsaValue::Opaque => Some(ParseValue::Opaque),
            SsaValue::ResolvedSite(_)
            | SsaValue::Alias(_)
            | SsaValue::Marker
            | SsaValue::Phi(_) => None,
        }
    }

    /// Human-readable representation for trace output.
    pub(crate) fn trace_display(&self) -> String {
        match self {
            SsaValue::LocalDef(i) => format!("LocalDef({i})"),
            SsaValue::ImportRef(i) => format!("ImportRef({i})"),
            SsaValue::Type(t) => format!("Type({t})"),
            SsaValue::ResolvedSite(site) => {
                format!("ResolvedSite({}:{}-{})", site.path, site.start, site.end)
            }
            SsaValue::Alias(a) => format!("Alias({a})"),
            SsaValue::Opaque => "Opaque".to_string(),
            SsaValue::Marker => "Marker".to_string(),
            SsaValue::Phi(id) => format!("φ{}", id.0),
        }
    }
}

// ── Phi node ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PhiNode<'a> {
    block: BlockId,
    variable: &'a str,
    operands: SmallVec<[SsaValue<'a>; 2]>,
    /// Witness caching (Section 3.1): first two distinct non-self operands.
    /// If both are still valid and distinct, the phi is non-trivial without
    /// scanning all operands.
    witnesses: [Option<SsaValue<'a>>; 2],
}

// ── Block ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Block {
    predecessors: SmallVec<[BlockId; 2]>,
    sealed: bool,
}

/// Visit state for the iterative reaching-def walk: `Enter` schedules a
/// block's predecessors, `Exit` combines them once they are resolved.
#[derive(Clone, Copy)]
enum Phase {
    Enter,
    Exit,
}

/// What a block needs on the way down a [`SsaEngine::resolve_postorder`] walk:
/// nothing (its value is already written), or these predecessors visited first.
enum Visit {
    Leaf,
    Branch(SmallVec<[BlockId; 2]>),
}

// ── SSA Resolver ────────────────────────────────────────────────

/// Parser-level SSA engine (Braun et al. algorithm).
///
/// All variable names are `&'a str` backed by `FileArena`.
pub(crate) struct SsaEngine<'a> {
    blocks: Vec<Block>,
    phis: Vec<PhiNode<'a>>,
    /// current_def[variable][block] = value
    current_def: FxHashMap<&'a str, FxHashMap<BlockId, SsaValue<'a>>>,
    /// Incomplete phis for unsealed blocks: block → variable → phi_id
    incomplete_phis: FxHashMap<BlockId, FxHashMap<&'a str, PhiId>>,
    pub stats: SsaStats,
    tracer: &'a Tracer,
    /// Reused across reads so the iterative walk doesn't allocate per call.
    read_work: Vec<(BlockId, Phase)>,
}

impl<'a> SsaEngine<'a> {
    pub(crate) fn new() -> Self {
        Self {
            blocks: Vec::with_capacity(32),
            phis: Vec::with_capacity(8),
            current_def: FxHashMap::with_capacity_and_hasher(64, Default::default()),
            incomplete_phis: FxHashMap::default(),
            stats: SsaStats::default(),
            tracer: super::super::trace::leaked_noop_tracer(),
            read_work: Vec::new(),
        }
    }

    pub(crate) fn with_tracer(mut self, tracer: &'a Tracer) -> Self {
        self.tracer = tracer;
        self
    }

    /// Create a new basic block. Returns its ID.
    pub(crate) fn add_block(&mut self) -> BlockId {
        let id = BlockId(self.blocks.len());
        self.blocks.push(Block {
            predecessors: SmallVec::new(),
            sealed: false,
        });
        self.stats.blocks_created += 1;
        trace!(self.tracer, SsaBlockCreated { block_id: id.0 });
        id
    }

    /// Add a predecessor edge: `pred` flows into `block`.
    pub(crate) fn add_predecessor(&mut self, block: BlockId, pred: BlockId) {
        self.blocks[block.0].predecessors.push(pred);
        trace!(
            self.tracer,
            SsaAddPredecessor {
                block_id: block.0,
                pred_id: pred.0,
            }
        );
    }

    /// Create a sealed successor block with a single predecessor.
    pub fn add_sealed_successor(&mut self, predecessor: BlockId) -> BlockId {
        let block = self.add_block();
        self.add_predecessor(block, predecessor);
        self.seal_block(block);
        block
    }

    /// Create and seal a join block from the provided predecessors.
    pub fn add_sealed_join<I>(&mut self, predecessors: I) -> BlockId
    where
        I: IntoIterator<Item = BlockId>,
    {
        let block = self.add_block();
        for predecessor in predecessors {
            self.add_predecessor(block, predecessor);
        }
        self.seal_block(block);
        block
    }

    /// Create a sealed branch block from a shared predecessor.
    pub fn add_branch_block(&mut self, predecessor: BlockId) -> BlockId {
        self.add_sealed_successor(predecessor)
    }

    /// Create a join block for one or more branch exits, with an optional
    /// fallthrough predecessor when one side of the branch is absent.
    pub fn add_branch_join<I>(&mut self, fallthrough: Option<BlockId>, branch_exits: I) -> BlockId
    where
        I: IntoIterator<Item = BlockId>,
    {
        let mut predecessors = SmallVec::<[BlockId; 3]>::new();
        predecessors.extend(branch_exits);
        if let Some(fallthrough) = fallthrough {
            predecessors.push(fallthrough);
        }
        self.add_sealed_join(predecessors)
    }

    /// Create a loop header/body pair from the current predecessor block.
    pub fn begin_loop(&mut self, predecessor: BlockId) -> (BlockId, BlockId) {
        let header = self.add_block();
        self.add_predecessor(header, predecessor);
        let body = self.add_sealed_successor(header);
        (header, body)
    }

    /// Close a loop by wiring the body exit back into the header and creating
    /// a sealed exit block.
    pub fn finish_loop(&mut self, header: BlockId, body_exit: BlockId) -> BlockId {
        self.add_predecessor(header, body_exit);
        self.seal_block(header);
        self.add_sealed_successor(header)
    }

    /// Seal a block — all predecessors are now known.
    /// Resolves any incomplete phi nodes that were deferred.
    pub(crate) fn seal_block(&mut self, block: BlockId) {
        if let Some(incomplete) = self.incomplete_phis.remove(&block) {
            for (variable, phi_id) in incomplete {
                self.add_phi_operands(variable, phi_id);
            }
        }
        self.blocks[block.0].sealed = true;
        trace!(self.tracer, SsaBlockSealed { block_id: block.0 });
    }

    /// Seal any blocks that haven't been sealed yet.
    pub(crate) fn seal_remaining(&mut self) {
        for id in 0..self.blocks.len() {
            if !self.blocks[id].sealed {
                self.seal_block(BlockId(id));
            }
        }
    }

    /// Record a variable definition: `variable` is defined as `value` in `block`.
    /// On-the-fly copy propagation (Section 3.1): if the value is an alias
    /// to another variable, resolve it immediately instead of deferring.
    /// Check if a variable has been written in a specific block.
    pub(crate) fn has_variable_in_block(&self, variable: &str, block: BlockId) -> bool {
        self.current_def
            .get(variable)
            .is_some_and(|blocks| blocks.contains_key(&block))
    }

    pub(crate) fn write_variable(
        &mut self,
        variable: &'a str,
        block: BlockId,
        value: SsaValue<'a>,
    ) {
        let resolved = if let SsaValue::Alias(alias_name) = value {
            let alias_val = self.read_variable_internal(alias_name, block);
            if alias_val != SsaValue::Opaque {
                alias_val
            } else {
                SsaValue::Alias(alias_name)
            }
        } else {
            value
        };
        trace!(
            self.tracer,
            SsaWrite {
                variable: variable.to_string(),
                block_id: block.0,
                value: resolved.trace_display(),
            }
        );
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, resolved);
        self.stats.writes += 1;
    }

    /// Look up a variable's reaching definitions without recording the read.
    pub(crate) fn read_variable_stateless(
        &mut self,
        variable: &'a str,
        block: BlockId,
    ) -> ReachingDefs<'a> {
        self.stats.reads += 1;
        let mut value = self.read_variable_internal(variable, block);
        // Chase aliases: if the value is Alias(target), read target in the
        // same block. Bounded depth to prevent cycles.
        let mut depth = 0;
        while let SsaValue::Alias(target) = &value {
            depth += 1;
            if depth > 8 {
                break;
            }
            let target_value = self.read_variable_internal(target, block);
            if matches!(target_value, SsaValue::Opaque | SsaValue::Marker) {
                break; // target has no useful value, keep the Alias
            }
            value = target_value;
        }
        let result = self.resolve_value(&value);
        trace!(
            self.tracer,
            SsaRead {
                variable: variable.to_string(),
                block_id: block.0,
                values: result.values.iter().map(|v| v.trace_display()).collect(),
            }
        );
        result
    }

    /// Return the raw SSA value for `variable` at `block`, without expanding phis.
    /// Use `expand_value` later to resolve once all blocks are sealed.
    pub(crate) fn read_variable_raw(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        self.stats.reads += 1;
        self.read_variable_internal(variable, block)
    }

    /// Expand a raw SSA value into its reaching definitions.
    pub(crate) fn expand_value(&self, value: &SsaValue<'a>) -> ReachingDefs<'a> {
        self.resolve_value(value)
    }

    // ── Internal: Braun et al. algorithm ────────────────────────

    fn read_variable_internal(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        // Local value numbering: check current block first
        if let Some(block_defs) = self.current_def.get(&variable)
            && let Some(value) = block_defs.get(&block)
        {
            self.stats.local_hits += 1;
            return value.clone();
        }

        // Global value numbering — resolved iteratively (see read_variable_iter)
        // so a deep predecessor chain can't overflow the worker stack.
        self.stats.recursive_lookups += 1;
        self.read_variable_iter(variable, block)
    }

    /// Internal write that takes an already-interned name.
    fn write_variable_interned(&mut self, variable: &'a str, block: BlockId, value: SsaValue<'a>) {
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, value);
    }

    fn new_phi(&mut self, block: BlockId, variable: &'a str) -> PhiId {
        self.stats.phis_created += 1;
        let id = PhiId(self.phis.len());
        self.phis.push(PhiNode {
            block,
            variable,
            operands: SmallVec::new(),
            witnesses: [None, None],
        });
        trace!(
            self.tracer,
            SsaPhiCreated {
                phi_id: id.0,
                block_id: block.0,
                variable: variable.to_string(),
            }
        );
        id
    }

    /// Populate a phi's operands by reading its variable at every
    /// predecessor, then attempt trivial-phi removal per Braun Algorithm 4.
    ///
    /// The return value is the phi itself (`SsaValue::Phi(phi_id)`) when
    /// the phi survives simplification, or the replacement value when
    /// every non-self operand is identical. Callers that created the
    /// phi via the marker path (`read_variable_marker`) propagate the
    /// replacement upward; `seal_block` ignores the return value because
    /// the replacement has already propagated through phi users and
    /// `current_def`.
    fn add_phi_operands(&mut self, variable: &'a str, phi_id: PhiId) -> SsaValue<'a> {
        let block = self.phis[phi_id.0].block;
        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        for pred in preds {
            let val = self.read_variable_internal(variable, pred);
            // Update witnesses: track first two distinct non-self operands.
            if val != SsaValue::Phi(phi_id) {
                let phi = &mut self.phis[phi_id.0];
                if phi.witnesses[0].is_none() {
                    phi.witnesses[0] = Some(val.clone());
                } else if phi.witnesses[1].is_none() && phi.witnesses[0].as_ref() != Some(&val) {
                    phi.witnesses[1] = Some(val.clone());
                }
            }
            self.phis[phi_id.0].operands.push(val);
        }
        self.try_remove_trivial_phi(phi_id)
    }

    /// Remove trivial phi: if it references only one real value (plus itself),
    /// replace it with that value. The cascade onto dependent phis runs through
    /// an explicit work stack rather than recursion, so a long phi chain can't
    /// overflow. Returns the replacement for `phi_id` (the value the caller
    /// asked about); cascade replacements are applied in place.
    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> SsaValue<'a> {
        let mut work = vec![phi_id];
        let mut result = SsaValue::Phi(phi_id);
        let mut captured = false;
        while let Some(pid) = work.pop() {
            let outcome = self.simplify_one_phi(pid, &mut work);
            if pid == phi_id && !captured {
                result = outcome;
                captured = true;
            }
        }
        result
    }

    /// Simplify a single phi. If trivial, rewrite it out of every user's
    /// operands and push those users onto `work` for re-checking; returns the
    /// replacement value. If non-trivial, returns `Phi(phi_id)` and pushes
    /// nothing.
    fn simplify_one_phi(&mut self, phi_id: PhiId, work: &mut Vec<PhiId>) -> SsaValue<'a> {
        // Witness cache fast path: if both witnesses are still distinct
        // and neither is the phi itself, the phi is non-trivial.
        let w = &self.phis[phi_id.0].witnesses;
        if let (Some(w0), Some(w1)) = (w[0].as_ref(), w[1].as_ref())
            && w0 != w1
            && *w0 != SsaValue::Phi(phi_id)
            && *w1 != SsaValue::Phi(phi_id)
        {
            return SsaValue::Phi(phi_id);
        }

        let mut same: Option<SsaValue<'a>> = None;

        for i in 0..self.phis[phi_id.0].operands.len() {
            let op = self.phis[phi_id.0].operands[i].clone();
            if op == SsaValue::Phi(phi_id) || Some(&op) == same.as_ref() {
                continue;
            }
            if same.is_some() {
                return SsaValue::Phi(phi_id);
            }
            same = Some(op);
        }

        let replacement = same.unwrap_or(SsaValue::Opaque);
        self.stats.phis_trivial += 1;
        trace!(
            self.tracer,
            SsaPhiTrivial {
                phi_id: phi_id.0,
                replacement: replacement.trace_display(),
            }
        );

        let variable = self.phis[phi_id.0].variable;
        let block = self.phis[phi_id.0].block;

        // Update current_def if it points to this phi
        if let Some(block_defs) = self.current_def.get_mut(&variable)
            && block_defs.get(&block) == Some(&SsaValue::Phi(phi_id))
        {
            block_defs.insert(block, replacement.clone());
        }

        // Check if any other phis using this one become trivial
        let phi_users: Vec<PhiId> = self
            .phis
            .iter()
            .enumerate()
            .filter(|(i, phi)| *i != phi_id.0 && phi.operands.contains(&SsaValue::Phi(phi_id)))
            .map(|(i, _)| PhiId(i))
            .collect();

        // Replace this phi in all users' operands and invalidate witnesses
        let phi_val = SsaValue::Phi(phi_id);
        for user_id in &phi_users {
            let user = &mut self.phis[user_id.0];
            for op in &mut user.operands {
                if *op == phi_val {
                    *op = replacement.clone();
                }
            }
            // Invalidate witnesses — they may reference the removed phi
            for w in &mut user.witnesses {
                if w.as_ref() == Some(&phi_val) {
                    *w = None;
                }
            }
        }

        // Re-check users without recursing.
        work.extend(phi_users);

        replacement
    }

    /// Iterative twin of [`Self::read_variable_recursive`]. Walks the
    /// predecessor graph with an explicit work stack instead of the call
    /// stack, so a chain thousands of blocks deep can't overflow. Same Braun
    /// marker scheme: write `Marker` on `Enter` to break cycles, combine the
    /// resolved predecessors on `Exit`.
    fn read_variable_iter(&mut self, variable: &'a str, start: BlockId) -> SsaValue<'a> {
        if let Some(v) = self.current_value(variable, start)
            && v != SsaValue::Marker
        {
            return v;
        }

        // Borrow the reusable work stack out of `self` for the walk, then
        // return it (with its grown capacity) for the next read.
        let mut work = std::mem::take(&mut self.read_work);
        self.resolve_postorder(
            start,
            &mut work,
            |this, block| {
                if this
                    .current_value(variable, block)
                    .is_some_and(|v| v != SsaValue::Marker)
                {
                    return Visit::Leaf;
                }
                if !this.blocks[block.0].sealed {
                    let phi_id = this.new_phi(block, variable);
                    this.incomplete_phis
                        .entry(block)
                        .or_default()
                        .insert(variable, phi_id);
                    this.write_variable_interned(variable, block, SsaValue::Phi(phi_id));
                    return Visit::Leaf;
                }
                if this.blocks[block.0].predecessors.is_empty() {
                    this.write_variable_interned(variable, block, SsaValue::Opaque);
                    return Visit::Leaf;
                }
                // In-progress sentinel; a predecessor already marked is an
                // ancestor on the stack (a cycle) and must not be re-pushed.
                this.write_variable_interned(variable, block, SsaValue::Marker);
                let mut to_visit = SmallVec::new();
                for i in 0..this.blocks[block.0].predecessors.len() {
                    let p = this.blocks[block.0].predecessors[i];
                    if this.current_value(variable, p).is_none() {
                        to_visit.push(p);
                    }
                }
                Visit::Branch(to_visit)
            },
            |this, block| {
                let val = this.combine_predecessors(variable, block);
                this.write_variable_interned(variable, block, val);
            },
        );
        self.read_work = work;

        self.current_value(variable, start)
            .unwrap_or(SsaValue::Opaque)
    }

    fn current_value(&self, variable: &str, block: BlockId) -> Option<SsaValue<'a>> {
        self.current_def
            .get(variable)
            .and_then(|m| m.get(&block))
            .cloned()
    }

    /// Post-order walk over the predecessor graph using an explicit work stack,
    /// so a chain thousands of blocks deep can't overflow. `enter` resolves a
    /// leaf or returns the predecessors to visit first; `exit` runs once they
    /// are resolved.
    fn resolve_postorder(
        &mut self,
        start: BlockId,
        work: &mut Vec<(BlockId, Phase)>,
        mut enter: impl FnMut(&mut Self, BlockId) -> Visit,
        mut exit: impl FnMut(&mut Self, BlockId),
    ) {
        work.clear();
        work.push((start, Phase::Enter));
        while let Some(&(block, phase)) = work.last() {
            match phase {
                Phase::Enter => match enter(self, block) {
                    Visit::Leaf => {
                        work.pop();
                    }
                    Visit::Branch(preds) => {
                        work.last_mut().unwrap().1 = Phase::Exit;
                        work.extend(preds.into_iter().map(|p| (p, Phase::Enter)));
                    }
                },
                Phase::Exit => {
                    work.pop();
                    exit(self, block);
                }
            }
        }
    }

    /// Combine a block's resolved predecessors: the single agreed value, or a
    /// phi when they disagree (or a cycle marker is present).
    fn combine_predecessors(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        let mut same: Option<SsaValue<'a>> = None;
        let mut need_phi = false;
        for &p in &self.blocks[block.0].predecessors.clone() {
            let pv = self.current_value(variable, p).unwrap_or(SsaValue::Opaque);
            if pv == SsaValue::Marker {
                need_phi = true;
                continue;
            }
            match &same {
                None => same = Some(pv),
                Some(s) if *s == pv => {}
                Some(_) => need_phi = true,
            }
        }
        if need_phi {
            let phi_id = self.new_phi(block, variable);
            self.write_variable_interned(variable, block, SsaValue::Phi(phi_id));
            self.add_phi_operands(variable, phi_id)
        } else {
            same.unwrap_or(SsaValue::Opaque)
        }
    }

    /// Recursive reference implementation, kept only to differentially test the
    /// iterative production path. This is the original Braun marker algorithm,
    /// self-contained so the oracle never routes through the code it checks.
    #[cfg(test)]
    pub(crate) fn read_variable_stateless_recursive(
        &mut self,
        variable: &'a str,
        block: BlockId,
    ) -> ReachingDefs<'a> {
        let mut value = self.read_ref(variable, block);
        let mut depth = 0;
        while let SsaValue::Alias(target) = &value {
            depth += 1;
            if depth > 8 {
                break;
            }
            let target_value = self.read_ref(target, block);
            if matches!(target_value, SsaValue::Opaque | SsaValue::Marker) {
                break;
            }
            value = target_value;
        }
        self.resolve_value(&value)
    }

    #[cfg(test)]
    fn read_ref(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        if let Some(block_defs) = self.current_def.get(&variable)
            && let Some(value) = block_defs.get(&block)
        {
            return value.clone();
        }
        self.read_recursive_ref(variable, block)
    }

    #[cfg(test)]
    fn read_recursive_ref(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        let val;
        let sealed = self.blocks[block.0].sealed;
        let num_preds = self.blocks[block.0].predecessors.len();
        if !sealed {
            let phi_id = self.new_phi(block, variable);
            self.incomplete_phis
                .entry(block)
                .or_default()
                .insert(variable, phi_id);
            val = SsaValue::Phi(phi_id);
        } else if num_preds == 0 {
            val = SsaValue::Opaque;
        } else if num_preds == 1 {
            let pred = self.blocks[block.0].predecessors[0];
            val = self.read_ref(variable, pred);
        } else {
            val = self.read_marker_ref(variable, block);
        }
        self.write_variable_interned(variable, block, val.clone());
        val
    }

    #[cfg(test)]
    fn read_marker_ref(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        self.write_variable_interned(variable, block, SsaValue::Marker);
        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        let mut same: Option<SsaValue<'a>> = None;
        let mut need_phi = false;
        for &pred in &preds {
            let pred_val = self.read_ref(variable, pred);
            if pred_val == SsaValue::Marker {
                need_phi = true;
                continue;
            }
            match &same {
                None => same = Some(pred_val),
                Some(s) if *s == pred_val => {}
                Some(_) => {
                    need_phi = true;
                    break;
                }
            }
        }
        if !need_phi {
            return same.unwrap_or(SsaValue::Opaque);
        }
        let phi_id = self.new_phi(block, variable);
        self.write_variable_interned(variable, block, SsaValue::Phi(phi_id));
        self.add_phi_operands(variable, phi_id)
    }

    /// Remove redundant phi SCCs (Algorithm 5, Section 3.2 of Braun et al.).
    ///
    /// After SSA construction and trivial phi removal, some phi nodes may
    /// form cycles where they reference only each other plus one external
    /// value. `try_remove_trivial_phi` can't detect these because each
    /// individual phi sees two distinct operands (itself + another phi in
    /// the cycle). Tarjan's SCC algorithm detects the cycle and collapses it.
    ///
    /// Call after `seal_remaining()`.
    pub fn remove_redundant_phi_sccs(&mut self) {
        let phi_ids: Vec<PhiId> = (0..self.phis.len()).map(PhiId).collect();
        self.remove_redundant_phi_sccs_inner(&phi_ids, 0);
    }

    const MAX_SCC_DEPTH: usize = 32;

    fn remove_redundant_phi_sccs_inner(&mut self, phi_ids: &[PhiId], depth: usize) {
        if phi_ids.len() < 2 {
            return;
        }
        if depth >= Self::MAX_SCC_DEPTH {
            tracing::warn!(
                depth,
                phis = phi_ids.len(),
                "SCC phi elimination hit max recursion depth"
            );
            return;
        }

        // Build a DiGraph of phi-to-phi edges for SCC computation.
        let mut phi_graph = DiGraph::<PhiId, ()>::new();
        let mut phi_to_node: FxHashMap<PhiId, NodeIndex> = FxHashMap::default();

        for &pid in phi_ids {
            phi_to_node.insert(pid, phi_graph.add_node(pid));
        }
        for &pid in phi_ids {
            for op in &self.phis[pid.0].operands {
                if let SsaValue::Phi(target) = op
                    && let (Some(&src), Some(&tgt)) =
                        (phi_to_node.get(&pid), phi_to_node.get(target))
                {
                    phi_graph.add_edge(src, tgt, ());
                }
            }
        }

        // tarjan_scc returns SCCs in reverse topological order.
        let sccs = tarjan_scc(&phi_graph);

        for scc_nodes in &sccs {
            if scc_nodes.len() <= 1 {
                continue;
            }

            let scc: Vec<PhiId> = scc_nodes.iter().map(|&n| phi_graph[n]).collect();
            let scc_set: FxHashSet<PhiId> = scc.iter().copied().collect();

            // Collect external operands (values outside the SCC).
            let mut outer_values: FxHashSet<SsaValue<'a>> = FxHashSet::default();
            let mut inner_phis: Vec<PhiId> = Vec::new();

            for &pid in &scc {
                let mut has_external = false;
                for op in &self.phis[pid.0].operands {
                    match op {
                        SsaValue::Phi(p) if scc_set.contains(p) => {}
                        other => {
                            outer_values.insert(other.clone());
                            has_external = true;
                        }
                    }
                }
                if !has_external {
                    inner_phis.push(pid);
                }
            }

            if outer_values.len() == 1 {
                // All phis in the SCC produce the same external value — collapse.
                let replacement = outer_values.into_iter().next().unwrap();
                trace!(
                    self.tracer,
                    SsaSccCollapse {
                        scc_size: scc.len(),
                        replacement: replacement.trace_display(),
                    }
                );
                let phi_vals: Vec<SsaValue<'a>> = scc.iter().map(|&p| SsaValue::Phi(p)).collect();
                for &pid in &scc {
                    // Update current_def
                    let variable = self.phis[pid.0].variable;
                    let block = self.phis[pid.0].block;
                    if let Some(block_defs) = self.current_def.get_mut(&variable)
                        && block_defs.get(&block) == Some(&SsaValue::Phi(pid))
                    {
                        block_defs.insert(block, replacement.clone());
                    }
                    self.stats.phis_trivial += 1;
                }
                // Replace all references to SCC phis in ALL phi operands.
                for phi in &mut self.phis {
                    for op in &mut phi.operands {
                        if phi_vals.contains(op) {
                            *op = replacement.clone();
                        }
                    }
                    // Invalidate witnesses that reference collapsed phis.
                    for w in &mut phi.witnesses {
                        if let Some(wv) = w
                            && phi_vals.contains(wv)
                        {
                            *w = None;
                        }
                    }
                }
            } else if outer_values.len() > 1 && !inner_phis.is_empty() {
                // Multiple external values — recurse on inner phis that
                // have no external operands (they might form a sub-SCC).
                self.remove_redundant_phi_sccs_inner(&inner_phis, depth + 1);
            }
        }
    }

    /// Expand a value into its concrete reaching definitions.
    /// Phi nodes are recursively expanded. Cycles are handled via visited set.
    fn resolve_value(&self, value: &SsaValue<'a>) -> ReachingDefs<'a> {
        // Fast path: non-Phi values resolve directly without allocating HashSets.
        match value {
            SsaValue::LocalDef(_)
            | SsaValue::ImportRef(_)
            | SsaValue::Type(_)
            | SsaValue::ResolvedSite(_)
            | SsaValue::Alias(_) => {
                return ReachingDefs {
                    values: smallvec::smallvec![value.clone()],
                };
            }
            SsaValue::Opaque | SsaValue::Marker => {
                return ReachingDefs::default();
            }
            SsaValue::Phi(_) => {} // fall through to full resolution
        }

        let mut values = SmallVec::new();
        let mut visited = FxHashSet::default();
        self.resolve_value_recursive(value, &mut values, &mut visited);

        let mut seen = FxHashSet::default();
        values.retain(|v| seen.insert(v.clone()));

        ReachingDefs { values }
    }

    fn resolve_value_recursive(
        &self,
        value: &SsaValue<'a>,
        out: &mut SmallVec<[SsaValue<'a>; 2]>,
        visited: &mut FxHashSet<PhiId>,
    ) {
        match value {
            SsaValue::Phi(phi_id) => {
                if !visited.insert(*phi_id) {
                    return; // cycle
                }
                for op in &self.phis[phi_id.0].operands {
                    self.resolve_value_recursive(op, out, visited);
                }
            }
            SsaValue::Opaque | SsaValue::Marker => {} // don't include in results
            other => out.push(other.clone()),
        }
    }
}

impl<'a> Default for SsaEngine<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_block_write_read() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, SsaValue::LocalDef(0));
        let result = ssa.read_variable_stateless("x", b);

        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    #[test]
    fn read_from_predecessor() {
        let mut ssa = SsaEngine::new();
        let b0 = ssa.add_block();
        let b1 = ssa.add_block();
        ssa.add_predecessor(b1, b0);
        ssa.seal_block(b0);
        ssa.seal_block(b1);

        ssa.write_variable("x", b0, SsaValue::LocalDef(0));
        let result = ssa.read_variable_stateless("x", b1);

        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    #[test]
    fn phi_at_join_point() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let then_b = ssa.add_block();
        let else_b = ssa.add_block();
        let join = ssa.add_block();

        ssa.add_predecessor(then_b, entry);
        ssa.add_predecessor(else_b, entry);
        ssa.add_predecessor(join, then_b);
        ssa.add_predecessor(join, else_b);

        ssa.seal_block(entry);
        ssa.seal_block(then_b);
        ssa.seal_block(else_b);
        ssa.seal_block(join);

        ssa.write_variable("x", then_b, SsaValue::LocalDef(0));
        ssa.write_variable("x", else_b, SsaValue::LocalDef(1));

        let result = ssa.read_variable_stateless("x", join);
        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains(&SsaValue::LocalDef(0)));
        assert!(result.values.contains(&SsaValue::LocalDef(1)));
    }

    #[test]
    fn trivial_phi_collapsed() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let then_b = ssa.add_block();
        let else_b = ssa.add_block();
        let join = ssa.add_block();

        ssa.add_predecessor(then_b, entry);
        ssa.add_predecessor(else_b, entry);
        ssa.add_predecessor(join, then_b);
        ssa.add_predecessor(join, else_b);

        ssa.seal_block(entry);
        ssa.seal_block(then_b);
        ssa.seal_block(else_b);
        ssa.seal_block(join);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));

        let result = ssa.read_variable_stateless("x", join);
        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    #[test]
    fn loop_phi() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let header = ssa.add_block();
        let body = ssa.add_block();
        let exit = ssa.add_block();

        ssa.add_predecessor(header, entry);
        ssa.add_predecessor(body, header);
        ssa.add_predecessor(header, body);
        ssa.add_predecessor(exit, header);

        ssa.seal_block(entry);
        ssa.seal_block(body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));
        ssa.write_variable("x", body, SsaValue::LocalDef(1));

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains(&SsaValue::LocalDef(0)));
        assert!(result.values.contains(&SsaValue::LocalDef(1)));
    }

    #[test]
    fn loop_no_redefinition_trivial_phi() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let header = ssa.add_block();
        let body = ssa.add_block();
        let exit = ssa.add_block();

        ssa.add_predecessor(header, entry);
        ssa.add_predecessor(body, header);
        ssa.add_predecessor(header, body);
        ssa.add_predecessor(exit, header);

        ssa.seal_block(entry);
        ssa.seal_block(body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    /// Braun et al. Algorithm 4: sealing a block must invoke
    /// `tryRemoveTrivialPhi` on every operand-less phi it resolves.
    /// When the deferred phi's predecessors all agree on a single
    /// value, the phi must collapse rather than survive as dead
    /// scaffolding in the SSA graph.
    #[test]
    fn unsealed_block_trivial_phi_collapses_on_seal() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let header = ssa.add_block();
        let body = ssa.add_block();
        let exit = ssa.add_block();

        ssa.add_predecessor(header, entry);
        ssa.add_predecessor(body, header);
        ssa.add_predecessor(exit, header);

        ssa.seal_block(entry);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));
        // Read while header is unsealed: forces an incomplete phi.
        let _ = ssa.read_variable_stateless("x", body);

        // Back-edge writes the same value as entry.
        ssa.write_variable("x", body, SsaValue::LocalDef(0));

        ssa.seal_block(body);
        ssa.add_predecessor(header, body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
        assert!(
            ssa.stats.phis_trivial > 0,
            "seal_block must route through tryRemoveTrivialPhi (Algorithm 4)",
        );
    }

    #[test]
    fn unsealed_block_deferred_phi() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let header = ssa.add_block();
        let body = ssa.add_block();
        let exit = ssa.add_block();

        ssa.add_predecessor(header, entry);
        ssa.add_predecessor(body, header);
        ssa.add_predecessor(exit, header);

        ssa.seal_block(entry);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));
        ssa.write_variable("x", body, SsaValue::LocalDef(1));

        ssa.seal_block(body);

        ssa.add_predecessor(header, body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        let result = ssa.read_variable_stateless("x", exit);
        assert!(result.values.contains(&SsaValue::LocalDef(0)));
        assert!(result.values.contains(&SsaValue::LocalDef(1)));
    }

    #[test]
    fn multiple_variables_independent() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, SsaValue::LocalDef(0));
        ssa.write_variable("y", b, SsaValue::LocalDef(1));

        let rx = ssa.read_variable_stateless("x", b);
        let ry = ssa.read_variable_stateless("y", b);

        assert_eq!(rx.values.as_slice(), &[SsaValue::LocalDef(0)]);
        assert_eq!(ry.values.as_slice(), &[SsaValue::LocalDef(1)]);
    }

    #[test]
    fn overwrite_in_same_block() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, SsaValue::LocalDef(0));
        ssa.write_variable("x", b, SsaValue::LocalDef(1));

        let result = ssa.read_variable_stateless("x", b);
        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(1)]);
    }

    #[test]
    fn undefined_variable_is_empty() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        let result = ssa.read_variable_stateless("x", b);
        assert!(result.values.is_empty());
    }

    #[test]
    fn nested_if_else() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let outer_then = ssa.add_block();
        let inner_then = ssa.add_block();
        let inner_else = ssa.add_block();
        let inner_join = ssa.add_block();
        let outer_else = ssa.add_block();
        let outer_join = ssa.add_block();

        ssa.add_predecessor(outer_then, entry);
        ssa.add_predecessor(outer_else, entry);
        ssa.add_predecessor(inner_then, outer_then);
        ssa.add_predecessor(inner_else, outer_then);
        ssa.add_predecessor(inner_join, inner_then);
        ssa.add_predecessor(inner_join, inner_else);
        ssa.add_predecessor(outer_join, inner_join);
        ssa.add_predecessor(outer_join, outer_else);

        for b in [
            entry, outer_then, inner_then, inner_else, inner_join, outer_else, outer_join,
        ] {
            ssa.seal_block(b);
        }

        ssa.write_variable("x", inner_then, SsaValue::LocalDef(0));
        ssa.write_variable("x", inner_else, SsaValue::LocalDef(1));
        ssa.write_variable("x", outer_else, SsaValue::LocalDef(2));

        let result = ssa.read_variable_stateless("x", outer_join);
        assert_eq!(result.values.len(), 3);
        assert!(result.values.contains(&SsaValue::LocalDef(0)));
        assert!(result.values.contains(&SsaValue::LocalDef(1)));
        assert!(result.values.contains(&SsaValue::LocalDef(2)));
    }

    #[test]
    fn import_and_def_values() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("os", b, SsaValue::ImportRef(0));
        ssa.write_variable("MyClass", b, SsaValue::LocalDef(0));

        let r1 = ssa.read_variable_stateless("os", b);
        let r2 = ssa.read_variable_stateless("MyClass", b);

        assert_eq!(r1.values.as_slice(), &[SsaValue::ImportRef(0)]);
        assert_eq!(r2.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    /// Irreducible control flow: two blocks that are mutual predecessors,
    /// each defining x via phi. Both phis reference each other + the same
    /// external value. SCC removal should collapse both to that value.
    ///
    /// ```text
    ///   entry (x = LocalDef(0))
    ///     |         |
    ///     v         v
    ///   left ←→ right
    ///     |         |
    ///     v         v
    ///       exit
    /// ```
    #[test]
    fn scc_mutual_phi_collapse() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let left = ssa.add_block();
        let right = ssa.add_block();
        let exit = ssa.add_block();

        // entry → left, entry → right
        ssa.add_predecessor(left, entry);
        ssa.add_predecessor(right, entry);
        // left ←→ right (irreducible)
        ssa.add_predecessor(left, right);
        ssa.add_predecessor(right, left);
        // both → exit
        ssa.add_predecessor(exit, left);
        ssa.add_predecessor(exit, right);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));

        ssa.seal_block(entry);
        ssa.seal_block(left);
        ssa.seal_block(right);
        ssa.seal_block(exit);

        // Before SCC removal: reading x in exit should produce LocalDef(0)
        // but the phis in left/right form a cycle referencing each other.
        // trivial phi removal can't collapse them because each phi sees
        // two operands: LocalDef(0) + Phi(other).
        // SCC removal detects the cycle and collapses both to LocalDef(0).
        ssa.remove_redundant_phi_sccs();

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    /// SCC with multiple external values — should NOT collapse.
    #[test]
    fn scc_no_collapse_multiple_values() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        let left = ssa.add_block();
        let right = ssa.add_block();
        let exit = ssa.add_block();

        ssa.add_predecessor(left, entry);
        ssa.add_predecessor(right, entry);
        ssa.add_predecessor(left, right);
        ssa.add_predecessor(right, left);
        ssa.add_predecessor(exit, left);
        ssa.add_predecessor(exit, right);

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));
        // Write a different value in one of the cycle blocks
        ssa.write_variable("x", left, SsaValue::LocalDef(1));

        ssa.seal_block(entry);
        ssa.seal_block(left);
        ssa.seal_block(right);
        ssa.seal_block(exit);

        ssa.remove_redundant_phi_sccs();

        let result = ssa.read_variable_stateless("x", exit);
        // Should have both values — SCC not collapsed
        assert!(
            result.values.contains(&SsaValue::LocalDef(0))
                && result.values.contains(&SsaValue::LocalDef(1)),
            "SCC with multiple external values must not be collapsed: got {:?}",
            result.values
        );
    }

    fn xorshift(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    struct CfgSpec {
        preds: Vec<Vec<usize>>,
        writes: Vec<(usize, u32)>,
    }

    fn gen_cfg(seed: u64) -> CfgSpec {
        let mut s = seed | 1;
        let n = 3 + (xorshift(&mut s) % 38) as usize;
        let mut preds = vec![Vec::new(); n];
        // Forward spine: every block has an earlier predecessor, so it is
        // reachable from the entry and the backbone is acyclic.
        for (i, block_preds) in preds.iter_mut().enumerate().skip(1) {
            block_preds.push((xorshift(&mut s) % i as u64) as usize);
        }
        // Back-edges: a later block flowing back to an earlier one. The earlier
        // block becomes a multi-predecessor loop header, so every cycle carries
        // a marker-bearing block and the recursive reference still terminates —
        // matching the structured CFGs the DSL actually emits.
        for (i, block_preds) in preds.iter_mut().enumerate().take(n - 1).skip(1) {
            if xorshift(&mut s).is_multiple_of(3) {
                let span = (n - 1 - i) as u64;
                let later = i + 1 + (xorshift(&mut s) % span) as usize;
                block_preds.push(later);
            }
        }
        let mut writes = vec![(0usize, 0u32)];
        let extra = (xorshift(&mut s) % 4) as usize;
        for j in 0..extra {
            let b = (xorshift(&mut s) % n as u64) as usize;
            writes.push((b, (j + 1) as u32));
        }
        CfgSpec { preds, writes }
    }

    fn build_engine(spec: &CfgSpec) -> (SsaEngine<'static>, Vec<BlockId>) {
        let mut ssa = SsaEngine::new();
        let ids: Vec<BlockId> = (0..spec.preds.len()).map(|_| ssa.add_block()).collect();
        for (i, ps) in spec.preds.iter().enumerate() {
            for &p in ps {
                ssa.add_predecessor(ids[i], ids[p]);
            }
        }
        for &(b, v) in &spec.writes {
            ssa.write_variable("v", ids[b], SsaValue::LocalDef(v));
        }
        for id in &ids {
            ssa.seal_block(*id);
        }
        (ssa, ids)
    }

    fn value_set(defs: &ReachingDefs<'_>) -> std::collections::BTreeSet<String> {
        defs.values.iter().map(|v| v.trace_display()).collect()
    }

    // The iterative read walk must produce the same reaching defs as the
    // recursive one on every CFG, including cycles and phi merges.
    #[test]
    fn iterative_matches_recursive_on_random_cfgs() {
        for seed in 1..400u64 {
            let spec = gen_cfg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            for query in 0..spec.preds.len() {
                let (mut rec, rec_ids) = build_engine(&spec);
                let (mut itr, itr_ids) = build_engine(&spec);
                let want = value_set(&rec.read_variable_stateless_recursive("v", rec_ids[query]));
                let got = value_set(&itr.read_variable_stateless("v", itr_ids[query]));
                assert_eq!(
                    want, got,
                    "seed {seed}, query block {query}: recursive vs iterative disagree"
                );
            }
        }
    }

    // 8k try/except joins (~16k-deep walk) on a 512 KiB stack: the recursive
    // path overflows here, the iterative one stays flat and resolves correctly.
    #[test]
    fn iterative_resolves_deep_chain_without_overflow() {
        let values = std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let mut ssa = SsaEngine::new();
                let entry = ssa.add_block();
                ssa.seal_block(entry);
                ssa.write_variable("np", entry, SsaValue::LocalDef(0));
                let mut pre = entry;
                for _ in 0..8_000 {
                    let arm_a = ssa.add_sealed_successor(pre);
                    let arm_b = ssa.add_sealed_successor(pre);
                    pre = ssa.add_sealed_join([arm_a, arm_b, pre]);
                }
                ssa.read_variable_stateless("np", pre).values.to_vec()
            })
            .unwrap()
            .join()
            .expect("iterative read overflowed the stack");

        assert_eq!(values.as_slice(), &[SsaValue::LocalDef(0)]);
    }
}
