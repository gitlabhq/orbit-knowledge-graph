//! SSA-based reaching definitions resolver.
//!
//! Implements the Braun et al. algorithm ("Simple and Efficient Construction of
//! Static Single Assignment Form", CC 2013) adapted for code-graph reference
//! resolution.
//!
//! All string data (variable names, type/alias values) is arena-backed via
//! `FileArena`. No `Intern<str>`, no global RwLock, no memory leak.

use super::state::{BlockId, PhiId, ReachingDefs, Value};
use super::stats::SsaStats;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

// ── Phi node ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PhiNode<'a> {
    block: BlockId,
    variable: &'a str,
    operands: SmallVec<[Value<'a>; 2]>,
    /// Witness caching (Section 3.1): first two distinct non-self operands.
    /// If both are still valid and distinct, the phi is non-trivial without
    /// scanning all operands.
    witnesses: [Option<Value<'a>>; 2],
}

// ── Block ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Block {
    predecessors: SmallVec<[BlockId; 2]>,
    sealed: bool,
}

// ── SSA Resolver ────────────────────────────────────────────────

/// SSA-based reaching definitions resolver (Braun et al. algorithm).
///
/// All variable names and string values are `&'a str` backed by
/// [`FileArena`]. No `Intern<str>`, no global RwLock.
pub struct SsaResolver<'a> {
    blocks: Vec<Block>,
    phis: Vec<PhiNode<'a>>,
    /// current_def[variable][block] = value
    current_def: FxHashMap<&'a str, FxHashMap<BlockId, Value<'a>>>,
    /// Incomplete phis for unsealed blocks: block → variable → phi_id
    incomplete_phis: FxHashMap<BlockId, FxHashMap<&'a str, PhiId>>,
    /// Counters for SSA operations.
    pub stats: SsaStats,
}

impl<'a> SsaResolver<'a> {
    pub fn new() -> Self {
        Self {
            blocks: Vec::with_capacity(32),
            phis: Vec::with_capacity(8),
            current_def: FxHashMap::with_capacity_and_hasher(64, Default::default()),
            incomplete_phis: FxHashMap::default(),
            stats: SsaStats::default(),
        }
    }

    /// Create a new basic block. Returns its ID.
    pub fn add_block(&mut self) -> BlockId {
        let id = BlockId(self.blocks.len());
        self.blocks.push(Block {
            predecessors: SmallVec::new(),
            sealed: false,
        });
        self.stats.blocks_created += 1;
        id
    }

    /// Add a predecessor edge: `pred` flows into `block`.
    pub fn add_predecessor(&mut self, block: BlockId, pred: BlockId) {
        self.blocks[block.0].predecessors.push(pred);
    }

    /// Seal a block — all predecessors are now known.
    /// Resolves any incomplete phi nodes that were deferred.
    pub fn seal_block(&mut self, block: BlockId) {
        if let Some(incomplete) = self.incomplete_phis.remove(&block) {
            for (variable, phi_id) in incomplete {
                self.add_phi_operands(variable, phi_id);
            }
        }
        self.blocks[block.0].sealed = true;
    }

    /// Seal any blocks that haven't been sealed yet.
    pub fn seal_remaining(&mut self) {
        for id in 0..self.blocks.len() {
            if !self.blocks[id].sealed {
                self.seal_block(BlockId(id));
            }
        }
    }

    /// Record a variable definition: `variable` is defined as `value` in `block`.
    /// On-the-fly copy propagation (Section 3.1): if the value is an alias
    /// to another variable, resolve it immediately instead of deferring.
    pub fn write_variable(&mut self, variable: &'a str, block: BlockId, value: Value<'a>) {
        let resolved = if let Value::Alias(alias_name) = value {
            let alias_val = self.read_variable_internal(alias_name, block);
            if alias_val != Value::Opaque {
                alias_val
            } else {
                Value::Alias(alias_name)
            }
        } else {
            value
        };
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, resolved);
        self.stats.writes += 1;
    }

    /// Look up a variable's reaching definitions without recording the read.
    pub fn read_variable_stateless(
        &mut self,
        variable: &'a str,
        block: BlockId,
    ) -> ReachingDefs<'a> {
        self.stats.reads += 1;
        let value = self.read_variable_internal(variable, block);
        self.resolve_value(&value)
    }

    // ── Internal: Braun et al. algorithm ────────────────────────

    fn read_variable_internal(&mut self, variable: &'a str, block: BlockId) -> Value<'a> {
        // Local value numbering: check current block first
        if let Some(block_defs) = self.current_def.get(&variable)
            && let Some(value) = block_defs.get(&block)
        {
            self.stats.local_hits += 1;
            return value.clone();
        }

        // Global value numbering
        self.stats.recursive_lookups += 1;
        self.read_variable_recursive(variable, block)
    }

    fn read_variable_recursive(&mut self, variable: &'a str, block: BlockId) -> Value<'a> {
        let val;
        let sealed = self.blocks[block.0].sealed;
        let num_preds = self.blocks[block.0].predecessors.len();

        if !sealed {
            // Incomplete CFG: defer with operandless phi (Algorithm 4)
            self.stats.unsealed_hits += 1;
            let phi_id = self.new_phi(block, variable);
            self.incomplete_phis
                .entry(block)
                .or_default()
                .insert(variable, phi_id);
            val = Value::Phi(phi_id);
        } else if num_preds == 0 {
            self.stats.dead_end_hits += 1;
            val = Value::Opaque;
        } else if num_preds == 1 {
            let pred = self.blocks[block.0].predecessors[0];
            val = self.read_variable_internal(variable, pred);
        } else {
            // Marker algorithm (Section 3.3): mark block before recursing.
            // Only place a phi if we detect a cycle (hit the marker) or
            // find different values from predecessors.
            val = self.read_variable_marker(variable, block);
        }

        self.write_variable_interned(variable, block, val.clone());
        val
    }

    /// Marker algorithm: mark block, collect values from predecessors,
    /// only create a phi if values differ or a cycle was detected.
    fn read_variable_marker(&mut self, variable: &'a str, block: BlockId) -> Value<'a> {
        // Place a marker sentinel so recursive lookups that reach this
        // block again will detect the cycle. Distinct from Opaque so
        // genuine dead-end values aren't misidentified as cycles.
        self.write_variable_interned(variable, block, Value::Marker);

        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        let mut same: Option<Value<'a>> = None;
        let mut need_phi = false;

        for &pred in &preds {
            let pred_val = self.read_variable_internal(variable, pred);
            if pred_val == Value::Marker {
                need_phi = true;
                continue;
            }
            match &same {
                None => same = Some(pred_val),
                Some(s) if *s == pred_val => {}
                Some(_) => {
                    need_phi = true;
                    // Still need to collect remaining operands for the phi
                    break;
                }
            }
        }

        if !need_phi {
            // All predecessors agree (or only one non-cycle predecessor).
            // No phi needed — zero temporary allocations.
            self.stats.markers_elided += 1;
            return same.unwrap_or(Value::Opaque);
        }

        // Different values or cycle detected: fall back to phi creation.
        // Re-collect all operands properly with cycle-breaking phi.
        let phi_id = self.new_phi(block, variable);
        self.write_variable_interned(variable, block, Value::Phi(phi_id));
        self.add_phi_operands(variable, phi_id);
        self.try_remove_trivial_phi(phi_id)
    }

    /// Internal write that takes an already-interned name.
    fn write_variable_interned(&mut self, variable: &'a str, block: BlockId, value: Value<'a>) {
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
        id
    }

    fn add_phi_operands(&mut self, variable: &'a str, phi_id: PhiId) {
        let block = self.phis[phi_id.0].block;
        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        for pred in preds {
            let val = self.read_variable_internal(variable, pred);
            // Update witnesses: track first two distinct non-self operands.
            if val != Value::Phi(phi_id) {
                let phi = &mut self.phis[phi_id.0];
                if phi.witnesses[0].is_none() {
                    phi.witnesses[0] = Some(val.clone());
                } else if phi.witnesses[1].is_none() && phi.witnesses[0].as_ref() != Some(&val) {
                    phi.witnesses[1] = Some(val.clone());
                }
            }
            self.phis[phi_id.0].operands.push(val);
        }
    }

    /// Remove trivial phi: if it references only one real value (plus itself),
    /// replace it with that value.
    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> Value<'a> {
        // Witness cache fast path: if both witnesses are still distinct
        // and neither is the phi itself, the phi is non-trivial.
        let w = &self.phis[phi_id.0].witnesses;
        if let (Some(w0), Some(w1)) = (w[0].as_ref(), w[1].as_ref())
            && w0 != w1
            && *w0 != Value::Phi(phi_id)
            && *w1 != Value::Phi(phi_id)
        {
            return Value::Phi(phi_id);
        }

        let mut same: Option<Value<'a>> = None;

        for i in 0..self.phis[phi_id.0].operands.len() {
            let op = self.phis[phi_id.0].operands[i].clone();
            if op == Value::Phi(phi_id) || Some(&op) == same.as_ref() {
                continue;
            }
            if same.is_some() {
                return Value::Phi(phi_id);
            }
            same = Some(op);
        }

        let replacement = same.unwrap_or(Value::Opaque);
        self.stats.phis_trivial += 1;

        let variable = self.phis[phi_id.0].variable;
        let block = self.phis[phi_id.0].block;

        // Update current_def if it points to this phi
        if let Some(block_defs) = self.current_def.get_mut(&variable)
            && block_defs.get(&block) == Some(&Value::Phi(phi_id))
        {
            block_defs.insert(block, replacement.clone());
        }

        // Check if any other phis using this one become trivial
        let phi_users: Vec<PhiId> = self
            .phis
            .iter()
            .enumerate()
            .filter(|(i, phi)| *i != phi_id.0 && phi.operands.contains(&Value::Phi(phi_id)))
            .map(|(i, _)| PhiId(i))
            .collect();

        // Replace this phi in all users' operands and invalidate witnesses
        let phi_val = Value::Phi(phi_id);
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

        // Recursively try to simplify users
        for user_id in phi_users {
            self.try_remove_trivial_phi(user_id);
        }

        replacement
    }

    /// Remove redundant phi SCCs (Algorithm 5, Section 3.2 of Braun et al.).
    /// Handles irreducible control flow where sets of phis reference only
    /// each other plus one external value. Call after SSA construction is complete.
    pub fn remove_redundant_phis(&mut self) {
        let phi_indices: Vec<PhiId> = (0..self.phis.len()).map(PhiId).collect();
        self.remove_redundant_phis_inner(&phi_indices);
    }

    fn remove_redundant_phis_inner(&mut self, phi_ids: &[PhiId]) {
        if phi_ids.is_empty() {
            return;
        }

        // Build a petgraph DiGraph of the phi subgraph for SCC computation.
        let mut phi_graph = petgraph::graph::DiGraph::<PhiId, ()>::new();
        let mut phi_to_node: FxHashMap<PhiId, petgraph::graph::NodeIndex> = FxHashMap::default();

        for &pid in phi_ids {
            let n = phi_graph.add_node(pid);
            phi_to_node.insert(pid, n);
        }
        for &pid in phi_ids {
            for op in &self.phis[pid.0].operands {
                if let Value::Phi(p) = op
                    && let (Some(&src), Some(&tgt)) = (phi_to_node.get(&pid), phi_to_node.get(p))
                {
                    phi_graph.add_edge(src, tgt, ());
                }
            }
        }

        // petgraph's tarjan_scc returns SCCs in reverse topological order
        let sccs = petgraph::algo::tarjan_scc(&phi_graph);

        // Process in reverse (tarjan_scc returns reverse topo order)
        for scc_nodes in sccs.iter().rev() {
            if scc_nodes.len() <= 1 {
                continue;
            }

            let scc: Vec<PhiId> = scc_nodes.iter().map(|&n| phi_graph[n]).collect();
            let scc_set: FxHashSet<PhiId> = scc.iter().copied().collect();
            let mut outer_ops: FxHashSet<Value<'a>> = FxHashSet::default();
            let mut inner: Vec<PhiId> = Vec::new();

            for &pid in &scc {
                let mut is_inner = true;
                for op in &self.phis[pid.0].operands {
                    if let Value::Phi(p) = op {
                        if !scc_set.contains(p) {
                            outer_ops.insert(op.clone());
                            is_inner = false;
                        }
                    } else {
                        outer_ops.insert(op.clone());
                        is_inner = false;
                    }
                }
                if is_inner {
                    inner.push(pid);
                }
            }

            if outer_ops.len() == 1 {
                // All phis in the SCC produce the same value — collapse.
                // Build reverse map first to avoid O(|SCC| × |total_phis|).
                let replacement = outer_ops.into_iter().next().unwrap();
                let mut users: FxHashMap<PhiId, Vec<usize>> = FxHashMap::default();
                for (i, phi) in self.phis.iter().enumerate() {
                    for op in &phi.operands {
                        if let Value::Phi(p) = op
                            && scc_set.contains(p)
                        {
                            users.entry(*p).or_default().push(i);
                        }
                    }
                }

                for &pid in &scc {
                    let variable = self.phis[pid.0].variable;
                    let block = self.phis[pid.0].block;
                    if let Some(block_defs) = self.current_def.get_mut(&variable)
                        && block_defs.get(&block) == Some(&Value::Phi(pid))
                    {
                        block_defs.insert(block, replacement.clone());
                    }
                    if let Some(user_indices) = users.get(&pid) {
                        for &ui in user_indices {
                            for op in &mut self.phis[ui].operands {
                                if *op == Value::Phi(pid) {
                                    *op = replacement.clone();
                                }
                            }
                        }
                    }
                    self.stats.phis_trivial += 1;
                }
            } else if outer_ops.len() > 1 {
                // Multiple outer values — recurse on inner phis
                self.remove_redundant_phis_inner(&inner);
            }
        }
    }

    /// Expand a value into its concrete reaching definitions.
    /// Phi nodes are recursively expanded. Cycles are handled via visited set.
    fn resolve_value(&self, value: &Value<'a>) -> ReachingDefs<'a> {
        let mut values = SmallVec::new();
        let mut visited = FxHashSet::default();
        self.resolve_value_recursive(value, &mut values, &mut visited);

        let mut seen = FxHashSet::default();
        values.retain(|v| seen.insert(v.clone()));

        ReachingDefs { values }
    }

    fn resolve_value_recursive(
        &self,
        value: &Value<'a>,
        out: &mut SmallVec<[Value<'a>; 2]>,
        visited: &mut FxHashSet<PhiId>,
    ) {
        match value {
            Value::Phi(phi_id) => {
                if !visited.insert(*phi_id) {
                    return; // cycle
                }
                for op in &self.phis[phi_id.0].operands {
                    self.resolve_value_recursive(op, out, visited);
                }
            }
            Value::Opaque | Value::Marker => {} // don't include in results
            other => out.push(other.clone()),
        }
    }
}

impl Default for SsaResolver<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_block_write_read() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, Value::Def(NodeIndex::new(0)));
        let result = ssa.read_variable_stateless("x", b);

        assert_eq!(result.values.as_slice(), &[Value::Def(NodeIndex::new(0))]);
    }

    #[test]
    fn read_from_predecessor() {
        let mut ssa = SsaResolver::new();
        let b0 = ssa.add_block();
        let b1 = ssa.add_block();
        ssa.add_predecessor(b1, b0);
        ssa.seal_block(b0);
        ssa.seal_block(b1);

        ssa.write_variable("x", b0, Value::Def(NodeIndex::new(0)));
        let result = ssa.read_variable_stateless("x", b1);

        assert_eq!(result.values.as_slice(), &[Value::Def(NodeIndex::new(0))]);
    }

    #[test]
    fn phi_at_join_point() {
        // if/else: x = A in then, x = B in else, read x after
        let mut ssa = SsaResolver::new();
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

        ssa.write_variable("x", then_b, Value::Def(NodeIndex::new(0)));
        ssa.write_variable("x", else_b, Value::Def(NodeIndex::new(1)));

        let result = ssa.read_variable_stateless("x", join);
        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains(&Value::Def(NodeIndex::new(0))));
        assert!(result.values.contains(&Value::Def(NodeIndex::new(1))));
    }

    #[test]
    fn trivial_phi_collapsed() {
        // if/else but only one branch defines x — should collapse to that
        let mut ssa = SsaResolver::new();
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

        // x defined in entry, not redefined in either branch
        ssa.write_variable("x", entry, Value::Def(NodeIndex::new(0)));

        let result = ssa.read_variable_stateless("x", join);
        assert_eq!(result.values.as_slice(), &[Value::Def(NodeIndex::new(0))]);
    }

    #[test]
    fn loop_phi() {
        // while loop: x = A before loop, x = B in loop body
        let mut ssa = SsaResolver::new();
        let entry = ssa.add_block();
        let header = ssa.add_block();
        let body = ssa.add_block();
        let exit = ssa.add_block();

        ssa.add_predecessor(header, entry);
        ssa.add_predecessor(body, header);
        ssa.add_predecessor(header, body); // back edge
        ssa.add_predecessor(exit, header);

        ssa.seal_block(entry);
        // header can't be sealed until back edge is added — but we already added it
        ssa.seal_block(body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        ssa.write_variable("x", entry, Value::Def(NodeIndex::new(0)));
        ssa.write_variable("x", body, Value::Def(NodeIndex::new(1)));

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains(&Value::Def(NodeIndex::new(0))));
        assert!(result.values.contains(&Value::Def(NodeIndex::new(1))));
    }

    #[test]
    fn loop_no_redefinition_trivial_phi() {
        // while loop: x = A before loop, NOT redefined in body
        let mut ssa = SsaResolver::new();
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

        ssa.write_variable("x", entry, Value::Def(NodeIndex::new(0)));

        let result = ssa.read_variable_stateless("x", exit);
        // Should collapse to single def (trivial phi)
        assert_eq!(result.values.as_slice(), &[Value::Def(NodeIndex::new(0))]);
    }

    #[test]
    fn unsealed_block_deferred_phi() {
        // Simulate loop construction: header can't be sealed until back edge exists.
        // Following Braun et al. §2.3: construct body first, then seal header.
        let mut ssa = SsaResolver::new();
        let entry = ssa.add_block();
        let header = ssa.add_block();
        let body = ssa.add_block();
        let exit = ssa.add_block();

        // Forward edges
        ssa.add_predecessor(header, entry);
        ssa.add_predecessor(body, header);
        ssa.add_predecessor(exit, header);

        ssa.seal_block(entry);
        // Header NOT sealed yet — back edge will come later

        // Write defs
        ssa.write_variable("x", entry, Value::Def(NodeIndex::new(0)));
        ssa.write_variable("x", body, Value::Def(NodeIndex::new(1)));

        // Seal body (its only predecessor `header` is already added)
        ssa.seal_block(body);

        // Now add back edge and seal header
        ssa.add_predecessor(header, body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        // Read after everything is sealed — should see both values via phi
        let result = ssa.read_variable_stateless("x", exit);
        assert!(result.values.contains(&Value::Def(NodeIndex::new(0))));
        assert!(result.values.contains(&Value::Def(NodeIndex::new(1))));
    }

    #[test]
    fn multiple_variables_independent() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, Value::Def(NodeIndex::new(0)));
        ssa.write_variable("y", b, Value::Def(NodeIndex::new(1)));

        let rx = ssa.read_variable_stateless("x", b);
        let ry = ssa.read_variable_stateless("y", b);

        assert_eq!(rx.values.as_slice(), &[Value::Def(NodeIndex::new(0))]);
        assert_eq!(ry.values.as_slice(), &[Value::Def(NodeIndex::new(1))]);
    }

    #[test]
    fn overwrite_in_same_block() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, Value::Def(NodeIndex::new(0)));
        ssa.write_variable("x", b, Value::Def(NodeIndex::new(1))); // overwrite

        let result = ssa.read_variable_stateless("x", b);
        assert_eq!(result.values.as_slice(), &[Value::Def(NodeIndex::new(1))]);
    }

    #[test]
    fn undefined_variable_is_empty() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        let result = ssa.read_variable_stateless("x", b);
        assert!(result.is_empty());
    }

    #[test]
    fn nested_if_else() {
        // if { if { x=A } else { x=B } } else { x=C }
        let mut ssa = SsaResolver::new();
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

        ssa.write_variable("x", inner_then, Value::Def(NodeIndex::new(0)));
        ssa.write_variable("x", inner_else, Value::Def(NodeIndex::new(1)));
        ssa.write_variable("x", outer_else, Value::Def(NodeIndex::new(2)));

        let result = ssa.read_variable_stateless("x", outer_join);
        assert_eq!(result.values.len(), 3);
        assert!(result.values.contains(&Value::Def(NodeIndex::new(0))));
        assert!(result.values.contains(&Value::Def(NodeIndex::new(1))));
        assert!(result.values.contains(&Value::Def(NodeIndex::new(2))));
    }

    #[test]
    fn import_and_def_values() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("os", b, Value::Import(NodeIndex::new(0)));
        ssa.write_variable("MyClass", b, Value::Def(NodeIndex::new(0)));

        let r1 = ssa.read_variable_stateless("os", b);
        let r2 = ssa.read_variable_stateless("MyClass", b);

        assert_eq!(r1.values.as_slice(), &[Value::Import(NodeIndex::new(0))]);
        assert_eq!(r2.values.as_slice(), &[Value::Def(NodeIndex::new(0))]);
    }
}
