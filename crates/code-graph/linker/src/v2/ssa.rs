//! SSA-based reaching definitions resolver.
//!
//! Implements the Braun et al. algorithm ("Simple and Efficient Construction of
//! Static Single Assignment Form", CC 2013) adapted for code-graph reference
//! resolution.
//!
//! Performance: variable names are interned (`Intern<str>`) so HashMap keys
//! are pointer-sized. Blocks use `SmallVec` for predecessors (most have ≤2).
//! `Value::Type` uses `Intern<str>` for zero-cost cloning.

use internment::Intern;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use std::fmt;

/// Interned variable name. Pointer-sized, O(1) clone and hash.
pub type VarName = Intern<str>;

// ── Value ───────────────────────────────────────────────────────

/// A value in the SSA graph — what a variable resolves to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    /// A definition in a file (file index + definition index).
    Def(usize, usize),
    /// An import in a file (file index + import index).
    Import(usize, usize),
    /// A type name (for type-flow languages: resolve members on this type).
    /// Interned for zero-cost cloning during chain resolution.
    Type(Intern<str>),
    /// Dead end — parameter, literal, or otherwise unresolvable.
    Opaque,
    /// Internal: a phi node (will be resolved to concrete values).
    Phi(PhiId),
}

impl Value {
    /// Create a Type value from a string (interned).
    pub fn type_of(s: &str) -> Self {
        Self::Type(Intern::from(s))
    }

    /// Create a Type value from an owned String (interned).
    pub fn type_from_string(s: String) -> Self {
        Self::Type(Intern::from(s.as_str()))
    }
}

/// Identifier for a phi node in the SSA graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhiId(usize);

/// Identifier for a basic block in the SSA graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(pub usize);

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b{}", self.0)
    }
}

// ── Phi node ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PhiNode {
    block: BlockId,
    variable: VarName,
    operands: SmallVec<[Value; 2]>,
}

// ── Block ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Block {
    predecessors: SmallVec<[BlockId; 2]>,
    sealed: bool,
}

// ── Read result ─────────────────────────────────────────────────

/// The concrete values a variable resolves to at a given program point.
/// Phi nodes are fully resolved into their constituent concrete values.
#[derive(Debug, Clone, Default)]
pub struct ReachingDefs {
    pub values: SmallVec<[Value; 2]>,
}

impl ReachingDefs {
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

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

// ── SSA Resolver ────────────────────────────────────────────────

/// SSA-based reaching definitions resolver (Braun et al. algorithm).
///
/// Create blocks, write variable definitions, read variable uses.
/// The resolver handles phi insertion and trivial phi elimination
/// automatically at control-flow join points.
pub struct SsaResolver {
    blocks: Vec<Block>,
    phis: Vec<PhiNode>,
    /// current_def[variable][block] = value
    current_def: FxHashMap<VarName, FxHashMap<BlockId, Value>>,
    /// Incomplete phis for unsealed blocks: block → variable → phi_id
    incomplete_phis: FxHashMap<BlockId, FxHashMap<VarName, PhiId>>,
    /// Counters for SSA operations.
    pub stats: SsaStats,
}

impl SsaResolver {
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
    pub fn write_variable(&mut self, variable: &str, block: BlockId, value: Value) {
        let var = Intern::from(variable);
        self.current_def
            .entry(var)
            .or_default()
            .insert(block, value);
        self.stats.writes += 1;
    }

    /// Look up a variable's reaching definitions without recording the read.
    pub fn read_variable_stateless(&mut self, variable: &str, block: BlockId) -> ReachingDefs {
        self.stats.reads += 1;
        let var = Intern::from(variable);
        let value = self.read_variable_internal(var, block);
        self.resolve_value(&value)
    }

    // ── Internal: Braun et al. algorithm ────────────────────────

    fn read_variable_internal(&mut self, variable: VarName, block: BlockId) -> Value {
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

    fn read_variable_recursive(&mut self, variable: VarName, block: BlockId) -> Value {
        let val;
        let sealed = self.blocks[block.0].sealed;
        let num_preds = self.blocks[block.0].predecessors.len();

        if !sealed {
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
            let phi_id = self.new_phi(block, variable);
            self.write_variable_interned(variable, block, Value::Phi(phi_id));
            self.add_phi_operands(variable, phi_id);
            val = self.try_remove_trivial_phi(phi_id);
        }

        self.write_variable_interned(variable, block, val.clone());
        val
    }

    /// Internal write that takes an already-interned name.
    fn write_variable_interned(&mut self, variable: VarName, block: BlockId, value: Value) {
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, value);
    }

    fn new_phi(&mut self, block: BlockId, variable: VarName) -> PhiId {
        self.stats.phis_created += 1;
        let id = PhiId(self.phis.len());
        self.phis.push(PhiNode {
            block,
            variable,
            operands: SmallVec::new(),
        });
        id
    }

    fn add_phi_operands(&mut self, variable: VarName, phi_id: PhiId) {
        let block = self.phis[phi_id.0].block;
        // Copy predecessors to avoid borrow conflict (SmallVec: stack-allocated for ≤2).
        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        for pred in preds {
            let val = self.read_variable_internal(variable, pred);
            self.phis[phi_id.0].operands.push(val);
        }
    }

    /// Remove trivial phi: if it references only one real value (plus itself),
    /// replace it with that value.
    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> Value {
        let mut same: Option<Value> = None;

        // Iterate by index to avoid cloning the operands vec.
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

        // Replace this phi in all users' operands
        for user_id in &phi_users {
            for op in &mut self.phis[user_id.0].operands {
                if *op == Value::Phi(phi_id) {
                    *op = replacement.clone();
                }
            }
        }

        // Recursively try to simplify users
        for user_id in phi_users {
            self.try_remove_trivial_phi(user_id);
        }

        replacement
    }

    /// Expand a value into its concrete reaching definitions.
    /// Phi nodes are recursively expanded. Cycles are handled via visited set.
    fn resolve_value(&self, value: &Value) -> ReachingDefs {
        let mut values = SmallVec::new();
        let mut visited = FxHashSet::default();
        self.resolve_value_recursive(value, &mut values, &mut visited);

        // Deduplicate
        let mut seen = FxHashSet::default();
        values.retain(|v| seen.insert(v.clone()));

        ReachingDefs { values }
    }

    fn resolve_value_recursive(
        &self,
        value: &Value,
        out: &mut SmallVec<[Value; 2]>,
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
            Value::Opaque => {} // don't include opaque in results
            other => out.push(other.clone()),
        }
    }
}

impl Default for SsaResolver {
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

        ssa.write_variable("x", b, Value::Def(0, 0));
        let result = ssa.read_variable_stateless("x", b);

        assert_eq!(result.values.as_slice(), &[Value::Def(0, 0)]);
    }

    #[test]
    fn read_from_predecessor() {
        let mut ssa = SsaResolver::new();
        let b0 = ssa.add_block();
        let b1 = ssa.add_block();
        ssa.add_predecessor(b1, b0);
        ssa.seal_block(b0);
        ssa.seal_block(b1);

        ssa.write_variable("x", b0, Value::Def(0, 0));
        let result = ssa.read_variable_stateless("x", b1);

        assert_eq!(result.values.as_slice(), &[Value::Def(0, 0)]);
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

        ssa.write_variable("x", then_b, Value::Def(0, 0));
        ssa.write_variable("x", else_b, Value::Def(0, 1));

        let result = ssa.read_variable_stateless("x", join);
        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains(&Value::Def(0, 0)));
        assert!(result.values.contains(&Value::Def(0, 1)));
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
        ssa.write_variable("x", entry, Value::Def(0, 0));

        let result = ssa.read_variable_stateless("x", join);
        assert_eq!(result.values.as_slice(), &[Value::Def(0, 0)]);
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

        ssa.write_variable("x", entry, Value::Def(0, 0));
        ssa.write_variable("x", body, Value::Def(0, 1));

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains(&Value::Def(0, 0)));
        assert!(result.values.contains(&Value::Def(0, 1)));
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

        ssa.write_variable("x", entry, Value::Def(0, 0));

        let result = ssa.read_variable_stateless("x", exit);
        // Should collapse to single def (trivial phi)
        assert_eq!(result.values.as_slice(), &[Value::Def(0, 0)]);
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
        ssa.write_variable("x", entry, Value::Def(0, 0));
        ssa.write_variable("x", body, Value::Def(0, 1));

        // Seal body (its only predecessor `header` is already added)
        ssa.seal_block(body);

        // Now add back edge and seal header
        ssa.add_predecessor(header, body);
        ssa.seal_block(header);
        ssa.seal_block(exit);

        // Read after everything is sealed — should see both values via phi
        let result = ssa.read_variable_stateless("x", exit);
        assert!(result.values.contains(&Value::Def(0, 0)));
        assert!(result.values.contains(&Value::Def(0, 1)));
    }

    #[test]
    fn multiple_variables_independent() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, Value::Def(0, 0));
        ssa.write_variable("y", b, Value::Def(0, 1));

        let rx = ssa.read_variable_stateless("x", b);
        let ry = ssa.read_variable_stateless("y", b);

        assert_eq!(rx.values.as_slice(), &[Value::Def(0, 0)]);
        assert_eq!(ry.values.as_slice(), &[Value::Def(0, 1)]);
    }

    #[test]
    fn overwrite_in_same_block() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, Value::Def(0, 0));
        ssa.write_variable("x", b, Value::Def(0, 1)); // overwrite

        let result = ssa.read_variable_stateless("x", b);
        assert_eq!(result.values.as_slice(), &[Value::Def(0, 1)]);
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

        ssa.write_variable("x", inner_then, Value::Def(0, 0));
        ssa.write_variable("x", inner_else, Value::Def(0, 1));
        ssa.write_variable("x", outer_else, Value::Def(0, 2));

        let result = ssa.read_variable_stateless("x", outer_join);
        assert_eq!(result.values.len(), 3);
        assert!(result.values.contains(&Value::Def(0, 0)));
        assert!(result.values.contains(&Value::Def(0, 1)));
        assert!(result.values.contains(&Value::Def(0, 2)));
    }

    #[test]
    fn import_and_def_values() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("os", b, Value::Import(0, 0));
        ssa.write_variable("MyClass", b, Value::Def(0, 0));

        let r1 = ssa.read_variable_stateless("os", b);
        let r2 = ssa.read_variable_stateless("MyClass", b);

        assert_eq!(r1.values.as_slice(), &[Value::Import(0, 0)]);
        assert_eq!(r2.values.as_slice(), &[Value::Def(0, 0)]);
    }
}
