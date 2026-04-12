//! SSA-based reaching definitions resolver.
//!
//! Implements the Braun et al. algorithm ("Simple and Efficient Construction of
//! Static Single Assignment Form", CC 2013) adapted for code-graph reference
//! resolution.
//!
//! The algorithm has three operations:
//! - `write_variable(name, block, value)` — record a definition
//! - `read_variable(name, block)` — look up a variable's reaching definition(s)
//! - `seal_block(block)` — all predecessors of this block are now known
//!
//! At control-flow join points (if/else, try/except, loops), phi nodes are
//! inserted automatically. Trivial phis (referencing only one real value) are
//! collapsed on the fly, producing minimal pruned SSA for reducible CFGs.
//!
//! The per-language `LanguageRules` trait drives the resolver by walking the
//! AST and calling these operations. The resolver itself is language-agnostic.

use rustc_hash::{FxHashMap, FxHashSet};
use std::fmt;

// ── Value ───────────────────────────────────────────────────────

/// A value in the SSA graph — what a variable resolves to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    /// A definition in a file (file index + definition index).
    Def(usize, usize),
    /// An import in a file (file index + import index).
    Import(usize, usize),
    /// A type name (for type-flow languages: resolve members on this type).
    Type(String),
    /// Dead end — parameter, literal, or otherwise unresolvable.
    Opaque,
    /// Internal: a phi node (will be resolved to concrete values).
    Phi(PhiId),
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
    variable: String,
    operands: Vec<Value>,
}

// ── Block ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Block {
    predecessors: Vec<BlockId>,
    sealed: bool,
}

// ── Read result ─────────────────────────────────────────────────

/// The concrete values a variable resolves to at a given program point.
/// Phi nodes are fully resolved into their constituent concrete values.
#[derive(Debug, Clone, Default)]
pub struct ReachingDefs {
    pub values: Vec<Value>,
}

impl ReachingDefs {
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get all Def values, ignoring imports/types/opaque.
    pub fn defs(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.values.iter().filter_map(|v| match v {
            Value::Def(file_idx, def_idx) => Some((*file_idx, *def_idx)),
            _ => None,
        })
    }

    /// Get all Import values.
    pub fn imports(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.values.iter().filter_map(|v| match v {
            Value::Import(file_idx, import_idx) => Some((*file_idx, *import_idx)),
            _ => None,
        })
    }

    /// Get all Type values.
    pub fn types(&self) -> impl Iterator<Item = &str> + '_ {
        self.values.iter().filter_map(|v| match v {
            Value::Type(t) => Some(t.as_str()),
            _ => None,
        })
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
    current_def: FxHashMap<String, FxHashMap<BlockId, Value>>,
    /// Incomplete phis for unsealed blocks: block → variable → phi_id
    incomplete_phis: FxHashMap<BlockId, FxHashMap<String, PhiId>>,
    /// Recorded reads: (block, reference_index) → variable name.
    /// The language rules call `read_variable` and we store the result.
    reads: Vec<(BlockId, String, ReachingDefs)>,
}

impl SsaResolver {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            phis: Vec::new(),
            current_def: FxHashMap::default(),
            incomplete_phis: FxHashMap::default(),
            reads: Vec::new(),
        }
    }

    /// Create a new basic block. Returns its ID.
    /// Predecessors can be added later, but must all be added before sealing.
    pub fn add_block(&mut self) -> BlockId {
        let id = BlockId(self.blocks.len());
        self.blocks.push(Block {
            predecessors: Vec::new(),
            sealed: false,
        });
        id
    }

    /// Add a predecessor edge: `pred` flows into `block`.
    pub fn add_predecessor(&mut self, block: BlockId, pred: BlockId) {
        self.blocks[block.0].predecessors.push(pred);
    }

    /// Seal a block — all predecessors are now known.
    /// Resolves any incomplete phi nodes that were deferred.
    pub fn seal_block(&mut self, block: BlockId) {
        // Process incomplete phis for this block
        if let Some(incomplete) = self.incomplete_phis.remove(&block) {
            for (variable, phi_id) in incomplete {
                self.add_phi_operands(&variable, phi_id);
            }
        }
        self.blocks[block.0].sealed = true;
    }

    /// Record a variable definition: `variable` is defined as `value` in `block`.
    pub fn write_variable(&mut self, variable: &str, block: BlockId, value: Value) {
        self.current_def
            .entry(variable.to_string())
            .or_default()
            .insert(block, value);
    }

    /// Look up a variable's reaching definition(s) at `block`.
    /// Returns the resolved values (phi nodes are expanded).
    pub fn read_variable(&mut self, variable: &str, block: BlockId) -> ReachingDefs {
        let value = self.read_variable_internal(variable, block);
        let defs = self.resolve_value(&value);
        self.reads.push((block, variable.to_string(), defs.clone()));
        defs
    }

    /// Get all recorded reads (for the ReachingResolver to produce edges).
    pub fn reads(&self) -> &[(BlockId, String, ReachingDefs)] {
        &self.reads
    }

    // ── Internal: Braun et al. algorithm ────────────────────────

    fn read_variable_internal(&mut self, variable: &str, block: BlockId) -> Value {
        // Local value numbering: check current block first
        if let Some(block_defs) = self.current_def.get(variable) {
            if let Some(value) = block_defs.get(&block) {
                return value.clone();
            }
        }

        // Global value numbering
        self.read_variable_recursive(variable, block)
    }

    fn read_variable_recursive(&mut self, variable: &str, block: BlockId) -> Value {
        let val;
        let sealed = self.blocks[block.0].sealed;
        let num_preds = self.blocks[block.0].predecessors.len();

        if !sealed {
            // Incomplete CFG: create a placeholder phi
            let phi_id = self.new_phi(block, variable);
            self.incomplete_phis
                .entry(block)
                .or_default()
                .insert(variable.to_string(), phi_id);
            val = Value::Phi(phi_id);
        } else if num_preds == 0 {
            // Entry block with no predecessors — variable is undefined
            val = Value::Opaque;
        } else if num_preds == 1 {
            // Single predecessor — no phi needed, just recurse
            let pred = self.blocks[block.0].predecessors[0];
            val = self.read_variable_internal(variable, pred);
        } else {
            // Multiple predecessors — insert phi, then fill operands
            let phi_id = self.new_phi(block, variable);
            // Write before recursing to break cycles
            self.write_variable(variable, block, Value::Phi(phi_id));
            self.add_phi_operands(variable, phi_id);
            val = self.try_remove_trivial_phi(phi_id);
        }

        self.write_variable(variable, block, val.clone());
        val
    }

    fn new_phi(&mut self, block: BlockId, variable: &str) -> PhiId {
        let id = PhiId(self.phis.len());
        self.phis.push(PhiNode {
            block,
            variable: variable.to_string(),
            operands: Vec::new(),
        });
        id
    }

    fn add_phi_operands(&mut self, variable: &str, phi_id: PhiId) {
        let block = self.phis[phi_id.0].block;
        let preds: Vec<BlockId> = self.blocks[block.0].predecessors.clone();
        for pred in preds {
            let val = self.read_variable_internal(variable, pred);
            self.phis[phi_id.0].operands.push(val);
        }
    }

    /// Remove trivial phi: if it references only one real value (plus itself),
    /// replace it with that value.
    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> Value {
        let mut same: Option<Value> = None;

        for op in self.phis[phi_id.0].operands.clone() {
            // Skip self-references and duplicates of `same`
            if op == Value::Phi(phi_id) || Some(&op) == same.as_ref() {
                continue;
            }
            if same.is_some() {
                // The phi merges at least two distinct values — not trivial
                return Value::Phi(phi_id);
            }
            same = Some(op);
        }

        let replacement = same.unwrap_or(Value::Opaque);

        // Replace all uses of this phi with the replacement
        let variable = self.phis[phi_id.0].variable.clone();
        let block = self.phis[phi_id.0].block;

        // Update current_def if it points to this phi
        if let Some(block_defs) = self.current_def.get_mut(&variable) {
            if block_defs.get(&block) == Some(&Value::Phi(phi_id)) {
                block_defs.insert(block, replacement.clone());
            }
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
        let mut values = Vec::new();
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
        out: &mut Vec<Value>,
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
        let result = ssa.read_variable("x", b);

        assert_eq!(result.values, vec![Value::Def(0, 0)]);
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
        let result = ssa.read_variable("x", b1);

        assert_eq!(result.values, vec![Value::Def(0, 0)]);
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

        let result = ssa.read_variable("x", join);
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

        let result = ssa.read_variable("x", join);
        assert_eq!(result.values, vec![Value::Def(0, 0)]);
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

        let result = ssa.read_variable("x", exit);
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

        let result = ssa.read_variable("x", exit);
        // Should collapse to single def (trivial phi)
        assert_eq!(result.values, vec![Value::Def(0, 0)]);
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
        let result = ssa.read_variable("x", exit);
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

        let rx = ssa.read_variable("x", b);
        let ry = ssa.read_variable("y", b);

        assert_eq!(rx.values, vec![Value::Def(0, 0)]);
        assert_eq!(ry.values, vec![Value::Def(0, 1)]);
    }

    #[test]
    fn overwrite_in_same_block() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        ssa.write_variable("x", b, Value::Def(0, 0));
        ssa.write_variable("x", b, Value::Def(0, 1)); // overwrite

        let result = ssa.read_variable("x", b);
        assert_eq!(result.values, vec![Value::Def(0, 1)]);
    }

    #[test]
    fn undefined_variable_is_empty() {
        let mut ssa = SsaResolver::new();
        let b = ssa.add_block();
        ssa.seal_block(b);

        let result = ssa.read_variable("x", b);
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

        let result = ssa.read_variable("x", outer_join);
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

        let r1 = ssa.read_variable("os", b);
        let r2 = ssa.read_variable("MyClass", b);

        assert_eq!(r1.values, vec![Value::Import(0, 0)]);
        assert_eq!(r2.values, vec![Value::Def(0, 0)]);
    }
}
