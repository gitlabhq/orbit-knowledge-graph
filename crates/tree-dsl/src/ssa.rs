//! SSA engine (Braun et al., CC 2013).
//!
//! Builds reaching-def information during a single pass over annotations.
//! Variable names are `u32` symbol ids from the Lang interner.

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

const MAX_READ_DEPTH: usize = 10_000;
const RED_ZONE: usize = 128 * 1024;
const STACK_SEGMENT: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhiId(usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    LocalDef(u32),
    ImportRef(u32),
    Type(u32),
    Alias(u32),
    Opaque,
    Marker,
    Phi(PhiId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParseValue {
    LocalDef(u32),
    ImportRef(u32),
    Type(u32),
    Opaque,
}

impl Value {
    pub fn to_parse_value(&self) -> Option<ParseValue> {
        match self {
            Value::LocalDef(i) => Some(ParseValue::LocalDef(*i)),
            Value::ImportRef(i) => Some(ParseValue::ImportRef(*i)),
            Value::Type(t) => Some(ParseValue::Type(*t)),
            Value::Opaque => Some(ParseValue::Opaque),
            Value::Alias(_) | Value::Marker | Value::Phi(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
struct PhiNode {
    block: BlockId,
    variable: u32,
    operands: SmallVec<[Value; 2]>,
    witnesses: [Option<Value>; 2],
}

#[derive(Debug, Clone)]
struct Block {
    predecessors: SmallVec<[BlockId; 2]>,
    sealed: bool,
}

pub struct SsaEngine {
    blocks: Vec<Block>,
    phis: Vec<PhiNode>,
    current_def: FxHashMap<u32, FxHashMap<BlockId, Value>>,
    incomplete_phis: FxHashMap<BlockId, FxHashMap<u32, PhiId>>,
    read_depth: usize,
}

impl SsaEngine {
    pub fn new() -> Self {
        Self {
            blocks: Vec::with_capacity(32),
            phis: Vec::with_capacity(8),
            current_def: FxHashMap::with_capacity_and_hasher(64, Default::default()),
            incomplete_phis: FxHashMap::default(),
            read_depth: 0,
        }
    }

    pub fn add_block(&mut self) -> BlockId {
        let id = BlockId(self.blocks.len());
        self.blocks.push(Block {
            predecessors: SmallVec::new(),
            sealed: false,
        });
        id
    }

    pub fn add_predecessor(&mut self, block: BlockId, pred: BlockId) {
        self.blocks[block.0].predecessors.push(pred);
    }

    pub fn seal_block(&mut self, block: BlockId) {
        if let Some(incomplete) = self.incomplete_phis.remove(&block) {
            for (variable, phi_id) in incomplete {
                self.add_phi_operands(variable, phi_id);
            }
        }
        self.blocks[block.0].sealed = true;
    }

    pub fn seal_remaining(&mut self) {
        for id in 0..self.blocks.len() {
            if !self.blocks[id].sealed {
                self.seal_block(BlockId(id));
            }
        }
    }

    pub fn add_sealed_successor(&mut self, predecessor: BlockId) -> BlockId {
        let block = self.add_block();
        self.add_predecessor(block, predecessor);
        self.seal_block(block);
        block
    }

    pub fn add_sealed_join(&mut self, predecessors: impl IntoIterator<Item = BlockId>) -> BlockId {
        let block = self.add_block();
        for p in predecessors {
            self.add_predecessor(block, p);
        }
        self.seal_block(block);
        block
    }

    pub fn begin_loop(&mut self, predecessor: BlockId) -> (BlockId, BlockId) {
        let header = self.add_block();
        self.add_predecessor(header, predecessor);
        let body = self.add_sealed_successor(header);
        (header, body)
    }

    pub fn finish_loop(&mut self, header: BlockId, body_exit: BlockId) -> BlockId {
        self.add_predecessor(header, body_exit);
        self.seal_block(header);
        self.add_sealed_successor(header)
    }

    pub fn has_variable_in_block(&self, variable: u32, block: BlockId) -> bool {
        self.current_def
            .get(&variable)
            .is_some_and(|blocks| blocks.contains_key(&block))
    }

    pub fn write_variable(&mut self, variable: u32, block: BlockId, value: Value) {
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
    }

    pub fn read_variable(&mut self, variable: u32, block: BlockId) -> Vec<ParseValue> {
        let mut value = self.read_variable_internal(variable, block);
        let mut depth = 0;
        while let Value::Alias(target) = &value {
            depth += 1;
            if depth > 8 {
                break;
            }
            let target_value = self.read_variable_internal(*target, block);
            if matches!(target_value, Value::Opaque | Value::Marker) {
                break;
            }
            value = target_value;
        }
        self.resolve_value(&value)
    }

    fn read_variable_internal(&mut self, variable: u32, block: BlockId) -> Value {
        if let Some(block_defs) = self.current_def.get(&variable)
            && let Some(value) = block_defs.get(&block)
        {
            return value.clone();
        }
        if self.read_depth >= MAX_READ_DEPTH {
            return Value::Opaque;
        }
        self.read_depth += 1;
        let val = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
            self.read_variable_recursive(variable, block)
        });
        self.read_depth -= 1;
        val
    }

    fn read_variable_recursive(&mut self, variable: u32, block: BlockId) -> Value {
        let sealed = self.blocks[block.0].sealed;
        let num_preds = self.blocks[block.0].predecessors.len();

        let val = if !sealed {
            let phi_id = self.new_phi(block, variable);
            self.incomplete_phis
                .entry(block)
                .or_default()
                .insert(variable, phi_id);
            Value::Phi(phi_id)
        } else if num_preds == 0 {
            Value::Opaque
        } else if num_preds == 1 {
            let pred = self.blocks[block.0].predecessors[0];
            self.read_variable_internal(variable, pred)
        } else {
            self.read_variable_marker(variable, block)
        };

        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, val.clone());
        val
    }

    fn read_variable_marker(&mut self, variable: u32, block: BlockId) -> Value {
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, Value::Marker);

        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        let mut same: Option<Value> = None;
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
                    break;
                }
            }
        }

        if !need_phi {
            return same.unwrap_or(Value::Opaque);
        }

        let phi_id = self.new_phi(block, variable);
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, Value::Phi(phi_id));
        self.add_phi_operands(variable, phi_id)
    }

    fn new_phi(&mut self, block: BlockId, variable: u32) -> PhiId {
        let id = PhiId(self.phis.len());
        self.phis.push(PhiNode {
            block,
            variable,
            operands: SmallVec::new(),
            witnesses: [None, None],
        });
        id
    }

    fn add_phi_operands(&mut self, variable: u32, phi_id: PhiId) -> Value {
        let block = self.phis[phi_id.0].block;
        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        for pred in preds {
            let val = self.read_variable_internal(variable, pred);
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
        self.try_remove_trivial_phi(phi_id)
    }

    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> Value {
        let w = &self.phis[phi_id.0].witnesses;
        if let (Some(w0), Some(w1)) = (w[0].as_ref(), w[1].as_ref())
            && w0 != w1
            && *w0 != Value::Phi(phi_id)
            && *w1 != Value::Phi(phi_id)
        {
            return Value::Phi(phi_id);
        }

        let mut same: Option<Value> = None;
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

        let variable = self.phis[phi_id.0].variable;
        let block = self.phis[phi_id.0].block;
        if let Some(block_defs) = self.current_def.get_mut(&variable)
            && block_defs.get(&block) == Some(&Value::Phi(phi_id))
        {
            block_defs.insert(block, replacement.clone());
        }

        let phi_users: Vec<PhiId> = self
            .phis
            .iter()
            .enumerate()
            .filter(|(i, phi)| *i != phi_id.0 && phi.operands.contains(&Value::Phi(phi_id)))
            .map(|(i, _)| PhiId(i))
            .collect();

        let phi_val = Value::Phi(phi_id);
        for user_id in &phi_users {
            let user = &mut self.phis[user_id.0];
            for op in &mut user.operands {
                if *op == phi_val {
                    *op = replacement.clone();
                }
            }
            for w in &mut user.witnesses {
                if w.as_ref() == Some(&phi_val) {
                    *w = None;
                }
            }
        }

        for user_id in phi_users {
            stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
                self.try_remove_trivial_phi(user_id)
            });
        }

        replacement
    }

    pub fn remove_redundant_phi_sccs(&mut self) {
        let phi_ids: Vec<PhiId> = (0..self.phis.len()).map(PhiId).collect();
        self.remove_redundant_phi_sccs_inner(&phi_ids, 0);
    }

    const MAX_SCC_DEPTH: usize = 32;

    fn remove_redundant_phi_sccs_inner(&mut self, phi_ids: &[PhiId], depth: usize) {
        if phi_ids.len() < 2 || depth >= Self::MAX_SCC_DEPTH {
            return;
        }

        // Build phi-to-phi dependency graph
        let mut index_map: FxHashMap<PhiId, usize> = FxHashMap::default();
        let mut adj: Vec<Vec<usize>> = Vec::with_capacity(phi_ids.len());
        for (i, &pid) in phi_ids.iter().enumerate() {
            index_map.insert(pid, i);
            adj.push(Vec::new());
        }
        for (i, &pid) in phi_ids.iter().enumerate() {
            for op in &self.phis[pid.0].operands {
                if let Value::Phi(target) = op
                    && let Some(&j) = index_map.get(target)
                {
                    adj[i].push(j);
                }
            }
        }

        // Tarjan's SCC (inline, no petgraph dependency)
        let sccs = tarjan_scc(&adj);

        for scc_indices in &sccs {
            if scc_indices.len() <= 1 {
                continue;
            }

            let scc: Vec<PhiId> = scc_indices.iter().map(|&i| phi_ids[i]).collect();
            let scc_set: FxHashSet<PhiId> = scc.iter().copied().collect();

            let mut outer_values: FxHashSet<Value> = FxHashSet::default();
            let mut inner_phis: Vec<PhiId> = Vec::new();

            for &pid in &scc {
                let mut has_external = false;
                for op in &self.phis[pid.0].operands {
                    match op {
                        Value::Phi(p) if scc_set.contains(p) => {}
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
                let replacement = outer_values.into_iter().next().unwrap();
                let phi_vals: Vec<Value> = scc.iter().map(|&p| Value::Phi(p)).collect();
                for &pid in &scc {
                    let variable = self.phis[pid.0].variable;
                    let block = self.phis[pid.0].block;
                    if let Some(block_defs) = self.current_def.get_mut(&variable)
                        && block_defs.get(&block) == Some(&Value::Phi(pid))
                    {
                        block_defs.insert(block, replacement.clone());
                    }
                }
                for phi in &mut self.phis {
                    for op in &mut phi.operands {
                        if phi_vals.contains(op) {
                            *op = replacement.clone();
                        }
                    }
                    for w in &mut phi.witnesses {
                        if let Some(wv) = w
                            && phi_vals.contains(wv)
                        {
                            *w = None;
                        }
                    }
                }
            } else if outer_values.len() > 1 && !inner_phis.is_empty() {
                self.remove_redundant_phi_sccs_inner(&inner_phis, depth + 1);
            }
        }
    }

    fn resolve_value(&self, value: &Value) -> Vec<ParseValue> {
        match value {
            Value::LocalDef(_) | Value::ImportRef(_) | Value::Type(_) | Value::Alias(_) => {
                value.to_parse_value().into_iter().collect()
            }
            Value::Opaque | Value::Marker => vec![],
            Value::Phi(_) => {
                let mut values = SmallVec::<[Value; 2]>::new();
                let mut visited = FxHashSet::default();
                self.resolve_value_recursive(value, &mut values, &mut visited);
                let mut seen = FxHashSet::default();
                values.retain(|v| seen.insert(v.clone()));
                values
                    .into_iter()
                    .filter_map(|v| v.to_parse_value())
                    .collect()
            }
        }
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
                    return;
                }
                for op in &self.phis[phi_id.0].operands {
                    stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
                        self.resolve_value_recursive(op, out, visited)
                    });
                }
            }
            Value::Opaque | Value::Marker => {}
            other => out.push(other.clone()),
        }
    }
}

impl Default for SsaEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Inline Tarjan's SCC (no petgraph dependency).
fn tarjan_scc(adj: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let n = adj.len();
    let mut index_counter = 0usize;
    let mut stack = Vec::new();
    let mut on_stack = vec![false; n];
    let mut indices = vec![usize::MAX; n];
    let mut lowlinks = vec![0usize; n];
    let mut result = Vec::new();

    fn strongconnect(
        v: usize,
        adj: &[Vec<usize>],
        index_counter: &mut usize,
        stack: &mut Vec<usize>,
        on_stack: &mut [bool],
        indices: &mut [usize],
        lowlinks: &mut [usize],
        result: &mut Vec<Vec<usize>>,
    ) {
        indices[v] = *index_counter;
        lowlinks[v] = *index_counter;
        *index_counter += 1;
        stack.push(v);
        on_stack[v] = true;

        for &w in &adj[v] {
            if indices[w] == usize::MAX {
                strongconnect(
                    w,
                    adj,
                    index_counter,
                    stack,
                    on_stack,
                    indices,
                    lowlinks,
                    result,
                );
                lowlinks[v] = lowlinks[v].min(lowlinks[w]);
            } else if on_stack[w] {
                lowlinks[v] = lowlinks[v].min(indices[w]);
            }
        }

        if lowlinks[v] == indices[v] {
            let mut scc = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack[w] = false;
                scc.push(w);
                if w == v {
                    break;
                }
            }
            result.push(scc);
        }
    }

    for v in 0..n {
        if indices[v] == usize::MAX {
            strongconnect(
                v,
                adj,
                &mut index_counter,
                &mut stack,
                &mut on_stack,
                &mut indices,
                &mut lowlinks,
                &mut result,
            );
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_block_write_read() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        ssa.write_variable(1, b, Value::LocalDef(0));
        let result = ssa.read_variable(1, b);
        assert_eq!(result, vec![ParseValue::LocalDef(0)]);
    }

    #[test]
    fn read_from_predecessor() {
        let mut ssa = SsaEngine::new();
        let b0 = ssa.add_block();
        ssa.seal_block(b0);
        let b1 = ssa.add_sealed_successor(b0);
        ssa.write_variable(1, b0, Value::LocalDef(0));
        let result = ssa.read_variable(1, b1);
        assert_eq!(result, vec![ParseValue::LocalDef(0)]);
    }

    #[test]
    fn phi_at_join_point() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        let then_b = ssa.add_sealed_successor(entry);
        let else_b = ssa.add_sealed_successor(entry);
        let join = ssa.add_sealed_join([then_b, else_b]);

        ssa.write_variable(1, then_b, Value::LocalDef(0));
        ssa.write_variable(1, else_b, Value::LocalDef(1));

        let result = ssa.read_variable(1, join);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&ParseValue::LocalDef(0)));
        assert!(result.contains(&ParseValue::LocalDef(1)));
    }

    #[test]
    fn trivial_phi_collapsed() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        let then_b = ssa.add_sealed_successor(entry);
        let else_b = ssa.add_sealed_successor(entry);
        let join = ssa.add_sealed_join([then_b, else_b]);

        ssa.write_variable(1, entry, Value::LocalDef(0));
        let result = ssa.read_variable(1, join);
        assert_eq!(result, vec![ParseValue::LocalDef(0)]);
    }

    #[test]
    fn loop_phi() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        let (header, body) = ssa.begin_loop(entry);

        ssa.write_variable(1, entry, Value::LocalDef(0));
        ssa.write_variable(1, body, Value::LocalDef(1));

        let exit = ssa.finish_loop(header, body);
        let result = ssa.read_variable(1, exit);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&ParseValue::LocalDef(0)));
        assert!(result.contains(&ParseValue::LocalDef(1)));
    }

    #[test]
    fn loop_no_redefinition_trivial_phi() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        let (header, body) = ssa.begin_loop(entry);

        ssa.write_variable(1, entry, Value::LocalDef(0));

        let exit = ssa.finish_loop(header, body);
        let result = ssa.read_variable(1, exit);
        assert_eq!(result, vec![ParseValue::LocalDef(0)]);
    }

    #[test]
    fn overwrite_in_same_block() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        ssa.write_variable(1, b, Value::LocalDef(0));
        ssa.write_variable(1, b, Value::LocalDef(1));
        let result = ssa.read_variable(1, b);
        assert_eq!(result, vec![ParseValue::LocalDef(1)]);
    }

    #[test]
    fn undefined_variable_is_empty() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        let result = ssa.read_variable(1, b);
        assert!(result.is_empty());
    }

    #[test]
    fn nested_if_else() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        let outer_then = ssa.add_sealed_successor(entry);
        let inner_then = ssa.add_sealed_successor(outer_then);
        let inner_else = ssa.add_sealed_successor(outer_then);
        let inner_join = ssa.add_sealed_join([inner_then, inner_else]);
        let outer_else = ssa.add_sealed_successor(entry);
        let outer_join = ssa.add_sealed_join([inner_join, outer_else]);

        ssa.write_variable(1, inner_then, Value::LocalDef(0));
        ssa.write_variable(1, inner_else, Value::LocalDef(1));
        ssa.write_variable(1, outer_else, Value::LocalDef(2));

        let result = ssa.read_variable(1, outer_join);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn scc_mutual_phi_collapse() {
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

        ssa.write_variable(1, entry, Value::LocalDef(0));
        ssa.seal_block(entry);
        ssa.seal_block(left);
        ssa.seal_block(right);
        ssa.seal_block(exit);

        ssa.remove_redundant_phi_sccs();
        let result = ssa.read_variable(1, exit);
        assert_eq!(result, vec![ParseValue::LocalDef(0)]);
    }

    #[test]
    fn import_and_def_values() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        ssa.write_variable(1, b, Value::ImportRef(0));
        ssa.write_variable(2, b, Value::LocalDef(0));

        assert_eq!(ssa.read_variable(1, b), vec![ParseValue::ImportRef(0)]);
        assert_eq!(ssa.read_variable(2, b), vec![ParseValue::LocalDef(0)]);
    }
}
