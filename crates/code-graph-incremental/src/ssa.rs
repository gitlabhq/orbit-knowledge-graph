//! SSA engine (Braun et al., CC 2013).
//!
//! Builds reaching-def information during a single pass over annotations.
//! Variable names are `u32` symbol ids from the Lang interner.

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

const FRAMES_PER_BLOCK: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhiId(u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    LocalDef(u32),
    ImportRef(u32),
    Type(u32),
    Call(u32),
    Opaque,
    Undefined,
    Marker,
    Phi(PhiId),
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
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

enum Frame {
    Read(BlockId),
    Single(BlockId),
    Merge {
        block: BlockId,
        next: usize,
        same: Option<Value>,
        need_phi: bool,
    },
    Operands {
        phi: PhiId,
        next: usize,
    },
    Phi(BlockId),
}

pub struct SsaEngine {
    blocks: Vec<Block>,
    phis: Vec<PhiNode>,
    users: Vec<SmallVec<[PhiId; 2]>>,
    current_def: FxHashMap<u32, FxHashMap<BlockId, Value>>,
    incomplete_phis: FxHashMap<BlockId, FxHashMap<u32, PhiId>>,
    frames: Vec<Frame>,
    written: FxHashSet<u32>,
    unsealed: usize,
}

impl SsaEngine {
    pub fn new() -> Self {
        Self {
            blocks: Vec::with_capacity(32),
            phis: Vec::with_capacity(8),
            users: Vec::with_capacity(8),
            current_def: FxHashMap::with_capacity_and_hasher(64, Default::default()),
            incomplete_phis: FxHashMap::default(),
            frames: Vec::new(),
            written: FxHashSet::default(),
            unsealed: 0,
        }
    }

    pub fn add_block(&mut self) -> BlockId {
        let id = BlockId(u32::try_from(self.blocks.len()).expect("block index exceeds u32"));
        self.blocks.push(Block {
            predecessors: SmallVec::new(),
            sealed: false,
        });
        self.unsealed += 1;
        id
    }

    pub fn add_predecessor(&mut self, block: BlockId, pred: BlockId) {
        self.blocks[block.0 as usize].predecessors.push(pred);
    }

    pub fn seal_block(&mut self, block: BlockId) {
        if let Some(incomplete) = self.incomplete_phis.remove(&block) {
            for (variable, phi_id) in incomplete {
                self.add_phi_operands(variable, phi_id);
            }
        }
        if !self.blocks[block.0 as usize].sealed {
            self.blocks[block.0 as usize].sealed = true;
            self.unsealed -= 1;
        }
    }

    pub fn seal_remaining(&mut self) {
        for id in 0..self.blocks.len() {
            if !self.blocks[id].sealed {
                self.seal_block(BlockId(u32::try_from(id).expect("block index exceeds u32")));
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

    #[cfg(test)]
    pub(crate) fn phi_count(&self) -> usize {
        self.phis.len()
    }

    pub fn is_defined(&self, variable: u32, block: BlockId) -> bool {
        let Some(defs) = self.current_def.get(&variable) else {
            return false;
        };
        let mut seen = FxHashSet::default();
        let mut stack = vec![block];
        while let Some(b) = stack.pop() {
            if defs.contains_key(&b) {
                return true;
            }
            if seen.insert(b) {
                stack.extend(self.blocks[b.0 as usize].predecessors.iter().copied());
            }
        }
        false
    }

    pub fn write_variable(&mut self, variable: u32, block: BlockId, value: Value) {
        self.written.insert(variable);
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, value);
    }

    pub fn read_variable(&mut self, variable: u32, block: BlockId) -> Vec<Value> {
        let value = self.read_variable_internal(variable, block);
        self.resolve_value(&value)
    }

    pub(crate) fn read_variable_internal(&mut self, variable: u32, block: BlockId) -> Value {
        if let Some(block_defs) = self.current_def.get(&variable)
            && let Some(value) = block_defs.get(&block)
        {
            return value.clone();
        }
        let mut frames = std::mem::take(&mut self.frames);
        let ret = self.descend(variable, block, &mut frames);
        self.run(variable, frames, ret)
    }

    fn set(&mut self, variable: u32, block: BlockId, value: Value) {
        self.current_def
            .entry(variable)
            .or_default()
            .insert(block, value);
    }

    fn run(&mut self, variable: u32, mut frames: Vec<Frame>, mut ret: Option<Value>) -> Value {
        while let Some(frame) = frames.last_mut() {
            match frame {
                Frame::Read(block) => {
                    let block = *block;
                    frames.pop();
                    ret = self.enter(variable, block, &mut frames);
                }
                Frame::Single(block) => {
                    let block = *block;
                    frames.pop();
                    let value = ret.take().expect("a read returns a value");
                    self.set(variable, block, value.clone());
                    ret = Some(value);
                }
                Frame::Phi(block) => {
                    let block = *block;
                    frames.pop();
                    let value = ret.take().expect("phi operands return a value");
                    self.set(variable, block, value.clone());
                    ret = Some(value);
                }
                Frame::Merge {
                    block,
                    next,
                    same,
                    need_phi,
                } => {
                    let block = *block;
                    let preds = self.blocks[block.0 as usize].predecessors.len();
                    if let Some(value) = ret.take() {
                        if value == Value::Marker {
                            *need_phi = true;
                        } else {
                            match same {
                                None => *same = Some(value),
                                Some(s) if *s == value => {}
                                Some(_) => {
                                    *need_phi = true;
                                    *next = preds;
                                }
                            }
                        }
                    }
                    if *next < preds {
                        let pred = self.blocks[block.0 as usize].predecessors[*next];
                        *next += 1;
                        frames.push(Frame::Read(pred));
                        continue;
                    }
                    let (need_phi, same) = (*need_phi, same.take());
                    frames.pop();
                    if need_phi {
                        let phi = self.new_phi(block, variable);
                        self.set(variable, block, Value::Phi(phi));
                        frames.push(Frame::Phi(block));
                        frames.push(Frame::Operands { phi, next: 0 });
                    } else {
                        let value = same.unwrap_or(Value::Opaque);
                        self.set(variable, block, value.clone());
                        ret = Some(value);
                    }
                }
                Frame::Operands { phi, next } => {
                    let phi_id = *phi;
                    if let Some(value) = ret.take() {
                        self.add_operand(phi_id, value);
                    }
                    let block = self.phis[phi_id.0 as usize].block;
                    if let Some(&pred) = self.blocks[block.0 as usize].predecessors.get(*next) {
                        *next += 1;
                        frames.push(Frame::Read(pred));
                        continue;
                    }
                    frames.pop();
                    ret = Some(self.try_remove_trivial_phi(phi_id));
                }
            }
        }
        self.frames = frames;
        ret.expect("a read returns a value")
    }

    fn enter(&mut self, variable: u32, block: BlockId, frames: &mut Vec<Frame>) -> Option<Value> {
        if let Some(value) = self
            .current_def
            .get(&variable)
            .and_then(|defs| defs.get(&block))
        {
            return Some(value.clone());
        }
        self.descend(variable, block, frames)
    }

    fn descend(&mut self, variable: u32, block: BlockId, frames: &mut Vec<Frame>) -> Option<Value> {
        if self.unsealed == 0 && !self.written.contains(&variable) {
            return Some(Value::Undefined);
        }
        if frames.len() > FRAMES_PER_BLOCK * (self.blocks.len() + 1) {
            return Some(Value::Opaque);
        }
        let node = &self.blocks[block.0 as usize];
        let shape = (
            node.sealed,
            node.predecessors.len(),
            node.predecessors[..].first().copied(),
        );
        let value = match shape {
            (false, ..) => {
                let phi_id = self.new_phi(block, variable);
                self.incomplete_phis
                    .entry(block)
                    .or_default()
                    .insert(variable, phi_id);
                Value::Phi(phi_id)
            }
            (true, 0, _) => Value::Undefined,
            (true, 1, Some(pred)) => {
                frames.push(Frame::Single(block));
                frames.push(Frame::Read(pred));
                return None;
            }
            _ => {
                self.set(variable, block, Value::Marker);
                frames.push(Frame::Merge {
                    block,
                    next: 0,
                    same: None,
                    need_phi: false,
                });
                return None;
            }
        };
        self.set(variable, block, value.clone());
        Some(value)
    }
    fn new_phi(&mut self, block: BlockId, variable: u32) -> PhiId {
        let id = PhiId(u32::try_from(self.phis.len()).expect("phi index exceeds u32"));
        self.phis.push(PhiNode {
            block,
            variable,
            operands: SmallVec::new(),
            witnesses: [None, None],
        });
        self.users.push(SmallVec::new());
        id
    }

    fn add_phi_operands(&mut self, variable: u32, phi_id: PhiId) -> Value {
        let mut frames = std::mem::take(&mut self.frames);
        frames.push(Frame::Operands {
            phi: phi_id,
            next: 0,
        });
        self.run(variable, frames, None)
    }

    fn add_operand(&mut self, phi_id: PhiId, val: Value) {
        if val != Value::Phi(phi_id) {
            let phi = &mut self.phis[phi_id.0 as usize];
            if phi.witnesses[0].is_none() {
                phi.witnesses[0] = Some(val.clone());
            } else if phi.witnesses[1].is_none() && phi.witnesses[0].as_ref() != Some(&val) {
                phi.witnesses[1] = Some(val.clone());
            }
        }
        if let Value::Phi(used) = val {
            self.users[used.0 as usize].push(phi_id);
        }
        self.phis[phi_id.0 as usize].operands.push(val);
    }

    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> Value {
        let mut pending: SmallVec<[PhiId; 8]> = SmallVec::from_slice(&[phi_id]);
        let mut first = None;
        while let Some(phi) = pending.pop() {
            let value = self.remove_if_trivial(phi, &mut pending);
            first.get_or_insert(value);
        }
        first.expect("the first phi is always checked")
    }

    fn remove_if_trivial(&mut self, phi_id: PhiId, pending: &mut SmallVec<[PhiId; 8]>) -> Value {
        let w = &self.phis[phi_id.0 as usize].witnesses;
        if let (Some(w0), Some(w1)) = (w[0].as_ref(), w[1].as_ref())
            && w0 != w1
            && *w0 != Value::Phi(phi_id)
            && *w1 != Value::Phi(phi_id)
        {
            return Value::Phi(phi_id);
        }

        let mut same: Option<Value> = None;
        for i in 0..self.phis[phi_id.0 as usize].operands.len() {
            let op = self.phis[phi_id.0 as usize].operands[i].clone();
            if op == Value::Phi(phi_id) || Some(&op) == same.as_ref() {
                continue;
            }
            if same.is_some() {
                return Value::Phi(phi_id);
            }
            same = Some(op);
        }

        let replacement = same.unwrap_or(Value::Opaque);

        let variable = self.phis[phi_id.0 as usize].variable;
        let block = self.phis[phi_id.0 as usize].block;
        if let Some(block_defs) = self.current_def.get_mut(&variable)
            && block_defs.get(&block) == Some(&Value::Phi(phi_id))
        {
            block_defs.insert(block, replacement.clone());
        }

        let phi_val = Value::Phi(phi_id);
        let mut phi_users = std::mem::take(&mut self.users[phi_id.0 as usize]);
        phi_users.retain(|user| {
            *user != phi_id && self.phis[user.0 as usize].operands.contains(&phi_val)
        });
        phi_users.sort_unstable_by_key(|user| user.0);
        phi_users.dedup();

        for user_id in &phi_users {
            let user = &mut self.phis[user_id.0 as usize];
            for op in &mut user.operands {
                if *op == phi_val {
                    *op = replacement.clone();
                }
            }
            if let Value::Phi(used) = replacement {
                self.users[used.0 as usize].push(*user_id);
            }
            let user = &mut self.phis[user_id.0 as usize];
            for w in &mut user.witnesses {
                if w.as_ref() == Some(&phi_val) {
                    *w = None;
                }
            }
        }

        pending.extend(phi_users.into_iter().rev());

        replacement
    }

    pub fn remove_redundant_phi_sccs(&mut self) {
        let phi_ids: Vec<PhiId> = (0..u32::try_from(self.phis.len())
            .expect("phi count exceeds u32"))
            .map(PhiId)
            .collect();
        self.remove_redundant_phi_sccs_inner(&phi_ids, 0);
    }

    const MAX_SCC_DEPTH: usize = 32;

    fn remove_redundant_phi_sccs_inner(&mut self, phi_ids: &[PhiId], depth: usize) {
        if phi_ids.len() < 2 || depth >= Self::MAX_SCC_DEPTH {
            return;
        }

        let mut index_map: FxHashMap<PhiId, usize> = FxHashMap::default();
        let mut adj: Vec<Vec<usize>> = Vec::with_capacity(phi_ids.len());
        for (i, &pid) in phi_ids.iter().enumerate() {
            index_map.insert(pid, i);
            adj.push(Vec::new());
        }
        for (i, &pid) in phi_ids.iter().enumerate() {
            for op in &self.phis[pid.0 as usize].operands {
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
                for op in &self.phis[pid.0 as usize].operands {
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
                let replacement = outer_values
                    .into_iter()
                    .next()
                    .expect("a trivial phi has exactly one outer value");
                let phi_vals: Vec<Value> = scc.iter().map(|&p| Value::Phi(p)).collect();
                for &pid in &scc {
                    let variable = self.phis[pid.0 as usize].variable;
                    let block = self.phis[pid.0 as usize].block;
                    if let Some(block_defs) = self.current_def.get_mut(&variable)
                        && block_defs.get(&block) == Some(&Value::Phi(pid))
                    {
                        block_defs.insert(block, replacement.clone());
                    }
                }
                for (i, phi) in self.phis.iter_mut().enumerate() {
                    for op in &mut phi.operands {
                        if phi_vals.contains(op) {
                            *op = replacement.clone();
                            if let Value::Phi(used) = replacement {
                                self.users[used.0 as usize]
                                    .push(PhiId(u32::try_from(i).expect("phi index exceeds u32")));
                            }
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

    pub(crate) fn resolve_value(&self, value: &Value) -> Vec<Value> {
        if !matches!(value, Value::Phi(_)) {
            return match value {
                Value::Undefined | Value::Marker => vec![],
                _ => vec![value.clone()],
            };
        }
        let mut pending = SmallVec::<[&Value; 8]>::from_slice(&[value]);
        let mut visited = FxHashSet::default();
        let mut values = Vec::new();
        while let Some(value) = pending.pop() {
            if !visited.insert(value) {
                continue;
            }
            match value {
                Value::Phi(phi) => pending.extend(self.phis[phi.0 as usize].operands.iter().rev()),
                Value::Undefined | Value::Marker => {}
                _ => values.push(value.clone()),
            }
        }
        values
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
    let mut calls: Vec<(usize, usize)> = Vec::new();

    for root in 0..n {
        if indices[root] != usize::MAX {
            continue;
        }
        calls.push((root, 0));
        while let Some((v, edge)) = calls.last_mut() {
            let v = *v;
            if indices[v] == usize::MAX {
                indices[v] = index_counter;
                lowlinks[v] = index_counter;
                index_counter += 1;
                stack.push(v);
                on_stack[v] = true;
            }
            if let Some(&w) = adj[v].get(*edge) {
                *edge += 1;
                if indices[w] == usize::MAX {
                    calls.push((w, 0));
                } else if on_stack[w] {
                    lowlinks[v] = lowlinks[v].min(indices[w]);
                }
                continue;
            }
            calls.pop();
            if let Some(&(caller, _)) = calls.last() {
                lowlinks[caller] = lowlinks[caller].min(lowlinks[v]);
            }
            if lowlinks[v] == indices[v] {
                let mut scc = Vec::new();
                loop {
                    let w = stack.pop().expect("scc stack holds the current node");
                    on_stack[w] = false;
                    scc.push(w);
                    if w == v {
                        break;
                    }
                }
                result.push(scc);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORACLE_MAX_READ_DEPTH: usize = 1_000;

    thread_local! {
        static ORACLE_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    impl SsaEngine {
        fn oracle_seal_block(&mut self, block: BlockId) {
            if let Some(incomplete) = self.incomplete_phis.remove(&block) {
                for (variable, phi_id) in incomplete {
                    self.oracle_add_phi_operands(variable, phi_id);
                }
            }
            if !self.blocks[block.0 as usize].sealed {
                self.blocks[block.0 as usize].sealed = true;
                self.unsealed -= 1;
            }
        }

        fn oracle_internal(&mut self, variable: u32, block: BlockId) -> Value {
            if let Some(block_defs) = self.current_def.get(&variable)
                && let Some(value) = block_defs.get(&block)
            {
                return value.clone();
            }
            if self.unsealed == 0 && !self.written.contains(&variable) {
                return Value::Undefined;
            }
            if ORACLE_DEPTH.get() >= ORACLE_MAX_READ_DEPTH {
                return Value::Opaque;
            }
            ORACLE_DEPTH.set(ORACLE_DEPTH.get() + 1);
            let val = self.oracle_recursive(variable, block);
            ORACLE_DEPTH.set(ORACLE_DEPTH.get() - 1);
            val
        }

        fn oracle_recursive(&mut self, variable: u32, block: BlockId) -> Value {
            let sealed = self.blocks[block.0 as usize].sealed;
            let num_preds = self.blocks[block.0 as usize].predecessors.len();

            let val = if !sealed {
                let phi_id = self.new_phi(block, variable);
                self.incomplete_phis
                    .entry(block)
                    .or_default()
                    .insert(variable, phi_id);
                Value::Phi(phi_id)
            } else if num_preds == 0 {
                Value::Undefined
            } else if num_preds == 1 {
                let pred = self.blocks[block.0 as usize].predecessors[0];
                self.oracle_internal(variable, pred)
            } else {
                self.oracle_marker(variable, block)
            };

            self.current_def
                .entry(variable)
                .or_default()
                .insert(block, val.clone());
            val
        }

        fn oracle_marker(&mut self, variable: u32, block: BlockId) -> Value {
            self.current_def
                .entry(variable)
                .or_default()
                .insert(block, Value::Marker);

            let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0 as usize].predecessors.clone();
            let mut same: Option<Value> = None;
            let mut need_phi = false;

            for &pred in &preds {
                let pred_val = self.oracle_internal(variable, pred);
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
            self.oracle_add_phi_operands(variable, phi_id)
        }

        fn oracle_add_phi_operands(&mut self, variable: u32, phi_id: PhiId) -> Value {
            let block = self.phis[phi_id.0 as usize].block;
            let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0 as usize].predecessors.clone();
            for pred in preds {
                let val = self.oracle_internal(variable, pred);
                if val != Value::Phi(phi_id) {
                    let phi = &mut self.phis[phi_id.0 as usize];
                    if phi.witnesses[0].is_none() {
                        phi.witnesses[0] = Some(val.clone());
                    } else if phi.witnesses[1].is_none() && phi.witnesses[0].as_ref() != Some(&val)
                    {
                        phi.witnesses[1] = Some(val.clone());
                    }
                }
                if let Value::Phi(used) = val {
                    self.users[used.0 as usize].push(phi_id);
                }
                self.phis[phi_id.0 as usize].operands.push(val);
            }
            self.oracle_try_remove_trivial_phi(phi_id)
        }

        fn oracle_try_remove_trivial_phi(&mut self, phi_id: PhiId) -> Value {
            let w = &self.phis[phi_id.0 as usize].witnesses;
            if let (Some(w0), Some(w1)) = (w[0].as_ref(), w[1].as_ref())
                && w0 != w1
                && *w0 != Value::Phi(phi_id)
                && *w1 != Value::Phi(phi_id)
            {
                return Value::Phi(phi_id);
            }

            let mut same: Option<Value> = None;
            for i in 0..self.phis[phi_id.0 as usize].operands.len() {
                let op = self.phis[phi_id.0 as usize].operands[i].clone();
                if op == Value::Phi(phi_id) || Some(&op) == same.as_ref() {
                    continue;
                }
                if same.is_some() {
                    return Value::Phi(phi_id);
                }
                same = Some(op);
            }

            let replacement = same.unwrap_or(Value::Opaque);

            let variable = self.phis[phi_id.0 as usize].variable;
            let block = self.phis[phi_id.0 as usize].block;
            if let Some(block_defs) = self.current_def.get_mut(&variable)
                && block_defs.get(&block) == Some(&Value::Phi(phi_id))
            {
                block_defs.insert(block, replacement.clone());
            }

            let phi_val = Value::Phi(phi_id);
            let mut phi_users = std::mem::take(&mut self.users[phi_id.0 as usize]);
            phi_users.retain(|user| {
                *user != phi_id && self.phis[user.0 as usize].operands.contains(&phi_val)
            });
            phi_users.sort_unstable_by_key(|user| user.0);
            phi_users.dedup();

            for user_id in &phi_users {
                let user = &mut self.phis[user_id.0 as usize];
                for op in &mut user.operands {
                    if *op == phi_val {
                        *op = replacement.clone();
                    }
                }
                if let Value::Phi(used) = replacement {
                    self.users[used.0 as usize].push(*user_id);
                }
                let user = &mut self.phis[user_id.0 as usize];
                for w in &mut user.witnesses {
                    if w.as_ref() == Some(&phi_val) {
                        *w = None;
                    }
                }
            }

            for user_id in phi_users {
                self.oracle_try_remove_trivial_phi(user_id);
            }

            replacement
        }

        fn oracle_read_variable(&mut self, variable: u32, block: BlockId) -> Vec<Value> {
            let value = self.oracle_internal(variable, block);
            self.resolve_value(&value)
        }
    }

    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[derive(Clone, Copy)]
    enum Op {
        Write(u32, usize, u32),
        Seal(usize),
        Read(u32, usize),
    }

    fn random_cfg(seed: u64) -> (Vec<Vec<usize>>, Vec<Op>) {
        let mut state = seed | 1;
        let spine = 2 + (next(&mut state) % 40) as usize;
        let cycle = match next(&mut state) % 3 {
            0 => 1 + (next(&mut state) % 4) as usize,
            _ => 0,
        };
        let n = spine + cycle;
        let mut preds: Vec<Vec<usize>> = (0..n)
            .map(|i| match i {
                0 => vec![],
                _ if i >= spine => vec![spine + (i - spine + 1) % cycle],
                _ => vec![(next(&mut state) % i as u64) as usize],
            })
            .collect();
        for (i, block_preds) in preds.iter_mut().enumerate().take(spine).skip(1) {
            match next(&mut state) % 6 {
                0 => block_preds.push(i),
                1 => block_preds.push((next(&mut state) % n as u64) as usize),
                2 => block_preds.push((next(&mut state) % i as u64) as usize),
                _ => {}
            }
        }
        let mut ops = Vec::new();
        let mut unsealed: Vec<usize> = (0..n).collect();
        for _ in 0..3 * n {
            match next(&mut state) % 4 {
                0 => ops.push(Op::Write(
                    1 + (next(&mut state) % 2) as u32,
                    (next(&mut state) % n as u64) as usize,
                    (next(&mut state) % 5) as u32,
                )),
                1 if !unsealed.is_empty() => {
                    let i = (next(&mut state) % unsealed.len() as u64) as usize;
                    ops.push(Op::Seal(unsealed.swap_remove(i)));
                }
                _ => ops.push(Op::Read(
                    1 + (next(&mut state) % 2) as u32,
                    (next(&mut state) % n as u64) as usize,
                )),
            }
        }
        (preds, ops)
    }

    type Replay = (
        Vec<Vec<Value>>,
        Vec<PhiNode>,
        Vec<SmallVec<[PhiId; 2]>>,
        FxHashMap<u32, FxHashMap<BlockId, Value>>,
        FxHashMap<BlockId, FxHashMap<u32, PhiId>>,
    );

    fn replay(preds: &[Vec<usize>], ops: &[Op], oracle: bool) -> Replay {
        let mut ssa = SsaEngine::new();
        let ids: Vec<BlockId> = preds.iter().map(|_| ssa.add_block()).collect();
        for (i, block_preds) in preds.iter().enumerate() {
            for &p in block_preds {
                ssa.add_predecessor(ids[i], ids[p]);
            }
        }
        let mut reads = Vec::new();
        let read = |ssa: &mut SsaEngine, variable: u32, block: BlockId| match oracle {
            true => ssa.oracle_read_variable(variable, block),
            false => ssa.read_variable(variable, block),
        };
        for &op in ops {
            match op {
                Op::Write(variable, block, def) => {
                    ssa.write_variable(variable, ids[block], Value::LocalDef(def))
                }
                Op::Seal(block) if oracle => ssa.oracle_seal_block(ids[block]),
                Op::Seal(block) => ssa.seal_block(ids[block]),
                Op::Read(variable, block) => reads.push(read(&mut ssa, variable, ids[block])),
            }
        }
        for &id in &ids {
            match oracle {
                true => ssa.oracle_seal_block(id),
                false => ssa.seal_block(id),
            }
        }
        ssa.remove_redundant_phi_sccs();
        for variable in [1, 2, 3] {
            for &id in &ids {
                reads.push(read(&mut ssa, variable, id));
            }
        }
        (
            reads,
            ssa.phis,
            ssa.users,
            ssa.current_def,
            ssa.incomplete_phis,
        )
    }

    #[test]
    fn iterative_reads_match_the_recursive_reads_on_random_cfgs() {
        std::thread::Builder::new()
            .stack_size(256 * 1024 * 1024)
            .spawn(compare_random_cfgs)
            .unwrap()
            .join()
            .unwrap();
    }

    fn compare_random_cfgs() {
        for seed in 1..2_000u64 {
            let (preds, ops) = random_cfg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            assert_eq!(
                replay(&preds, &ops, true),
                replay(&preds, &ops, false),
                "seed {seed}"
            );
            for query in 0..preds.len() {
                let read = [Op::Seal(query), Op::Read(1, query)];
                let ops: Vec<Op> = ops
                    .iter()
                    .copied()
                    .filter(|op| !matches!(op, Op::Read(..)))
                    .chain(read)
                    .collect();
                assert_eq!(
                    replay(&preds, &ops, true),
                    replay(&preds, &ops, false),
                    "seed {seed}, query {query}"
                );
            }
        }
    }

    #[test]
    fn a_read_through_a_long_chain_resolves_on_a_small_stack() {
        let values = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let mut ssa = SsaEngine::new();
                let entry = ssa.add_block();
                ssa.seal_block(entry);
                ssa.write_variable(1, entry, Value::LocalDef(0));
                let mut block = entry;
                for _ in 0..50_000 {
                    let left = ssa.add_sealed_successor(block);
                    let right = ssa.add_sealed_successor(block);
                    block = ssa.add_sealed_join([left, right]);
                    block = ssa.add_sealed_successor(block);
                }
                ssa.read_variable(1, block)
            })
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(values, vec![Value::LocalDef(0)]);
    }

    #[test]
    fn a_read_around_a_cycle_without_a_merge_block_ends() {
        let mut ssa = SsaEngine::new();
        let elsewhere = ssa.add_block();
        let a = ssa.add_block();
        let b = ssa.add_block();
        ssa.add_predecessor(a, b);
        ssa.add_predecessor(b, a);
        for block in [elsewhere, a, b] {
            ssa.seal_block(block);
        }
        ssa.write_variable(1, elsewhere, Value::LocalDef(0));
        assert_eq!(ssa.read_variable(1, a), vec![Value::Opaque]);
    }

    #[test]
    fn single_block_write_read() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        ssa.write_variable(1, b, Value::LocalDef(0));
        let result = ssa.read_variable(1, b);
        assert_eq!(result, vec![Value::LocalDef(0)]);
    }

    #[test]
    fn read_from_predecessor() {
        let mut ssa = SsaEngine::new();
        let b0 = ssa.add_block();
        ssa.seal_block(b0);
        let b1 = ssa.add_sealed_successor(b0);
        ssa.write_variable(1, b0, Value::LocalDef(0));
        let result = ssa.read_variable(1, b1);
        assert_eq!(result, vec![Value::LocalDef(0)]);
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
        assert!(result.contains(&Value::LocalDef(0)));
        assert!(result.contains(&Value::LocalDef(1)));
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
        assert_eq!(result, vec![Value::LocalDef(0)]);
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
        assert!(result.contains(&Value::LocalDef(0)));
        assert!(result.contains(&Value::LocalDef(1)));
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
        assert_eq!(result, vec![Value::LocalDef(0)]);
    }

    #[test]
    fn overwrite_in_same_block() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        ssa.write_variable(1, b, Value::LocalDef(0));
        ssa.write_variable(1, b, Value::LocalDef(1));
        let result = ssa.read_variable(1, b);
        assert_eq!(result, vec![Value::LocalDef(1)]);
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
        assert_eq!(result, vec![Value::LocalDef(0)]);
    }

    #[test]
    fn scc_search_on_a_long_phi_cycle_fits_a_small_stack() {
        let n = 100_000;
        let adj: Vec<Vec<usize>> = (0..n).map(|v| vec![(v + 1) % n]).collect();
        let sccs = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || tarjan_scc(&adj))
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(sccs.len(), 1);
        assert_eq!(sccs[0].len(), n);
    }

    #[test]
    fn import_and_def_values() {
        let mut ssa = SsaEngine::new();
        let b = ssa.add_block();
        ssa.seal_block(b);
        ssa.write_variable(1, b, Value::ImportRef(0));
        ssa.write_variable(2, b, Value::LocalDef(0));

        assert_eq!(ssa.read_variable(1, b), vec![Value::ImportRef(0)]);
        assert_eq!(ssa.read_variable(2, b), vec![Value::LocalDef(0)]);
    }
}
