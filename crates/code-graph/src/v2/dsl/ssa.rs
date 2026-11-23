use crate::trace_event;
use crate::v2::trace::Tracer;
use crate::v2::types::ssa::ParseValue;
use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

/// Past this reaching-def recursion depth a read degrades to `Opaque` rather
/// than overflowing the worker stack (an uncatchable SIGSEGV).
const MAX_SSA_READ_DEPTH: usize = 10_000;

const SSA_READ_RED_ZONE: usize = 128 * 1024;
const SSA_READ_STACK_SEGMENT: usize = 4 * 1024 * 1024;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SsaValue<'a> {
    LocalDef(u32),
    ImportRef(u32),
    Type(&'a str),
    ResolvedSite(ResolvedSite<'a>),
    Alias(&'a str),
    Opaque,
    Marker,
    Phi(PhiId),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SsaStats {
    pub reads: u64,
    pub local_hits: u64,
    pub recursive_lookups: u64,
    pub unsealed_hits: u64,
    pub dead_end_hits: u64,
    pub phis_created: u64,
    pub phis_trivial: u64,
    pub markers_elided: u64,
    pub writes: u64,
    pub blocks_created: u64,
    pub depth_capped: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ReachingDefs<'a> {
    pub values: SmallVec<[SsaValue<'a>; 2]>,
}

impl SsaValue<'_> {
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

#[derive(Debug, Clone)]
struct PhiNode<'a> {
    block: BlockId,
    variable: &'a str,
    operands: SmallVec<[SsaValue<'a>; 2]>,
    witnesses: [Option<SsaValue<'a>>; 2],
}

#[derive(Debug, Clone)]
struct Block {
    predecessors: SmallVec<[BlockId; 2]>,
    sealed: bool,
}

pub(crate) struct SsaEngine<'a> {
    blocks: Vec<Block>,
    phis: Vec<PhiNode<'a>>,
    current_def: FxHashMap<&'a str, FxHashMap<BlockId, SsaValue<'a>>>,
    incomplete_phis: FxHashMap<BlockId, FxHashMap<&'a str, PhiId>>,
    pub stats: SsaStats,
    tracer: &'a Tracer,
    budget: Option<treesitter_visit::CpuBudget>,
    timed_out: bool,
    read_depth: usize,
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
            budget: None,
            timed_out: false,
            read_depth: 0,
        }
    }

    pub(crate) fn set_budget(&mut self, budget: Option<std::time::Duration>) {
        self.budget = budget.map(treesitter_visit::CpuBudget::start);
    }

    pub(crate) fn timed_out(&self) -> bool {
        self.timed_out
    }

    fn deadline_tripped(&mut self) -> bool {
        if !self.timed_out && self.budget.is_some_and(|b| b.expired()) {
            self.timed_out = true;
        }
        self.timed_out
    }

    pub(crate) fn with_tracer(mut self, tracer: &'a Tracer) -> Self {
        self.tracer = tracer;
        self
    }

    pub(crate) fn add_block(&mut self) -> BlockId {
        let id = BlockId(self.blocks.len());
        self.blocks.push(Block {
            predecessors: SmallVec::new(),
            sealed: false,
        });
        self.stats.blocks_created += 1;
        trace_event!(self.tracer, SsaBlockCreated { block_id: id.0 });
        id
    }

    pub(crate) fn add_predecessor(&mut self, block: BlockId, pred: BlockId) {
        self.blocks[block.0].predecessors.push(pred);
        trace_event!(
            self.tracer,
            SsaAddPredecessor {
                block_id: block.0,
                pred_id: pred.0,
            }
        );
    }

    pub fn add_sealed_successor(&mut self, predecessor: BlockId) -> BlockId {
        let block = self.add_block();
        self.add_predecessor(block, predecessor);
        self.seal_block(block);
        block
    }

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

    pub fn add_branch_block(&mut self, predecessor: BlockId) -> BlockId {
        self.add_sealed_successor(predecessor)
    }

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

    pub(crate) fn seal_block(&mut self, block: BlockId) {
        if let Some(incomplete) = self.incomplete_phis.remove(&block) {
            for (variable, phi_id) in incomplete {
                self.add_phi_operands(variable, phi_id);
            }
        }
        self.blocks[block.0].sealed = true;
        trace_event!(self.tracer, SsaBlockSealed { block_id: block.0 });
    }

    pub(crate) fn seal_remaining(&mut self) {
        for id in 0..self.blocks.len() {
            if !self.blocks[id].sealed {
                self.seal_block(BlockId(id));
            }
        }
    }

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
        trace_event!(
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

    pub(crate) fn read_variable_stateless(
        &mut self,
        variable: &'a str,
        block: BlockId,
    ) -> ReachingDefs<'a> {
        self.stats.reads += 1;
        let mut value = self.read_variable_internal(variable, block);
        let mut depth = 0;
        while let SsaValue::Alias(target) = &value {
            depth += 1;
            if depth > 8 {
                break;
            }
            let target_value = self.read_variable_internal(target, block);
            if matches!(target_value, SsaValue::Opaque | SsaValue::Marker) {
                break;
            }
            value = target_value;
        }
        let result = self.resolve_value(&value);
        trace_event!(
            self.tracer,
            SsaRead {
                variable: variable.to_string(),
                block_id: block.0,
                values: result.values.iter().map(|v| v.trace_display()).collect(),
            }
        );
        result
    }

    pub(crate) fn read_variable_raw(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        self.stats.reads += 1;
        self.read_variable_internal(variable, block)
    }

    pub(crate) fn expand_value(&self, value: &SsaValue<'a>) -> ReachingDefs<'a> {
        self.resolve_value(value)
    }

    fn read_variable_internal(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        if let Some(block_defs) = self.current_def.get(&variable)
            && let Some(value) = block_defs.get(&block)
        {
            self.stats.local_hits += 1;
            return value.clone();
        }

        if self.deadline_tripped() {
            return SsaValue::Opaque;
        }

        self.stats.recursive_lookups += 1;
        if self.read_depth >= MAX_SSA_READ_DEPTH {
            self.stats.depth_capped += 1;
            return SsaValue::Opaque;
        }
        self.read_depth += 1;
        let val = stacker::maybe_grow(SSA_READ_RED_ZONE, SSA_READ_STACK_SEGMENT, || {
            self.read_variable_recursive(variable, block)
        });
        self.read_depth -= 1;
        val
    }

    fn read_variable_recursive(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        let sealed = self.blocks[block.0].sealed;
        let num_preds = self.blocks[block.0].predecessors.len();

        let val = if !sealed {
            self.stats.unsealed_hits += 1;
            let phi_id = self.new_phi(block, variable);
            self.incomplete_phis
                .entry(block)
                .or_default()
                .insert(variable, phi_id);
            SsaValue::Phi(phi_id)
        } else if num_preds == 0 {
            self.stats.dead_end_hits += 1;
            SsaValue::Opaque
        } else if num_preds == 1 {
            let pred = self.blocks[block.0].predecessors[0];
            self.read_variable_internal(variable, pred)
        } else {
            self.read_variable_marker(variable, block)
        };

        self.write_variable_interned(variable, block, val.clone());
        val
    }

    fn read_variable_marker(&mut self, variable: &'a str, block: BlockId) -> SsaValue<'a> {
        self.write_variable_interned(variable, block, SsaValue::Marker);

        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        let mut same: Option<SsaValue<'a>> = None;
        let mut need_phi = false;

        for &pred in &preds {
            let pred_val = self.read_variable_internal(variable, pred);
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
            self.stats.markers_elided += 1;
            return same.unwrap_or(SsaValue::Opaque);
        }

        let phi_id = self.new_phi(block, variable);
        self.write_variable_interned(variable, block, SsaValue::Phi(phi_id));
        self.add_phi_operands(variable, phi_id)
    }

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
        trace_event!(
            self.tracer,
            SsaPhiCreated {
                phi_id: id.0,
                block_id: block.0,
                variable: variable.to_string(),
            }
        );
        id
    }

    fn add_phi_operands(&mut self, variable: &'a str, phi_id: PhiId) -> SsaValue<'a> {
        let block = self.phis[phi_id.0].block;
        let preds: SmallVec<[BlockId; 2]> = self.blocks[block.0].predecessors.clone();
        for pred in preds {
            let val = self.read_variable_internal(variable, pred);
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

    fn try_remove_trivial_phi(&mut self, phi_id: PhiId) -> SsaValue<'a> {
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
        trace_event!(
            self.tracer,
            SsaPhiTrivial {
                phi_id: phi_id.0,
                replacement: replacement.trace_display(),
            }
        );

        let variable = self.phis[phi_id.0].variable;
        let block = self.phis[phi_id.0].block;

        if let Some(block_defs) = self.current_def.get_mut(&variable)
            && block_defs.get(&block) == Some(&SsaValue::Phi(phi_id))
        {
            block_defs.insert(block, replacement.clone());
        }

        let phi_users: Vec<PhiId> = self
            .phis
            .iter()
            .enumerate()
            .filter(|(i, phi)| *i != phi_id.0 && phi.operands.contains(&SsaValue::Phi(phi_id)))
            .map(|(i, _)| PhiId(i))
            .collect();

        let phi_val = SsaValue::Phi(phi_id);
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
            stacker::maybe_grow(SSA_READ_RED_ZONE, SSA_READ_STACK_SEGMENT, || {
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
        if phi_ids.len() < 2 || self.deadline_tripped() {
            return;
        }
        if depth >= Self::MAX_SCC_DEPTH {
            orbit_log_fields::warn!(
                safe(LIMIT) = Self::MAX_SCC_DEPTH,
                safe(PHI_COUNT) = phi_ids.len(),
                "SCC phi elimination hit max recursion depth"
            );
            return;
        }

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

        let sccs = tarjan_scc(&phi_graph);

        for scc_nodes in &sccs {
            if scc_nodes.len() <= 1 {
                continue;
            }

            let scc: Vec<PhiId> = scc_nodes.iter().map(|&n| phi_graph[n]).collect();
            let scc_set: FxHashSet<PhiId> = scc.iter().copied().collect();

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
                let replacement = outer_values.into_iter().next().unwrap();
                trace_event!(
                    self.tracer,
                    SsaSccCollapse {
                        scc_size: scc.len(),
                        replacement: replacement.trace_display(),
                    }
                );
                let phi_vals: Vec<SsaValue<'a>> = scc.iter().map(|&p| SsaValue::Phi(p)).collect();
                for &pid in &scc {
                    let variable = self.phis[pid.0].variable;
                    let block = self.phis[pid.0].block;
                    if let Some(block_defs) = self.current_def.get_mut(&variable)
                        && block_defs.get(&block) == Some(&SsaValue::Phi(pid))
                    {
                        block_defs.insert(block, replacement.clone());
                    }
                    self.stats.phis_trivial += 1;
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

    fn resolve_value(&self, value: &SsaValue<'a>) -> ReachingDefs<'a> {
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
            SsaValue::Phi(_) => {}
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
                    return;
                }
                for op in &self.phis[phi_id.0].operands {
                    stacker::maybe_grow(SSA_READ_RED_ZONE, SSA_READ_STACK_SEGMENT, || {
                        self.resolve_value_recursive(op, out, visited)
                    });
                }
            }
            SsaValue::Opaque | SsaValue::Marker => {}
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
        let _ = ssa.read_variable_stateless("x", body);

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

        ssa.write_variable("x", entry, SsaValue::LocalDef(0));

        ssa.seal_block(entry);
        ssa.seal_block(left);
        ssa.seal_block(right);
        ssa.seal_block(exit);

        ssa.remove_redundant_phi_sccs();

        let result = ssa.read_variable_stateless("x", exit);
        assert_eq!(result.values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

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
        ssa.write_variable("x", left, SsaValue::LocalDef(1));

        ssa.seal_block(entry);
        ssa.seal_block(left);
        ssa.seal_block(right);
        ssa.seal_block(exit);

        ssa.remove_redundant_phi_sccs();

        let result = ssa.read_variable_stateless("x", exit);
        assert!(
            result.values.contains(&SsaValue::LocalDef(0))
                && result.values.contains(&SsaValue::LocalDef(1)),
            "SCC with multiple external values must not be collapsed: got {:?}",
            result.values
        );
    }

    #[test]
    fn ssa_budget_degrades_deep_read() {
        let mut ssa = SsaEngine::new();
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        ssa.write_variable("x", entry, SsaValue::LocalDef(0));
        let mut pre = entry;
        for _ in 0..2_000 {
            pre = ssa.add_sealed_successor(pre);
        }
        ssa.set_budget(Some(std::time::Duration::ZERO));
        let result = ssa.read_variable_stateless("x", pre);
        assert!(
            ssa.timed_out(),
            "a zero SSA budget must trip during a deep read"
        );
        assert!(
            result.values.is_empty(),
            "a timed-out read degrades to Opaque"
        );
    }

    fn build_try_except_chain(ssa: &mut SsaEngine<'static>, steps: usize) -> (BlockId, BlockId) {
        let entry = ssa.add_block();
        ssa.seal_block(entry);
        ssa.write_variable("np", entry, SsaValue::LocalDef(0));

        let mut pre = entry;
        for _ in 0..steps {
            let arm_a = ssa.add_block();
            ssa.add_predecessor(arm_a, pre);
            ssa.seal_block(arm_a);
            let arm_b = ssa.add_block();
            ssa.add_predecessor(arm_b, pre);
            ssa.seal_block(arm_b);
            let join = ssa.add_block();
            ssa.add_predecessor(join, arm_a);
            ssa.add_predecessor(join, arm_b);
            ssa.add_predecessor(join, pre);
            ssa.seal_block(join);
            pre = join;
        }
        (entry, pre)
    }

    #[test]
    fn wide_function_resolves_without_overflow() {
        let (values, depth_capped) = std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let mut ssa = SsaEngine::new();
                let (_, bottom) = build_try_except_chain(&mut ssa, 4_000);
                let result = ssa.read_variable_stateless("np", bottom);
                (result.values.to_vec(), ssa.stats.depth_capped)
            })
            .unwrap()
            .join()
            .expect("reaching-def read overflowed the worker stack");

        assert_eq!(depth_capped, 0, "4k-deep chain is under the cap");
        assert_eq!(values.as_slice(), &[SsaValue::LocalDef(0)]);
    }

    #[test]
    fn pathologically_wide_function_caps_instead_of_overflowing() {
        let depth_capped = std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let mut ssa = SsaEngine::new();
                let (_, bottom) = build_try_except_chain(&mut ssa, 10_000);
                let _ = ssa.read_variable_stateless("np", bottom);
                ssa.stats.depth_capped
            })
            .unwrap()
            .join()
            .expect("reaching-def read overflowed the worker stack");

        assert!(
            depth_capped > 0,
            "a chain past MAX_SSA_READ_DEPTH must degrade, not recurse unbounded"
        );
    }

    #[test]
    fn deep_phi_chain_resolves_without_overflow() {
        std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let mut ssa = SsaEngine::new();
                let block = ssa.add_block();
                let mut prev = SsaValue::LocalDef(0);
                for _ in 0..50_000 {
                    let phi = ssa.new_phi(block, "x");
                    ssa.phis[phi.0].operands.push(prev);
                    prev = SsaValue::Phi(phi);
                }
                let mut out = SmallVec::new();
                let mut visited = FxHashSet::default();
                ssa.resolve_value_recursive(&prev, &mut out, &mut visited);
                assert_eq!(out.as_slice(), &[SsaValue::LocalDef(0)]);
            })
            .unwrap()
            .join()
            .expect("deep phi-operand resolution overflowed the worker stack");
    }
}
