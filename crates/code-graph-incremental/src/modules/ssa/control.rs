use std::rc::Rc;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use super::{BindingKey, BlockId, Bound, Frame, Ssa, Value};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Val, Vals, of, one};
use crate::tree::{CallResolution, EdgeKind};

type LoopSpans = (FxHashMap<u32, u32>, FxHashMap<u32, Vec<u32>>, Vec<u32>);

fn loop_spans(root: Cursor, wildcard: u32) -> LoopSpans {
    let (mut ends, mut assigned) = (FxHashMap::default(), FxHashMap::<u32, Vec<u32>>::default());
    let mut wildcards = Vec::new();
    let mut stack = vec![(root, false)];
    let mut last = root.index();
    while let Some((node, leaving)) = stack.pop() {
        if leaving {
            ends.insert(node.index(), last + 1);
            continue;
        }
        last = node.index();
        if node.is(C::Import) || node.is(C::ImportType) {
            for name in std::iter::once(node).chain(node.descendants()) {
                if name.sym() == wildcard && wildcards.last() != Some(&last) {
                    wildcards.push(last);
                }
                assigned.entry(name.sym()).or_default().push(last);
            }
            continue;
        }
        if [C::Binding, C::DefName, C::Alias, C::SsaHint]
            .into_iter()
            .any(|kind| node.is(kind))
            || node.has(C::Declaration)
        {
            assigned.entry(node.sym()).or_default().push(last);
        }
        if node.is(C::SsaLoop) {
            stack.push((node, true));
        }
        stack.extend(node.children_rev().map(|child| (child, false)));
    }
    (ends, assigned, wildcards)
}

impl<'t> Cx<'_, 't, Ssa> {
    pub(super) fn leave(&mut self) {
        match self.write().frames.pop() {
            Some(Frame::Scope { wildcards, block }) => {
                self.exit_scope();
                let s = self.write();
                if s.wildcards.len() > wildcards {
                    Rc::make_mut(&mut s.wildcards).truncate(wildcards);
                }
                if let Some(block) = block {
                    s.cur = block;
                }
            }
            Some(Frame::Binding(Some(bound))) => self.exit_binding(bound),
            Some(Frame::Branch {
                lhs,
                pre: Some(pre),
                exits,
                ..
            }) => {
                let exits = exits.into_iter().chain(lhs.is_none().then_some(pre));
                let s = self.write();
                s.cur = s.graph.add_sealed_join(exits);
            }
            Some(Frame::Arm(arm)) => self.exit_arm(self.at(arm)),
            Some(Frame::Loop(header)) => {
                let s = self.write();
                s.loops.pop();
                s.cur = s.graph.finish_loop(header, s.cur);
                if s.loops.is_empty() {
                    self.resolve_pending();
                }
            }
            Some(Frame::Lambda(parent)) => self.write().cur = parent,
            Some(Frame::Binding(None) | Frame::Branch { .. }) | None => {}
        }
    }

    pub(super) fn destructure(&mut self, c: Cursor<'t>) {
        for slot in c.children_of(C::Binding) {
            let binding = self.declare_binding(slot, slot.sym());
            let s = self.write();
            s.graph
                .write_variable(binding, s.cur, Value::Call(slot.index()));
        }
    }

    pub(super) fn fork(&mut self, c: Cursor<'t>) {
        let s = self.write();
        let lhs = s.value_sink.remove(&c.index());
        s.frames.push(Frame::Branch {
            node: c.index(),
            lhs,
            pre: None,
            exits: SmallVec::new(),
        });
    }

    pub(super) fn arm(&mut self, arm: Cursor<'t>) {
        let s = self.write();
        let cur = s.cur;
        let Some(Frame::Branch { lhs, pre, .. }) = s.frames.last_mut() else {
            return;
        };
        let (lhs, pre) = (*lhs, *pre.get_or_insert(cur));
        s.cur = s.graph.add_sealed_successor(pre);
        let tail = arm.tail_expr();
        if let Some(lhs) = lhs.filter(|_| tail.is(C::SsaBranch)) {
            s.value_sink.insert(tail.index(), lhs);
        }
        s.frames.push(Frame::Arm(arm.index()));
    }

    fn exit_arm(&mut self, arm: Cursor<'t>) {
        let tail = arm.tail_expr();
        let lhs = match self.state().frames.last() {
            Some(Frame::Branch { lhs, .. }) => *lhs,
            _ => None,
        };
        if let Some(lhs) = lhs.filter(|_| !tail.is(C::SsaBranch) && !tail.is(C::SsaReturn)) {
            let val = self.classify_tail(tail);
            if val != Value::Opaque {
                let s = self.write();
                s.graph.write_variable(lhs, s.cur, val);
            }
            self.bind_fields(lhs, tail);
        }
        let s = self.write();
        let cur = s.cur;
        if let Some(Frame::Branch { exits, .. }) = s.frames.last_mut() {
            exits.push(cur);
        }
    }

    pub(super) fn begin_loop(&mut self, c: Cursor<'t>) {
        let root = self.tree().root();
        let s = self.write();
        if s.loop_ends.is_empty() {
            (s.loop_ends, s.assigned, s.loop_wildcards) = loop_spans(root, s.wildcard);
        }
        let span = (c.index(), s.loop_ends[&c.index()]);
        self.assign_module_names(span);
        let s = self.write();
        s.loops.push((s.cur, span));
        let (header, body) = s.graph.begin_loop(s.cur);
        s.cur = body;
        s.frames.push(Frame::Loop(header));
    }

    fn assign_module_names(&mut self, (start, end): (u32, u32)) {
        let s = self.state();
        let first = s.loop_wildcards.partition_point(|&at| at < start);
        let last = s.loop_wildcards.partition_point(|&at| at < end);
        let imports: Vec<u32> = s.loop_wildcards.drain(first..last).collect();
        for at in imports {
            let Some(module) = self.inline_module(self.at(at)) else {
                continue;
            };
            let names = module
                .children()
                .filter(|d| d.is(C::Def))
                .filter_map(|d| d.child_sym(C::DefName));
            for name in names {
                let positions = self.state().assigned.entry(name).or_default();
                if let Err(slot) = positions.binary_search(&at) {
                    positions.insert(slot, at);
                }
            }
        }
    }

    pub(super) fn target(&mut self, c: Cursor<'t>) -> Vals {
        let field = match self.member_binding(c) {
            Some((root, field)) => {
                let key = self.key(BindingKey::Field(root, field));
                let fields = self.state().fields.entry(root).or_default();
                Some(*fields.entry(field).or_insert(key))
            }
            None => None,
        };
        let lhs = match c.has(C::Declaration) && c.child_sym(C::Declaration).is_none() {
            true => self.binding_key(c, c.sym()),
            false => match field {
                Some(field) => field,
                None => self.binding(c.sym()),
            },
        };
        match lhs == 0 || (c.has(C::Ivar) && field.is_none()) {
            true => Vals::new(),
            false => one(Val::Sym(lhs)),
        }
    }

    pub(super) fn bind_binding(&mut self, c: Cursor<'t>, lhs: u32) {
        let declaration = c.has(C::Declaration) && c.child_sym(C::Declaration).is_none();
        let s = self.write();
        if s.graph.has_variable_in_block(lhs, s.cur) {
            s.cur = s.graph.add_sealed_successor(s.cur);
        }
        let rhs = c.child(C::Rhs);
        if rhs.is_none() && self.bind_elements(c, lhs) {
            if declaration {
                self.bind(c.sym(), lhs);
            }
            self.write().frames.push(Frame::Binding(None));
            return;
        }
        if rhs.is_none() && c.child_sym(C::Declaration).is_some() {
            let s = self.write();
            if c.parent().is_some_and(|parent| parent.is(C::SsaLoop)) {
                s.graph.write_variable(lhs, s.cur, Value::Opaque);
            }
            s.frames.push(Frame::Binding(None));
            return;
        }
        let branch = rhs.and_then(|r| r.child(C::SsaBranch));
        let first_edge = self.emitted();
        let s = self.write();
        if let Some(branch) = branch {
            s.value_sink.insert(branch.index(), lhs);
        }
        s.frames.push(Frame::Binding(Some(Bound {
            binding: c.index(),
            lhs,
            first_edge,
            declaration,
            branch: branch.is_some(),
        })));
    }

    pub(super) fn bind_children(&mut self, c: Cursor<'t>) -> Vals {
        if !matches!(self.state().frames.last(), Some(Frame::Binding(Some(_)))) {
            return Vals::new();
        }
        let Some(rhs) = c.child(C::Rhs) else {
            return Vals::new();
        };
        let calls = c
            .children()
            .take_while(|child| !child.is(C::Rhs))
            .filter(|child| child.is(C::Call));
        match rhs.child(C::SsaBranch) {
            Some(branch) => {
                let effects = rhs.children().filter(|child| child.is(C::SideEffect));
                of(std::iter::once(branch).chain(effects).chain(calls))
            }
            None => of(rhs.children().chain(calls)),
        }
    }

    fn bind_elements(&mut self, c: Cursor<'t>, lhs: u32) -> bool {
        let iterable = c
            .parent()
            .filter(|parent| parent.is(C::SsaLoop))
            .and_then(|parent| {
                parent
                    .children()
                    .take_while(|node| node.index() != c.index())
                    .find(|node| node.is(C::Arr))
            });
        let s = self.write();
        let pre = s.cur;
        let arms: Vec<BlockId> = iterable
            .iter()
            .flat_map(|array| array.children())
            .filter(|element| element.is(C::Call))
            .map(|element| {
                let arm = s.graph.add_sealed_successor(pre);
                s.graph
                    .write_variable(lhs, arm, Value::Call(element.index()));
                arm
            })
            .collect();
        let bound = !arms.is_empty();
        if bound {
            s.cur = s.graph.add_sealed_join(arms);
        }
        bound
    }

    fn exit_binding(&mut self, bound: Bound) {
        let c = self.at(bound.binding);
        let rhs = c.child(C::Rhs);
        if !bound.branch {
            if rhs.is_some_and(|rhs| rhs.value_child(C::Member).is_some()) {
                for edge in self.edges_since(bound.first_edge) {
                    if edge.kind == EdgeKind::Imports
                        && edge.site.is_none()
                        && edge.call_resolution == CallResolution::Reference
                    {
                        edge.site = Some(c.index());
                    }
                }
            }
            let tail = rhs.map(Cursor::tail_expr);
            let typed = c.has(C::SsaTyped)
                || tail.is_some_and(|tail| tail.is(C::Member) && self.field_slot(tail).is_none());
            let mut val = match (typed, tail) {
                (true, _) => Value::Call(c.index()),
                (false, Some(tail)) => self.classify_tail(tail),
                (false, None) => Value::Opaque,
            };
            if let Some(rhs) = c.bare_rhs()
                && self
                    .lookup(rhs.sym())
                    .iter()
                    .any(|v| matches!(v, Value::ImportRef(_)))
            {
                val = Value::Call(c.index());
            }
            let s = self.write();
            s.graph.write_variable(bound.lhs, s.cur, val);
            if let Some(tail) = tail {
                self.bind_fields(bound.lhs, tail);
            }
        }
        if bound.declaration {
            self.bind(c.sym(), bound.lhs);
        }
    }
}
