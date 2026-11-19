use std::collections::hash_map::Entry;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::{SmallVec, smallvec};

use super::{BindingKey, Bound, Frame};
use super::{BlockId, Value};
use crate::canonical::Canonical as C;
use crate::engine::Cursor;
use crate::engine::{Cx, Val, Vals, of};

fn member_of(node: Cursor<'_>) -> Option<Cursor<'_>> {
    node.is(C::Member)
        .then_some(node)
        .or_else(|| node.child(C::Member))
        .or_else(|| node.child(C::Ivar))
}

fn object_of(member: Cursor<'_>) -> Option<Cursor<'_>> {
    member
        .child(C::Object)
        .filter(|object| !object.has(C::Qualifier))
}

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

impl<'t> Cx<'t> {
    pub(crate) fn scope(&mut self, scope: Cursor<'t>, block: Option<BlockId>) {
        self.ssa.frames.push(Frame::Scope {
            wildcards: self.ssa.wildcards.len(),
            block,
        });
        self.enter_scope(scope);
    }

    pub(crate) fn leave(&mut self) {
        match self.ssa.frames.pop() {
            Some(Frame::Scope { wildcards, block }) => {
                self.exit_scope();
                if self.ssa.wildcards.len() > wildcards {
                    Rc::make_mut(&mut self.ssa.wildcards).truncate(wildcards);
                }
                if let Some(block) = block {
                    self.ssa.cur = block;
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
                self.ssa.cur = self.ssa.graph.add_sealed_join(exits);
            }
            Some(Frame::Arm(arm)) => self.exit_arm(self.at(arm)),
            Some(Frame::Loop(header)) => {
                self.ssa.loops.pop();
                self.ssa.cur = self.ssa.graph.finish_loop(header, self.ssa.cur);
                if self.ssa.loops.is_empty()
                    && let Err(k) = self.resolve_pending()
                {
                    self.killed = Some(k);
                }
            }
            Some(Frame::Lambda(parent)) => self.ssa.cur = parent,
            Some(Frame::Binding(None) | Frame::Branch { .. }) | None => {}
        }
    }

    pub(crate) fn destructure(&mut self, c: Cursor<'t>) {
        for slot in c.children_of(C::Binding) {
            let binding = self.declare_binding(slot, slot.sym());
            self.ssa
                .graph
                .write_variable(binding, self.ssa.cur, Value::Call(slot.index()));
        }
    }

    pub(crate) fn branch(&mut self, c: Cursor<'t>) {
        self.ssa.frames.push(Frame::Branch {
            node: c.index(),
            lhs: self.ssa.value_sink.remove(&c.index()),
            pre: None,
            exits: SmallVec::new(),
        });
    }

    pub(crate) fn arm(&mut self, arm: Cursor<'t>) {
        let Some(Frame::Branch { lhs, pre, .. }) = self.ssa.frames.last_mut() else {
            return;
        };
        let (lhs, pre) = (*lhs, *pre.get_or_insert(self.ssa.cur));
        self.ssa.cur = self.ssa.graph.add_sealed_successor(pre);
        let tail = arm.tail_expr();
        if let Some(lhs) = lhs.filter(|_| tail.is(C::SsaBranch)) {
            self.ssa.value_sink.insert(tail.index(), lhs);
        }
        self.ssa.frames.push(Frame::Arm(arm.index()));
    }

    fn exit_arm(&mut self, arm: Cursor<'t>) {
        let tail = arm.tail_expr();
        let lhs = match self.ssa.frames.last() {
            Some(Frame::Branch { lhs, .. }) => *lhs,
            _ => None,
        };
        if let Some(lhs) = lhs.filter(|_| !tail.is(C::SsaBranch) && !tail.is(C::SsaReturn)) {
            let val = self.classify_tail(tail);
            if val != Value::Opaque {
                self.ssa.graph.write_variable(lhs, self.ssa.cur, val);
            }
            self.bind_fields(lhs, tail);
        }
        if let Some(Frame::Branch { exits, .. }) = self.ssa.frames.last_mut() {
            exits.push(self.ssa.cur);
        }
    }

    pub(crate) fn begin_loop(&mut self, c: Cursor<'t>) {
        if self.ssa.loop_ends.is_empty() {
            (
                self.ssa.loop_ends,
                self.ssa.assigned,
                self.ssa.loop_wildcards,
            ) = loop_spans(self.tree().root(), self.ssa.wildcard);
        }
        let span = (c.index(), self.ssa.loop_ends[&c.index()]);
        self.assign_module_names(span);
        self.ssa.loops.push((self.ssa.cur, span));
        let (header, body) = self.ssa.graph.begin_loop(self.ssa.cur);
        self.ssa.cur = body;
        self.ssa.frames.push(Frame::Loop(header));
    }

    pub(crate) fn target(&mut self, c: Cursor<'t>) -> Vals {
        let field = self.member_binding(c).map(|(root, field)| {
            let key = self.key(BindingKey::Field(root, field));
            *self
                .ssa
                .fields
                .entry(root)
                .or_default()
                .entry(field)
                .or_insert(key)
        });
        let lhs = match c.has(C::Declaration) && c.child_sym(C::Declaration).is_none() {
            true => self.binding_key(c, c.sym()),
            false => field.unwrap_or_else(|| self.binding(c.sym())),
        };
        match lhs == 0 || (c.has(C::Ivar) && field.is_none()) {
            true => Vals::new(),
            false => smallvec![Val::Sym(lhs)],
        }
    }

    pub(crate) fn bind_binding(&mut self, c: Cursor<'t>, lhs: u32) {
        let declaration = c.has(C::Declaration) && c.child_sym(C::Declaration).is_none();
        if self.ssa.graph.has_variable_in_block(lhs, self.ssa.cur) {
            self.ssa.cur = self.ssa.graph.add_sealed_successor(self.ssa.cur);
        }
        let rhs = c.child(C::Rhs);
        if rhs.is_none() && self.bind_elements(c, lhs) {
            if declaration {
                self.bind(c.sym(), lhs);
            }
            self.ssa.frames.push(Frame::Binding(None));
            return;
        }
        if rhs.is_none() && c.child_sym(C::Declaration).is_some() {
            if c.parent().is_some_and(|parent| parent.is(C::SsaLoop)) {
                self.ssa
                    .graph
                    .write_variable(lhs, self.ssa.cur, Value::Opaque);
            }
            self.ssa.frames.push(Frame::Binding(None));
            return;
        }
        let branch = rhs.and_then(|r| r.child(C::SsaBranch));
        if let Some(branch) = branch {
            self.ssa.value_sink.insert(branch.index(), lhs);
        }
        self.ssa.frames.push(Frame::Binding(Some(Bound {
            binding: c.index(),
            lhs,
            first_edge: self.edges.len(),
            declaration,
            branch: branch.is_some(),
        })));
    }

    pub(crate) fn bind_children(&mut self, c: Cursor<'t>) -> Vals {
        if !matches!(self.ssa.frames.last(), Some(Frame::Binding(Some(_)))) {
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
        let pre = self.ssa.cur;
        let arms: Vec<BlockId> = iterable
            .iter()
            .flat_map(|array| array.children())
            .filter(|element| element.is(C::Call))
            .map(|element| {
                let arm = self.ssa.graph.add_sealed_successor(pre);
                self.ssa
                    .graph
                    .write_variable(lhs, arm, Value::Call(element.index()));
                arm
            })
            .collect();
        let bound = !arms.is_empty();
        if bound {
            self.ssa.cur = self.ssa.graph.add_sealed_join(arms);
        }
        bound
    }

    fn exit_binding(&mut self, bound: Bound) {
        let c = self.at(bound.binding);
        let rhs = c.child(C::Rhs);
        if !bound.branch {
            if rhs.is_some_and(|rhs| rhs.value_child(C::Member).is_some()) {
                for edge in &mut self.edges[bound.first_edge..] {
                    if edge.kind == crate::tree::EdgeKind::Imports
                        && edge.site.is_none()
                        && edge.call_resolution == crate::tree::CallResolution::Reference
                    {
                        edge.site = Some(c.index());
                    }
                }
            }
            let tail = rhs.map(Cursor::tail_expr);
            let mut val = if c.has(C::SsaTyped)
                || tail.is_some_and(|tail| tail.is(C::Member) && self.field_slot(tail).is_none())
            {
                Value::Call(c.index())
            } else {
                tail.map_or(Value::Opaque, |tail| self.classify_tail(tail))
            };
            if let Some(rhs) = c.bare_rhs() {
                val = self.bare_rhs(c, rhs).unwrap_or(val);
            }
            self.ssa.graph.write_variable(bound.lhs, self.ssa.cur, val);
            if let Some(tail) = tail {
                self.bind_fields(bound.lhs, tail);
            }
        }
        if bound.declaration {
            self.bind(c.sym(), bound.lhs);
        }
    }

    pub(crate) fn read_value(&mut self, binding: u32) -> Value {
        match self.ssa.graph.read_variable_internal(binding, self.ssa.cur) {
            Value::Undefined => Value::Opaque,
            value => value,
        }
    }

    pub(crate) fn binding(&self, name: u32) -> u32 {
        self.ssa
            .visible
            .get(&name)
            .map_or_else(|| self.key(BindingKey::Name(name)), |&(binding, _)| binding)
    }

    fn bound_at(&self, name: u32, at: usize) -> Option<u32> {
        let hit = self.ssa.scopes[at + 1..]
            .iter()
            .flat_map(|(_, undo)| undo)
            .find_map(|&(n, previous)| (n == name).then_some(previous));
        hit.unwrap_or_else(|| self.ssa.visible.get(&name).copied())
            .filter(|&(_, depth)| depth == at)
            .map(|(binding, _)| binding)
    }

    pub(crate) fn bind(&mut self, name: u32, binding: u32) {
        let Some(depth) = self.ssa.scopes.len().checked_sub(1) else {
            return;
        };
        self.ssa.binds += 1;
        let previous = self.ssa.visible.insert(name, (binding, depth));
        if previous.is_none_or(|(key, at)| at != depth || self.ssa.statics.contains_key(&key)) {
            self.ssa.scopes[depth].1.push((name, previous));
        }
    }

    fn exit_scope(&mut self) {
        let undo = self
            .ssa
            .scopes
            .pop()
            .map(|(_, undo)| undo)
            .unwrap_or_default();
        for (name, previous) in undo.into_iter().rev() {
            match previous {
                Some(previous) => self.ssa.visible.insert(name, previous),
                None => self.ssa.visible.remove(&name),
            };
        }
    }

    fn binding_key(&self, node: Cursor<'_>, name: u32) -> u32 {
        self.key(BindingKey::Declaration(node.index(), name))
    }

    pub(crate) fn key(&self, key: BindingKey) -> u32 {
        if key == BindingKey::Name(0) {
            return 0;
        }
        let mut keys = self.ssa.keys.borrow_mut();
        let next = keys.len() as u32 + 1;
        *keys.entry(key).or_insert(next)
    }

    pub(crate) fn declare_binding(&mut self, node: Cursor<'_>, name: u32) -> u32 {
        let binding = if node.child_sym(C::Declaration).is_some() {
            self.binding(name)
        } else {
            self.binding_key(node, name)
        };
        self.bind(name, binding);
        binding
    }

    pub(crate) fn declare_static(&mut self, node: Cursor<'_>, name: u32, value: Value) -> u32 {
        let binding = self.declare_binding(node, name);
        self.ssa
            .graph
            .write_variable(binding, self.ssa.cur, value.clone());
        self.ssa.statics.insert(binding, value);
        binding
    }

    pub(crate) fn lookup_static(&self, sym: u32) -> Option<Value> {
        let (binding, depth) = *self.ssa.visible.get(&sym)?;
        if let Some(value) = self.ssa.statics.get(&binding) {
            return Some(value.clone());
        }
        self.ssa.scopes[..=depth]
            .iter()
            .rev()
            .flat_map(|(_, undo)| undo.iter().rev())
            .filter(|(name, _)| *name == sym)
            .map(|(_, previous)| *previous)
            .take_while(Option::is_some)
            .find_map(|previous| self.ssa.statics.get(&previous?.0).cloned())
    }

    fn enter_scope(&mut self, scope: Cursor<'t>) {
        let label = scope.tag(self.tags.scoped).unwrap_or(scope.sym());
        self.ssa.scopes.push((label, Vec::new()));
        if scope.has_tag(self.tags.hoisted) {
            self.predeclare(scope);
        }
        if label == 0 {
            return;
        }
        let scoped = self.tags.scoped;
        let boundary =
            move |node: Cursor| (node.is(C::Scope) && node.sym() == label) || node.has_tag(scoped);
        let mut declarations: Vec<_> = scope
            .descendants_pruned(boundary)
            .filter(|node| node.child_sym(C::Declaration) == Some(label))
            .collect();
        declarations.sort_by_key(|node| !node.has(C::Alias));
        for declaration in declarations {
            let name = declaration
                .child_sym(C::DefName)
                .unwrap_or(declaration.sym());
            let key = self.binding_key(declaration, name);
            let unbound = self.key(BindingKey::Name(name));
            let depth = self.ssa.scopes.len() - 1;
            if self
                .ssa
                .visible
                .get(&name)
                .is_some_and(|&(_, at)| at == depth)
            {
                continue;
            }
            if let Some(owner) = declaration.child_sym(C::Alias) {
                let owner = self.ssa.scopes[..depth]
                    .iter()
                    .rposition(|(label, _)| *label == owner);
                if let Some(at) = owner {
                    self.bind(name, self.bound_at(name, at).unwrap_or(unbound));
                }
                continue;
            }
            self.bind(name, key);
            self.ssa
                .graph
                .write_variable(key, self.ssa.cur, Value::Opaque);
        }
    }

    pub(crate) fn predeclare(&mut self, scope: Cursor<'t>) {
        for d in scope
            .children()
            .filter(|d| d.is(C::Def) && d.child(C::Declaration).is_none())
        {
            if let Some(name) = d.child_sym(C::DefName) {
                self.register_def(d);
                self.declare(d, name);
            }
        }
    }

    pub(crate) fn declare(&mut self, c: Cursor<'t>, name: u32) {
        for sym in std::iter::once(name).chain(c.children_of(C::Alias).map(|a| a.sym())) {
            if sym == name && c.has(C::ImplBlock) && !self.lookup(name).is_empty() {
                continue;
            }
            self.declare_static(c, sym, Value::LocalDef(c.index()));
        }
    }

    fn member_binding(&self, node: Cursor<'_>) -> Option<(u32, u32)> {
        let member = member_of(node)?;
        let mut object = object_of(member)?;
        let mut path = Vec::new();
        while object.sym() == 0 {
            let inner = member_of(object)?;
            path.push(inner.sym());
            object = object_of(inner)?;
        }
        let root = path
            .into_iter()
            .rev()
            .try_fold(self.binding(object.sym()), |binding, field| {
                self.field_of(binding, field)
            })?;
        (root != 0 && !self.ssa.statics.contains_key(&root)).then_some((root, member.sym()))
    }

    fn field_of(&self, binding: u32, field: u32) -> Option<u32> {
        if binding == 0 || self.ssa.statics.contains_key(&binding) {
            return None;
        }
        self.ssa.fields.get(&binding)?.get(&field).copied()
    }

    pub(crate) fn field_slot(&self, node: Cursor<'_>) -> Option<u32> {
        let (root, field) = self.member_binding(node)?;
        self.ssa.fields.get(&root)?.get(&field).copied()
    }

    pub(crate) fn bind_fields(&mut self, lhs: u32, value: Cursor<'t>) {
        let record = value
            .is(C::Obj)
            .then_some(value)
            .or_else(|| value.child(C::Obj))
            .or_else(|| {
                value.child(C::Args).filter(|_| {
                    value
                        .child(C::Callee)
                        .is_some_and(|callee| self.chain_is_class(callee))
                })
            });
        let sources: FxHashMap<_, _> = if let Some(record) = record {
            record
                .children_of(C::ConfigField)
                .map(|field| {
                    let source = field
                        .child_sym(C::Binding)
                        .or_else(|| field.child_sym(C::Rhs));
                    (field.sym(), self.binding(source.unwrap_or(0)))
                })
                .collect()
        } else {
            let source = self
                .field_slot(value)
                .or_else(|| {
                    value
                        .children()
                        .next()
                        .is_none()
                        .then(|| self.binding(value.sym()))
                })
                .unwrap_or(0);
            if source != lhs
                && self.reaches(source, lhs)
                && let Some(shared) = self.ssa.fields.get(&source).cloned()
            {
                self.ssa.fields.insert(lhs, shared);
                return;
            }
            self.ssa.fields.get(&source).cloned().unwrap_or_default()
        };
        let mut copies: SmallVec<[_; 4]> = smallvec![(lhs, sources)];
        let mut field_updates = SmallVec::<[_; 4]>::new();
        let mut value_updates = SmallVec::<[_; 8]>::new();
        let mut copied = FxHashMap::default();
        let mut reset = FxHashSet::default();
        let mut aliases = SmallVec::<[_; 4]>::new();
        while let Some((target, sources)) = copies.pop() {
            if let Err(killed) = self.budget() {
                self.killed = Some(killed);
                return;
            }
            if sources.is_empty() && !self.ssa.fields.contains_key(&target) {
                continue;
            }
            let mut fields = self.ssa.fields.get(&target).cloned().unwrap_or_default();
            for field in sources.keys() {
                fields
                    .entry(*field)
                    .or_insert_with(|| self.key(BindingKey::Field(target, *field)));
            }
            for (&field, &target) in &fields {
                let source = sources.get(&field).copied().unwrap_or(0);
                if target == source {
                    continue;
                }
                let value = self.read_value(source);
                value_updates.push((target, value));
                if source == 0 {
                    if self.ssa.fields.contains_key(&target) && reset.insert(target) {
                        copies.push((target, FxHashMap::default()));
                    }
                } else if self.ssa.fields.contains_key(&source)
                    || self.ssa.fields.contains_key(&target)
                {
                    match copied.entry(source) {
                        Entry::Occupied(first) => aliases.push((target, *first.get())),
                        Entry::Vacant(slot) => {
                            slot.insert(target);
                            let sources = self.ssa.fields.get(&source).cloned().unwrap_or_default();
                            copies.push((target, sources));
                        }
                    }
                }
            }
            field_updates.push((target, fields));
        }
        self.ssa.fields.extend(field_updates);
        for (target, first) in aliases {
            if target != first
                && let Some(shared) = self.ssa.fields.get(&first).cloned()
            {
                self.ssa.fields.insert(target, shared);
            }
        }
        for (target, value) in value_updates {
            self.ssa.graph.write_variable(target, self.ssa.cur, value);
        }
    }

    fn reaches(&self, from: u32, to: u32) -> bool {
        let mut seen = FxHashSet::default();
        let mut stack = vec![from];
        while let Some(binding) = stack.pop() {
            if binding == to {
                return true;
            }
            if seen.insert(binding) {
                stack.extend(
                    self.ssa
                        .fields
                        .get(&binding)
                        .into_iter()
                        .flat_map(|f| f.values()),
                );
            }
        }
        false
    }

    pub(crate) fn classify_tail(&mut self, tail: Cursor<'_>) -> Value {
        if C::try_from_u16(tail.kind()).is_some_and(C::is_opaque_value) {
            return Value::Opaque;
        }
        if let Some(slot) = self.field_slot(tail) {
            return self.read_value(slot);
        }
        if tail.is(C::Call) {
            return Value::Call(tail.index());
        }
        if tail.is(C::Ivar) {
            let field = self
                .enclosing_class(self.enclosing())
                .and_then(|cls| self.ivar_type(cls, tail.sym()));
            return field.map_or(Value::Opaque, |f| Value::Call(Self::producer_of(f)));
        }
        let sym = Self::tail_sym(tail);
        if sym == 0 {
            return Value::Opaque;
        }
        let block = self.name_block(sym);
        match self
            .ssa
            .graph
            .read_variable_internal(self.binding(sym), block)
        {
            Value::Undefined => Value::Opaque,
            Value::LocalDef(node) if self.at(node).is_class() => Value::Type(node),
            value => value,
        }
    }

    fn tail_sym(node: Cursor<'_>) -> u32 {
        match node.last_named().filter(|c| node.stands_for(*c)) {
            Some(c) if c.size() > 1 => Self::tail_sym(c),
            Some(c) => c.sym(),
            None => node.sym(),
        }
    }

    fn assign_module_names(&mut self, (start, end): (u32, u32)) {
        let first = self.ssa.loop_wildcards.partition_point(|&at| at < start);
        let last = self.ssa.loop_wildcards.partition_point(|&at| at < end);
        let imports: Vec<u32> = self.ssa.loop_wildcards.drain(first..last).collect();
        for at in imports {
            let Some(module) = self.inline_module(self.at(at)) else {
                continue;
            };
            let names = module
                .children()
                .filter(|d| d.is(C::Def))
                .filter_map(|d| d.child_sym(C::DefName));
            for name in names {
                let positions = self.ssa.assigned.entry(name).or_default();
                if let Err(slot) = positions.binary_search(&at) {
                    positions.insert(slot, at);
                }
            }
        }
    }

    fn name_block(&self, sym: u32) -> BlockId {
        let assigned = self.ssa.assigned.get(&sym).map_or(&[][..], Vec::as_slice);
        let inner = self.ssa.loops.partition_point(|&(_, (start, end))| {
            let first = assigned.partition_point(|&at| at < start);
            assigned.get(first).is_some_and(|&at| at < end)
        });
        self.ssa
            .loops
            .get(inner)
            .map_or(self.ssa.cur, |&(preheader, _)| preheader)
    }

    pub(crate) fn read_phi(&mut self, sym: u32) -> Option<Value> {
        let value = self
            .ssa
            .graph
            .read_variable_internal(self.binding(sym), self.name_block(sym));
        matches!(value, Value::Phi(_)).then_some(value)
    }

    pub(crate) fn lookup(&mut self, sym: u32) -> Vec<Value> {
        for block in [self.name_block(sym), BlockId(0)] {
            let result = self.ssa.graph.read_variable(self.binding(sym), block);
            if !result.is_empty() {
                return result;
            }
        }
        Vec::new()
    }
}
