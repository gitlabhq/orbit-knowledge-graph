use std::collections::hash_map::Entry;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::{SmallVec, smallvec};

use super::{BindingKey, Bound, Cursor, Fold, Frame};
use crate::canonical::Canonical as C;
use crate::rules::Val;
use crate::ssa::{BlockId, Value};

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

impl<'t> Fold<'t> {
    pub(super) fn body(&mut self, c: Cursor<'t>) -> Val {
        if !c.has_tag(self.tags.scoped) {
            return Val::None;
        }
        let initializer = c.initializer().filter(|_| c.is_class()).map(Cursor::index);
        Val::of(
            c.children()
                .filter(|node| Some(node.index()) != initializer),
        )
    }

    pub(super) fn scope(&mut self, scope: Cursor<'t>, block: Option<BlockId>) {
        self.frames.push(Frame::Scope {
            wildcards: self.wildcards.len(),
            block,
        });
        self.enter_scope(scope);
    }

    pub(super) fn leave(&mut self) {
        match self.frames.pop() {
            Some(Frame::Scope { wildcards, block }) => {
                self.exit_scope();
                if self.wildcards.len() > wildcards {
                    Rc::make_mut(&mut self.wildcards).truncate(wildcards);
                }
                if let Some(block) = block {
                    self.cur = block;
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
                self.cur = self.ssa.add_sealed_join(exits);
            }
            Some(Frame::Arm(arm)) => self.exit_arm(self.at(arm)),
            Some(Frame::Loop(header)) => {
                self.loops.pop();
                self.cur = self.ssa.finish_loop(header, self.cur);
                if self.loops.is_empty()
                    && let Err(k) = self.resolve_pending()
                {
                    self.killed = Some(k);
                }
            }
            Some(Frame::Lambda(parent)) => self.cur = parent,
            Some(Frame::Binding(None) | Frame::Branch { .. }) | None => {}
        }
    }

    pub(super) fn destructure(&mut self, c: Cursor<'t>) {
        for slot in c.children_of(C::Binding) {
            let binding = self.declare_binding(slot, slot.sym());
            self.ssa
                .write_variable(binding, self.cur, Value::Call(slot.index()));
        }
    }

    pub(super) fn branch(&mut self, c: Cursor<'t>) {
        self.frames.push(Frame::Branch {
            node: c.index(),
            lhs: self.value_sink.remove(&c.index()),
            pre: None,
            exits: SmallVec::new(),
        });
    }

    pub(super) fn arm(&mut self, arm: Cursor<'t>) {
        let Some(Frame::Branch { lhs, pre, .. }) = self.frames.last_mut() else {
            return;
        };
        let (lhs, pre) = (*lhs, *pre.get_or_insert(self.cur));
        self.cur = self.ssa.add_sealed_successor(pre);
        let tail = arm.tail_expr();
        if let Some(lhs) = lhs.filter(|_| tail.is(C::SsaBranch)) {
            self.value_sink.insert(tail.index(), lhs);
        }
        self.frames.push(Frame::Arm(arm.index()));
    }

    fn exit_arm(&mut self, arm: Cursor<'t>) {
        let tail = arm.tail_expr();
        let lhs = match self.frames.last() {
            Some(Frame::Branch { lhs, .. }) => *lhs,
            _ => None,
        };
        if let Some(lhs) = lhs.filter(|_| !tail.is(C::SsaBranch) && !tail.is(C::SsaReturn)) {
            let val = self.classify_tail(tail);
            if val != Value::Opaque {
                self.ssa.write_variable(lhs, self.cur, val);
            }
            self.bind_fields(lhs, tail);
        }
        if let Some(Frame::Branch { exits, .. }) = self.frames.last_mut() {
            exits.push(self.cur);
        }
    }

    pub(super) fn loop_children(&mut self, c: Cursor<'t>) -> Val {
        let header = c
            .children()
            .position(|node| node.is(C::Binding) && node.has(C::Declaration) && !node.lifted())
            .unwrap_or(0);
        let children = c.children().take(header);
        let rest = c.children().skip(header);
        Val::of(children.chain(std::iter::once(c)).chain(rest))
    }

    pub(super) fn begin_loop(&mut self, c: Cursor<'t>) {
        if self.loop_ends.is_empty() {
            (self.loop_ends, self.assigned, self.loop_wildcards) =
                loop_spans(self.tree.root(), self.wildcard);
        }
        let span = (c.index(), self.loop_ends[&c.index()]);
        self.assign_module_names(span);
        self.loops.push((self.cur, span));
        let (header, body) = self.ssa.begin_loop(self.cur);
        self.cur = body;
        self.frames.push(Frame::Loop(header));
    }

    pub(super) fn target(&mut self, c: Cursor<'t>) -> Val {
        let field = self.member_binding(c).map(|(root, field)| {
            let key = self.key(BindingKey::Field(root, field));
            *self
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
            true => Val::None,
            false => Val::Lit(lhs, 0),
        }
    }

    pub(super) fn bind_binding(&mut self, c: Cursor<'t>, lhs: u32) {
        let declaration = c.has(C::Declaration) && c.child_sym(C::Declaration).is_none();
        if self.ssa.has_variable_in_block(lhs, self.cur) {
            self.cur = self.ssa.add_sealed_successor(self.cur);
        }
        let rhs = c.child(C::Rhs);
        if rhs.is_none() && self.bind_elements(c, lhs) {
            if declaration {
                self.bind(c.sym(), lhs);
            }
            self.frames.push(Frame::Binding(None));
            return;
        }
        if rhs.is_none() && c.child_sym(C::Declaration).is_some() {
            if c.parent().is_some_and(|parent| parent.is(C::SsaLoop)) {
                self.ssa.write_variable(lhs, self.cur, Value::Opaque);
            }
            self.frames.push(Frame::Binding(None));
            return;
        }
        let branch = rhs.and_then(|r| r.child(C::SsaBranch));
        if let Some(branch) = branch {
            self.value_sink.insert(branch.index(), lhs);
        }
        self.frames.push(Frame::Binding(Some(Bound {
            binding: c.index(),
            lhs,
            first_edge: self.edges.len(),
            declaration,
            branch: branch.is_some(),
        })));
    }

    pub(super) fn bind_children(&mut self, c: Cursor<'t>) -> Val {
        if !matches!(self.frames.last(), Some(Frame::Binding(Some(_)))) {
            return Val::None;
        }
        let Some(rhs) = c.child(C::Rhs) else {
            return Val::None;
        };
        let calls = c
            .children()
            .take_while(|child| !child.is(C::Rhs))
            .filter(|child| child.is(C::Call));
        match rhs.child(C::SsaBranch) {
            Some(branch) => {
                let effects = rhs.children().filter(|child| child.is(C::SideEffect));
                Val::of(std::iter::once(branch).chain(effects).chain(calls))
            }
            None => Val::of(rhs.children().chain(calls)),
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
        let pre = self.cur;
        let arms: Vec<BlockId> = iterable
            .iter()
            .flat_map(|array| array.children())
            .filter(|element| element.is(C::Call))
            .map(|element| {
                let arm = self.ssa.add_sealed_successor(pre);
                self.ssa
                    .write_variable(lhs, arm, Value::Call(element.index()));
                arm
            })
            .collect();
        let bound = !arms.is_empty();
        if bound {
            self.cur = self.ssa.add_sealed_join(arms);
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
            self.ssa.write_variable(bound.lhs, self.cur, val);
            if let Some(tail) = tail {
                self.bind_fields(bound.lhs, tail);
            }
        }
        if bound.declaration {
            self.bind(c.sym(), bound.lhs);
        }
    }

    pub(super) fn read_value(&mut self, binding: u32) -> Value {
        match self.ssa.read_variable_internal(binding, self.cur) {
            Value::Undefined => Value::Opaque,
            value => value,
        }
    }

    pub(super) fn binding(&self, name: u32) -> u32 {
        self.visible
            .get(&name)
            .map_or_else(|| self.key(BindingKey::Name(name)), |&(binding, _)| binding)
    }

    fn bound_at(&self, name: u32, at: usize) -> Option<u32> {
        let hit = self.scopes[at + 1..]
            .iter()
            .flat_map(|(_, undo)| undo)
            .find_map(|&(n, previous)| (n == name).then_some(previous));
        hit.unwrap_or_else(|| self.visible.get(&name).copied())
            .filter(|&(_, depth)| depth == at)
            .map(|(binding, _)| binding)
    }

    pub(super) fn bind(&mut self, name: u32, binding: u32) {
        let Some(depth) = self.scopes.len().checked_sub(1) else {
            return;
        };
        self.binds += 1;
        let previous = self.visible.insert(name, (binding, depth));
        if previous.is_none_or(|(key, at)| at != depth || self.statics.contains_key(&key)) {
            self.scopes[depth].1.push((name, previous));
        }
    }

    fn exit_scope(&mut self) {
        let undo = self.scopes.pop().map(|(_, undo)| undo).unwrap_or_default();
        for (name, previous) in undo.into_iter().rev() {
            match previous {
                Some(previous) => self.visible.insert(name, previous),
                None => self.visible.remove(&name),
            };
        }
    }

    fn binding_key(&self, node: Cursor<'_>, name: u32) -> u32 {
        self.key(BindingKey::Declaration(node.index(), name))
    }

    pub(super) fn key(&self, key: BindingKey) -> u32 {
        if key == BindingKey::Name(0) {
            return 0;
        }
        let mut keys = self.keys.borrow_mut();
        let next = keys.len() as u32 + 1;
        *keys.entry(key).or_insert(next)
    }

    pub(super) fn declare_binding(&mut self, node: Cursor<'_>, name: u32) -> u32 {
        let binding = if node.child_sym(C::Declaration).is_some() {
            self.binding(name)
        } else {
            self.binding_key(node, name)
        };
        self.bind(name, binding);
        binding
    }

    pub(super) fn declare_static(&mut self, node: Cursor<'_>, name: u32, value: Value) -> u32 {
        let binding = self.declare_binding(node, name);
        self.ssa.write_variable(binding, self.cur, value.clone());
        self.statics.insert(binding, value);
        binding
    }

    pub(super) fn lookup_static(&self, sym: u32) -> Option<Value> {
        let (binding, depth) = *self.visible.get(&sym)?;
        if let Some(value) = self.statics.get(&binding) {
            return Some(value.clone());
        }
        self.scopes[..=depth]
            .iter()
            .rev()
            .flat_map(|(_, undo)| undo.iter().rev())
            .filter(|(name, _)| *name == sym)
            .map(|(_, previous)| *previous)
            .take_while(Option::is_some)
            .find_map(|previous| self.statics.get(&previous?.0).cloned())
    }

    fn enter_scope(&mut self, scope: Cursor<'t>) {
        let label = scope.tag(self.tags.scoped).unwrap_or(scope.sym());
        self.scopes.push((label, Vec::new()));
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
            let depth = self.scopes.len() - 1;
            if self.visible.get(&name).is_some_and(|&(_, at)| at == depth) {
                continue;
            }
            if let Some(owner) = declaration.child_sym(C::Alias) {
                let owner = self.scopes[..depth]
                    .iter()
                    .rposition(|(label, _)| *label == owner);
                if let Some(at) = owner {
                    self.bind(name, self.bound_at(name, at).unwrap_or(unbound));
                }
                continue;
            }
            self.bind(name, key);
            self.ssa.write_variable(key, self.cur, Value::Opaque);
        }
    }

    pub(super) fn predeclare(&mut self, scope: Cursor<'t>) {
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

    pub(super) fn declare(&mut self, c: Cursor<'t>, name: u32) {
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
        (root != 0 && !self.statics.contains_key(&root)).then_some((root, member.sym()))
    }

    fn field_of(&self, binding: u32, field: u32) -> Option<u32> {
        if binding == 0 || self.statics.contains_key(&binding) {
            return None;
        }
        self.fields.get(&binding)?.get(&field).copied()
    }

    pub(super) fn field_slot(&self, node: Cursor<'_>) -> Option<u32> {
        let (root, field) = self.member_binding(node)?;
        self.fields.get(&root)?.get(&field).copied()
    }

    pub(super) fn bind_fields(&mut self, lhs: u32, value: Cursor<'t>) {
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
                && let Some(shared) = self.fields.get(&source).cloned()
            {
                self.fields.insert(lhs, shared);
                return;
            }
            self.fields.get(&source).cloned().unwrap_or_default()
        };
        let mut copies: SmallVec<[_; 4]> = smallvec![(lhs, sources)];
        let mut field_updates = SmallVec::<[_; 4]>::new();
        let mut value_updates = SmallVec::<[_; 8]>::new();
        let mut copied = FxHashMap::default();
        let mut reset = FxHashSet::default();
        let mut aliases = SmallVec::<[_; 4]>::new();
        while let Some((target, sources)) = copies.pop() {
            if let Err(killed) = self.run.check().and_then(|()| self.file.check()) {
                self.killed = Some(killed);
                return;
            }
            if sources.is_empty() && !self.fields.contains_key(&target) {
                continue;
            }
            let mut fields = self.fields.get(&target).cloned().unwrap_or_default();
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
                    if self.fields.contains_key(&target) && reset.insert(target) {
                        copies.push((target, FxHashMap::default()));
                    }
                } else if self.fields.contains_key(&source) || self.fields.contains_key(&target) {
                    match copied.entry(source) {
                        Entry::Occupied(first) => aliases.push((target, *first.get())),
                        Entry::Vacant(slot) => {
                            slot.insert(target);
                            let sources = self.fields.get(&source).cloned().unwrap_or_default();
                            copies.push((target, sources));
                        }
                    }
                }
            }
            field_updates.push((target, fields));
        }
        self.fields.extend(field_updates);
        for (target, first) in aliases {
            if target != first
                && let Some(shared) = self.fields.get(&first).cloned()
            {
                self.fields.insert(target, shared);
            }
        }
        for (target, value) in value_updates {
            self.ssa.write_variable(target, self.cur, value);
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
                    self.fields
                        .get(&binding)
                        .into_iter()
                        .flat_map(|f| f.values()),
                );
            }
        }
        false
    }

    pub(super) fn classify_tail(&mut self, tail: Cursor<'_>) -> Value {
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
        match self.ssa.read_variable_internal(self.binding(sym), block) {
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
        let first = self.loop_wildcards.partition_point(|&at| at < start);
        let last = self.loop_wildcards.partition_point(|&at| at < end);
        let imports: Vec<u32> = self.loop_wildcards.drain(first..last).collect();
        for at in imports {
            let Some(module) = self.inline_module(self.at(at)) else {
                continue;
            };
            let names = module
                .children()
                .filter(|d| d.is(C::Def))
                .filter_map(|d| d.child_sym(C::DefName));
            for name in names {
                let positions = self.assigned.entry(name).or_default();
                if let Err(slot) = positions.binary_search(&at) {
                    positions.insert(slot, at);
                }
            }
        }
    }

    fn name_block(&self, sym: u32) -> BlockId {
        let assigned = self.assigned.get(&sym).map_or(&[][..], Vec::as_slice);
        let inner = self.loops.partition_point(|&(_, (start, end))| {
            let first = assigned.partition_point(|&at| at < start);
            assigned.get(first).is_some_and(|&at| at < end)
        });
        self.loops
            .get(inner)
            .map_or(self.cur, |&(preheader, _)| preheader)
    }

    pub(super) fn read_phi(&mut self, sym: u32) -> Option<Value> {
        let value = self
            .ssa
            .read_variable_internal(self.binding(sym), self.name_block(sym));
        matches!(value, Value::Phi(_)).then_some(value)
    }

    pub(super) fn lookup(&mut self, sym: u32) -> Vec<Value> {
        for block in [self.name_block(sym), BlockId(0)] {
            let result = self.ssa.read_variable(self.binding(sym), block);
            if !result.is_empty() {
                return result;
            }
        }
        Vec::new()
    }
}
