use std::collections::hash_map::Entry;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::{SmallVec, smallvec};

use super::{Deferred, Key, Ssa, Value, to_vals};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Val, one};
use crate::tree::{Edge, EdgeKind, is_name_text};

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

fn tail_sym(node: Cursor<'_>) -> u32 {
    match node.last_named().filter(|c| node.stands_for(*c)) {
        Some(c) if c.size() > 1 => tail_sym(c),
        Some(c) => c.sym(),
        None => node.sym(),
    }
}

impl<'t> Cx<'_, 't, Ssa> {
    fn member_binding(&mut self, node: Cursor<'_>) -> Option<(u32, u32)> {
        let member = member_of(node)?;
        let mut object = object_of(member)?;
        let mut path = Vec::new();
        while object.sym() == 0 {
            let inner = member_of(object)?;
            path.push(inner.sym());
            object = object_of(inner)?;
        }
        let mut root = self.binding(object.sym());
        for field in path.into_iter().rev() {
            let s = self.state();
            if root == 0 || s.statics.contains_key(&root) {
                return None;
            }
            root = *s.fields.get(&root)?.get(&field)?;
        }
        (root != 0 && !self.state().statics.contains_key(&root)).then_some((root, member.sym()))
    }

    pub(super) fn field_slot(&mut self, node: Cursor<'_>) -> Option<u32> {
        let (root, field) = self.member_binding(node)?;
        self.state().fields.get(&root)?.get(&field).copied()
    }

    /// The variable a binding writes: a new one for a declaration, a field
    /// variable for a member target, else the variable its name means.
    pub(super) fn target(&mut self, c: Cursor<'t>) -> Option<u32> {
        let field = match self.member_binding(c) {
            Some((root, field)) => {
                let key = self.key(Key::Field(root, field));
                let fields = self.state().fields.entry(root).or_default();
                Some(*fields.entry(field).or_insert(key))
            }
            None => None,
        };
        let lhs = match (
            c.has(C::Declaration) && c.child_sym(C::Declaration).is_none(),
            field,
        ) {
            (true, _) => self.key(Key::Declaration(c.index(), c.sym())),
            (false, Some(field)) => field,
            (false, None) => self.binding(c.sym()),
        };
        (lhs != 0 && !(c.has(C::Ivar) && field.is_none())).then_some(lhs)
    }

    fn value(&self, val: Val) -> Option<Value> {
        match val {
            Val::Type(loc) => Some(Value::Type(loc.node)),
            Val::Node(loc) => Some(match self.at(loc.node).kind() {
                k if k == C::Def => Value::LocalDef(loc.node),
                k if k == C::Name => Value::ImportRef(loc.node),
                _ => Value::Call(loc.node),
            }),
            Val::Opaque => Some(Value::Opaque),
            _ => None,
        }
    }

    /// What `node` holds, read where its name is safe to read.
    fn held(&mut self, node: Cursor<'t>) -> Value {
        if let Some(slot) = self.field_slot(node) {
            return self.read_value(slot);
        }
        let sym = tail_sym(node);
        if sym == 0 {
            return Value::Opaque;
        }
        let block = self.name_block(sym);
        let binding = self.binding(sym);
        match self.state().graph.read_variable_internal(binding, block) {
            Value::Undefined => Value::Opaque,
            Value::LocalDef(def) if self.at(def).is_class() => Value::Type(def),
            value => value,
        }
    }

    /// Writes `values` to the variable `target` names, or what `from` holds
    /// when `values` is empty, then gives it the fields of `from`.
    pub(super) fn bind(&mut self, target: Cursor<'t>, values: &[Val], from: Option<Cursor<'t>>) {
        let Some(lhs) = self.target(target) else {
            return;
        };
        let record = from.filter(|f| f.is(C::Obj) || f.is(C::Args));
        let written: SmallVec<[Value; 2]> = match (values.is_empty(), from) {
            (false, _) => values.iter().filter_map(|&v| self.value(v)).collect(),
            (true, Some(from)) if record.is_none() => smallvec![self.held(from)],
            _ => SmallVec::new(),
        };
        if !written.is_empty() {
            let s = self.write();
            if s.graph.has_variable_in_block(lhs, s.cur) {
                s.cur = s.graph.add_sealed_successor(s.cur);
            }
            match written.as_slice() {
                [value] => s.graph.write_variable(lhs, s.cur, value.clone()),
                _ => {
                    let pre = s.cur;
                    let arms: Vec<_> = written
                        .into_iter()
                        .map(|value| {
                            let arm = s.graph.add_sealed_successor(pre);
                            s.graph.write_variable(lhs, arm, value);
                            arm
                        })
                        .collect();
                    s.cur = s.graph.add_sealed_join(arms);
                }
            }
        }
        if let Some(from) = from {
            self.bind_fields(lhs, from);
        }
    }

    fn bind_fields(&mut self, lhs: u32, from: Cursor<'t>) {
        let sources: FxHashMap<_, _> = if from.is(C::Obj) || from.is(C::Args) {
            from.children_of(C::ConfigField)
                .map(|field| {
                    let source = field
                        .child_sym(C::Binding)
                        .or_else(|| field.child_sym(C::Rhs));
                    (field.sym(), self.binding(source.unwrap_or(0)))
                })
                .collect()
        } else {
            let source = match self.field_slot(from) {
                Some(slot) => slot,
                None if from.children().next().is_none() => self.binding(from.sym()),
                None => 0,
            };
            if source != lhs
                && self.reaches(source, lhs)
                && let Some(shared) = self.state().fields.get(&source).cloned()
            {
                self.write().fields.insert(lhs, shared);
                return;
            }
            self.state()
                .fields
                .get(&source)
                .cloned()
                .unwrap_or_default()
        };
        let mut copies: SmallVec<[_; 4]> = smallvec![(lhs, sources)];
        let mut field_updates = SmallVec::<[_; 4]>::new();
        let mut value_updates = SmallVec::<[_; 8]>::new();
        let mut copied = FxHashMap::default();
        let mut reset = FxHashSet::default();
        let mut aliases = SmallVec::<[_; 4]>::new();
        while let Some((target, sources)) = copies.pop() {
            if let Err(killed) = self.check() {
                self.kill(killed);
                return;
            }
            if sources.is_empty() && !self.state().fields.contains_key(&target) {
                continue;
            }
            let mut fields = self
                .state()
                .fields
                .get(&target)
                .cloned()
                .unwrap_or_default();
            for field in sources.keys() {
                if !fields.contains_key(field) {
                    let key = self.key(Key::Field(target, *field));
                    fields.insert(*field, key);
                }
            }
            for (&field, &target) in &fields {
                let source = sources.get(&field).copied().unwrap_or(0);
                if target == source {
                    continue;
                }
                value_updates.push((target, self.read_value(source)));
                let s = self.state();
                if source == 0 {
                    if s.fields.contains_key(&target) && reset.insert(target) {
                        copies.push((target, FxHashMap::default()));
                    }
                } else if s.fields.contains_key(&source) || s.fields.contains_key(&target) {
                    match copied.entry(source) {
                        Entry::Occupied(first) => aliases.push((target, *first.get())),
                        Entry::Vacant(slot) => {
                            slot.insert(target);
                            let sources = s.fields.get(&source).cloned().unwrap_or_default();
                            copies.push((target, sources));
                        }
                    }
                }
            }
            field_updates.push((target, fields));
        }
        let s = self.write();
        s.fields.extend(field_updates);
        for (target, first) in aliases {
            if target != first
                && let Some(shared) = s.fields.get(&first).cloned()
            {
                s.fields.insert(target, shared);
            }
        }
        for (target, value) in value_updates {
            s.graph.write_variable(target, s.cur, value);
        }
    }

    fn reaches(&mut self, from: u32, to: u32) -> bool {
        let fields = &self.state().fields;
        let mut seen = FxHashSet::default();
        let mut stack = vec![from];
        while let Some(binding) = stack.pop() {
            if binding == to {
                return true;
            }
            if seen.insert(binding) {
                stack.extend(fields.get(&binding).into_iter().flat_map(|f| f.values()));
            }
        }
        false
    }

    pub(super) fn pending_value(&mut self, callee: Cursor<'t>) -> Option<Value> {
        let implicit = callee
            .parent()
            .is_some_and(|c| c.has_tag(self.tags().implicit_self));
        match self.field_slot(callee) {
            Some(slot) => Some(self.read_value(slot)),
            None => callee
                .sym_opt()
                .filter(|_| !implicit)
                .and_then(|sym| self.read_phi(sym)),
        }
    }

    /// Defers a call whose callee, or whose receiver inside a loop, holds a
    /// phi that only the end of the loop completes.
    pub(super) fn defer(&mut self, c: Cursor<'t>) {
        let from = self.enclosing();
        if let Some(callee) = c.child(C::Callee)
            && let Some(value) = self.pending_value(callee)
        {
            let fallback = (callee.sym_opt().is_some()
                && !callee.has(C::Member)
                && !callee.has(C::Ivar)
                && is_name_text(self.env().lang.syms.resolve(callee.sym())))
            .then(|| Rc::clone(&self.state().wildcards));
            let deferred = Deferred::Callee(fallback);
            self.write()
                .pending
                .push((value, from, c.index(), deferred));
            return;
        }
        let Some(obj) = c.member().and_then(|m| m.child(C::Object)) else {
            return;
        };
        let leaf = !self.state().loops.is_empty() && obj.children().next().is_none();
        if let Some(phi) = leaf.then(|| self.read_phi(obj.sym())).flatten() {
            let seen = self.lookup(obj.sym());
            let deferred = Deferred::Receiver(obj.index(), seen);
            self.write().pending.push((phi, from, c.index(), deferred));
        }
    }

    fn refs(&mut self, target: Val, edges: &mut Vec<(EdgeKind, Loc)>) {
        for (kind, f) in [
            (EdgeKind::Calls, crate::fn_id!("core", "callable")),
            (EdgeKind::Imports, crate::fn_id!("core", "imported")),
            (EdgeKind::TypeFlow, crate::fn_id!("core", "produced")),
        ] {
            let found = self.call(f, &[one(target)]);
            edges.extend(found.iter().filter_map(|v| v.loc()).map(|loc| (kind, loc)));
        }
    }

    pub(super) fn resolve_pending(&mut self) {
        let pending = std::mem::take(&mut self.write().pending);
        for (value, from, site, deferred) in pending {
            if let Err(killed) = self.check() {
                self.kill(killed);
                return;
            }
            let mut targets = self.write().graph.resolve_value(&value);
            let mut edges = Vec::new();
            match deferred {
                Deferred::Callee(fallback) => {
                    if targets.is_empty() {
                        let wildcards = fallback.iter().flat_map(|w| w.iter());
                        targets.extend(wildcards.map(|&w| Value::ImportRef(w)));
                    }
                    for target in to_vals(&targets) {
                        self.refs(target, &mut edges);
                    }
                }
                Deferred::Receiver(object, seen) => {
                    targets.retain(|value| !seen.contains(value));
                    let Some(member) = self.at(object).parent() else {
                        continue;
                    };
                    let member = one(Val::Node(member.into()));
                    for target in to_vals(&targets) {
                        let args = [one(target), member.clone()];
                        let calls = self.call(crate::fn_id!("core", "member"), &args);
                        let calls = calls.iter().filter_map(|v| v.loc());
                        edges.extend(calls.map(|l| (EdgeKind::Calls, l)));
                        for unmatched in self.call(crate::fn_id!("core", "unmatched"), &args) {
                            self.refs(unmatched, &mut edges);
                        }
                    }
                }
            }
            for (kind, to) in edges {
                self.emit(Edge {
                    site: Some(site),
                    ..Edge::local(from, to.node, kind)
                });
            }
        }
    }
}
