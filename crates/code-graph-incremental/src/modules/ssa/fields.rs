use std::collections::hash_map::Entry;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::{SmallVec, smallvec};

use super::{BindingKey, Ssa, Value};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Val};

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
    pub(super) fn member_binding(&mut self, node: Cursor<'_>) -> Option<(u32, u32)> {
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
            root = self.field_of(root, field)?;
        }
        (root != 0 && !self.state().statics.contains_key(&root)).then_some((root, member.sym()))
    }

    fn field_of(&mut self, binding: u32, field: u32) -> Option<u32> {
        let s = self.state();
        if binding == 0 || s.statics.contains_key(&binding) {
            return None;
        }
        s.fields.get(&binding)?.get(&field).copied()
    }

    pub(super) fn field_slot(&mut self, node: Cursor<'_>) -> Option<u32> {
        let (root, field) = self.member_binding(node)?;
        self.state().fields.get(&root)?.get(&field).copied()
    }

    fn is_class_ref(&mut self, c: Cursor<'t>) -> bool {
        let found = self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()));
        found
            .iter()
            .filter_map(|v| v.loc())
            .any(|loc| self.jump(loc).is(C::Def) && self.jump(loc).is_class())
    }

    pub(super) fn bind_fields(&mut self, lhs: u32, value: Cursor<'t>) {
        let record = match value
            .is(C::Obj)
            .then_some(value)
            .or_else(|| value.child(C::Obj))
        {
            Some(record) => Some(record),
            None => value.child(C::Args).filter(|_| {
                value
                    .child(C::Callee)
                    .is_some_and(|callee| self.is_class_ref(callee))
            }),
        };
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
            let source = match self.field_slot(value) {
                Some(slot) => slot,
                None if value.children().next().is_none() => self.binding(value.sym()),
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
                    let key = self.key(BindingKey::Field(target, *field));
                    fields.insert(*field, key);
                }
            }
            for (&field, &target) in &fields {
                let source = sources.get(&field).copied().unwrap_or(0);
                if target == source {
                    continue;
                }
                let value = self.read_value(source);
                value_updates.push((target, value));
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

    pub(super) fn classify_tail(&mut self, tail: Cursor<'t>) -> Value {
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
            let producer = self.call1(crate::fn_id!("core", "ivar"), Val::Node(tail.into()));
            return producer
                .first()
                .and_then(|v| v.loc())
                .map_or(Value::Opaque, |loc| Value::Call(loc.node));
        }
        let sym = tail_sym(tail);
        if sym == 0 {
            return Value::Opaque;
        }
        let block = self.name_block(sym);
        let binding = self.binding(sym);
        match self.state().graph.read_variable_internal(binding, block) {
            Value::Undefined => Value::Opaque,
            Value::LocalDef(node) if self.at(node).is_class() => Value::Type(node),
            value => value,
        }
    }
}
