use std::collections::hash_map::Entry;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::{SmallVec, smallvec};

use super::{Key, Ssa, Value};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Store, Val};

type Fields = FxHashMap<u32, u32>;

fn member_of<S: Store>(node: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    Some(node)
        .filter(|n| n.is(C::Member))
        .or_else(|| node.child(C::Member))
        .or_else(|| node.child(C::Ivar))
}

fn object_of<S: Store>(member: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    member.child(C::Object).filter(|o| !o.has(C::Qualifier))
}

fn tail_sym<S: Store>(node: Cursor<'_, S>) -> u32 {
    match super::tail_child(node) {
        Some(c) if c.size() > 1 => tail_sym(c),
        Some(c) => c.sym(),
        None => node.sym(),
    }
}

impl<'t, S: Store> Cx<'_, 't, Ssa, S> {
    fn member_binding(&mut self, node: Cursor<'_, S>) -> Option<(u32, u32)> {
        let member = member_of(node)?;
        let root = self.object_binding(object_of(member)?);
        let tracked = root != 0 && !self.state().statics.contains_key(&root);
        tracked.then_some((root, member.sym()))
    }

    /// The binding a chain object stands for, or 0: its name's binding, or
    /// the field its inner member reads off the object below it.
    fn object_binding(&mut self, object: Cursor<'_, S>) -> u32 {
        let mut path = SmallVec::<[(u32, u32); 4]>::new();
        let mut at = object;
        let mut root = loop {
            if at.sym() != 0 {
                break self.binding(at.sym());
            }
            if let Some(&known) = self.state().chains.get(&at.index()) {
                break known;
            }
            let Some((inner, below)) = member_of(at).and_then(|m| Some((m, object_of(m)?))) else {
                break 0;
            };
            path.push((at.index(), inner.sym()));
            at = below;
        };
        while let Some((node, field)) = path.pop() {
            let s = self.state();
            root = match root == 0 || s.statics.contains_key(&root) {
                true => 0,
                false => s
                    .fields
                    .get(&root)
                    .and_then(|f| f.get(&field))
                    .copied()
                    .unwrap_or(0),
            };
            s.chains.insert(node, root);
        }
        root
    }

    pub(super) fn field_slot(&mut self, node: Cursor<'_, S>) -> Option<u32> {
        let (root, field) = self.member_binding(node)?;
        self.state().fields.get(&root)?.get(&field).copied()
    }

    pub(super) fn target(&mut self, c: Cursor<'t, S>) -> Option<u32> {
        let field = self.member_binding(c).map(|(root, field)| {
            let key = self.key(Key::Field(root, field));
            *self
                .state()
                .fields
                .entry(root)
                .or_default()
                .entry(field)
                .or_insert(key)
        });
        let declares = c.has(C::Declaration) && c.child_sym(C::Declaration).is_none();
        let lhs = match (declares || c.has(C::Qualifier), field) {
            (true, _) => self.key(Key::Declaration(c.index(), c.sym())),
            (false, Some(field)) => field,
            (false, None) => self.binding(c.sym()),
        };
        (lhs != 0 && !(c.has(C::Ivar) && field.is_none())).then_some(lhs)
    }

    fn value(&self, val: Val) -> Option<Value> {
        match val {
            Val::Type(loc) => Some(Value::Type(loc.node)),
            Val::Node(loc) => Some(match self.jump(loc) {
                n if n.is(C::Def) => Value::LocalDef(loc.node),
                n if n.is(C::Name) => Value::ImportRef(loc.node),
                _ => Value::Call(loc.node),
            }),
            Val::Opaque => Some(Value::Opaque),
            _ => None,
        }
    }

    fn held(&mut self, node: Cursor<'t, S>) -> Value {
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
            Value::LocalDef(def) if super::is_class(self.at(def)) => Value::Type(def),
            value => value,
        }
    }

    pub(super) fn bind(
        &mut self,
        target: Cursor<'t, S>,
        values: &[Val],
        from: Option<Cursor<'t, S>>,
    ) {
        let Some(lhs) = self.target(target) else {
            return;
        };
        let record = from.is_some_and(|f| f.is(C::Obj) || f.is(C::Args));
        let written: SmallVec<[Value; 2]> = match (values.is_empty(), from) {
            (false, _) => values.iter().filter_map(|&v| self.value(v)).collect(),
            (true, Some(from)) if !record => smallvec![self.held(from)],
            _ => SmallVec::new(),
        };
        let s = self.write();
        if !written.is_empty() && s.graph.has_variable_in_block(lhs, s.cur) {
            s.cur = s.graph.add_sealed_successor(s.cur);
        }
        match written.as_slice() {
            [] => {}
            [value] => s.graph.write_variable(lhs, s.cur, value.clone()),
            _ => {
                let pre = s.cur;
                let arms: SmallVec<[_; 4]> = written
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
        if target.has(C::Qualifier)
            && let [value] = values
            && let Some(value) = self.value(*value)
        {
            self.write().statics.insert(lhs, value);
        }
        if let Some(from) = from {
            self.bind_fields(lhs, from);
        }
        let declares = target.has(C::Declaration) && target.child_sym(C::Declaration).is_none();
        if declares || target.has(C::Qualifier) {
            self.bind_name(target.sym(), lhs);
        }
    }

    fn bind_fields(&mut self, lhs: u32, from: Cursor<'t, S>) {
        let sources: Fields = if from.is(C::Obj) || from.is(C::Args) {
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
        let mut copies: SmallVec<[(u32, Fields); 4]> = smallvec![(lhs, sources)];
        let mut field_updates = SmallVec::<[(u32, Fields); 4]>::new();
        let mut value_updates = SmallVec::<[(u32, Value); 8]>::new();
        let mut copied = FxHashMap::default();
        let mut reset = FxHashSet::default();
        let mut aliases = SmallVec::<[(u32, u32); 4]>::new();
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
            for &field in sources.keys() {
                if let Entry::Vacant(slot) = fields.entry(field) {
                    slot.insert(self.key(Key::Field(target, field)));
                }
            }
            for (&field, &slot) in &fields {
                let source = sources.get(&field).copied().unwrap_or(0);
                if slot == source {
                    continue;
                }
                value_updates.push((slot, self.read_value(source)));
                let s = self.state();
                if source == 0 {
                    if s.fields.contains_key(&slot) && reset.insert(slot) {
                        copies.push((slot, Fields::default()));
                    }
                } else if s.fields.contains_key(&source) || s.fields.contains_key(&slot) {
                    match copied.entry(source) {
                        Entry::Occupied(first) => aliases.push((slot, *first.get())),
                        Entry::Vacant(entry) => {
                            entry.insert(slot);
                            copies.push((slot, s.fields.get(&source).cloned().unwrap_or_default()));
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
}
