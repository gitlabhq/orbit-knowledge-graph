use std::rc::Rc;

use super::{BlockId, Frame, Key, Ssa, Value};
use crate::canonical::Canonical as C;
use crate::constants::PATH_SEP;
use crate::engine::{Cursor, Cx, Store, Vals};

impl<'t, S: Store> Cx<'_, 't, Ssa, S> {
    pub(super) fn key(&mut self, key: Key) -> u32 {
        if key == Key::Name(0) {
            return 0;
        }
        let keys = &mut self.state().keys;
        let next = keys.len() as u32 + 1;
        *keys.entry(key).or_insert(next)
    }

    pub(super) fn binding(&mut self, name: u32) -> u32 {
        match self.state().visible.get(&name) {
            Some(&(binding, _)) => binding,
            None => self.key(Key::Name(name)),
        }
    }

    pub(super) fn bind_name(&mut self, name: u32, binding: u32) {
        let s = self.write();
        let depth = s.scopes.len() - 1;
        let previous = s.visible.insert(name, (binding, depth));
        if previous.is_none_or(|(key, at)| at != depth || s.statics.contains_key(&key)) {
            s.scopes[depth].1.push((name, previous));
        }
    }

    fn declare(&mut self, node: Cursor<'_, S>, name: u32) -> u32 {
        let binding = match node.child_sym(C::Declaration) {
            Some(_) => self.binding(name),
            None => self.key(Key::Declaration(node.index(), name)),
        };
        self.bind_name(name, binding);
        binding
    }

    pub(super) fn define(&mut self, node: Cursor<'_, S>, name: u32, value: Value) {
        let binding = self.declare(node, name);
        let s = self.write();
        s.graph.write_variable(binding, s.cur, value.clone());
        s.statics.insert(binding, value);
    }

    fn lookup_static(&mut self, name: u32) -> Option<Value> {
        let s = self.state();
        let (binding, depth) = *s.visible.get(&name)?;
        if let Some(value) = s.statics.get(&binding) {
            return Some(value.clone());
        }
        s.scopes[..=depth]
            .iter()
            .rev()
            .flat_map(|(_, undo)| undo.iter().rev())
            .filter(|(n, _)| *n == name)
            .map(|(_, previous)| *previous)
            .take_while(Option::is_some)
            .find_map(|previous| s.statics.get(&previous?.0).cloned())
    }

    fn bound_at(&mut self, name: u32, at: usize) -> Option<u32> {
        let s = self.state();
        let hit = s.scopes[at + 1..]
            .iter()
            .flat_map(|(_, undo)| undo)
            .find_map(|&(n, previous)| (n == name).then_some(previous));
        hit.unwrap_or_else(|| s.visible.get(&name).copied())
            .filter(|&(_, depth)| depth == at)
            .map(|(binding, _)| binding)
    }

    pub(super) fn scope(
        &mut self,
        scope: Cursor<'t, S>,
        label: Option<u32>,
        hoisted: bool,
        resume: Option<BlockId>,
    ) {
        let label = label.unwrap_or(scope.sym());
        let s = self.write();
        let wildcards = s.wildcards.len();
        s.frames.push(Frame::Scope { wildcards, resume });
        s.scopes.push((label, Vec::new()));
        if hoisted {
            self.predeclare(scope);
        }
        if label == 0 {
            return;
        }
        let boundary = move |n: Cursor<'_, S>| {
            n.is(C::Def) || n.is(C::Lambda) || (n.is(C::Scope) && n.sym() == label)
        };
        let mut declarations: Vec<_> = scope
            .descendants_pruned(boundary)
            .filter(|n| n.child_sym(C::Declaration) == Some(label))
            .collect();
        declarations.sort_by_key(|n| !n.has(C::Alias));
        for declaration in declarations {
            let name = declaration
                .child_sym(C::DefName)
                .unwrap_or(declaration.sym());
            let key = self.key(Key::Declaration(declaration.index(), name));
            let unbound = self.key(Key::Name(name));
            let s = self.state();
            let depth = s.scopes.len() - 1;
            if s.visible.get(&name).is_some_and(|&(_, at)| at == depth) {
                continue;
            }
            if let Some(owner) = declaration.child_sym(C::Alias) {
                let at = s.scopes[..depth]
                    .iter()
                    .rposition(|(l, _)| *l == owner)
                    .unwrap_or(0);
                let bound = self.bound_at(name, at).unwrap_or(unbound);
                self.bind_name(name, bound);
                continue;
            }
            self.bind_name(name, key);
            let s = self.write();
            s.graph.write_variable(key, s.cur, Value::Opaque);
        }
    }

    pub(super) fn predeclare(&mut self, scope: Cursor<'t, S>) {
        for def in scope
            .children()
            .filter(|d| d.is(C::Def) && !d.has(C::Declaration))
        {
            self.declare_def(def);
        }
    }

    fn declare_def(&mut self, def: Cursor<'t, S>) {
        let Some(name) = def.child_sym(C::DefName) else {
            return;
        };
        let aliases = def.children_of(C::Alias).map(|a| a.sym());
        for sym in std::iter::once(name).chain(aliases) {
            let merged = sym == name && def.has(C::ImplBlock) && !self.lookup(name).is_empty();
            if !merged {
                self.define(def, sym, Value::LocalDef(def.index()));
            }
        }
    }

    pub(super) fn enter_def(&mut self, def: Cursor<'t, S>, scoped: Option<Option<u32>>) {
        if def.child_sym(C::DefName).is_none() {
            return;
        }
        self.declare_def(def);
        let s = self.write();
        let parent = s.cur;
        s.cur = s.graph.add_sealed_successor(parent);
        if let Some(label) = scoped {
            self.scope(def, label, false, Some(parent));
        }
    }

    pub(super) fn bind_import(
        &mut self,
        import: Cursor<'t, S>,
        shadows: bool,
        inline: bool,
        type_only: u32,
    ) {
        if !self.write().bound.insert(import.index()) {
            return;
        }
        let module = inline.then(|| self.inline_module(import)).flatten();
        for name in import.names() {
            let local = name
                .child_sym(C::Alias)
                .or(name.child_sym(C::SsaHint))
                .unwrap_or(name.sym());
            if import.is(C::ImportType) || name.has_tag(type_only) {
                let binding = self.declare(name, local);
                let s = self.write();
                s.graph.write_variable(binding, s.cur, Value::Opaque);
                continue;
            }
            if module.is_some_and(|m| self.bind_from_def(m, name, local)) {
                continue;
            }
            if !shadows
                && self
                    .lookup(local)
                    .iter()
                    .any(|v| matches!(v, Value::LocalDef(_)))
            {
                continue;
            }
            if local == self.state().wildcard {
                Rc::make_mut(&mut self.write().wildcards).push(name.index());
            }
            self.define(name, local, Value::ImportRef(name.index()));
            let binding = self.binding(local);
            for alias in [C::Alias, C::SsaHint]
                .into_iter()
                .filter_map(|k| name.child_sym(k))
            {
                self.bind_name(alias, binding);
            }
        }
    }

    fn inline_module(&mut self, import: Cursor<'t, S>) -> Option<Cursor<'t, S>> {
        let syms = &self.lang().syms;
        let path = import
            .child_sym(C::SourcePath)
            .map_or("", |p| syms.resolve(p));
        let mut segments = path.split(PATH_SEP).filter(|s| !s.is_empty());
        let Some(first) = segments.next() else {
            return Some(self.tree().root());
        };
        let mut module = self
            .lookup(syms.lookup(first))
            .into_iter()
            .find_map(|v| match v {
                Value::LocalDef(d) => Some(self.at(d)),
                _ => None,
            })?;
        for segment in segments {
            module = child_def(module, syms.lookup(segment))?;
        }
        Some(module)
    }

    fn bind_from_def(&mut self, module: Cursor<'t, S>, name: Cursor<'t, S>, local: u32) -> bool {
        let targets: Vec<(u32, u32)> = match name.sym() == self.state().wildcard {
            true => module
                .children()
                .filter(|d| d.is(C::Def))
                .filter_map(|d| Some((d.child_sym(C::DefName)?, d.index())))
                .collect(),
            false => child_def(module, name.sym())
                .map(|d| (local, d.index()))
                .into_iter()
                .collect(),
        };
        for &(bind_as, def) in &targets {
            self.define(name, bind_as, Value::LocalDef(def));
            let s = self.state();
            if !s.loops.is_empty() {
                let positions = s.assigned.entry(bind_as).or_default();
                if let Err(at) = positions.binary_search(&name.index()) {
                    positions.insert(at, name.index());
                }
            }
        }
        !targets.is_empty()
    }

    // The preheader of the innermost loop that never assigns the name keeps reads off unfinished phis.
    pub(super) fn name_block(&mut self, name: u32) -> BlockId {
        let s = self.state();
        let assigned = s.assigned.get(&name).map_or(&[][..], Vec::as_slice);
        let inner = s.loops.partition_point(|&(_, start, end)| {
            let first = assigned.partition_point(|&at| at < start);
            assigned.get(first).is_some_and(|&at| at < end)
        });
        s.loops
            .get(inner)
            .map_or(s.cur, |&(preheader, ..)| preheader)
    }

    pub(super) fn read_value(&mut self, binding: u32) -> Value {
        let s = self.state();
        match s.graph.read_variable_internal(binding, s.cur) {
            Value::Undefined => Value::Opaque,
            value => value,
        }
    }

    pub(super) fn read_phi(&mut self, name: u32) -> Option<Value> {
        let binding = self.binding(name);
        let block = self.name_block(name);
        let value = self.state().graph.read_variable_internal(binding, block);
        matches!(value, Value::Phi(_)).then_some(value)
    }

    pub(super) fn lookup(&mut self, name: u32) -> Vec<Value> {
        let binding = self.binding(name);
        for block in [self.name_block(name), BlockId(0)] {
            let found = self.state().graph.read_variable(binding, block);
            if !found.is_empty() {
                return found;
            }
        }
        Vec::new()
    }

    pub(super) fn read(&mut self, c: Cursor<'t, S>) -> Vals {
        if c.has(C::Qualifier) {
            let found = self.lookup_static(c.sym());
            return self.vals(found.as_slice());
        }
        if let Some(slot) = self.field_slot(c) {
            let value = self.read_value(slot);
            let found = self.state().graph.resolve_value(&value);
            return self.vals(&found);
        }
        if c.is(C::Member) {
            return Vals::new();
        }
        let found = self.lookup(c.sym());
        self.vals(&found)
    }
}

fn child_def<S: Store>(module: Cursor<'_, S>, name: u32) -> Option<Cursor<'_, S>> {
    module
        .children()
        .find(|d| d.is(C::Def) && d.child_sym(C::DefName) == Some(name))
}
