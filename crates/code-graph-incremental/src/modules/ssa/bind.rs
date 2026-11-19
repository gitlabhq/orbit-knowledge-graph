use std::rc::Rc;

use super::{BindingKey, BlockId, Frame, Ssa, Value};
use crate::canonical::Canonical as C;
use crate::constants::PATH_SEP;
use crate::engine::{Cursor, Cx, Val};

fn child_def<'t>(module: Cursor<'t>, name: u32) -> Option<Cursor<'t>> {
    module
        .children()
        .find(|d| d.is(C::Def) && d.child_sym(C::DefName) == Some(name))
}

impl<'t> Cx<'_, 't, Ssa> {
    pub(super) fn key(&mut self, key: BindingKey) -> u32 {
        if key == BindingKey::Name(0) {
            return 0;
        }
        let keys = &mut self.state().keys;
        let next = keys.len() as u32 + 1;
        *keys.entry(key).or_insert(next)
    }

    pub(super) fn binding_key(&mut self, node: Cursor<'_>, name: u32) -> u32 {
        self.key(BindingKey::Declaration(node.index(), name))
    }

    pub(super) fn binding(&mut self, name: u32) -> u32 {
        match self.state().visible.get(&name) {
            Some(&(binding, _)) => binding,
            None => self.key(BindingKey::Name(name)),
        }
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

    pub(super) fn bind(&mut self, name: u32, binding: u32) {
        let s = self.write();
        let Some(depth) = s.scopes.len().checked_sub(1) else {
            return;
        };
        let previous = s.visible.insert(name, (binding, depth));
        if previous.is_none_or(|(key, at)| at != depth || s.statics.contains_key(&key)) {
            s.scopes[depth].1.push((name, previous));
        }
    }

    pub(super) fn exit_scope(&mut self) {
        let s = self.write();
        let undo = s.scopes.pop().map(|(_, undo)| undo).unwrap_or_default();
        for (name, previous) in undo.into_iter().rev() {
            match previous {
                Some(previous) => s.visible.insert(name, previous),
                None => s.visible.remove(&name),
            };
        }
    }

    pub(super) fn declare_binding(&mut self, node: Cursor<'_>, name: u32) -> u32 {
        let binding = match node.child_sym(C::Declaration) {
            Some(_) => self.binding(name),
            None => self.binding_key(node, name),
        };
        self.bind(name, binding);
        binding
    }

    pub(super) fn declare_static(&mut self, node: Cursor<'_>, name: u32, value: Value) -> u32 {
        let binding = self.declare_binding(node, name);
        let s = self.write();
        s.graph.write_variable(binding, s.cur, value.clone());
        s.statics.insert(binding, value);
        binding
    }

    pub(super) fn lookup_static(&mut self, sym: u32) -> Option<Value> {
        let s = self.state();
        let (binding, depth) = *s.visible.get(&sym)?;
        if let Some(value) = s.statics.get(&binding) {
            return Some(value.clone());
        }
        s.scopes[..=depth]
            .iter()
            .rev()
            .flat_map(|(_, undo)| undo.iter().rev())
            .filter(|(name, _)| *name == sym)
            .map(|(_, previous)| *previous)
            .take_while(Option::is_some)
            .find_map(|previous| s.statics.get(&previous?.0).cloned())
    }

    pub(super) fn scope(&mut self, scope: Cursor<'t>, block: Option<BlockId>) {
        let s = self.write();
        let wildcards = s.wildcards.len();
        s.frames.push(Frame::Scope { wildcards, block });
        self.enter_scope(scope);
    }

    fn enter_scope(&mut self, scope: Cursor<'t>) {
        let tags = self.tags();
        let label = scope.tag(tags.scoped).unwrap_or(scope.sym());
        self.write().scopes.push((label, Vec::new()));
        if scope.has_tag(tags.hoisted) {
            self.predeclare(scope);
        }
        if label == 0 {
            return;
        }
        let boundary = move |node: Cursor| {
            (node.is(C::Scope) && node.sym() == label) || node.has_tag(tags.scoped)
        };
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
            let s = self.state();
            let depth = s.scopes.len() - 1;
            if s.visible.get(&name).is_some_and(|&(_, at)| at == depth) {
                continue;
            }
            if let Some(owner) = declaration.child_sym(C::Alias) {
                let owner = s.scopes[..depth]
                    .iter()
                    .rposition(|(label, _)| *label == owner);
                if let Some(at) = owner {
                    let bound = self.bound_at(name, at).unwrap_or(unbound);
                    self.bind(name, bound);
                }
                continue;
            }
            self.bind(name, key);
            let s = self.write();
            s.graph.write_variable(key, s.cur, Value::Opaque);
        }
    }

    pub(super) fn predeclare(&mut self, scope: Cursor<'t>) {
        for d in scope
            .children()
            .filter(|d| d.is(C::Def) && d.child(C::Declaration).is_none())
        {
            if let Some(name) = d.child_sym(C::DefName) {
                self.register(d);
                self.declare(d, name);
            }
        }
    }

    pub(super) fn register(&mut self, def: Cursor<'_>) {
        let s = self.write();
        if s.registered.insert(def.index()) && def.child_sym(C::DefName).is_some() {
            s.declared.push(def.index());
            self.call1(crate::fn_id!("core", "define"), Val::Node(def.into()));
        }
    }

    fn declare(&mut self, c: Cursor<'t>, name: u32) {
        for sym in std::iter::once(name).chain(c.children_of(C::Alias).map(|a| a.sym())) {
            if sym == name && c.has(C::ImplBlock) && !self.lookup(name).is_empty() {
                continue;
            }
            self.declare_static(c, sym, Value::LocalDef(c.index()));
        }
    }

    pub(super) fn enter_def(&mut self, c: Cursor<'t>) {
        let Some(name) = c.child_sym(C::DefName) else {
            return;
        };
        self.register(c);
        self.declare(c, name);
        let s = self.write();
        let parent = s.cur;
        s.cur = s.graph.add_sealed_successor(parent);
        if c.has_tag(self.tags().scoped) {
            self.scope(c, Some(parent));
        }
    }

    pub(super) fn bind_import(&mut self, c: Cursor<'t>) {
        let module = self.inline_module(c);
        let type_only = self.tags().type_only;
        let shadow_locals = self.link_config().imports_shadow_locals;
        for n in c.names() {
            let sym = n.sym();
            let local = n
                .child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(sym);
            if c.is(C::ImportType) || n.has_tag(type_only) {
                let binding = self.declare_binding(n, local);
                let s = self.write();
                s.graph.write_variable(binding, s.cur, Value::Opaque);
                continue;
            }
            if let Some(module) = module
                && self.bind_from_def(module, n, local)
            {
                continue;
            }
            if !shadow_locals
                && self
                    .lookup(local)
                    .iter()
                    .any(|r| matches!(r, Value::LocalDef(_)))
            {
                continue;
            }
            if local == self.state().wildcard {
                Rc::make_mut(&mut self.write().wildcards).push(n.index());
            }
            let binding = self.declare_static(n, local, Value::ImportRef(n.index()));
            for alias in [C::Alias, C::SsaHint]
                .into_iter()
                .filter_map(|kind| n.child_sym(kind))
            {
                self.bind(alias, binding);
            }
        }
    }

    pub(super) fn inline_module(&mut self, import: Cursor<'t>) -> Option<Cursor<'t>> {
        if !self.link_config().inline_modules {
            return None;
        }
        let syms = &self.env().lang.syms;
        let path = syms.resolve(import.child_sym(C::SourcePath)?);
        let mut segments = path.split(PATH_SEP).filter(|s| !s.is_empty());
        let Some(first) = segments.next() else {
            return Some(self.tree().root());
        };
        let mut module = self
            .lookup(syms.lookup(first))
            .into_iter()
            .find_map(|r| match r {
                Value::LocalDef(d) => Some(self.at(d)),
                _ => None,
            })?;
        for segment in segments {
            module = child_def(module, syms.lookup(segment))?;
        }
        Some(module)
    }

    fn bind_from_def(&mut self, module: Cursor<'t>, name: Cursor<'t>, local: u32) -> bool {
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
        for &(bind_as, node) in &targets {
            self.register(self.at(node));
            self.declare_static(name, bind_as, Value::LocalDef(node));
        }
        !targets.is_empty()
    }

    pub(super) fn name_block(&mut self, sym: u32) -> BlockId {
        let s = self.state();
        let assigned = s.assigned.get(&sym).map_or(&[][..], Vec::as_slice);
        let inner = s.loops.partition_point(|&(_, (start, end))| {
            let first = assigned.partition_point(|&at| at < start);
            assigned.get(first).is_some_and(|&at| at < end)
        });
        s.loops
            .get(inner)
            .map_or(s.cur, |&(preheader, _)| preheader)
    }

    pub(super) fn read_value(&mut self, binding: u32) -> Value {
        let s = self.state();
        match s.graph.read_variable_internal(binding, s.cur) {
            Value::Undefined => Value::Opaque,
            value => value,
        }
    }

    pub(super) fn read_phi(&mut self, sym: u32) -> Option<Value> {
        let binding = self.binding(sym);
        let block = self.name_block(sym);
        let value = self.state().graph.read_variable_internal(binding, block);
        matches!(value, Value::Phi(_)).then_some(value)
    }

    pub(super) fn lookup(&mut self, sym: u32) -> Vec<Value> {
        let binding = self.binding(sym);
        for block in [self.name_block(sym), BlockId(0)] {
            let result = self.state().graph.read_variable(binding, block);
            if !result.is_empty() {
                return result;
            }
        }
        Vec::new()
    }
}
