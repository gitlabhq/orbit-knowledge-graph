use std::rc::Rc;

use super::{Deferred, to_vals};
use crate::canonical::Canonical as C;
use crate::constants::PATH_SEP;
use crate::engine::{Cursor, Cx, Loc};
use crate::modules::core::refs;
use crate::modules::ssa::Value;
use crate::resolve::CLASS_LIKE;
use crate::sentinel::Killed;
use crate::tree::{CallResolution, Edge, EdgeKind, Role, is_name_text};

fn is_name(value: &Value) -> bool {
    matches!(
        value,
        Value::LocalDef(_) | Value::ImportRef(_) | Value::Type(_)
    )
}

fn child_def<'t>(module: Cursor<'t>, name: u32) -> Option<Cursor<'t>> {
    module
        .children()
        .find(|d| d.is(C::Def) && d.child_sym(C::DefName) == Some(name))
}

impl<'t> Cx<'t> {
    pub(crate) fn handle_inline_imports(&mut self, c: Cursor<'t>) {
        for import in c.children().filter(|i| i.is(C::Import)) {
            self.handle_import(import);
        }
    }

    pub(crate) fn handle_import(&mut self, c: Cursor<'t>) {
        let module = self.inline_module(c);
        for n in c.names() {
            let sym = n.sym();
            let local = n
                .child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(sym);
            if c.is(C::ImportType) || n.has_tag(self.tags.type_only) {
                let binding = self.declare_binding(n, local);
                self.ssa
                    .graph
                    .write_variable(binding, self.ssa.cur, Value::Opaque);
                continue;
            }
            if let Some(module) = module
                && self.bind_from_def(module, n, local)
            {
                continue;
            }
            if !self.link_config().imports_shadow_locals
                && self
                    .lookup(local)
                    .iter()
                    .any(|r| matches!(r, Value::LocalDef(_)))
            {
                continue;
            }
            if local == self.ssa.wildcard {
                Rc::make_mut(&mut self.ssa.wildcards).push(n.index());
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

    pub(crate) fn inline_module(&mut self, import: Cursor<'t>) -> Option<Cursor<'t>> {
        if !self.link_config().inline_modules {
            return None;
        }
        let path = self.env.lang.syms.resolve(import.child_sym(C::SourcePath)?);
        let mut segments = path.split(PATH_SEP).filter(|s| !s.is_empty());
        let Some(first) = segments.next() else {
            return Some(self.tree().root());
        };
        let mut module = self
            .lookup(self.env.lang.syms.lookup(first))
            .into_iter()
            .find_map(|r| match r {
                Value::LocalDef(d) => Some(self.at(d)),
                _ => None,
            })?;
        for segment in segments {
            module = child_def(module, self.env.lang.syms.lookup(segment))?;
        }
        Some(module)
    }

    fn bind_from_def(&mut self, module: Cursor<'t>, name: Cursor<'t>, local: u32) -> bool {
        let targets: Vec<(u32, u32)> = match name.sym() == self.ssa.wildcard {
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
            self.register_def(self.at(node));
            self.declare_static(name, bind_as, Value::LocalDef(node));
        }
        !targets.is_empty()
    }

    pub(crate) fn handle_def(&mut self, c: Cursor<'t>) {
        let Some(name) = c.child_sym(C::DefName) else {
            return;
        };
        let idx = c.index();
        self.register_def(c);
        for supertype in c.children_of(C::SuperType) {
            self.handle_inline_imports(supertype);
        }
        let supertypes: Vec<Cursor<'t>> = c.children().filter(|s| s.is(C::SuperType)).collect();
        let mut supers = Vec::new();
        for supertype in supertypes {
            for r in self.lookup_chain(supertype) {
                if let Value::LocalDef(d) = r {
                    supers.push(d);
                }
            }
        }
        self.ssa.supertypes.insert(idx, supers);
        self.declare(c, name);
        let parent_block = self.ssa.cur;
        self.ssa.cur = self.ssa.graph.add_sealed_successor(parent_block);
        if c.has_tag(self.tags.scoped) {
            self.scope(c, Some(parent_block));
        }
    }

    pub(crate) fn register_def(&mut self, node: Cursor<'_>) {
        if self.ssa.registered.insert(node.index())
            && let Some(name) = node.child_sym(C::DefName)
        {
            self.ssa.declared.push(node.index());
            self.ssa
                .defs_by_name
                .entry(name)
                .or_default()
                .push(node.index());
        }
    }

    pub(crate) fn bare_rhs(&mut self, c: Cursor<'t>, rhs: Cursor<'t>) -> Option<Value> {
        let (from, site) = (self.enclosing(), Some(c.index()));
        let mut typed = None;
        for value in self.lookup(rhs.sym()) {
            match value {
                Value::ImportRef(node) => {
                    typed = Some(Value::Call(c.index()));
                    self.edges.push(Edge {
                        site,
                        call_resolution: CallResolution::Reference,
                        ..Edge::local(from, node, EdgeKind::Imports)
                    });
                    self.edges.push(Edge {
                        site,
                        ..Edge::local(from, c.index(), EdgeKind::TypeFlow)
                    });
                }
                Value::LocalDef(node)
                    if self
                        .at(node)
                        .initializer()
                        .and_then(Cursor::rhs_callee)
                        .is_some() =>
                {
                    self.edges.push(Edge {
                        site,
                        ..Edge::local(from, node, EdgeKind::Calls)
                    });
                }
                _ => {}
            }
        }
        typed
    }

    pub(crate) fn enclosing_class(&self, node: u32) -> Option<u32> {
        let c = self.at(node);
        let class = Some(c)
            .filter(|c| c.is_class())
            .or_else(|| c.enclosing_def(CLASS_LIKE));
        class.map(|n| n.index())
    }

    pub(crate) fn binding_type(&self, node: u32) -> Option<Cursor<'t>> {
        let v = self.at(node);
        if v.is(C::Binding) {
            v.typed()
        } else {
            v.child(C::Callee).filter(|_| v.is(C::Call))
        }
    }

    pub(crate) fn field_value(&self, def: u32) -> Option<Value> {
        let d = self.at(def);
        let binding = d.child(C::Binding).filter(|b| b.typed().is_some())?;
        let stored = d.has(C::FieldDef) || d.has(C::Property);
        stored.then(|| Value::Call(Self::producer_of(binding)))
    }

    pub(crate) fn producer_of(binding: Cursor) -> u32 {
        let call = binding.child(C::Rhs).and_then(Cursor::rhs_call);
        let member_call = call
            .filter(|_| !binding.has(C::SsaTyped))
            .filter(|c| c.child(C::Callee).is_some_and(|k| k.has(C::Member)));
        member_call.map_or(binding.index(), |c| c.index())
    }

    pub(crate) fn ivar_type(&self, class: u32, attr: u32) -> Option<Cursor<'t>> {
        let mut ivars = self.ssa.ivars.borrow_mut();
        let index = ivars.entry(class).or_insert_with(|| {
            let mut index = rustc_hash::FxHashMap::default();
            for n in self.at(class).descendants() {
                if n.is(C::Binding)
                    && let Some(iv) = n.child_sym(C::Ivar)
                    && n.typed().is_some()
                {
                    index.entry(iv).or_insert(n.index());
                }
            }
            index
        });
        index.get(&attr).map(|&n| self.at(n))
    }

    pub(crate) fn member_for(&mut self, value: Value, site: Cursor<'t>) -> Value {
        let Value::LocalDef(def) = value else {
            return value;
        };
        let target = self.at(def);
        let tags = self.tags;
        let both = |d: Cursor<'t>| d.has_tag(tags.value) && d.has_tag(tags.callable);
        let role = site.role();
        let filled = match role {
            Role::Value => target.has_tag(tags.value),
            Role::Callable => target.has_tag(tags.callable) && !both(target),
            Role::Type => target.has_tag(tags.declares_type),
        };
        let Some(name) = target.child_sym(C::DefName).filter(|_| !filled) else {
            return value;
        };
        let declaring = |up: Cursor<'t>| {
            up.index() == def
                || up.is(C::Binding) && up.has(C::Declaration) && up.sym_opt() == Some(name)
        };
        let class = target
            .ancestors()
            .filter(|node| node.is(C::Def))
            .find(|node| node.is_class())
            .filter(|class| target.is_member_of(*class) || site.ancestors().any(declaring));
        let member = class
            .and_then(|class| {
                crate::modules::core::ops::members(self, class, name, role)
                    .first()
                    .map(|m| m.index())
            })
            .filter(|&member| !(both(target) && both(self.at(member))));
        member.map_or(value, Value::LocalDef)
    }

    pub(crate) fn chain_is_class(&mut self, c: Cursor<'t>) -> bool {
        let targets = self.lookup_chain(c);
        targets
            .iter()
            .any(|r| matches!(r, Value::LocalDef(n) if self.at(*n).is_class()))
    }

    pub(crate) fn ssa_defs_named(&self, name: u32) -> Option<Vec<u32>> {
        self.ssa.defs_by_name.get(&name).cloned()
    }

    pub(crate) fn supers_of(&self, def: u32) -> Vec<u32> {
        self.ssa.supertypes.get(&def).cloned().unwrap_or_default()
    }

    pub(crate) fn shadow(&mut self, c: Cursor<'t>) {
        let Some(callee) = c.child(C::Callee) else {
            return;
        };
        let root = callee
            .child(C::Member)
            .and_then(|member| member.child(C::Object))
            .map_or(callee, |object| object.chain_root());
        if !callee.has(C::Ivar)
            && !root.is(C::Call)
            && root.value_child(C::Call).is_none()
            && !self.lookup(root.sym()).iter().all(is_name)
        {
            self.ssa.shadowed_calls.insert(c.index());
        }
    }

    pub(crate) fn pending_value(&mut self, callee: Cursor<'t>) -> Option<Value> {
        let implicit = callee
            .parent()
            .is_some_and(|c| c.has_tag(self.tags.implicit_self));
        self.field_slot(callee)
            .map(|slot| self.read_value(slot))
            .or_else(|| {
                callee
                    .sym_opt()
                    .filter(|_| !implicit)
                    .and_then(|sym| self.read_phi(sym))
            })
    }

    pub(crate) fn defer(&mut self, c: Cursor<'t>) {
        let Some(callee) = c.child(C::Callee) else {
            return;
        };
        let Some(value) = self.pending_value(callee) else {
            return;
        };
        let fallback = (callee.sym_opt().is_some()
            && !callee.has(C::Member)
            && !callee.has(C::Ivar)
            && is_name_text(self.env.lang.syms.resolve(callee.sym())))
        .then(|| Rc::clone(&self.ssa.wildcards));
        let from = self.enclosing();
        self.ssa
            .pending_calls
            .push((value, from, c.index(), Deferred::Callee(fallback)));
    }

    pub(crate) fn defer_receiver(&mut self, c: Cursor<'t>) {
        let Some(m) = c.member() else { return };
        let Some(obj) = m.child(C::Object) else {
            return;
        };
        let leaf = !self.ssa.loops.is_empty() && obj.children().next().is_none();
        if let Some(phi) = leaf.then(|| self.read_phi(obj.sym())).flatten() {
            let seen = self.lookup(obj.sym());
            let receiver = Deferred::Receiver(obj.index(), seen);
            let from = self.enclosing();
            self.ssa
                .pending_calls
                .push((phi, from, c.index(), receiver));
        }
    }

    pub(crate) fn implicit_targets(&mut self, sym: u32, site: Cursor<'t>) -> Vec<Value> {
        if self.ssa.graph.is_defined(self.binding(sym), self.ssa.cur) {
            let mut bound = self.lookup(sym);
            if !bound.is_empty() {
                bound.retain(|r| matches!(r, Value::LocalDef(_) | Value::ImportRef(_)));
                return bound
                    .into_iter()
                    .map(|r| self.member_for(r, site))
                    .collect();
            }
        }
        let class = self.enclosing_class(self.enclosing());
        let members = class
            .map(|cls| crate::modules::core::ops::members(self, self.at(cls), sym, site.role()));
        let members: Vec<Value> = members
            .into_iter()
            .flatten()
            .map(|m| Value::LocalDef(m.index()))
            .collect();
        if !members.is_empty() {
            return members;
        }
        self.name_targets(sym, site, true)
    }

    pub(crate) fn name_targets(
        &mut self,
        sym: u32,
        site: Cursor<'t>,
        callable_only: bool,
    ) -> Vec<Value> {
        let targets: Vec<Value> = self
            .lookup(sym)
            .into_iter()
            .map(|r| self.member_for(r, site))
            .collect();
        if !targets.is_empty() || !is_name_text(self.env.lang.syms.resolve(sym)) {
            return targets;
        }
        let callable = |&n: &u32| {
            let import = self.at(n).parent();
            import.is_some_and(|i| i.has_tag(self.tags.callable))
        };
        let wild = self
            .ssa
            .wildcards
            .iter()
            .filter(|n| !callable_only || callable(n));
        wild.map(|&n| Value::ImportRef(n)).collect()
    }

    pub(crate) fn resolve_pending(&mut self) -> Result<(), Killed> {
        for (value, from, site, deferred) in std::mem::take(&mut self.ssa.pending_calls) {
            self.check()?;
            let mut targets = self.ssa.graph.resolve_value(&value);
            let mut edges: Vec<(EdgeKind, Loc)> = Vec::new();
            match deferred {
                Deferred::Callee(fallback) => {
                    if targets.is_empty() {
                        let wildcards = fallback.iter().flat_map(|w| w.iter());
                        targets.extend(wildcards.map(|&w| Value::ImportRef(w)));
                    }
                    for target in to_vals(&targets) {
                        edges.extend(refs(self, target));
                    }
                }
                Deferred::Receiver(object, seen) => {
                    targets.retain(|value| !seen.contains(value));
                    let Some(member) = self.at(object).parent() else {
                        continue;
                    };
                    for target in to_vals(&targets) {
                        let calls = self.member(target, member);
                        edges.extend(
                            calls
                                .iter()
                                .filter_map(|v| v.loc())
                                .map(|l| (EdgeKind::Calls, l)),
                        );
                        for unmatched in self.unmatched(target, member) {
                            edges.extend(refs(self, unmatched));
                        }
                    }
                }
            }
            for (kind, to) in edges {
                self.edges.push(Edge {
                    site: Some(site),
                    ..Edge::local(from, to.node, kind)
                });
            }
        }
        Ok(())
    }
}
