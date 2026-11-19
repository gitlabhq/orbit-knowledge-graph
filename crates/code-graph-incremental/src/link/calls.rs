use std::rc::Rc;

use super::{Cursor, Deferred, Fold, Values, Wildcards};
use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, RED_ZONE, STACK_SEGMENT};
use crate::resolve::CLASS_LIKE;
use crate::rules::members;
use crate::sentinel::Killed;
use crate::ssa::Value;
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

impl<'t> Fold<'t> {
    pub(super) fn handle_inline_imports(&mut self, c: Cursor<'t>) {
        for import in c.children().filter(|i| i.is(C::Import)) {
            self.handle_import(import);
        }
    }

    pub(super) fn handle_import(&mut self, c: Cursor<'t>) {
        let module = self.inline_module(c);
        for n in c.names() {
            let sym = n.sym();
            let local = n
                .child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(sym);
            if c.is(C::ImportType) || n.has_tag(self.tags.type_only) {
                let binding = self.declare_binding(n, local);
                self.ssa.write_variable(binding, self.cur, Value::Opaque);
                continue;
            }
            if let Some(module) = module
                && self.bind_from_def(module, n, local)
            {
                continue;
            }
            if !self.config.imports_shadow_locals
                && self
                    .lookup(local)
                    .iter()
                    .any(|r| matches!(r, Value::LocalDef(_)))
            {
                continue;
            }
            if local == self.wildcard {
                Rc::make_mut(&mut self.wildcards).push(n.index());
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
        if !self.config.inline_modules {
            return None;
        }
        let path = self.syms.resolve(import.child_sym(C::SourcePath)?);
        let mut segments = path.split(PATH_SEP).filter(|s| !s.is_empty());
        let Some(first) = segments.next() else {
            return Some(self.tree.root());
        };
        let mut module =
            self.lookup(self.syms.lookup(first))
                .into_iter()
                .find_map(|r| match r {
                    Value::LocalDef(d) => Some(self.at(d)),
                    _ => None,
                })?;
        for segment in segments {
            module = child_def(module, self.syms.lookup(segment))?;
        }
        Some(module)
    }

    fn bind_from_def(&mut self, module: Cursor<'t>, name: Cursor<'t>, local: u32) -> bool {
        let targets: Vec<(u32, u32)> = match name.sym() == self.wildcard {
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

    pub(super) fn handle_def(&mut self, c: Cursor<'t>) {
        let Some(name) = c.child_sym(C::DefName) else {
            return;
        };
        let idx = c.index();
        if let Some(name) = c.child(C::DefName).filter(|_| c.has(C::ImplBlock)) {
            self.link_reference(name);
        }
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
        self.supertypes.insert(idx, supers.clone());
        self.declare(c, name);
        let parent_block = self.cur;
        self.cur = self.ssa.add_sealed_successor(parent_block);
        if let Some(&parent) = self.def_stack.last() {
            self.edges.push(Edge::local(parent, idx, EdgeKind::Defines));
        }
        for dn in supers {
            self.edges.push(Edge::local(idx, dn, EdgeKind::Extends));
        }
        if c.has_tag(self.tags.scoped) {
            self.scope(c, Some(parent_block));
        }
    }

    pub(super) fn register_def(&mut self, node: Cursor<'_>) {
        if self.registered.insert(node.index())
            && let Some(name) = node.child_sym(C::DefName)
        {
            self.defs.push(node.index());
            self.defs_by_name
                .entry(name)
                .or_default()
                .push(node.index());
        }
    }

    pub(super) fn handle_call(&mut self, c: Cursor<'t>) {
        let Some(callee) = c.child(C::Callee) else {
            return;
        };
        if c.has(C::Property) && self.chain_is_class(callee) {
            return;
        }
        let root = callee
            .child(C::Member)
            .and_then(|member| member.child(C::Object))
            .map_or(callee, |object| object.chain_root());
        if !callee.has(C::Ivar)
            && !root.is(C::Call)
            && root.value_child(C::Call).is_none()
            && !self.lookup(root.sym()).iter().all(is_name)
        {
            self.shadowed_calls.insert(c.index());
        }
        let from = self.enclosing();
        let first = self.edges.len();
        if let Some(member) = callee.child(C::Member) {
            if member
                .child(C::Dispatch)
                .is_some_and(|dispatch| !dispatch.has(C::Object))
            {
                for reference in member
                    .children()
                    .filter(|node| node.is(C::Object) || node.is(C::Dispatch))
                {
                    self.link_reference(reference);
                }
                return;
            }
            if let Some(receiver) = member
                .child(C::Dispatch)
                .and_then(|dispatch| dispatch.child(C::Object))
            {
                for value in self.lookup(receiver.sym()) {
                    if let Value::Call(node) = value {
                        self.edges.push(Edge {
                            site: Some(c.index()),
                            ..Edge::local(from, node, EdgeKind::TypeFlow)
                        });
                    }
                }
                if member.child(C::Object).is_some_and(|object| {
                    self.lookup(object.sym()).iter().any(
                        |value| matches!(value, Value::LocalDef(node) if self.at(*node).is_dispatch_contract()),
                    )
                }) {
                    return;
                }
            }
        }
        let value = self
            .field_slot(callee)
            .map(|slot| self.read_value(slot))
            .or_else(|| {
                callee
                    .sym_opt()
                    .filter(|_| !c.has_tag(self.tags.implicit_self))
                    .and_then(|sym| self.read_phi(sym))
            });
        if let Some(value) = value {
            let fallback = (callee.sym_opt().is_some()
                && !callee.has(C::Member)
                && !callee.has(C::Ivar)
                && is_name_text(self.syms.resolve(callee.sym())))
            .then(|| Rc::clone(&self.wildcards));
            self.pending_calls
                .push((value, from, c.index(), Deferred::Callee(fallback)));
        } else if let Some(m) = callee.child(C::Member) {
            if let Some(obj) = m.child(C::Object) {
                self.resolve_obj(obj, m.sym(), from, c.role());
                if obj.value_child(C::Member).is_some() {
                    self.flow_chain_root(obj, from);
                }
                let leaf = !self.loops.is_empty() && obj.children().next().is_none();
                if let Some(phi) = leaf.then(|| self.read_phi(obj.sym())).flatten() {
                    let seen = self.lookup(obj.sym());
                    let receiver = Deferred::Receiver(obj.index(), m.sym(), seen);
                    self.pending_calls.push((phi, from, c.index(), receiver));
                }
            }
        } else if let Some(iv) = callee.child(C::Ivar) {
            if let Some(cls) = self.enclosing_class(from) {
                let members = self.members(cls, iv.sym(), c.role());
                self.push_calls(from, members);
            }
        } else if let Some(sym) = callee.sym_opt() {
            if c.has_tag(self.tags.implicit_self) {
                self.resolve_implicit(sym, from, c);
            } else {
                self.resolve_name(sym, from, c, Wildcards::All);
            }
        }
        for edge in &mut self.edges[first..] {
            edge.site = Some(c.index());
        }
    }

    pub(super) fn handle_standalone_member(&mut self, c: Cursor<'t>) {
        let in_chain = c.parent().is_some_and(|parent| {
            parent.is(C::Callee)
                || parent.is(C::Object) && parent.parent().is_some_and(|owner| owner.is(C::Member))
        });
        if in_chain || self.field_slot(c).is_some() {
            return;
        }
        if let Some(obj) = c.child(C::Object) {
            let first = self.edges.len();
            self.resolve_obj(obj, c.sym(), self.enclosing(), Role::Value);
            for edge in &mut self.edges[first..] {
                edge.call_resolution = CallResolution::Reference;
            }
        }
    }

    fn link_reference(&mut self, reference: Cursor<'t>) {
        self.handle_inline_imports(reference);
        if let [Value::LocalDef(target) | Value::ImportRef(target)] =
            self.lookup_chain(reference).as_slice()
            && *target != reference.parent().map_or(reference.index(), Cursor::index)
        {
            self.edges.push(Edge {
                call_resolution: CallResolution::Reference,
                ..Edge::local(reference.index(), *target, EdgeKind::Imports)
            });
        }
    }

    pub(super) fn bare_rhs(&mut self, c: Cursor<'t>, rhs: Cursor<'t>) -> Option<Value> {
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

    pub(super) fn resolve_pending(&mut self) -> Result<(), Killed> {
        for (value, from, site, deferred) in std::mem::take(&mut self.pending_calls) {
            self.check()?;
            let first = self.edges.len();
            let mut targets = self.ssa.resolve_value(&value);
            match deferred {
                Deferred::Callee(fallback) => {
                    if targets.is_empty() {
                        let wildcards = fallback.iter().flat_map(|w| w.iter());
                        targets.extend(wildcards.map(|&w| Value::ImportRef(w)));
                    }
                    self.emit_targets(&targets, from);
                }
                Deferred::Receiver(object, method, seen) => {
                    targets.retain(|value| !seen.contains(value));
                    let role = self.at(site).role();
                    self.resolve_values(self.at(object), method, from, role, targets);
                }
            }
            for edge in &mut self.edges[first..] {
                edge.site = Some(site);
            }
        }
        Ok(())
    }

    fn emit(&mut self, r: &Value, from: u32) {
        let edge = match *r {
            Value::LocalDef(node) if self.at(node).has_tag(self.tags.callable) => {
                Edge::local(from, node, EdgeKind::Calls)
            }
            Value::ImportRef(node) => Edge::local(from, node, EdgeKind::Imports),
            Value::Call(call) => Edge::local(from, call, EdgeKind::TypeFlow),
            _ => return,
        };
        self.edges.push(edge);
    }

    fn resolve_obj(&mut self, obj: Cursor<'t>, method: u32, from: u32, role: Role) {
        if !self.obj_seen.insert(obj.index()) {
            return;
        }
        stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
            let values = self.lookup_chain(obj);
            self.resolve_values(obj, method, from, role, values.into_vec());
        });
        self.obj_seen.remove(&obj.index());
    }

    fn resolve_values(
        &mut self,
        obj: Cursor<'t>,
        method: u32,
        from: u32,
        role: Role,
        values: Vec<Value>,
    ) {
        for r in values {
            match r {
                Value::Type(node) => {
                    let members = self.members(node, method, role);
                    self.push_calls(from, members);
                }
                Value::LocalDef(node) => {
                    let members = self.members(node, method, role);
                    if members.is_empty() && obj.is(C::Object) {
                        self.emit(&r, from);
                    }
                    self.push_calls(from, members);
                }
                Value::Call(n) if let Some(ty) = self.binding_type(n) => {
                    self.resolve_obj(ty, method, from, role);
                    if obj.is(C::Object) {
                        self.emit(&r, from);
                    }
                }
                _ if obj.is(C::Object) => self.emit(&r, from),
                _ => {}
            }
        }
    }

    fn resolve_implicit(&mut self, sym: u32, from: u32, site: Cursor<'t>) {
        if self.ssa.is_defined(self.binding(sym), self.cur) {
            let mut bound = self.lookup(sym);
            if !bound.is_empty() {
                bound.retain(|r| matches!(r, Value::LocalDef(_) | Value::ImportRef(_)));
                for r in bound {
                    let target = self.member_for(r, site);
                    self.emit(&target, from);
                }
                return;
            }
        }
        let members = self
            .enclosing_class(from)
            .map(|cls| self.members(cls, sym, site.role()))
            .unwrap_or_default();
        if !members.is_empty() {
            self.push_calls(from, members);
            return;
        }
        self.resolve_name(sym, from, site, Wildcards::Callable);
    }

    fn resolve_name(&mut self, sym: u32, from: u32, site: Cursor<'t>, fallback: Wildcards) {
        let mut targets: Vec<Value> = self
            .lookup(sym)
            .into_iter()
            .map(|r| self.member_for(r, site))
            .collect();
        if targets.is_empty() && is_name_text(self.syms.resolve(sym)) {
            let callable = |&n: &u32| {
                let import = self.at(n).parent();
                import.is_some_and(|i| i.has_tag(self.tags.callable))
            };
            let wild = self.wildcards.iter().filter(|n| match fallback {
                Wildcards::All => true,
                Wildcards::Callable => callable(n),
            });
            targets = wild.map(|&n| Value::ImportRef(n)).collect();
        }
        self.emit_targets(&targets, from);
    }

    fn emit_targets(&mut self, targets: &[Value], from: u32) {
        for r in targets {
            match r {
                Value::Type(target) => {
                    let targets = match self.at(*target).child_sym(C::Callable) {
                        Some(name) => self.members(*target, name, Role::Callable),
                        None => vec![*target],
                    };
                    self.push_calls(from, targets);
                }
                _ => self.emit(r, from),
            }
        }
    }

    pub(super) fn enclosing_class(&self, node: u32) -> Option<u32> {
        let c = self.at(node);
        let class = Some(c)
            .filter(|c| c.is_class())
            .or_else(|| c.enclosing_def(CLASS_LIKE));
        class.map(|n| n.index())
    }

    fn flow_chain_root(&mut self, obj: Cursor<'t>, from: u32) {
        for r in self.lookup_chain(obj.chain_root()) {
            if let Value::Call(n) = r
                && self.binding_type(n).is_some()
            {
                self.edges.push(Edge::local(from, n, EdgeKind::TypeFlow));
            }
        }
    }

    pub(super) fn binding_type(&self, node: u32) -> Option<Cursor<'t>> {
        let v = self.at(node);
        if v.is(C::Binding) {
            v.typed()
        } else {
            v.child(C::Callee).filter(|_| v.is(C::Call))
        }
    }

    pub(super) fn field_value(&self, def: u32) -> Option<Value> {
        let d = self.at(def);
        let binding = d.child(C::Binding).filter(|b| b.typed().is_some())?;
        let stored = d.has(C::FieldDef) || d.has(C::Property);
        stored.then(|| Value::Call(Self::producer_of(binding)))
    }

    pub(super) fn producer_of(binding: Cursor) -> u32 {
        let call = binding.child(C::Rhs).and_then(Cursor::rhs_call);
        let member_call = call
            .filter(|_| !binding.has(C::SsaTyped))
            .filter(|c| c.child(C::Callee).is_some_and(|k| k.has(C::Member)));
        member_call.map_or(binding.index(), |c| c.index())
    }

    pub(super) fn ivar_type(&self, class: u32, attr: u32) -> Option<Cursor<'t>> {
        let mut ivars = self.ivars.borrow_mut();
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

    pub(super) fn member_for(&mut self, value: Value, site: Cursor<'t>) -> Value {
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
            .and_then(|class| self.members(class.index(), name, role).first().copied())
            .filter(|&member| !(both(target) && both(self.at(member))));
        member.map_or(value, Value::LocalDef)
    }

    pub(super) fn chain_is_class(&mut self, c: Cursor<'t>) -> bool {
        let targets: Values = self.lookup_chain(c);
        targets
            .iter()
            .any(|r| matches!(r, Value::LocalDef(n) if self.at(*n).is_class()))
    }

    fn push_calls(&mut self, from: u32, targets: Vec<u32>) {
        for m in targets {
            self.edges.push(Edge::local(from, m, EdgeKind::Calls));
        }
    }

    pub(super) fn members(&mut self, container: u32, name: u32, role: Role) -> Vec<u32> {
        let found = members(self, self.at(container), name, role);
        found.into_iter().map(Cursor::index).collect()
    }
}
