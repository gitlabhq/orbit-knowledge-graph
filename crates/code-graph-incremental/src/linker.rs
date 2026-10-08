use std::cell::RefCell;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::smallvec;

use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, WILDCARD};
use crate::env::Env;
use crate::resolver::CLASS_LIKE;
use crate::rules::LinkConfig;
use crate::sentinel::{Killed, Sentinel};
use crate::ssa::{BlockId, SsaEngine, Value};
use crate::tags::ReservedTags;
use crate::tree::{Compact, Edge, EdgeKind, members_by_level};
type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

enum WorkItem {
    Visit(u32),
    ExitScope {
        wildcards: usize,
        block: Option<BlockId>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum BindingKey {
    Name(u32),
    Declaration(u32, u32),
    Field(u32, u32),
}

/// Which wildcard imports an unresolved bare name may fall back to.
#[derive(Clone, Copy)]
enum Wildcards {
    None,
    All,
    Callable,
}

struct Fold<'t> {
    tree: &'t Tree,
    ssa: SsaEngine,
    cur: BlockId,
    registered: FxHashSet<u32>,
    defs: Vec<u32>,
    defs_by_name: FxHashMap<u32, Vec<u32>>,
    /// Per class: member name to first def, and ivar name to its typed binding.
    members: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    ivars: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    supertypes: FxHashMap<u32, Vec<u32>>,
    wildcards: Vec<u32>,
    tags: ReservedTags,
    config: &'t LinkConfig,
    syms: &'t crate::intern::Interner,
    run: &'t Sentinel,
    file: Sentinel,
    killed: Option<Killed>,
    def_stack: Vec<u32>,
    chain_seen: FxHashSet<u32>,
    obj_seen: FxHashSet<u32>,
    chain_memo: FxHashMap<u32, Vec<Value>>,
    wildcard: u32,
    edges: Vec<Edge>,
    value_sink: FxHashMap<u32, u32>,
    pending_calls: Vec<(Value, u32, u32, Vec<u32>)>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    scopes: Vec<(u32, FxHashMap<u32, u32>)>,
    keys: RefCell<FxHashMap<BindingKey, u32>>,
}

impl<'t> Fold<'t> {
    fn enclosing(&self) -> u32 {
        self.def_stack.last().copied().unwrap_or(0)
    }

    /// Stops at the first failed check; `link` reports it after the walk.
    fn run(&mut self, mut stack: Vec<WorkItem>) {
        while let Some(item) = stack.pop() {
            if self.killed.is_some() {
                return;
            }
            if let Err(k) = self.run.check().and_then(|()| self.file.check()) {
                self.killed = Some(k);
                return;
            }
            match item {
                WorkItem::ExitScope { wildcards, block } => {
                    self.scopes.pop();
                    self.wildcards.truncate(wildcards);
                    if let Some(block) = block {
                        self.def_stack.pop();
                        self.cur = block;
                    }
                }
                WorkItem::Visit(i) => self.dispatch(self.tree.cursor(i), &mut stack),
            }
        }
    }

    fn push_children(c: Cursor, stack: &mut Vec<WorkItem>) {
        stack.extend(c.children_rev().map(|ch| WorkItem::Visit(ch.index())));
    }

    /// Imports spelled inline in a call or a supertype path (`crate::a::f()`,
    /// `impl zoo::T for X`) bind in the enclosing scope before the node itself;
    /// the walk into the node then skips them.
    fn handle_inline_imports(&mut self, c: Cursor<'t>) {
        for import in c.children().filter(|i| i.is(C::Import)) {
            self.handle_import(import);
        }
    }

    fn walk_children(&mut self, c: Cursor) {
        let mut stack = Vec::new();
        Self::push_children(c, &mut stack);
        self.run(stack);
    }

    fn dispatch(&mut self, c: Cursor<'t>, stack: &mut Vec<WorkItem>) {
        self.chain_memo.clear();
        let k = c.kind();
        if k == C::Import || k == C::ImportType {
            self.handle_import(c);
        } else if c.is(C::Def) {
            self.handle_def(c, stack);
        } else if k == C::Call {
            self.handle_inline_imports(c);
            self.handle_call(c);
        } else if k == C::Member {
            let is_callee = c.parent().is_some_and(|p| p.kind() == C::Callee);
            if !is_callee {
                self.handle_standalone_member(c);
            }
        } else if k == C::Binding {
            if self.handle_binding(c) {
                return;
            }
        } else if k == C::Scope {
            self.enter_scope(c);
            stack.push(WorkItem::ExitScope {
                wildcards: self.wildcards.len(),
                block: None,
            });
        } else if k == C::Destructure {
            for slot in c.children_of(C::Binding) {
                let binding = self.declare_binding(slot, slot.sym());
                self.ssa
                    .write_variable(binding, self.cur, Value::Call(slot.index()));
            }
            stack.extend(
                c.children_rev()
                    .filter(|child| !child.is(C::Binding))
                    .map(|child| WorkItem::Visit(child.index())),
            );
            return;
        } else if k == C::SsaBranch {
            let sink = self.value_sink.remove(&c.index());
            self.handle_branch(c, sink);
            return;
        } else if k == C::SsaLoop {
            self.handle_loop(c);
            return;
        }
        if k == C::Import || k == C::ImportType || k == C::Def {
            return;
        }
        stack.extend(
            c.children_rev()
                .filter(|child| !((k == C::Call || k == C::SuperType) && child.is(C::Import)))
                .map(|child| WorkItem::Visit(child.index())),
        );
    }

    fn handle_branch(&mut self, branch: Cursor<'t>, lhs: Option<u32>) {
        for child in branch.children().filter(|c| !c.is(C::SsaArm)) {
            self.run(vec![WorkItem::Visit(child.index())]);
        }
        let arms: Vec<Cursor<'t>> = branch.children().filter(|c| c.is(C::SsaArm)).collect();
        if arms.is_empty() {
            return;
        }
        let pre = self.cur;
        let mut exit_blocks = Vec::new();
        for arm in arms {
            self.cur = self.ssa.add_sealed_successor(pre);
            let tail = arm.tail_expr();
            if let Some(lhs) = lhs.filter(|_| tail.is(C::SsaBranch)) {
                self.value_sink.insert(tail.index(), lhs);
            }
            self.walk_children(arm);
            if let Some(lhs) = lhs.filter(|_| !tail.is(C::SsaBranch) && !tail.is(C::SsaReturn)) {
                let val = self.classify_tail(tail);
                if val != Value::Opaque {
                    self.ssa.write_variable(lhs, self.cur, val);
                }
                self.bind_fields(lhs, tail);
            }
            exit_blocks.push(self.cur);
        }
        if lhs.is_none() {
            exit_blocks.push(pre);
        }
        self.cur = self.ssa.add_sealed_join(exit_blocks);
    }

    fn handle_loop(&mut self, c: Cursor<'t>) {
        let (header, body) = self.ssa.begin_loop(self.cur);
        self.cur = body;
        self.walk_children(c);
        self.cur = self.ssa.finish_loop(header, self.cur);
    }

    fn handle_import(&mut self, c: Cursor<'t>) {
        let module = self.inline_module(c);
        for n in c.names() {
            let sym = n.sym();
            let local = n
                .child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(sym);
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
            self.wildcards
                .extend((local == self.wildcard).then_some(n.index()));
            let binding = self.declare_binding(n, local);
            self.ssa
                .write_variable(binding, self.cur, Value::ImportRef(n.index()));
            for alias in [C::Alias, C::SsaHint]
                .into_iter()
                .filter_map(|kind| n.child_sym(kind))
            {
                if let Some((_, scope)) = self.scopes.last_mut() {
                    scope.insert(alias, binding);
                }
            }
        }
    }

    /// The def an import path names when every segment is a def in this file
    /// (`use crate::dep::Service` with `mod dep {}` above it); the file root
    /// for an empty path (`use crate::dep as dep_mod`).
    fn inline_module(&mut self, import: Cursor<'t>) -> Option<Cursor<'t>> {
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
                    Value::LocalDef(d) => Some(self.tree.cursor(d)),
                    _ => None,
                })?;
        for segment in segments {
            module = child_def(module, self.syms.lookup(segment))?;
        }
        Some(module)
    }

    /// Binds an imported name to the def of that name inside `module`, or
    /// every def in it for a wildcard. False when there is no such def.
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
        for (bind_as, node) in &targets {
            self.register_def(self.tree.cursor(*node));
            let binding = self.declare_binding(name, *bind_as);
            self.ssa
                .write_variable(binding, self.cur, Value::LocalDef(*node));
        }
        !targets.is_empty()
    }

    fn handle_def(&mut self, c: Cursor<'t>, stack: &mut Vec<WorkItem>) {
        let Some(name) = c.child_sym(C::DefName) else {
            return;
        };
        let idx = c.index();
        self.register_def(c);
        for supertype in c.children_of(C::SuperType) {
            self.handle_inline_imports(supertype);
        }
        let supers = c
            .children()
            .filter(|s| s.is(C::SuperType))
            .flat_map(|s| self.lookup_chain(s))
            .filter_map(|r| match r {
                Value::LocalDef(d) => Some(d),
                _ => None,
            })
            .collect::<Vec<_>>();
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
            self.enter_scope(c);
            self.def_stack.push(idx);
            stack.push(WorkItem::ExitScope {
                wildcards: self.wildcards.len(),
                block: Some(parent_block),
            });
            let initializer = c
                .initializer()
                .filter(|_| c.is_class())
                .map(|node| node.index());
            stack.extend(
                c.children_rev()
                    .filter(|node| Some(node.index()) != initializer)
                    .map(|node| WorkItem::Visit(node.index())),
            );
        }
    }

    fn predeclare(&mut self, scope: Cursor<'t>) {
        for d in scope.children().filter(|d| d.is(C::Def)) {
            if let Some(name) = d.child_sym(C::DefName) {
                self.register_def(d);
                self.declare(d, name);
            }
        }
    }

    fn declare(&mut self, c: Cursor<'t>, name: u32) {
        for sym in std::iter::once(name).chain(c.children_of(C::Alias).map(|a| a.sym())) {
            if sym == name && c.has(C::ImplBlock) && !self.lookup(name).is_empty() {
                continue;
            }
            let binding = self.declare_binding(c, sym);
            self.ssa
                .write_variable(binding, self.cur, Value::LocalDef(c.index()));
        }
    }

    fn defs_named(&self, sym: u32) -> impl Iterator<Item = u32> + '_ {
        self.defs_by_name.get(&sym).into_iter().flatten().copied()
    }

    fn register_def(&mut self, node: Cursor<'_>) {
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

    fn handle_call(&mut self, c: Cursor<'t>) {
        let Some(callee) = c.child(C::Callee) else {
            return;
        };
        if c.has(C::Property) && self.chain_is_class(callee) {
            return;
        }
        let from = self.enclosing();
        let first = self.edges.len();
        let value = self
            .field_slot(callee)
            .map(|slot| self.read_value(slot))
            .or_else(|| {
                let sym = callee
                    .sym_opt()
                    .filter(|_| !c.has_tag(self.tags.implicit_self))?;
                let value = self.ssa.read_variable_internal(self.binding(sym), self.cur);
                matches!(value, Value::Phi(_)).then_some(value)
            });
        if let Some(value) = value {
            let fallback = if callee
                .sym_opt()
                .is_some_and(|sym| !self.config.builtins.contains(&sym))
                && !callee.has(C::Member)
                && !callee.has(C::Ivar)
            {
                self.wildcards.clone()
            } else {
                Vec::new()
            };
            self.pending_calls.push((value, from, c.index(), fallback));
        } else if let Some(m) = callee.child(C::Member) {
            if let Some(obj) = m.child(C::Object) {
                self.resolve_obj(obj, m.sym(), from);
                if obj.child(C::Member).is_some() {
                    self.flow_chain_root(obj, from);
                }
            }
        } else if let Some(iv) = callee.child(C::Ivar) {
            if let Some(cls) = self.enclosing_class(from) {
                self.push_calls(from, self.find_method_in(cls, iv.sym()));
            }
        } else if let Some(sym) = callee.sym_opt() {
            if c.has_tag(self.tags.implicit_self) {
                self.resolve_implicit(sym, from);
            } else {
                let fallback = match self.config.builtins.contains(&sym) {
                    true => Wildcards::None,
                    false => Wildcards::All,
                };
                self.resolve_name(sym, from, fallback);
            }
        }
        for edge in &mut self.edges[first..] {
            edge.site = Some(c.index());
        }
    }

    fn handle_standalone_member(&mut self, c: Cursor<'t>) {
        if self.field_slot(c).is_some() {
            return;
        }
        if let Some(obj) = c.child(C::Object) {
            self.resolve_obj(obj, c.sym(), self.enclosing());
        }
    }

    fn handle_binding(&mut self, c: Cursor<'t>) -> bool {
        let field = self.member_binding(c).map(|(root, field)| {
            let key = self.key(BindingKey::Field(root, field));
            *self
                .fields
                .entry(root)
                .or_default()
                .entry(field)
                .or_insert(key)
        });
        let declaration = c.has(C::Declaration) && c.child_sym(C::Declaration).is_none();
        let lhs = if declaration {
            self.binding_key(c, c.sym())
        } else {
            field.unwrap_or_else(|| self.binding(c.sym()))
        };
        if lhs == 0 || (c.has(C::Ivar) && field.is_none()) {
            return false;
        }
        if self.ssa.has_variable_in_block(lhs, self.cur) {
            self.cur = self.ssa.add_sealed_successor(self.cur);
        }

        let rhs = c.child(C::Rhs);
        if rhs.is_none() && c.child_sym(C::Declaration).is_some() {
            return true;
        }
        if let Some(branch) = rhs.and_then(|r| r.child(C::SsaBranch)) {
            self.handle_branch(branch, Some(lhs));
        } else {
            if let Some(rhs) = rhs {
                self.walk_children(rhs);
            }
            let tail = rhs.map(Cursor::tail_expr);
            let mut val = if c.has(C::SsaTyped)
                || tail.is_some_and(|tail| tail.is(C::Member) && self.field_slot(tail).is_none())
            {
                Value::Call(c.index())
            } else {
                tail.map_or(Value::Opaque, |tail| self.classify_tail(tail))
            };
            if let Some(rhs) = rhs.filter(|rhs| rhs.children().next().is_none()) {
                for value in self.lookup(rhs.sym()) {
                    match value {
                        Value::ImportRef(_) => {
                            val = Value::Call(c.index());
                            self.edges.push(Edge {
                                site: Some(c.index()),
                                ..Edge::local(self.enclosing(), c.index(), EdgeKind::TypeFlow)
                            });
                        }
                        Value::LocalDef(node)
                            if self
                                .tree
                                .cursor(node)
                                .initializer()
                                .and_then(Cursor::rhs_callee)
                                .is_some() =>
                        {
                            self.edges.push(Edge {
                                site: Some(c.index()),
                                ..Edge::local(self.enclosing(), node, EdgeKind::Calls)
                            });
                        }
                        _ => {}
                    }
                }
            }
            self.ssa.write_variable(lhs, self.cur, val);
            if let Some(tail) = tail {
                self.bind_fields(lhs, tail);
            }
        }
        if declaration && let Some((_, scope)) = self.scopes.last_mut() {
            scope.insert(c.sym(), lhs);
        }
        true
    }

    fn read_value(&mut self, binding: u32) -> Value {
        match self.ssa.read_variable_internal(binding, self.cur) {
            Value::Undefined => Value::Opaque,
            value => value,
        }
    }

    fn binding(&self, name: u32) -> u32 {
        self.scopes
            .iter()
            .rev()
            .find_map(|(_, scope)| scope.get(&name).copied())
            .unwrap_or_else(|| self.key(BindingKey::Name(name)))
    }

    fn binding_key(&self, node: Cursor<'_>, name: u32) -> u32 {
        self.key(BindingKey::Declaration(node.index(), name))
    }

    fn key(&self, key: BindingKey) -> u32 {
        if key == BindingKey::Name(0) {
            return 0;
        }
        let mut keys = self.keys.borrow_mut();
        let next = keys.len() as u32 + 1;
        *keys.entry(key).or_insert(next)
    }

    fn declare_binding(&mut self, node: Cursor<'_>, name: u32) -> u32 {
        let binding = if node.child_sym(C::Declaration).is_some() {
            self.binding(name)
        } else {
            self.binding_key(node, name)
        };
        if let Some((_, scope)) = self.scopes.last_mut() {
            scope.insert(name, binding);
        }
        binding
    }

    fn enter_scope(&mut self, scope: Cursor<'t>) {
        let label = scope.tag(self.tags.scoped).unwrap_or(scope.sym());
        self.scopes.push((label, FxHashMap::default()));
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
            let Some(((_, current), parents)) = self.scopes.split_last_mut() else {
                return;
            };
            if !current.contains_key(&name) {
                if let Some(owner) = declaration.child_sym(C::Alias) {
                    let binding = parents.iter().rev().find_map(|(label, names)| {
                        (*label == owner).then(|| names.get(&name).copied().unwrap_or(unbound))
                    });
                    if let Some(binding) = binding {
                        current.insert(name, binding);
                    }
                    continue;
                }
                current.insert(name, key);
                self.ssa.write_variable(key, self.cur, Value::Opaque);
            }
        }
    }

    fn member_binding(&self, node: Cursor<'_>) -> Option<(u32, u32)> {
        let member = node
            .is(C::Member)
            .then_some(node)
            .or_else(|| node.child(C::Member))
            .or_else(|| node.child(C::Ivar))?;
        let root = self.binding(member.child(C::Object)?.sym());
        Some((root, member.sym()))
    }

    fn field_slot(&self, node: Cursor<'_>) -> Option<u32> {
        let (root, field) = self.member_binding(node)?;
        self.fields.get(&root)?.get(&field).copied()
    }

    fn bind_fields(&mut self, lhs: u32, value: Cursor<'t>) {
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
                    (
                        field.sym(),
                        self.binding(
                            field
                                .child_sym(C::Binding)
                                .or_else(|| field.child_sym(C::Rhs))
                                .unwrap_or(0),
                        ),
                    )
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
            self.fields.get(&source).cloned().unwrap_or_default()
        };
        let mut copies: smallvec::SmallVec<[_; 4]> = smallvec![(lhs, sources)];
        let mut field_updates = smallvec::SmallVec::<[_; 4]>::new();
        let mut value_updates = smallvec::SmallVec::<[_; 8]>::new();
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
                if self.fields.contains_key(&source) || self.fields.contains_key(&target) {
                    let sources = self.fields.get(&source).cloned().unwrap_or_default();
                    copies.push((target, sources));
                }
            }
            field_updates.push((target, fields));
        }
        self.fields.extend(field_updates);
        for (target, value) in value_updates {
            self.ssa.write_variable(target, self.cur, value);
        }
    }

    fn classify_tail(&mut self, tail: Cursor<'_>) -> Value {
        if tail.is(C::Obj) {
            return Value::Opaque;
        }
        if let Some(slot) = self.field_slot(tail) {
            return self.read_value(slot);
        }
        if tail.is(C::Call) {
            return Value::Call(tail.index());
        }
        let sym = self.tail_sym(tail);
        if sym == 0 {
            return Value::Opaque;
        }
        let value = self.read_value(self.binding(sym));
        if let Value::LocalDef(node) = value
            && self.tree.cursor(node).is_class()
        {
            Value::Type(node)
        } else {
            value
        }
    }

    fn tail_sym(&self, node: Cursor<'_>) -> u32 {
        match node.last_named() {
            Some(c) if c.size() > 1 => self.tail_sym(c),
            Some(c) => c.sym(),
            None => node.sym(),
        }
    }

    fn lookup(&mut self, sym: u32) -> Vec<Value> {
        for block in [self.cur, BlockId(0)] {
            let result = self.ssa.read_variable(self.binding(sym), block);
            if !result.is_empty() {
                return result;
            }
        }
        Vec::new()
    }

    fn emit(&mut self, r: &Value, from: u32) {
        match r {
            Value::LocalDef(node) => {
                if self.tree.cursor(*node).has_tag(self.tags.callable) {
                    self.edges.push(Edge::local(from, *node, EdgeKind::Calls));
                }
            }
            Value::ImportRef(node) => self.edges.push(Edge::local(from, *node, EdgeKind::Imports)),
            Value::Call(call) => self
                .edges
                .push(Edge::local(from, *call, EdgeKind::TypeFlow)),
            _ => {}
        }
    }

    fn any_class(&self, resolved: &[Value]) -> bool {
        resolved
            .iter()
            .any(|r| matches!(r, Value::LocalDef(n) if self.tree.cursor(*n).is_class()))
    }

    fn resolve_obj(&mut self, obj: Cursor, method: u32, from: u32) {
        // step = step() types the binding by a call whose receiver is the binding.
        if !self.obj_seen.insert(obj.index()) {
            return;
        }
        self.resolve_obj_inner(obj, method, from);
        self.obj_seen.remove(&obj.index());
    }

    fn resolve_obj_inner(&mut self, obj: Cursor, method: u32, from: u32) {
        for r in self.lookup_chain(obj) {
            match r {
                Value::Type(node) => self.push_calls(from, self.find_method_in(node, method)),
                Value::LocalDef(node) => {
                    let members = self.find_method_in(node, method);
                    if members.is_empty() && obj.is(C::Object) {
                        self.emit(&Value::LocalDef(node), from);
                    }
                    self.push_calls(from, members);
                }
                Value::Call(n) if let Some(ty) = self.binding_type(n) => {
                    self.resolve_obj(ty, method, from);
                    if obj.is(C::Object) {
                        self.emit(&r, from);
                    }
                }
                _ if obj.is(C::Object) => self.emit(&r, from),
                _ => {}
            }
        }
    }

    fn resolve_implicit(&mut self, sym: u32, from: u32) {
        if self.ssa.is_defined(self.binding(sym), self.cur) {
            let mut bound = self.lookup(sym);
            bound.retain(|r| matches!(r, Value::LocalDef(_) | Value::ImportRef(_)));
            for r in &bound {
                self.emit(r, from);
            }
            return;
        }
        let members = self
            .enclosing_class(from)
            .map(|cls| self.find_method_in(cls, sym))
            .unwrap_or_default();
        if !members.is_empty() {
            self.push_calls(from, members);
            return;
        }
        self.resolve_name(sym, from, Wildcards::Callable);
    }

    fn resolve_name(&mut self, sym: u32, from: u32, fallback: Wildcards) {
        let mut targets = self.lookup(sym);
        if targets.is_empty() {
            let callable_import = |&n: &u32| {
                let import = self.tree.cursor(n).parent();
                import.is_some_and(|i| i.has_tag(self.tags.callable))
            };
            let wild = self.wildcards.iter().filter(|n| match fallback {
                Wildcards::None => false,
                Wildcards::All => true,
                Wildcards::Callable => callable_import(n),
            });
            targets = wild.map(|&n| Value::ImportRef(n)).collect();
        }
        self.emit_targets(&targets, from);
    }

    fn emit_targets(&mut self, targets: &[Value], from: u32) {
        for r in targets {
            match r {
                Value::Type(target) => {
                    let callable = self.tree.cursor(*target).child_sym(C::Callable);
                    let targets =
                        callable.map_or(vec![*target], |name| self.find_method_in(*target, name));
                    self.push_calls(from, targets);
                }
                _ => self.emit(r, from),
            }
        }
    }

    fn enclosing_class(&self, node: u32) -> Option<u32> {
        let c = self.tree.cursor(node);
        let class = Some(c)
            .filter(|c| c.is_class())
            .or_else(|| c.enclosing_def(CLASS_LIKE));
        class.map(|n| n.index())
    }

    fn flow_chain_root(&mut self, obj: Cursor, from: u32) {
        for r in self.lookup_chain(obj.chain_root()) {
            if let Value::Call(n) = r
                && self.binding_type(n).is_some()
            {
                self.edges.push(Edge::local(from, n, EdgeKind::TypeFlow));
            }
        }
    }

    fn binding_type(&self, node: u32) -> Option<Cursor<'t>> {
        let v = self.tree.cursor(node);
        if v.is(C::Binding) {
            v.typed()
        } else {
            v.child(C::Callee).filter(|_| v.is(C::Call))
        }
    }

    fn field_value(&self, def: u32) -> Option<Value> {
        let d = self.tree.cursor(def);
        let binding = d.child(C::Binding).filter(|b| b.typed().is_some())?;
        let stored = d.has(C::FieldDef) || d.has(C::Property);
        stored.then(|| Value::Call(Self::producer_of(binding)))
    }

    fn producer_of(binding: Cursor) -> u32 {
        let call = binding.child(C::Rhs).and_then(|r| r.child(C::Call));
        let member_call = call
            .filter(|_| !binding.has(C::SsaTyped))
            .filter(|c| c.child(C::Callee).is_some_and(|k| k.has(C::Member)));
        member_call.map_or(binding.index(), |c| c.index())
    }

    fn ivar_type(&self, class: u32, attr: u32) -> Option<Cursor<'t>> {
        let mut ivars = self.ivars.borrow_mut();
        let index = ivars.entry(class).or_insert_with(|| {
            let mut m = FxHashMap::default();
            for n in self.tree.cursor(class).descendants() {
                if n.is(C::Binding)
                    && let Some(iv) = n.child_sym(C::Ivar)
                    && n.typed().is_some()
                {
                    m.entry(iv).or_insert(n.index());
                }
            }
            m
        });
        index.get(&attr).map(|&n| self.tree.cursor(n))
    }

    /// `find_method_in` for one container, indexed on first use.
    fn member_of(&self, container: u32, name: u32) -> Option<u32> {
        let mut members = self.members.borrow_mut();
        let index = members.entry(container).or_insert_with(|| {
            let class = self.tree.cursor(container);
            let mut m = FxHashMap::default();
            let nested = |n: Cursor| n.index() != container && n.is_class() && !n.has(C::ImplBlock);
            for n in class.descendants_pruned(nested).filter(|n| n.is(C::Def)) {
                if let Some(dn) = n.child_sym(C::DefName) {
                    m.entry(dn).or_insert(n.index());
                }
            }
            m
        });
        index.get(&name).copied()
    }

    fn chain_is_class(&mut self, c: Cursor) -> bool {
        let targets = self.lookup_chain(c);
        self.any_class(&targets)
    }

    // x = x.foo() roots x's type in itself: in-progress nodes yield nothing.
    fn lookup_chain(&mut self, c: Cursor) -> Vec<Value> {
        if let Some(done) = self.chain_memo.get(&c.index()) {
            return done.clone();
        }
        if !self.chain_seen.insert(c.index()) {
            return vec![];
        }
        let out = self.lookup_chain_inner(c);
        self.chain_seen.remove(&c.index());
        self.chain_memo.insert(c.index(), out.clone());
        out
    }

    fn lookup_chain_inner(&mut self, c: Cursor) -> Vec<Value> {
        if let Some(call) = c.child(C::Call)
            && !(call.has(C::Property) && self.chain_is_class(c.reference()))
        {
            return vec![Value::Call(call.index())];
        }
        let c = c.reference();
        let Some(m) = c.has(C::Object).then_some(c).or_else(|| c.child(C::Member)) else {
            let ivar = c.child(C::Ivar);
            let sym = ivar.map_or(c.sym(), |iv| iv.sym());
            let mut bound = if ivar.is_some() {
                vec![]
            } else {
                self.lookup(sym)
            };
            if bound.is_empty() {
                let field = self
                    .enclosing_class(self.enclosing())
                    .and_then(|cls| self.ivar_type(cls, sym));
                bound.extend(field.map(|f| Value::Call(Self::producer_of(f))));
            }
            return bound
                .into_iter()
                .map(|r| match r {
                    Value::LocalDef(d) => self.field_value(d).unwrap_or(r),
                    _ => r,
                })
                .collect();
        };
        let Some(obj) = m.child(C::Object) else {
            return vec![];
        };
        let mut targets = Vec::new();
        for r in self.lookup_chain(obj) {
            match r {
                Value::Type(node) => targets.push(Value::LocalDef(node)),
                Value::Call(n) if let Some(ty) = self.binding_type(n) => {
                    targets.extend(self.lookup_chain(ty));
                    targets.push(r);
                }
                _ => targets.push(r),
            }
        }
        targets
            .into_iter()
            .flat_map(|r| match r {
                Value::LocalDef(d) => self
                    .find_method_in(d, m.sym())
                    .into_iter()
                    .map(Value::LocalDef)
                    .collect(),
                Value::ImportRef(_) => vec![r],
                _ => vec![],
            })
            .collect()
    }

    fn push_calls(&mut self, from: u32, targets: Vec<u32>) {
        for m in targets {
            self.edges.push(Edge::local(from, m, EdgeKind::Calls));
        }
    }

    fn find_method_in(&self, container: u32, name: u32) -> Vec<u32> {
        let wrappers = |dn: u32| {
            let c = self.tree.cursor(dn);
            let same_name = c
                .child_sym(C::DefName)
                .into_iter()
                .flat_map(|n| self.defs_named(n))
                .filter(move |&d| {
                    d != dn && (c.has(C::ImplBlock) || self.tree.cursor(d).has(C::ImplBlock))
                });
            std::iter::once(dn).chain(same_name).collect::<Vec<_>>()
        };
        let supers = |dn: u32| {
            let supers = self.supertypes.get(&dn).into_iter().flatten().copied();
            supers.flat_map(wrappers).collect::<Vec<_>>()
        };
        let found = |dn| self.member_of(dn, name);
        members_by_level(wrappers(container), supers, found)
    }
}

pub fn link(tree: &Tree, env: &Env, run: &Sentinel) -> Result<Vec<Edge>, Killed> {
    let (lang, config) = (&env.lang, &env.rules_for(&tree.label).config.link);
    let file = Sentinel::new("link", &tree.label, env.limits.file_link_ms);
    let tags = ReservedTags::new(lang);
    let root = tree.root();
    let root_label = root.tag(tags.scoped).unwrap_or(root.sym());
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        tree,
        ssa,
        cur: entry,
        registered: FxHashSet::default(),
        defs: Vec::new(),
        defs_by_name: FxHashMap::default(),
        members: RefCell::default(),
        ivars: RefCell::default(),
        supertypes: FxHashMap::default(),
        wildcards: Vec::new(),
        tags,
        config,
        syms: &lang.syms,
        run,
        file,
        killed: None,
        def_stack: Vec::new(),
        chain_seen: FxHashSet::default(),
        obj_seen: FxHashSet::default(),
        chain_memo: FxHashMap::default(),
        wildcard: lang.syms.intern(WILDCARD),
        edges: Vec::new(),
        value_sink: FxHashMap::default(),
        pending_calls: Vec::new(),
        fields: FxHashMap::default(),
        scopes: vec![(root_label, FxHashMap::default())],
        keys: RefCell::default(),
    };

    f.predeclare(root);
    f.walk_children(root);
    if let Some(k) = f.killed.take() {
        return Err(k);
    }

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    for (value, from, site, fallback) in std::mem::take(&mut f.pending_calls) {
        f.run.check().and_then(|()| f.file.check())?;
        let first = f.edges.len();
        let mut targets = f.ssa.resolve_value(&value);
        if targets.is_empty() {
            targets.extend(fallback.into_iter().map(Value::ImportRef));
        }
        f.emit_targets(&targets, from);
        for edge in &mut f.edges[first..] {
            edge.site = Some(site);
        }
    }

    f.cur = entry;
    for dn in f.defs.clone() {
        f.run.check().and_then(|()| f.file.check())?;
        for c in tree.cursor(dn).children().filter(|c| c.is(C::Decorator)) {
            for target in f.lookup_chain(c) {
                match target {
                    Value::LocalDef(target) => {
                        f.edges.push(Edge::local(dn, target, EdgeKind::Calls))
                    }
                    Value::ImportRef(target) => f.edges.push(Edge {
                        site: Some(c.index()),
                        ..Edge::local(dn, target, EdgeKind::Imports)
                    }),
                    _ => {}
                }
            }
        }
    }

    Ok(f.edges)
}

fn child_def<'t>(module: Cursor<'t>, name: u32) -> Option<Cursor<'t>> {
    module
        .children()
        .find(|d| d.is(C::Def) && d.child_sym(C::DefName) == Some(name))
}
