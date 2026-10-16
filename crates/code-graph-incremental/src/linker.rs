use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::smallvec;

use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, RED_ZONE, STACK_SEGMENT, WILDCARD};
use crate::env::Env;
use crate::resolver::CLASS_LIKE;
use crate::rules::LinkConfig;
use crate::sentinel::{Killed, Sentinel};
use crate::ssa::{BlockId, SsaEngine, Value};
use crate::tags::ReservedTags;
use crate::tree::{Compact, Edge, EdgeKind, members_by_level};
type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

type WildcardImports = Rc<Vec<u32>>;

fn edge_limit(edges: &[Edge], cap: usize, path: &str) -> Result<(), Killed> {
    if cap == 0 || edges.len() <= cap {
        return Ok(());
    }
    Err(Killed::limit("edges", path, edges.len(), cap))
}

enum WorkItem {
    Visit(u32),
    ExitScope {
        wildcards: usize,
        block: Option<BlockId>,
    },
    EnterArms {
        branch: u32,
        lhs: Option<u32>,
    },
    EnterArm {
        arm: u32,
        pre: BlockId,
        lhs: Option<u32>,
    },
    ExitArm {
        tail: u32,
        lhs: Option<u32>,
    },
    JoinArms {
        pre: BlockId,
        lhs: Option<u32>,
        first_exit: usize,
    },
    ExitLoop(BlockId),
    ExitBinding {
        binding: u32,
        lhs: u32,
        first_edge: usize,
        declaration: bool,
        branch: bool,
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
    All,
    Callable,
}

struct Fold<'t> {
    tree: &'t Tree,
    max_edges: usize,
    ssa: SsaEngine,
    cur: BlockId,
    registered: FxHashSet<u32>,
    defs: Vec<u32>,
    defs_by_name: FxHashMap<u32, Vec<u32>>,
    /// Per class: member name to first def, and ivar name to its typed binding.
    members: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    ivars: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    supertypes: FxHashMap<u32, Vec<u32>>,
    wildcards: WildcardImports,
    tags: ReservedTags,
    config: &'t LinkConfig,
    syms: &'t crate::intern::Interner,
    run: &'t Sentinel,
    file: Sentinel,
    killed: Option<Killed>,
    limited: Option<Killed>,
    def_stack: Vec<u32>,
    chain_seen: FxHashSet<u32>,
    obj_seen: FxHashSet<u32>,
    chain_memo: FxHashMap<u32, Vec<Value>>,
    member_memo: FxHashMap<(u32, u32), Vec<Value>>,
    member_busy: FxHashSet<(u32, u32)>,
    member_memo_at: (BlockId, usize, usize, usize),
    loops: Vec<(BlockId, (u32, u32))>,
    loop_ends: FxHashMap<u32, u32>,
    assigned: FxHashMap<u32, Vec<u32>>,
    loop_wildcards: Vec<u32>,
    wildcard: u32,
    edges: Vec<Edge>,
    value_sink: FxHashMap<u32, u32>,
    pending_calls: Vec<(Value, u32, u32, Option<WildcardImports>)>,
    shadowed_calls: FxHashSet<u32>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    scopes: Vec<(u32, Undo)>,
    visible: FxHashMap<u32, (u32, usize)>,
    keys: RefCell<FxHashMap<BindingKey, u32>>,
    arm_exits: Vec<BlockId>,
}

type Undo = Vec<(u32, Option<(u32, usize)>)>;

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
    fn enclosing(&self) -> u32 {
        self.def_stack.last().copied().unwrap_or(0)
    }

    /// Stops at the first failed check; `link` reports it after the walk.
    fn run(&mut self, mut stack: Vec<WorkItem>) {
        while let Some(item) = stack.pop() {
            if self.killed.is_some() {
                return;
            }
            if let Err(k) = self
                .run
                .check()
                .and_then(|()| self.file.check())
                .and_then(|()| edge_limit(&self.edges, self.max_edges, &self.tree.label))
            {
                self.killed = Some(k);
                return;
            }
            if !matches!(item, WorkItem::Visit(_)) {
                self.chain_memo.clear();
            }
            match item {
                WorkItem::ExitScope { wildcards, block } => {
                    self.exit_scope();
                    if self.wildcards.len() > wildcards {
                        Rc::make_mut(&mut self.wildcards).truncate(wildcards);
                    }
                    if let Some(block) = block {
                        self.def_stack.pop();
                        self.cur = block;
                    }
                }
                WorkItem::Visit(i) => self.dispatch(self.tree.cursor(i), &mut stack),
                WorkItem::EnterArms { branch, lhs } => {
                    self.enter_arms(self.tree.cursor(branch), lhs, &mut stack)
                }
                WorkItem::EnterArm { arm, pre, lhs } => {
                    self.enter_arm(self.tree.cursor(arm), pre, lhs, &mut stack)
                }
                WorkItem::ExitArm { tail, lhs } => self.exit_arm(self.tree.cursor(tail), lhs),
                WorkItem::JoinArms {
                    pre,
                    lhs,
                    first_exit,
                } => {
                    let exits = self.arm_exits.drain(first_exit..);
                    let exits = exits.chain(lhs.is_none().then_some(pre));
                    self.cur = self.ssa.add_sealed_join(exits);
                }
                WorkItem::ExitLoop(header) => {
                    self.loops.pop();
                    self.cur = self.ssa.finish_loop(header, self.cur);
                }
                WorkItem::ExitBinding {
                    binding,
                    lhs,
                    first_edge,
                    declaration,
                    branch,
                } => self.exit_binding(
                    self.tree.cursor(binding),
                    lhs,
                    first_edge,
                    declaration,
                    branch,
                ),
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
        let k = c.kind();
        if Self::rebinds(c) {
            self.chain_memo.clear();
        }
        if k == C::Import || k == C::ImportType {
            self.handle_import(c);
        } else if c.is(C::Def) {
            self.handle_def(c, stack);
        } else if k == C::Call {
            self.handle_inline_imports(c);
            self.handle_call(c);
        } else if k == C::Member {
            let in_chain = c.parent().is_some_and(|parent| {
                parent.is(C::Callee)
                    || parent.is(C::Object)
                        && parent.parent().is_some_and(|owner| owner.is(C::Member))
            });
            if !in_chain {
                self.handle_standalone_member(c);
            }
        } else if k == C::Binding {
            if self.handle_binding(c, stack) {
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
            Self::handle_branch(c, sink, stack);
            return;
        } else if k == C::SsaLoop {
            if self.loop_ends.is_empty() {
                (self.loop_ends, self.assigned, self.loop_wildcards) =
                    loop_spans(self.tree.root(), self.wildcard);
            }
            let span = (c.index(), self.loop_ends[&c.index()]);
            self.assign_module_names(span);
            self.loops.push((self.cur, span));
            let (header, body) = self.ssa.begin_loop(self.cur);
            self.cur = body;
            stack.push(WorkItem::ExitLoop(header));
            Self::push_children(c, stack);
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

    fn rebinds(c: Cursor<'t>) -> bool {
        let kinds = [
            C::Import,
            C::ImportType,
            C::Binding,
            C::Scope,
            C::Destructure,
            C::SsaBranch,
            C::SsaLoop,
        ];
        c.is(C::Def)
            || kinds.iter().any(|&kind| c.is(kind))
            || c.is(C::Call) && c.children().any(|child| child.is(C::Import))
    }

    fn handle_branch(branch: Cursor<'t>, lhs: Option<u32>, stack: &mut Vec<WorkItem>) {
        stack.push(WorkItem::EnterArms {
            branch: branch.index(),
            lhs,
        });
        stack.extend(
            branch
                .children_rev()
                .filter(|c| !c.is(C::SsaArm))
                .map(|c| WorkItem::Visit(c.index())),
        );
    }

    fn enter_arms(&mut self, branch: Cursor<'t>, lhs: Option<u32>, stack: &mut Vec<WorkItem>) {
        let pre = self.cur;
        let mut arms = branch.children_rev().filter(|c| c.is(C::SsaArm)).peekable();
        if arms.peek().is_none() {
            return;
        }
        stack.push(WorkItem::JoinArms {
            pre,
            lhs,
            first_exit: self.arm_exits.len(),
        });
        stack.extend(arms.map(|arm| WorkItem::EnterArm {
            arm: arm.index(),
            pre,
            lhs,
        }));
    }

    fn enter_arm(
        &mut self,
        arm: Cursor<'t>,
        pre: BlockId,
        lhs: Option<u32>,
        stack: &mut Vec<WorkItem>,
    ) {
        self.cur = self.ssa.add_sealed_successor(pre);
        let tail = arm.tail_expr();
        if let Some(lhs) = lhs.filter(|_| tail.is(C::SsaBranch)) {
            self.value_sink.insert(tail.index(), lhs);
        }
        stack.push(WorkItem::ExitArm {
            tail: tail.index(),
            lhs,
        });
        Self::push_children(arm, stack);
    }

    fn exit_arm(&mut self, tail: Cursor<'t>, lhs: Option<u32>) {
        if let Some(lhs) = lhs.filter(|_| !tail.is(C::SsaBranch) && !tail.is(C::SsaReturn)) {
            let val = self.classify_tail(tail);
            if val != Value::Opaque {
                self.ssa.write_variable(lhs, self.cur, val);
            }
            self.bind_fields(lhs, tail);
        }
        self.arm_exits.push(self.cur);
    }

    fn handle_import(&mut self, c: Cursor<'t>) {
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
            let binding = self.declare_binding(n, local);
            self.ssa
                .write_variable(binding, self.cur, Value::ImportRef(n.index()));
            for alias in [C::Alias, C::SsaHint]
                .into_iter()
                .filter_map(|kind| n.child_sym(kind))
            {
                self.bind(alias, binding);
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
        if c.has(C::ImplBlock) {
            self.link_reference(c.child(C::DefName).unwrap());
        }
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
        let root = callee
            .child(C::Member)
            .and_then(|member| member.child(C::Object))
            .map_or(callee, |object| object.chain_root());
        if !callee.has(C::Ivar)
            && !root.is(C::Call)
            && !root.has(C::Call)
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
                    self.lookup(object.sym()).iter().any(|value| {
                        matches!(value, Value::LocalDef(node) if self.tree.cursor(*node).is_dispatch_contract())
                    })
                }) {
                    return;
                }
            }
        }
        let value = self
            .field_slot(callee)
            .map(|slot| self.read_value(slot))
            .or_else(|| {
                let sym = callee
                    .sym_opt()
                    .filter(|_| !c.has_tag(self.tags.implicit_self))?;
                let block = self.name_block(sym);
                let value = self.ssa.read_variable_internal(self.binding(sym), block);
                matches!(value, Value::Phi(_)).then_some(value)
            });
        if let Some(value) = value {
            let fallback =
                (callee.sym_opt().is_some() && !callee.has(C::Member) && !callee.has(C::Ivar))
                    .then(|| Rc::clone(&self.wildcards));
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
                self.resolve_name(sym, from, Wildcards::All);
            }
        }
        self.finish_call(first, c.index());
    }

    fn finish_call(&mut self, first: usize, site: u32) {
        let call = self.tree.cursor(site);
        let callee = (!call.has(C::Property))
            .then(|| call.child(C::Callee).and_then(callee_name))
            .flatten();
        for i in first..self.edges.len() {
            let target = self.tree.cursor(self.edges[i].to_node);
            let remap = callee.is_some()
                && self.edges[i].kind == EdgeKind::Calls
                && target.child_sym(C::DefName) == callee;
            let to = match remap {
                true => self.callable_in(target),
                false => target.index(),
            };
            let edge = &mut self.edges[i];
            edge.site = Some(site);
            edge.to_node = to;
        }
        self.dedupe_from(first);
    }

    fn callable_in(&self, target: Cursor<'t>) -> u32 {
        let found = crate::tree::callable_target(target);
        if !found.is_field() {
            return found.index();
        }
        self.type_parts(found, true)
            .find_map(|part| self.member_of(part, found.child_sym(C::DefName)?))
            .filter(|&member| !self.tree.cursor(member).is_field())
            .unwrap_or(found.index())
    }

    fn field_in(&self, target: Cursor<'t>) -> u32 {
        let found = crate::tree::field_target(target);
        if found.is_field() {
            return found.index();
        }
        self.type_parts(found, false)
            .find_map(|part| self.member_of(part, found.child_sym(C::DefName)?))
            .filter(|&member| self.tree.cursor(member).is_field())
            .unwrap_or(found.index())
    }

    fn type_parts(&self, member: Cursor<'t>, blocks: bool) -> impl Iterator<Item = u32> + '_ {
        member
            .ancestors()
            .find(|node| node.is_class())
            .and_then(|class| class.child_sym(C::DefName))
            .into_iter()
            .flat_map(|name| self.defs_named(name))
            .filter(move |&part| self.tree.cursor(part).has(C::ImplBlock) == blocks)
    }

    fn dedupe_from(&mut self, first: usize) {
        if self.edges.len() - first < 2 {
            return;
        }
        let mut seen = FxHashSet::default();
        let tail: Vec<Edge> = self
            .edges
            .drain(first..)
            .filter(|e| seen.insert((e.kind, e.to_node)))
            .collect();
        self.edges.extend(tail);
    }

    fn handle_standalone_member(&mut self, c: Cursor<'t>) {
        if self.field_slot(c).is_some() {
            return;
        }
        if let Some(obj) = c.child(C::Object) {
            let first = self.edges.len();
            self.resolve_obj(obj, c.sym(), self.enclosing());
            for i in first..self.edges.len() {
                let target = self.tree.cursor(self.edges[i].to_node);
                let to = match self.edges[i].kind == EdgeKind::Calls {
                    true => self.field_in(target),
                    false => target.index(),
                };
                let edge = &mut self.edges[i];
                edge.call_resolution = crate::tree::CallResolution::Reference;
                edge.to_node = to;
            }
            self.dedupe_from(first);
        }
    }

    fn link_reference(&mut self, reference: Cursor<'t>) {
        self.handle_inline_imports(reference);
        if let [Value::LocalDef(target) | Value::ImportRef(target)] =
            self.lookup_chain(reference).as_slice()
            && *target != reference.parent().map_or(reference.index(), Cursor::index)
        {
            self.edges.push(Edge {
                call_resolution: crate::tree::CallResolution::Reference,
                ..Edge::local(reference.index(), *target, EdgeKind::Imports)
            });
        }
    }

    fn handle_binding(&mut self, c: Cursor<'t>, stack: &mut Vec<WorkItem>) -> bool {
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
        let branch = rhs.and_then(|r| r.child(C::SsaBranch));
        stack.push(WorkItem::ExitBinding {
            binding: c.index(),
            lhs,
            first_edge: self.edges.len(),
            declaration,
            branch: branch.is_some(),
        });
        if let Some(branch) = branch {
            Self::handle_branch(branch, Some(lhs), stack);
        } else if let Some(rhs) = rhs {
            Self::push_children(rhs, stack);
        }
        true
    }

    fn exit_binding(
        &mut self,
        c: Cursor<'t>,
        lhs: u32,
        first_edge: usize,
        declaration: bool,
        branch: bool,
    ) {
        let rhs = c.child(C::Rhs);
        if !branch {
            if let Some(rhs) = rhs {
                for edge in &mut self.edges[first_edge..] {
                    if rhs.child(C::Member).is_some()
                        && edge.kind == EdgeKind::Imports
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
                for value in self.lookup(rhs.sym()) {
                    match value {
                        Value::ImportRef(node) => {
                            val = Value::Call(c.index());
                            self.edges.push(Edge {
                                site: Some(c.index()),
                                call_resolution: crate::tree::CallResolution::Reference,
                                ..Edge::local(self.enclosing(), node, EdgeKind::Imports)
                            });
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
        if declaration {
            self.bind(c.sym(), lhs);
        }
    }

    fn read_value(&mut self, binding: u32) -> Value {
        match self.ssa.read_variable_internal(binding, self.cur) {
            Value::Undefined => Value::Opaque,
            value => value,
        }
    }

    fn binding(&self, name: u32) -> u32 {
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

    fn bind(&mut self, name: u32, binding: u32) {
        let Some(depth) = self.scopes.len().checked_sub(1) else {
            return;
        };
        let previous = self.visible.insert(name, (binding, depth));
        if previous.is_none_or(|(_, at)| at != depth) {
            self.scopes[depth].1.push((name, previous));
        }
    }

    fn exit_scope(&mut self) {
        for (name, previous) in self.scopes.pop().map(|(_, undo)| undo).unwrap_or_default() {
            match previous {
                Some(previous) => self.visible.insert(name, previous),
                None => self.visible.remove(&name),
            };
        }
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
        self.bind(name, binding);
        binding
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
            if source != lhs
                && self.reaches(source, lhs)
                && let Some(shared) = self.fields.get(&source).cloned()
            {
                self.fields.insert(lhs, shared);
                return;
            }
            self.fields.get(&source).cloned().unwrap_or_default()
        };
        let mut copies: smallvec::SmallVec<[_; 4]> = smallvec![(lhs, sources)];
        let mut field_updates = smallvec::SmallVec::<[_; 4]>::new();
        let mut value_updates = smallvec::SmallVec::<[_; 8]>::new();
        let mut copied = FxHashMap::default();
        let mut reset = FxHashSet::default();
        let mut aliases = smallvec::SmallVec::<[_; 4]>::new();
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
        let block = self.name_block(sym);
        let value = match self.ssa.read_variable_internal(self.binding(sym), block) {
            Value::Undefined => Value::Opaque,
            value => value,
        };
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

    fn assign_module_names(&mut self, (start, end): (u32, u32)) {
        let first = self.loop_wildcards.partition_point(|&at| at < start);
        let last = self.loop_wildcards.partition_point(|&at| at < end);
        let imports: Vec<u32> = self.loop_wildcards.drain(first..last).collect();
        for at in imports {
            let Some(module) = self.inline_module(self.tree.cursor(at)) else {
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

    fn lookup(&mut self, sym: u32) -> Vec<Value> {
        for block in [self.name_block(sym), BlockId(0)] {
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
        stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
            self.resolve_obj_inner(obj, method, from)
        });
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
            if !bound.is_empty() {
                bound.retain(|r| matches!(r, Value::LocalDef(_) | Value::ImportRef(_)));
                for r in &bound {
                    self.emit(r, from);
                }
                return;
            }
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
        for target in &mut targets {
            if let Value::LocalDef(d) = target {
                *d = crate::tree::callable_target(self.tree.cursor(*d)).index();
            }
        }
        if targets.is_empty() {
            let callable_import = |&n: &u32| {
                let import = self.tree.cursor(n).parent();
                import.is_some_and(|i| i.has_tag(self.tags.callable))
            };
            let wild = self.wildcards.iter().filter(|n| match fallback {
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
        let out = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || self.lookup_chain_inner(c));
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
                    Value::LocalDef(d) => {
                        let d = match c.is(C::Callee) {
                            true => crate::tree::callable_target(self.tree.cursor(d)),
                            false => crate::tree::field_target(self.tree.cursor(d)),
                        }
                        .index();
                        self.field_value(d).unwrap_or(Value::LocalDef(d))
                    }
                    _ => r,
                })
                .collect();
        };
        let Some(obj) = m.child(C::Object) else {
            return vec![];
        };
        let name = obj.reference();
        let bare = obj.child(C::Call).is_none()
            && !name.has(C::Object)
            && name.child(C::Member).is_none()
            && name.child(C::Ivar).is_none();
        let key = bare.then(|| (name.sym(), m.sym()));
        let at = (
            self.cur,
            self.ssa.writes(),
            self.scopes.len(),
            self.def_stack.len(),
        );
        if self.member_memo_at != at {
            self.member_memo.clear();
            self.member_memo_at = at;
        }
        if let Some(found) = key.and_then(|key| self.member_memo.get(&key)) {
            return found.clone();
        }
        if let Some(key) = key
            && !self.member_busy.insert(key)
        {
            return vec![];
        }
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
        let found: Vec<Value> = targets
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
            .collect();
        if let Some(key) = key {
            self.member_busy.remove(&key);
            self.member_memo.insert(key, found.clone());
        }
        found
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

pub fn link(tree: &Tree, env: &Env, run: &Sentinel) -> Result<(Vec<Edge>, Option<Killed>), Killed> {
    let (lang, config) = (&env.lang, &env.rules_for(&tree.label).config.link);
    let file = Sentinel::new("link", &tree.label, env.limits.file_link_ms).on_thread_cpu();
    let tags = ReservedTags::new(lang);
    let root = tree.root();
    let root_label = root.tag(tags.scoped).unwrap_or(root.sym());
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        tree,
        max_edges: env.limits.max_file_edges,
        ssa,
        cur: entry,
        registered: FxHashSet::default(),
        defs: Vec::new(),
        defs_by_name: FxHashMap::default(),
        members: RefCell::default(),
        ivars: RefCell::default(),
        supertypes: FxHashMap::default(),
        wildcards: Rc::default(),
        tags,
        config,
        syms: &lang.syms,
        run,
        file,
        killed: None,
        limited: None,
        def_stack: Vec::new(),
        chain_seen: FxHashSet::default(),
        obj_seen: FxHashSet::default(),
        chain_memo: FxHashMap::default(),
        member_memo: FxHashMap::default(),
        member_busy: FxHashSet::default(),
        member_memo_at: (entry, 0, 0, 0),
        loops: Vec::new(),
        loop_ends: FxHashMap::default(),
        assigned: FxHashMap::default(),
        loop_wildcards: Vec::new(),
        wildcard: lang.syms.intern(WILDCARD),
        edges: Vec::new(),
        value_sink: FxHashMap::default(),
        pending_calls: Vec::new(),
        shadowed_calls: FxHashSet::default(),
        fields: FxHashMap::default(),
        scopes: vec![(root_label, Vec::new())],
        visible: FxHashMap::default(),
        keys: RefCell::default(),
        arm_exits: Vec::new(),
    };

    f.predeclare(root);
    f.walk_children(root);
    if let Some(k) = f.killed.take() {
        return Err(k);
    }

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    for (value, from, site, fallback) in std::mem::take(&mut f.pending_calls) {
        f.run
            .check()
            .and_then(|()| f.file.check())
            .and_then(|()| edge_limit(&f.edges, f.max_edges, &tree.label))?;
        let first = f.edges.len();
        let mut targets = f.ssa.resolve_value(&value);
        if targets.is_empty() {
            targets.extend(
                fallback
                    .iter()
                    .flat_map(|w| w.iter())
                    .map(|&w| Value::ImportRef(w)),
            );
        }
        f.emit_targets(&targets, from);
        f.finish_call(first, site);
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

    for edge in &mut f.edges {
        if edge.kind == EdgeKind::TypeFlow && f.shadowed_calls.contains(&edge.to_node) {
            edge.call_resolution = crate::tree::CallResolution::NonCallable;
        }
        if edge.kind == EdgeKind::Imports
            && (tree.cursor(edge.to_node).sym() == f.wildcard
                || tree.cursor(edge.to_node).child_sym(C::SsaHint) == Some(f.wildcard))
            && edge
                .site
                .and_then(|site| tree.cursor(site).child_sym(C::Callee))
                .is_some_and(|name| {
                    config.builtins.contains(&name)
                        || tree
                            .cursor(edge.to_node)
                            .parent()
                            .and_then(|import| import.child_sym(C::SourcePath))
                            .is_some_and(|source| {
                                config.providers.iter().any(|(module, symbols)| {
                                    *module == source && symbols.contains(&name)
                                })
                            })
                })
        {
            edge.call_resolution = crate::tree::CallResolution::Reference;
        }
    }
    Ok((f.edges, f.limited))
}

fn callee_name(callee: Cursor) -> Option<u32> {
    callee
        .child(C::Member)
        .map(|member| member.sym())
        .or_else(|| callee.child(C::Ivar).map(|ivar| ivar.sym()))
        .or_else(|| callee.sym_opt())
}

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
