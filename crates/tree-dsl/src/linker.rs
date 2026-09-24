use std::cell::RefCell;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::env::Env;
use crate::resolver::CLASS_LIKE;
use crate::rules::LinkConfig;
use crate::sentinel::{Killed, Sentinel};
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tags::ReservedTags;
use crate::tree::{Cursor, Edge, EdgeKind, Tree, members_by_level};

#[derive(Clone)]
enum Linked {
    Def(u32),
    Import(u32),
    Type(u32),
    Call(u32),
}

enum WorkItem {
    Visit(u32),
    ExitScope(usize),
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
    predeclared: FxHashMap<u32, u32>,
    defs: Vec<u32>,
    defs_by_name: FxHashMap<u32, Vec<u32>>,
    /// Per class: member name to first def, and ivar name to its typed binding.
    members: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    ivars: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    supertypes: FxHashMap<u32, Vec<u32>>,
    imports: Vec<u32>,
    wildcards: Vec<u32>,
    tags: ReservedTags,
    config: &'t LinkConfig,
    run: &'t Sentinel,
    file: Sentinel,
    killed: Option<Killed>,
    def_stack: Vec<(Option<u32>, BlockId)>,
    chain_seen: FxHashSet<u32>,
    obj_seen: FxHashSet<u32>,
    chain_memo: FxHashMap<u32, Vec<Linked>>,
    wildcard: u32,
    edges: Vec<Edge>,
    value_sink: FxHashMap<u32, u32>,
}

impl<'t> Fold<'t> {
    fn enclosing(&self) -> u32 {
        self.def_stack.last().and_then(|&(d, _)| d).unwrap_or(0)
    }

    fn exit_scope(&mut self, wildcards: usize) {
        self.wildcards.truncate(wildcards);
        if self.def_stack.len() > 1
            && let Some((_, saved)) = self.def_stack.pop()
        {
            self.cur = saved;
        }
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
                WorkItem::ExitScope(wildcards) => self.exit_scope(wildcards),
                WorkItem::Visit(i) => self.dispatch(self.tree.cursor(i), &mut stack),
            }
        }
    }

    fn push_children(c: Cursor, stack: &mut Vec<WorkItem>) {
        stack.extend(c.children_rev().map(|ch| WorkItem::Visit(ch.index())));
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
            self.handle_call(c);
            Self::push_children(c, stack);
        } else if k == C::Member {
            let is_callee = c.parent().is_some_and(|p| p.kind() == C::Callee);
            if !is_callee {
                self.handle_standalone_member(c);
            }
            Self::push_children(c, stack);
        } else if k == C::Binding {
            if !self.handle_binding(c) {
                Self::push_children(c, stack);
            }
        } else if k == C::Destructure {
            for slot in c.children().filter(|s| s.is(C::Binding)) {
                self.ssa
                    .write_variable(slot.sym(), self.cur, Value::Call(slot.index()));
            }
            stack.extend(
                c.children()
                    .filter(|s| !s.is(C::Binding))
                    .map(|s| WorkItem::Visit(s.index())),
            );
        } else if k == C::SsaBranch {
            let sink = self.value_sink.remove(&c.index());
            self.handle_branch(c, sink);
        } else if k == C::SsaLoop {
            self.handle_loop(c);
        } else {
            Self::push_children(c, stack);
        }
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
        for n in c.names() {
            let sym = n.sym();
            let local = n
                .child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(sym);
            if !self.config.imports_shadow_locals
                && self
                    .lookup(local)
                    .iter()
                    .any(|r| matches!(r, Linked::Def(_)))
            {
                continue;
            }
            let import_idx = self.imports.len() as u32;
            self.imports.push(n.index());
            self.wildcards
                .extend((local == self.wildcard).then_some(n.index()));
            self.ssa
                .write_variable(sym, self.cur, Value::ImportRef(import_idx));
            for kind in [C::Alias, C::SsaHint] {
                if let Some(alias) = n.child_sym(kind)
                    && alias != sym
                {
                    self.ssa
                        .write_variable(alias, self.cur, Value::ImportRef(import_idx));
                }
            }
        }
    }

    fn handle_def(&mut self, c: Cursor<'t>, stack: &mut Vec<WorkItem>) {
        let Some(name) = c.child_sym(C::DefName) else {
            return;
        };
        let idx = c.index();
        let def_idx = match self.predeclared.remove(&idx) {
            Some(i) => i,
            None => self.register_def(idx),
        };
        let supers = c
            .children()
            .filter(|s| s.is(C::SuperType))
            .flat_map(|s| self.lookup_chain(s))
            .filter_map(|r| match r {
                Linked::Def(d) => Some(d),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.supertypes.insert(idx, supers.clone());
        self.declare(c, name, def_idx);
        let parent_block = self.cur;
        self.cur = self.ssa.add_sealed_successor(parent_block);
        if let Some(&(Some(parent), _)) = self.def_stack.last() {
            self.edges.push(Edge::local(parent, idx, EdgeKind::Defines));
        }
        for dn in supers {
            self.edges.push(Edge::local(idx, dn, EdgeKind::Extends));
        }
        if c.has_tag(self.tags.scoped) {
            if c.has_tag(self.tags.hoisted) {
                self.predeclare(c);
            }
            self.def_stack.push((Some(idx), parent_block));
            stack.push(WorkItem::ExitScope(self.wildcards.len()));
            Self::push_children(c, stack);
        }
    }

    fn predeclare(&mut self, scope: Cursor<'t>) {
        for d in scope.children().filter(|d| d.is(C::Def)) {
            if let Some(name) = d.child_sym(C::DefName) {
                let def_idx = self.register_def(d.index());
                self.predeclared.insert(d.index(), def_idx);
                self.declare(d, name, def_idx);
            }
        }
    }

    fn declare(&mut self, c: Cursor<'t>, name: u32, def_idx: u32) {
        for sym in std::iter::once(name).chain(c.children_of(C::Alias).map(|a| a.sym())) {
            if sym == name && c.has(C::ImplBlock) && !self.lookup(name).is_empty() {
                continue;
            }
            self.ssa
                .write_variable(sym, self.cur, Value::LocalDef(def_idx));
        }
    }

    fn defs_named(&self, sym: u32) -> impl Iterator<Item = u32> + '_ {
        self.defs_by_name.get(&sym).into_iter().flatten().copied()
    }

    fn register_def(&mut self, node: u32) -> u32 {
        self.defs.push(node);
        if let Some(name) = self.tree.cursor(node).child_sym(C::DefName) {
            self.defs_by_name.entry(name).or_default().push(node);
        }
        self.defs.len() as u32 - 1
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
        if let Some(m) = callee.child(C::Member) {
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
        if let Some(obj) = c.child(C::Object) {
            self.resolve_obj(obj, c.sym(), self.enclosing());
        }
    }

    fn handle_binding(&mut self, c: Cursor<'t>) -> bool {
        let lhs = c.sym();
        if lhs == 0 || c.has(C::Ivar) {
            return false;
        }
        if self.ssa.has_variable_in_block(lhs, self.cur) {
            self.cur = self.ssa.add_sealed_successor(self.cur);
        }

        let rhs = c.child(C::Rhs);
        if let Some(branch) = rhs.and_then(|r| r.child(C::SsaBranch)) {
            self.handle_branch(branch, Some(lhs));
            return true;
        }
        if let Some(rhs) = rhs {
            self.walk_children(rhs);
        }
        let val = match (c.child_sym(C::SsaTyped), rhs) {
            (Some(_), _) => Value::Call(c.index()),
            (None, Some(rhs)) if rhs.tail_expr().is(C::Member) => Value::Call(c.index()),
            (None, Some(rhs)) => self.classify_tail(rhs.tail_expr()),
            (None, None) => Value::Opaque,
        };
        self.ssa.write_variable(lhs, self.cur, val);
        true
    }

    fn classify_tail(&mut self, tail: Cursor<'_>) -> Value {
        if tail.is(C::Call) {
            return Value::Call(tail.index());
        }
        let sym = self.tail_sym(tail);
        if sym == 0 {
            return Value::Opaque;
        }
        let r = self.lookup(sym);
        if self.any_class(&r) {
            Value::Type(sym)
        } else {
            Value::Alias(sym)
        }
    }

    fn tail_sym(&self, node: Cursor<'_>) -> u32 {
        match node.last_named() {
            Some(c) if c.size() > 1 => self.tail_sym(c),
            Some(c) => c.sym(),
            None => node.sym(),
        }
    }

    fn to_linked(&self, pv: &ParseValue) -> Option<Linked> {
        match pv {
            ParseValue::LocalDef(di) => Some(Linked::Def(self.defs[*di as usize])),
            ParseValue::ImportRef(ii) => self.imports.get(*ii as usize).map(|&n| Linked::Import(n)),
            ParseValue::Type(ts) if *ts != 0 => Some(Linked::Type(*ts)),
            ParseValue::Call(call) => Some(Linked::Call(*call)),
            _ => None,
        }
    }

    fn lookup(&mut self, sym: u32) -> Vec<Linked> {
        for block in [self.cur, BlockId(0)] {
            let vals = self.ssa.read_variable(sym, block);
            let result: Vec<Linked> = vals.iter().filter_map(|pv| self.to_linked(pv)).collect();
            if !result.is_empty() {
                return result;
            }
        }
        Vec::new()
    }

    fn emit(&mut self, r: &Linked, from: u32) {
        match r {
            Linked::Def(node) => {
                if self.tree.cursor(*node).has_tag(self.tags.callable) {
                    self.edges.push(Edge::local(from, *node, EdgeKind::Calls));
                }
            }
            Linked::Import(node) => self.edges.push(Edge::local(from, *node, EdgeKind::Imports)),
            Linked::Call(call) => self
                .edges
                .push(Edge::local(from, *call, EdgeKind::TypeFlow)),
            Linked::Type(_) => {}
        }
    }

    fn any_class(&self, resolved: &[Linked]) -> bool {
        resolved
            .iter()
            .any(|r| matches!(r, Linked::Def(n) if self.tree.cursor(*n).is_class()))
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
                Linked::Type(ts) => self.resolve_method(ts, method, from),
                Linked::Def(node) => {
                    let members = self.find_method_in(node, method);
                    if members.is_empty() && obj.is(C::Object) {
                        self.emit(&Linked::Def(node), from);
                    }
                    self.push_calls(from, members);
                }
                Linked::Call(n) if let Some(ty) = self.binding_type(n) => {
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
        if self.ssa.is_defined(sym, self.cur) {
            let mut bound = self.lookup(sym);
            bound.retain(|r| matches!(r, Linked::Def(_) | Linked::Import(_)));
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
            targets = wild.map(|&n| Linked::Import(n)).collect();
        }
        for r in &targets {
            match r {
                Linked::Type(ts) => {
                    for inner in self.lookup(*ts) {
                        if let Linked::Def(target) = inner {
                            let callable = self.tree.cursor(target).child_sym(C::Callable);
                            let targets = callable
                                .map_or(vec![target], |name| self.find_method_in(target, name));
                            self.push_calls(from, targets);
                        }
                    }
                }
                _ => self.emit(r, from),
            }
        }
    }

    fn resolve_method(&mut self, type_sym: u32, method: u32, from: u32) {
        for r in self.lookup(type_sym) {
            if let Linked::Def(cls) = r {
                self.push_calls(from, self.find_method_in(cls, method));
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
            if let Linked::Call(n) = r
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

    fn field_value(&self, def: u32) -> Option<Linked> {
        let d = self.tree.cursor(def);
        let binding = d.child(C::Binding).filter(|b| b.typed().is_some())?;
        let stored = d.has(C::FieldDef) || d.has(C::Property);
        stored.then(|| Linked::Call(Self::producer_of(binding)))
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

    // x = x.foo() makes x's type depend on a chain rooted at x, and a phi with
    // many reaching values fans out at every level, so in-progress nodes yield
    // nothing and finished nodes are memoised for the current dispatch.
    fn lookup_chain(&mut self, c: Cursor) -> Vec<Linked> {
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

    fn lookup_chain_inner(&mut self, c: Cursor) -> Vec<Linked> {
        if let Some(call) = c.child(C::Call)
            && !(call.has(C::Property) && self.chain_is_class(c.reference()))
        {
            return vec![Linked::Call(call.index())];
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
                bound.extend(field.map(|f| Linked::Call(Self::producer_of(f))));
            }
            return bound
                .into_iter()
                .map(|r| match r {
                    Linked::Def(d) => self.field_value(d).unwrap_or(r),
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
                Linked::Type(s) => targets.extend(self.lookup(s)),
                Linked::Call(n) if let Some(ty) = self.binding_type(n) => {
                    targets.extend(self.lookup_chain(ty));
                    targets.push(r);
                }
                _ => targets.push(r),
            }
        }
        targets
            .into_iter()
            .flat_map(|r| match r {
                Linked::Def(d) => self
                    .find_method_in(d, m.sym())
                    .into_iter()
                    .map(Linked::Def)
                    .collect(),
                Linked::Import(_) => vec![r],
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
    let (lang, config) = (&env.lang, &env.rules.config.link);
    let file = Sentinel::new("link", &tree.label, env.limits.file_link_ms);
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        tree,
        ssa,
        cur: entry,
        predeclared: FxHashMap::default(),
        defs: Vec::new(),
        defs_by_name: FxHashMap::default(),
        members: RefCell::default(),
        ivars: RefCell::default(),
        supertypes: FxHashMap::default(),
        imports: Vec::new(),
        wildcards: Vec::new(),
        tags: ReservedTags::new(lang),
        config,
        run,
        file,
        killed: None,
        def_stack: vec![(None, entry)],
        chain_seen: FxHashSet::default(),
        obj_seen: FxHashSet::default(),
        chain_memo: FxHashMap::default(),
        wildcard: lang.syms.intern(WILDCARD),
        edges: Vec::new(),
        value_sink: FxHashMap::default(),
    };

    let root = tree.root();
    f.predeclare(root);
    f.walk_children(root);
    if let Some(k) = f.killed.take() {
        return Err(k);
    }

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    f.cur = entry;
    for dn in f.defs.clone() {
        f.run.check().and_then(|()| f.file.check())?;
        for c in tree.cursor(dn).children().filter(|c| c.is(C::Decorator)) {
            for target in f.lookup_chain(c) {
                if let Linked::Def(target) = target {
                    f.edges.push(Edge::local(dn, target, EdgeKind::Calls));
                }
            }
        }
    }

    Ok(f.edges)
}
