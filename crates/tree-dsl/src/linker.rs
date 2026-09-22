use rustc_hash::FxHashMap;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::intern::Lang;
use crate::resolver::CLASS_LIKE;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{Cursor, Edge, EdgeKind, Step, Tree, find_method_in, members_by_level};

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

struct Fold<'t> {
    tree: &'t Tree,
    ssa: SsaEngine,
    cur: BlockId,
    predeclared: FxHashMap<u32, u32>,
    defs: Vec<u32>,
    supertypes: FxHashMap<u32, Vec<u32>>,
    imports: Vec<u32>,
    wildcards: Vec<u32>,
    callable_key: u32,
    non_shadowing_key: u32,
    def_stack: Vec<(Option<u32>, BlockId)>,
    wildcard: u32,
    scoped_key: u32,
    hoisted_key: u32,
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

    fn run(&mut self, mut stack: Vec<WorkItem>) {
        while let Some(item) = stack.pop() {
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
            if c.has_tag(self.non_shadowing_key)
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
        let def_idx = self.predeclared.remove(&idx).unwrap_or_else(|| {
            self.defs.push(idx);
            self.defs.len() as u32 - 1
        });
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
        if c.has_tag(self.scoped_key) {
            if c.has_tag(self.hoisted_key) {
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
                let def_idx = self.defs.len() as u32;
                self.defs.push(d.index());
                self.predeclared.insert(d.index(), def_idx);
                self.declare(d, name, def_idx);
            }
        }
    }

    fn declare(&mut self, c: Cursor<'t>, name: u32, def_idx: u32) {
        for sym in std::iter::once(name).chain(c.children_of(C::Alias).map(|a| a.sym())) {
            if sym == name && c.has(C::ImplBlock) && self.lookup(name).iter().any(|r| matches!(r, Linked::Def(d) if self.tree.cursor(*d).is_class() && !self.tree.cursor(*d).has(C::ImplBlock))) {
                continue;
            }
            self.ssa
                .write_variable(sym, self.cur, Value::LocalDef(def_idx));
        }
    }

    fn defs_named(&self, sym: u32) -> impl Iterator<Item = u32> + '_ {
        self.defs
            .iter()
            .copied()
            .filter(move |&dn| self.tree.cursor(dn).child_sym(C::DefName) == Some(sym))
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
                if let Some(call) = obj.child(C::Call) {
                    if call.has(C::Property) && self.chain_is_class(obj) {
                        self.resolve_obj(obj, m.sym(), from);
                    } else {
                        self.edges
                            .push(Edge::local(from, call.index(), EdgeKind::TypeFlow));
                    }
                } else if let Some(iv) = obj.child(C::Ivar) {
                    if let Some(cls) = self.enclosing_class(from)
                        && let Some(ty) = self.ivar_type(cls, iv.sym())
                    {
                        if let Some(call) = ty.parent().filter(|p| p.is(C::Call)) {
                            self.edges
                                .push(Edge::local(from, call.index(), EdgeKind::TypeFlow));
                        }
                        self.resolve_obj(ty, m.sym(), from);
                    }
                } else {
                    self.resolve_obj(obj, m.sym(), from);
                }
            }
        } else if let Some(iv) = callee.child(C::Ivar) {
            if let Some(cls) = self.enclosing_class(from) {
                self.push_calls(from, self.find_method_in(cls, iv.sym()));
            }
        } else if let Some(sym) = callee.sym_opt() {
            if callee.has(C::Implicit) || callee.has(C::SimpleName) {
                self.resolve_implicit(sym, from, callee.has(C::SimpleName));
            } else {
                self.resolve_name(sym, from, !callee.has(C::Predeclared));
            }
        }
        for edge in &mut self.edges[first..] {
            edge.site = Some(c.index());
        }
    }

    fn handle_standalone_member(&mut self, c: Cursor<'t>) {
        let Some(obj) = c.child_sym(C::Object) else {
            return;
        };
        let method = c.sym();
        let from = self.enclosing();
        for r in self.lookup(obj) {
            match r {
                Linked::Type(ts) if method != 0 => self.resolve_method(ts, method, from),
                Linked::Import(node) => self.edges.push(Edge::local(from, node, EdgeKind::Imports)),
                _ => {}
            }
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
            (Some(ts), _) => Value::Type(ts),
            (None, Some(rhs)) => self.classify_tail(rhs.tail_expr()),
            (None, None) => Value::Opaque,
        };
        self.ssa.write_variable(lhs, self.cur, val);
        true
    }

    fn classify_tail(&mut self, tail: Cursor<'_>) -> Value {
        if tail.is(C::Call) {
            if let Some(sym) = tail.child_sym(C::Callee) {
                let targets = self.lookup(sym);
                if self.any_class(&targets) {
                    return Value::Type(sym);
                }
            }
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
                if self.tree.cursor(*node).has_tag(self.callable_key) {
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
                _ if obj.is(C::Object) => self.emit(&r, from),
                _ => {}
            }
        }
    }

    fn resolve_implicit(&mut self, sym: u32, from: u32, locals_first: bool) {
        if locals_first && self.ssa.is_defined(sym, self.cur) {
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
        let mut targets = self.lookup(sym);
        targets.retain(|r| matches!(r, Linked::Def(_) | Linked::Import(_)));
        let supplies_callees = |&n: &u32| {
            let import = self.tree.cursor(n).parent();
            import.is_some_and(|i| i.has_tag(self.callable_key))
        };
        if targets.is_empty() {
            let wild = self.wildcards.iter().copied().filter(supplies_callees);
            targets = wild.map(Linked::Import).collect();
        }
        for r in &targets {
            self.emit(r, from);
        }
    }

    fn resolve_name(&mut self, sym: u32, from: u32, imported: bool) {
        let mut targets = self.lookup(sym);
        if targets.is_empty() && imported {
            targets = self.wildcards.iter().map(|&n| Linked::Import(n)).collect();
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

    fn ivar_type(&self, class: u32, attr: u32) -> Option<Cursor<'t>> {
        self.tree.cursor(class).descend(|n| {
            if n.is(C::Binding)
                && n.child(C::Ivar).is_some_and(|iv| iv.sym() == attr)
                && let Some(s) = n.typed()
            {
                return Step::Out(s);
            }
            Step::Into
        })
    }

    fn chain_is_class(&mut self, c: Cursor) -> bool {
        let targets = self.lookup_chain(c);
        self.any_class(&targets)
    }

    fn lookup_chain(&mut self, c: Cursor) -> Vec<Linked> {
        let c = c.reference();
        let Some(m) = c.has(C::Object).then_some(c).or_else(|| c.child(C::Member)) else {
            return self.lookup(c.sym());
        };
        let Some(obj) = m.child(C::Object) else {
            return vec![];
        };
        let targets: Vec<_> = self
            .lookup_chain(obj)
            .into_iter()
            .flat_map(|r| match r {
                Linked::Type(s) => self.lookup(s),
                _ => vec![r],
            })
            .collect();
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
        let found = |dn| find_method_in(self.tree.cursor(dn), name).map(|m| m.index());
        members_by_level(wrappers(container), supers, found)
    }
}

pub fn link(tree: &Tree, lang: &Lang) -> Vec<Edge> {
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        tree,
        ssa,
        cur: entry,
        predeclared: FxHashMap::default(),
        defs: Vec::new(),
        supertypes: FxHashMap::default(),
        imports: Vec::new(),
        wildcards: Vec::new(),
        callable_key: lang.syms.intern("callable"),
        non_shadowing_key: lang.syms.intern("non_shadowing"),
        def_stack: vec![(None, entry)],
        wildcard: lang.syms.intern(WILDCARD),
        scoped_key: lang.syms.intern("scoped"),
        hoisted_key: lang.syms.intern("hoisted"),
        edges: Vec::new(),
        value_sink: FxHashMap::default(),
    };

    let root = tree.root();
    f.predeclare(root);
    f.walk_children(root);

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    f.cur = entry;
    for dn in f.defs.clone() {
        for c in tree.cursor(dn).children().filter(|c| c.is(C::Decorator)) {
            for target in f.lookup_chain(c) {
                if let Linked::Def(target) = target {
                    f.edges.push(Edge::local(dn, target, EdgeKind::Calls));
                }
            }
        }
    }

    f.edges
}
