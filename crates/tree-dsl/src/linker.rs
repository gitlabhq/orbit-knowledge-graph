use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::intern::Lang;
use crate::resolver::CLASS_LIKE;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{
    Cursor, Edge, EdgeKind, Step, Tree, find_method_in, infer_return_type, reachable,
};

enum Linked {
    Def(u32),
    Import(u32),
    Type(u32),
}

enum WorkItem {
    Visit(u32),
    ExitScope,
}

enum Receiver {
    Ivar(u32),
    Sym(u32),
}

enum CalleeShape {
    Method { recv: Receiver, method: u32 },
    IvarCall(u32),
    Name(u32),
}

fn callee_shape(callee: Cursor) -> Option<CalleeShape> {
    if let Some(m) = callee.child(C::Member) {
        let recv = match m.object_ivar() {
            Some(iv) => Receiver::Ivar(iv.sym()),
            None => Receiver::Sym(root_object_sym(m)),
        };
        return Some(CalleeShape::Method {
            recv,
            method: m.sym(),
        });
    }
    if let Some(iv) = callee.child(C::Ivar) {
        return iv.sym_opt().map(CalleeShape::IvarCall);
    }
    callee.sym_opt().map(CalleeShape::Name)
}

struct Fold<'t> {
    tree: &'t Tree,
    ssa: SsaEngine,
    cur: BlockId,
    root_seen: u32,
    import_count: u32,
    defs: Vec<u32>,
    imports: Vec<u32>,
    wildcards: Vec<u32>,
    def_stack: Vec<(Option<u32>, BlockId)>,
    wildcard: u32,
    callable_key: u32,
    scoped_key: u32,
    edges: Vec<Edge>,
}

impl<'t> Fold<'t> {
    fn enclosing(&self) -> u32 {
        self.def_stack.last().and_then(|&(d, _)| d).unwrap_or(0)
    }

    fn exit_scope(&mut self) {
        if self.def_stack.len() > 1
            && let Some((_, saved)) = self.def_stack.pop()
        {
            self.cur = saved;
        }
    }

    fn run(&mut self, mut stack: Vec<WorkItem>) {
        while let Some(item) = stack.pop() {
            match item {
                WorkItem::ExitScope => self.exit_scope(),
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
        } else if k == C::SsaBranch {
            self.handle_branch(c);
        } else if k == C::SsaLoop {
            self.handle_loop(c);
        } else {
            Self::push_children(c, stack);
        }
    }

    fn handle_branch(&mut self, branch: Cursor<'t>) {
        let non_arms: Vec<u32> = branch
            .children()
            .filter(|c| !c.is(C::SsaArm))
            .map(|c| c.index())
            .collect();
        for &child in &non_arms {
            self.run(vec![WorkItem::Visit(child)]);
        }

        let pre = self.cur;
        let arms: Vec<u32> = branch
            .children()
            .filter(|c| c.is(C::SsaArm))
            .map(|c| c.index())
            .collect();

        let mut exit_blocks = Vec::with_capacity(arms.len());
        for &arm in &arms {
            let entry = self.ssa.add_sealed_successor(pre);
            self.cur = entry;
            self.walk_children(self.tree.cursor(arm));
            exit_blocks.push(self.cur);
        }
        exit_blocks.push(pre);
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
            self.import_count += 1;
            self.imports.push(n.index());
            let local = n
                .child_sym(C::Alias)
                .or(n.child_sym(C::SsaHint))
                .unwrap_or(sym);
            self.wildcards
                .extend((local == self.wildcard).then_some(n.index()));
            self.ssa
                .write_variable(sym, self.cur, Value::ImportRef(self.import_count - 1));
            for kind in [C::Alias, C::SsaHint] {
                if let Some(alias) = n.child_sym(kind)
                    && alias != sym
                {
                    self.ssa.write_variable(
                        alias,
                        self.cur,
                        Value::ImportRef(self.import_count - 1),
                    );
                }
            }
        }
    }

    fn handle_def(&mut self, c: Cursor<'t>, stack: &mut Vec<WorkItem>) {
        let Some(name) = c.child_sym(C::DefName) else {
            return;
        };
        let idx = c.index();
        let def_idx = if self.defs.get(self.root_seen as usize) == Some(&idx) {
            self.root_seen += 1;
            self.root_seen - 1
        } else {
            self.defs.push(idx);
            self.defs.len() as u32 - 1
        };
        self.declare(c, name, def_idx);
        let parent_block = self.cur;
        self.cur = self.ssa.add_sealed_successor(parent_block);
        if let Some(&(Some(parent), _)) = self.def_stack.last() {
            self.edges.push(Edge::local(parent, idx, EdgeKind::Defines));
        }
        let supers: Vec<u32> = c
            .children_of(C::SuperType)
            .filter_map(|st| self.defs_named(st.sym()).next())
            .collect();
        for dn in supers {
            self.edges.push(Edge::local(idx, dn, EdgeKind::Extends));
        }
        if c.has_tag(self.scoped_key) {
            self.def_stack.push((Some(idx), parent_block));
            stack.push(WorkItem::ExitScope);
            stack.extend(c.children_rev().map(|ch| WorkItem::Visit(ch.index())));
        }
    }

    fn declare(&mut self, c: Cursor<'t>, name: u32, def_idx: u32) {
        for sym in std::iter::once(name).chain(c.children_of(C::Alias).map(|a| a.sym())) {
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
        let from = self.enclosing();
        let Some(shape) = callee_shape(callee) else {
            return;
        };
        match shape {
            CalleeShape::Method {
                recv: Receiver::Ivar(ivar_sym),
                method,
            } => {
                if let Some(cls) = self.enclosing_class(from)
                    && let Some(ts) = self.ivar_type(cls, ivar_sym)
                {
                    self.resolve_method(ts, method, from);
                }
            }
            CalleeShape::Method {
                recv: Receiver::Sym(obj_sym),
                method,
            } => {
                self.resolve_obj(obj_sym, method, from);
            }
            CalleeShape::IvarCall(ivar_sym) => {
                if let Some(cls) = self.enclosing_class(from)
                    && let Some(m) = self.find_method_in(cls, ivar_sym)
                {
                    self.edges.push(Edge::local(from, m, EdgeKind::Calls));
                }
            }
            CalleeShape::Name(sym) => {
                self.resolve_name(sym, from);
            }
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

        if let Some(rhs) = c.child(C::Rhs) {
            if let Some(branch) = rhs.child(C::SsaBranch) {
                self.walk_branch_binding(branch, lhs);
                return true;
            }

            self.walk_children(rhs);

            let val = self.classify_rhs_value(rhs, c.index());
            self.ssa.write_variable(lhs, self.cur, val);
            return true;
        }

        let val = if let Some(ts) = c.child_sym(C::SsaTyped) {
            Value::Type(ts)
        } else {
            Value::Opaque
        };

        self.ssa.write_variable(lhs, self.cur, val);
        true
    }

    fn classify_rhs_value(&mut self, rhs: Cursor<'_>, binding: u32) -> Value {
        if let Some(ts) = self.tree.cursor(binding).child_sym(C::SsaTyped) {
            return Value::Type(ts);
        }

        if let Some(callee) = rhs.child(C::Call).and_then(|call| call.child(C::Callee)) {
            let Some(shape) = callee_shape(callee) else {
                return Value::Opaque;
            };
            return match shape {
                CalleeShape::Method {
                    recv: Receiver::Ivar(ivar_sym),
                    method,
                } => self.value_from_method(ivar_sym, method, binding, true),
                CalleeShape::Method {
                    recv: Receiver::Sym(obj_sym),
                    method,
                } => self.value_from_method(obj_sym, method, binding, false),
                CalleeShape::IvarCall(_) => Value::Opaque,
                CalleeShape::Name(sym) => self.value_from_name(sym),
            };
        }

        let sym = self.tail_sym(rhs);
        if sym != 0 {
            self.tail_value(sym)
        } else {
            Value::Opaque
        }
    }

    fn tail_value(&mut self, sym: u32) -> Value {
        let r = self.lookup(sym);
        if self.any_class(&r) {
            Value::Type(sym)
        } else {
            Value::Alias(sym)
        }
    }

    fn walk_branch_binding(&mut self, branch: Cursor<'_>, lhs: u32) {
        let pre = self.cur;
        let mut exits = Vec::new();

        for arm in branch.children().filter(|c| c.is(C::SsaArm)) {
            let block = self.ssa.add_sealed_successor(pre);
            self.cur = block;
            self.walk_children(arm);
            let sym = self.tail_sym(arm);
            if sym != 0 {
                let val = self.tail_value(sym);
                self.ssa.write_variable(lhs, self.cur, val);
            }
            exits.push(self.cur);
        }

        self.cur = self.ssa.add_sealed_join(exits);
    }

    fn tail_sym(&self, node: Cursor<'_>) -> u32 {
        match node.last_named() {
            Some(c) if c.size() == 1 && c.sym_opt().is_some() => c.sym(),
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
            Linked::Type(_) => {}
        }
    }

    fn is_class(&self, node: u32) -> bool {
        let c = self.tree.cursor(node);
        CLASS_LIKE.iter().any(|&k| c.has(k))
    }

    fn any_class(&self, resolved: &[Linked]) -> bool {
        resolved
            .iter()
            .any(|r| matches!(r, Linked::Def(n) if self.is_class(*n)))
    }

    fn resolve_obj(&mut self, obj: u32, method: u32, from: u32) {
        for r in self.lookup(obj) {
            match r {
                Linked::Type(ts) => self.resolve_method(ts, method, from),
                Linked::Def(node) => {
                    if let Some(m) = self.find_method_in(node, method) {
                        self.edges.push(Edge::local(from, m, EdgeKind::Calls));
                    } else {
                        self.emit(&Linked::Def(node), from);
                    }
                }
                _ => self.emit(&r, from),
            }
        }
    }

    fn resolve_name(&mut self, sym: u32, from: u32) {
        let mut targets = self.lookup(sym);
        if targets.is_empty() {
            targets = self.wildcards.iter().map(|&n| Linked::Import(n)).collect();
        }
        for r in &targets {
            match r {
                Linked::Type(ts) => {
                    for inner in self.lookup(*ts) {
                        if let Linked::Def(target) = inner {
                            if let Some(callable) = self.tree.cursor(target).child_sym(C::Callable)
                            {
                                if let Some(m) = self.find_method_in(target, callable) {
                                    self.edges.push(Edge::local(from, m, EdgeKind::Calls));
                                }
                            } else {
                                self.edges.push(Edge::local(from, target, EdgeKind::Calls));
                            }
                        }
                    }
                }
                _ => self.emit(r, from),
            }
        }
    }

    fn resolve_method(&mut self, type_sym: u32, method: u32, from: u32) {
        for r in self.lookup(type_sym) {
            if let Linked::Def(cls) = r
                && let Some(m) = self.find_method_in(cls, method)
            {
                self.edges.push(Edge::local(from, m, EdgeKind::Calls));
            }
        }
    }

    fn enclosing_class(&self, node: u32) -> Option<u32> {
        let c = self.tree.cursor(node);
        if CLASS_LIKE.iter().any(|&k| c.has(k)) {
            Some(node)
        } else {
            c.enclosing_def(CLASS_LIKE).map(|n| n.index())
        }
    }

    fn ivar_type(&self, class: u32, attr: u32) -> Option<u32> {
        self.tree.cursor(class).descend(|n| {
            if n.is(C::Binding)
                && n.child(C::Ivar).is_some_and(|iv| iv.sym() == attr)
                && let Some(s) = n.rhs_callee()
            {
                return Step::Out(s);
            }
            Step::Into
        })
    }

    fn find_method_in(&self, container: u32, name: u32) -> Option<u32> {
        let succ = |dn: u32| {
            let c = self.tree.cursor(dn);
            let same_name = c
                .child_sym(C::DefName)
                .into_iter()
                .flat_map(|n| self.defs_named(n))
                .filter(move |&d| d != dn);
            let supers = c
                .children_of(C::SuperType)
                .flat_map(|s| self.defs_named(s.sym()));
            same_name.chain(supers).collect::<Vec<_>>()
        };
        reachable(container, succ)
            .find_map(|dn| find_method_in(self.tree.cursor(dn), name).map(|m| m.index()))
    }

    fn value_from_name(&mut self, sym: u32) -> Value {
        let resolved = self.lookup(sym);
        if self.any_class(&resolved) {
            return Value::Type(sym);
        }
        for r in &resolved {
            if let Linked::Def(node) = r
                && let Some(rt) = infer_return_type(self.tree.cursor(*node))
            {
                return self.classify_return(rt);
            }
        }
        Value::Opaque
    }

    fn value_from_method(&mut self, obj: u32, method: u32, binding: u32, is_ivar: bool) -> Value {
        let obj_type = if is_ivar {
            self.enclosing_class(binding)
                .and_then(|cls| self.ivar_type(cls, obj))
        } else if obj != 0 {
            self.lookup(obj).into_iter().find_map(|r| {
                if let Linked::Type(ts) = r {
                    Some(ts)
                } else {
                    None
                }
            })
        } else {
            None
        };
        let Some(ts) = obj_type else {
            return Value::Opaque;
        };
        for r in self.lookup(ts) {
            if let Linked::Def(cls) = r
                && let Some(m) = self.find_method_in(cls, method)
                && let Some(rt) = infer_return_type(self.tree.cursor(m))
            {
                return Value::Type(rt);
            }
        }
        Value::Opaque
    }

    fn classify_return(&self, rt_sym: u32) -> Value {
        match self
            .defs
            .iter()
            .position(|&dn| self.tree.cursor(dn).child_sym(C::DefName) == Some(rt_sym))
        {
            Some(di) if self.is_class(self.defs[di]) => Value::Type(rt_sym),
            Some(di) => Value::LocalDef(di as u32),
            None => Value::Type(rt_sym),
        }
    }
}

fn root_object_sym(member: Cursor) -> u32 {
    let Some(obj) = member.child(C::Object) else {
        return 0;
    };
    if obj.child(C::Ivar).is_some() {
        return 0;
    }
    if let Some(s) = obj.sym_opt() {
        return s;
    }
    if let Some(inner) = obj.child(C::Member) {
        return root_object_sym(inner);
    }
    0
}

pub fn link(tree: &Tree, lang: &Lang) -> Vec<Edge> {
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        tree,
        ssa,
        cur: entry,
        root_seen: 0,
        import_count: 0,
        defs: Vec::new(),
        imports: Vec::new(),
        wildcards: Vec::new(),
        def_stack: vec![(None, entry)],
        wildcard: lang.syms.intern(WILDCARD),
        callable_key: lang.syms.intern("callable"),
        scoped_key: lang.syms.intern("scoped"),
        edges: Vec::new(),
    };

    let root = tree.root();
    let root_defs = root.children().filter(|d| d.is(C::Def));
    for (d, name) in root_defs.filter_map(|d| Some((d, d.child_sym(C::DefName)?))) {
        f.defs.push(d.index());
        f.declare(d, name, f.defs.len() as u32 - 1);
    }
    let mut stack = Vec::new();
    Fold::push_children(root, &mut stack);
    f.run(stack);

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    for &dn in &f.defs {
        for c in tree.cursor(dn).children_of(C::Decorator) {
            for pv in &f.ssa.read_variable(c.sym(), entry) {
                if let ParseValue::LocalDef(di) = pv {
                    f.edges
                        .push(Edge::local(dn, f.defs[*di as usize], EdgeKind::Calls));
                }
            }
        }
    }

    f.edges
}
