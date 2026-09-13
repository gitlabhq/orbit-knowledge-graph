use crate::canonical::Canonical as C;
use crate::lang::Lang;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{Cursor, EdgeKind, Step, Tree, infer_return_type};

// ── Resolved: what a symbol maps to after SSA resolution ──

enum Resolved {
    Def(u32),
    Import(u32),
    Type(u32),
}

struct Fold {
    ssa: SsaEngine,
    cur: BlockId,
    def_count: u32,
    import_count: u32,
    defs: Vec<u32>,
    imports: Vec<u32>,
    import_names: Vec<u32>,
    def_stack: Vec<(Option<u32>, u32, BlockId)>,
    branch_stack: Vec<BranchFrame>,
    wildcard: u32,
    class_sym: u32,
    containers: Vec<u32>,
}

struct BranchFrame {
    arms: Vec<(u32, u32)>,
    entries: Vec<BlockId>,
    exits: Vec<BlockId>,
    pre: BlockId,
    end: u32,
}

impl Fold {
    fn enclosing(&self) -> u32 {
        self.def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0)
    }

    // ── SSA resolution: translate ParseValue → concrete node indices ──

    fn resolve(&mut self, sym: u32) -> Vec<Resolved> {
        self.ssa
            .read_variable(sym, self.cur)
            .iter()
            .filter_map(|pv| match pv {
                ParseValue::LocalDef(di) => Some(Resolved::Def(self.defs[*di as usize])),
                ParseValue::ImportRef(ii) => {
                    self.imports.get(*ii as usize).map(|&n| Resolved::Import(n))
                }
                ParseValue::Type(ts) if *ts != 0 => Some(Resolved::Type(*ts)),
                _ => None,
            })
            .collect()
    }

    fn emit(&self, tree: &Tree, r: &Resolved, from: u32) {
        match r {
            Resolved::Def(node) => tree.add_edge(from, *node, EdgeKind::Calls),
            Resolved::Import(node) => tree.add_edge(from, *node, EdgeKind::Imports),
            Resolved::Type(_) => {}
        }
    }

    fn is_class(&self, tree: &Tree, node: u32) -> bool {
        tree.cursor(node).child_sym(C::DefType) == Some(self.class_sym)
    }

    fn any_class(&self, tree: &Tree, resolved: &[Resolved]) -> bool {
        resolved
            .iter()
            .any(|r| matches!(r, Resolved::Def(n) if self.is_class(tree, *n)))
    }

    // ── Handlers ──

    fn handle_import(&mut self, tree: &Tree, i: u32) {
        for c in tree.cursor(i).names() {
            let sym = c.sym();
            self.import_count += 1;
            self.imports.push(c.index());
            self.import_names.push(sym);
            self.ssa
                .write_variable(sym, self.cur, Value::ImportRef(self.import_count - 1));
            if let Some(alias) = c.child_sym(C::Alias) {
                if alias != sym {
                    self.ssa.write_variable(
                        alias,
                        self.cur,
                        Value::ImportRef(self.import_count - 1),
                    );
                }
            }
        }
    }

    fn handle_def(&mut self, tree: &Tree, i: u32, end: u32) {
        let c = tree.cursor(i);
        let name = match c.child_sym(C::DefName) {
            Some(n) => n,
            None => return,
        };
        let parent_block = self.cur;
        self.cur = self.ssa.add_sealed_successor(parent_block);
        let idx = self.def_count;
        self.def_count += 1;
        self.defs.push(i);
        self.ssa
            .write_variable(name, parent_block, Value::LocalDef(idx));
        if let Some(&(Some(parent), _, _)) = self.def_stack.last() {
            tree.add_edge(parent, i, EdgeKind::Defines);
        }
        if c.has(C::Scope) {
            self.def_stack.push((Some(i), end, parent_block));
        }
    }

    fn handle_call(&mut self, tree: &Tree, i: u32) {
        let callee = match tree.cursor(i).child(C::Callee) {
            Some(c) => c,
            None => return,
        };
        let from = self.enclosing();

        if let Some(member) = callee.child(C::Member) {
            let method = member.sym();
            if let Some(ivar) = member.child(C::Object).and_then(|o| o.child(C::Ivar)) {
                if let Some(cls) = self.enclosing_class(tree, from) {
                    if let Some(ts) = self.ivar_type(tree, cls, ivar.sym()) {
                        self.resolve_method(tree, ts, method, from);
                    }
                }
            } else {
                self.resolve_obj(
                    tree,
                    member.child(C::Object).map(|o| o.sym()).unwrap_or(0),
                    method,
                    from,
                );
            }
        } else if let Some(ivar) = callee.child(C::Ivar) {
            if ivar.sym() != 0 {
                if let Some(cls) = self.enclosing_class(tree, from) {
                    if let Some(m) = self.find_method_in(tree, cls, ivar.sym()) {
                        tree.add_edge(from, m, EdgeKind::Calls);
                    }
                }
            }
        } else if callee.sym() != 0 {
            self.resolve_name(tree, callee.sym(), from);
        }
    }

    fn handle_standalone_member(&mut self, tree: &Tree, i: u32) {
        let obj = tree
            .cursor(i)
            .child(C::Object)
            .map(|o| o.sym())
            .unwrap_or(0);
        if obj == 0 {
            return;
        }
        let method = tree.sym(i);
        let from = self.enclosing();
        for r in self.resolve(obj) {
            match r {
                Resolved::Type(ts) if method != 0 => self.resolve_method(tree, ts, method, from),
                Resolved::Import(node) => tree.add_edge(from, node, EdgeKind::Imports),
                _ => {}
            }
        }
    }

    fn handle_binding(&mut self, tree: &Tree, i: u32) {
        let lhs = tree.sym(i);
        if lhs == 0 || tree.cursor(i).has(C::Ivar) {
            return;
        }
        if self.ssa.has_variable_in_block(lhs, self.cur) {
            self.cur = self.ssa.add_sealed_successor(self.cur);
        }
        let val = self.classify_rhs(tree, i);
        self.ssa.write_variable(lhs, self.cur, val);
        if let Some(br) = self.branch_stack.last_mut() {
            for (idx, &(start, end)) in br.arms.iter().enumerate() {
                if i >= start && i < end {
                    br.exits[idx] = self.cur;
                    break;
                }
            }
        }
    }

    // ── Resolution ──

    fn resolve_obj(&mut self, tree: &Tree, obj: u32, method: u32, from: u32) {
        for r in self.resolve(obj) {
            match r {
                Resolved::Type(ts) => self.resolve_method(tree, ts, method, from),
                _ => self.emit(tree, &r, from),
            }
        }
    }

    fn resolve_name(&mut self, tree: &Tree, sym: u32, from: u32) {
        let mut targets = self.resolve(sym);
        if targets.is_empty() {
            targets = self.resolve(self.wildcard);
            for r in &targets {
                self.emit(tree, r, from);
            }
        }
        for r in &targets {
            match r {
                Resolved::Type(ts) => {
                    for inner in self.resolve(*ts) {
                        if let Resolved::Def(target) = inner {
                            if let Some(callable) = tree.cursor(target).child_sym(C::Callable) {
                                if let Some(m) = self.find_method_in(tree, target, callable) {
                                    tree.add_edge(from, m, EdgeKind::Calls);
                                }
                            } else {
                                tree.add_edge(from, target, EdgeKind::Calls);
                            }
                        }
                    }
                }
                _ => self.emit(tree, r, from),
            }
        }
    }

    fn resolve_method(&mut self, tree: &Tree, type_sym: u32, method: u32, from: u32) {
        for r in self.resolve(type_sym) {
            if let Resolved::Def(cls) = r {
                if let Some(m) = self.find_method_in(tree, cls, method) {
                    tree.add_edge(from, m, EdgeKind::Calls);
                }
            }
        }
    }

    // ── Helpers ──

    fn enclosing_class(&self, tree: &Tree, node: u32) -> Option<u32> {
        let c = tree.cursor(node);
        let check = |n: Cursor| {
            n.child_sym(C::DefType)
                .is_some_and(|dt| self.containers.contains(&dt))
        };
        if check(c) {
            Some(node)
        } else {
            c.enclosing(check).map(|n| n.index())
        }
    }

    fn ivar_type(&self, tree: &Tree, class: u32, attr: u32) -> Option<u32> {
        tree.cursor(class).descend(|n| {
            if n.is(C::Binding) && n.child(C::Ivar).is_some_and(|iv| iv.sym() == attr) {
                if let Some(s) = n
                    .child(C::Rhs)
                    .and_then(|r| r.child(C::Call))
                    .and_then(|c| c.child_sym(C::Callee))
                {
                    return Step::Out(s);
                }
            }
            Step::Into
        })
    }

    fn find_method_in(&self, tree: &Tree, container: u32, name: u32) -> Option<u32> {
        let mut search = vec![container];
        let mut si = 0;
        while si < search.len() {
            if let Some(m) = crate::tree::find_method_in(tree.cursor(search[si]), name) {
                return Some(m.index());
            }
            for c in tree
                .cursor(search[si])
                .children()
                .filter(|c| c.is(C::SuperType) && c.sym() != 0)
            {
                for &dn in &self.defs {
                    if tree.cursor(dn).child_sym(C::DefName) == Some(c.sym())
                        && !search.contains(&dn)
                    {
                        search.push(dn);
                    }
                }
            }
            si += 1;
        }
        None
    }

    // ── RHS classification ──

    fn classify_rhs(&mut self, tree: &Tree, node: u32) -> Value {
        let c = tree.cursor(node);
        let Some(rhs) = c.child(C::Rhs) else {
            return Value::Opaque;
        };

        if let Some(callee) = rhs.child(C::Call).and_then(|call| call.child(C::Callee)) {
            if let Some(member) = callee.child(C::Member) {
                let method = member.sym();
                let obj_node = member.child(C::Object);
                let ivar = obj_node.and_then(|o| o.child(C::Ivar));
                let obj_sym = ivar.or(obj_node).map(|n| n.sym()).unwrap_or(0);
                return self.value_from_method(tree, obj_sym, method, node, ivar.is_some());
            }
            if callee.child(C::Ivar).is_some() {
                return Value::Opaque;
            }
            if callee.sym() != 0 {
                return self.value_from_name(tree, callee.sym());
            }
            return Value::Opaque;
        }

        let sym = rhs.sym();
        if sym != 0 {
            let r = self.resolve(sym);
            if self.any_class(tree, &r) {
                Value::Type(sym)
            } else {
                Value::Alias(sym)
            }
        } else {
            Value::Opaque
        }
    }

    fn value_from_name(&mut self, tree: &Tree, sym: u32) -> Value {
        let resolved = self.resolve(sym);
        if self.any_class(tree, &resolved) {
            return Value::Type(sym);
        }
        for r in &resolved {
            if let Resolved::Def(node) = r {
                if let Some(rt) = infer_return_type(tree.cursor(*node)) {
                    return self.classify_return(tree, rt);
                }
            }
        }
        Value::Opaque
    }

    fn value_from_method(
        &mut self,
        tree: &Tree,
        obj: u32,
        method: u32,
        binding: u32,
        is_ivar: bool,
    ) -> Value {
        let obj_type = if is_ivar {
            self.enclosing_class(tree, binding)
                .and_then(|cls| self.ivar_type(tree, cls, obj))
        } else if obj != 0 {
            self.resolve(obj).into_iter().find_map(|r| {
                if let Resolved::Type(ts) = r {
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
        for r in self.resolve(ts) {
            if let Resolved::Def(cls) = r {
                if let Some(m) = self.find_method_in(tree, cls, method) {
                    if let Some(rt) = infer_return_type(tree.cursor(m)) {
                        return Value::Type(rt);
                    }
                }
            }
        }
        Value::Opaque
    }

    fn classify_return(&self, tree: &Tree, rt_sym: u32) -> Value {
        match self
            .defs
            .iter()
            .position(|&dn| tree.cursor(dn).child_sym(C::DefName) == Some(rt_sym))
        {
            Some(di) if self.is_class(tree, self.defs[di]) => Value::Type(rt_sym),
            Some(di) => Value::LocalDef(di as u32),
            None => Value::Type(rt_sym),
        }
    }
}

// ── SSA fold main loop ──

pub fn link(tree: &Tree, lang: &mut Lang) {
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        ssa,
        cur: entry,
        def_count: 0,
        import_count: 0,
        defs: Vec::new(),
        imports: Vec::new(),
        import_names: Vec::new(),
        def_stack: vec![(None, u32::MAX, entry)],
        branch_stack: Vec::new(),
        wildcard: lang.syms.intern("*"),
        class_sym: lang.syms.intern("Class"),
        containers: vec![
            lang.syms.intern("Class"),
            lang.syms.intern("Impl"),
            lang.syms.intern("Trait"),
        ],
    };

    let mut i = 0u32;
    let len = tree.len();

    while i < len {
        let n = tree.nodes[i as usize];
        if n.dead {
            i += n.size.max(1);
            continue;
        }
        let k = n.kind;
        let end = i + n.size;

        while f.def_stack.len() > 1 {
            let &(_, e, saved) = f.def_stack.last().unwrap();
            if i >= e {
                f.def_stack.pop();
                f.cur = saved;
            } else {
                break;
            }
        }

        while let Some(br) = f.branch_stack.last() {
            if i >= br.end {
                let mut preds = br.exits.clone();
                preds.push(br.pre);
                let br_end = br.end;
                f.cur = f.ssa.add_sealed_join(preds);
                f.branch_stack.pop();
                if let Some(outer) = f.branch_stack.last_mut() {
                    for (idx, &(a, b)) in outer.arms.iter().enumerate() {
                        if br_end > a && br_end <= b {
                            outer.exits[idx] = f.cur;
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if let Some(br) = f.branch_stack.last() {
            for (idx, &(a, b)) in br.arms.iter().enumerate() {
                if i >= a && i < b {
                    f.cur = br.entries[idx];
                    break;
                }
            }
        }

        if k == C::Import || k == C::ImportType {
            f.handle_import(tree, i);
            i += n.size.max(1);
            continue;
        }
        if tree.cursor(i).child_sym(C::DefType).is_some() {
            f.handle_def(tree, i, end);
            i += 1;
            continue;
        }
        if k == C::Call {
            f.handle_call(tree, i);
            i += 1;
            continue;
        }
        if k == C::Member {
            if tree.cursor(i).parent().map_or(0, |p| p.kind()) != C::Call
                && tree.cursor(i).parent().map_or(0, |p| p.kind()) != C::Callee
            {
                f.handle_standalone_member(tree, i);
            }
            i += 1;
            continue;
        }
        if k == C::Binding {
            f.handle_binding(tree, i);
            i += 1;
            continue;
        }
        if k == C::Branch {
            let pre = f.cur;
            let arms: Vec<(u32, u32)> = tree
                .cursor(i)
                .children()
                .filter(|c| c.is(C::Arm))
                .map(|c| (c.index(), c.index() + c.size()))
                .collect();
            let entries: Vec<BlockId> = arms
                .iter()
                .map(|_| f.ssa.add_sealed_successor(pre))
                .collect();
            let exits = entries.clone();
            f.branch_stack.push(BranchFrame {
                arms,
                entries,
                exits,
                pre,
                end,
            });
            i += 1;
            continue;
        }
        if k == C::Loop {
            let (h, _) = f.ssa.begin_loop(f.cur);
            f.cur = f.ssa.finish_loop(h, f.cur);
            i += 1;
            continue;
        }
        i += 1;
    }

    while !f.branch_stack.is_empty() {
        let br = f.branch_stack.pop().unwrap();
        let mut preds = br.exits;
        preds.push(br.pre);
        let _ = f.ssa.add_sealed_join(preds);
    }

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    let mut meta: Vec<(u32, u32)> = Vec::new();
    for &dn in &f.defs {
        for c in tree
            .cursor(dn)
            .children()
            .filter(|c| (c.is(C::SuperType) || c.is(C::Decorator)) && c.sym() != 0)
        {
            for pv in &f.ssa.read_variable(c.sym(), entry) {
                if let ParseValue::LocalDef(di) = pv {
                    meta.push((dn, f.defs[*di as usize]));
                }
            }
        }
    }
    for (from, to) in meta {
        tree.add_edge(from, to, EdgeKind::Calls);
    }
}
