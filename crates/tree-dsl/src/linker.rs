use crate::canonical::Canonical as C;
use crate::lang::Lang;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{Cursor, EdgeKind, Step, Tree, infer_return_type};

enum Linked {
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
    loop_stack: Vec<(BlockId, u32)>,
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

    // ── Walk ──

    fn walk_range(&mut self, tree: &Tree, start: u32, range_end: u32) {
        let mut i = start;
        while i < range_end {
            let n = tree.nodes[i as usize];
            if n.dead {
                i += n.size.max(1);
                continue;
            }
            self.close_scopes(i);
            self.enter_arm(i);
            i += self.dispatch(tree, i);
        }
    }

    fn close_scopes(&mut self, i: u32) {
        while self.def_stack.len() > 1 {
            let &(_, e, saved) = self.def_stack.last().unwrap();
            if i >= e {
                self.def_stack.pop();
                self.cur = saved;
            } else {
                break;
            }
        }
        while let Some(br) = self.branch_stack.last() {
            if i >= br.end {
                let mut preds = br.exits.clone();
                preds.push(br.pre);
                let br_end = br.end;
                self.cur = self.ssa.add_sealed_join(preds);
                self.branch_stack.pop();
                if let Some(outer) = self.branch_stack.last_mut() {
                    for (idx, &(a, b)) in outer.arms.iter().enumerate() {
                        if br_end > a && br_end <= b {
                            outer.exits[idx] = self.cur;
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }
        while self.loop_stack.last().is_some_and(|&(_, end)| i >= end) {
            let (header, _) = self.loop_stack.pop().unwrap();
            self.cur = self.ssa.finish_loop(header, self.cur);
        }
    }

    fn enter_arm(&mut self, i: u32) {
        if let Some(br) = self.branch_stack.last() {
            for (idx, &(a, b)) in br.arms.iter().enumerate() {
                if i >= a && i < b {
                    self.cur = br.entries[idx];
                    break;
                }
            }
        }
    }

    fn dispatch(&mut self, tree: &Tree, i: u32) -> u32 {
        let n = &tree.nodes[i as usize];
        let k = n.kind;
        let size = n.size;

        if k == C::Import || k == C::ImportType {
            self.handle_import(tree, i);
            return size.max(1);
        }
        if crate::canonical::has_def_type(tree.cursor(i)) {
            self.handle_def(tree, i, i + size);
            return 1;
        }
        if k == C::Call {
            self.handle_call(tree, i);
            return 1;
        }
        if k == C::Member {
            if tree.cursor(i).parent().map_or(0, |p| p.kind()) != C::Call
                && tree.cursor(i).parent().map_or(0, |p| p.kind()) != C::Callee
            {
                self.handle_standalone_member(tree, i);
            }
            return 1;
        }
        if k == C::Binding {
            if self.handle_binding(tree, i) {
                return size.max(1);
            }
            return 1;
        }
        if k == C::SsaBranch {
            self.open_branch(tree, i, i + size);
            return 1;
        }
        if k == C::SsaLoop {
            let (header, body) = self.ssa.begin_loop(self.cur);
            self.loop_stack.push((header, tree.hop(i)));
            self.cur = body;
            return 1;
        }
        1
    }

    fn open_branch(&mut self, tree: &Tree, i: u32, end: u32) {
        let pre = self.cur;
        let arms: Vec<(u32, u32)> = tree
            .cursor(i)
            .children()
            .filter(|c| c.is(C::SsaArm))
            .map(|c| (c.index(), c.index() + c.size()))
            .collect();
        let entries: Vec<BlockId> = arms
            .iter()
            .map(|_| self.ssa.add_sealed_successor(pre))
            .collect();
        let exits = entries.clone();
        self.branch_stack.push(BranchFrame {
            arms,
            entries,
            exits,
            pre,
            end,
        });
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
            for kind in [C::Alias, C::SsaHint] {
                if let Some(alias) = c.child_sym(kind) {
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
        for alias in c.children().filter(|ch| ch.is(C::Alias) && ch.sym() != 0) {
            self.ssa
                .write_variable(alias.sym(), parent_block, Value::LocalDef(idx));
        }
        if let Some(&(Some(parent), _, _)) = self.def_stack.last() {
            tree.add_edge(parent, i, EdgeKind::Defines);
        }
        for st in c
            .children()
            .filter(|ch| ch.is(C::SuperType) && ch.sym() != 0)
        {
            let st_sym = st.sym();
            for &dn in &self.defs {
                if tree.cursor(dn).child_sym(C::DefName) == Some(st_sym) {
                    tree.add_edge(i, dn, EdgeKind::Extends);
                    break;
                }
            }
        }
        if crate::canonical::is_scoped_def(tree.cursor(i)) {
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
                let obj_sym = root_object_sym(member);
                self.resolve_obj(tree, obj_sym, method, from);
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
        for r in self.lookup(obj) {
            match r {
                Linked::Type(ts) if method != 0 => self.resolve_method(tree, ts, method, from),
                Linked::Import(node) => tree.add_edge(from, node, EdgeKind::Imports),
                _ => {}
            }
        }
    }

    fn handle_binding(&mut self, tree: &Tree, i: u32) -> bool {
        let lhs = tree.sym(i);
        if lhs == 0 || tree.cursor(i).has(C::Ivar) {
            return false;
        }
        if self.ssa.has_variable_in_block(lhs, self.cur) {
            self.cur = self.ssa.add_sealed_successor(self.cur);
        }

        let c = tree.cursor(i);

        if let Some(rhs) = c.child(C::Rhs) {
            if let Some(branch) = rhs.child(C::SsaBranch) {
                self.walk_branch_binding(tree, branch, lhs);
                self.update_branch_exit(i);
                return true;
            }

            let rhs_idx = rhs.index();
            let rhs_end = rhs_idx + rhs.size();
            self.walk_range(tree, rhs_idx + 1, rhs_end);

            let val = self.classify_rhs_value(tree, rhs, i);
            self.ssa.write_variable(lhs, self.cur, val);
            self.update_branch_exit(i);
            return true;
        }

        let val = if let Some(ts) = c.child_sym(C::SsaTyped) {
            Value::Type(ts)
        } else {
            Value::Opaque
        };

        self.ssa.write_variable(lhs, self.cur, val);
        self.update_branch_exit(i);
        true
    }

    fn classify_rhs_value(&mut self, tree: &Tree, rhs: Cursor<'_>, binding: u32) -> Value {
        if let Some(ts) = tree.cursor(binding).child_sym(C::SsaTyped) {
            return Value::Type(ts);
        }

        if let Some(callee) = rhs.child(C::Call).and_then(|call| call.child(C::Callee)) {
            if let Some(member) = callee.child(C::Member) {
                let method = member.sym();
                let obj_node = member.child(C::Object);
                let ivar = obj_node.and_then(|o| o.child(C::Ivar));
                let obj_sym = ivar.or(obj_node).map(|n| n.sym()).unwrap_or(0);
                return self.value_from_method(tree, obj_sym, method, binding, ivar.is_some());
            }
            if callee.child(C::Ivar).is_some() {
                return Value::Opaque;
            }
            if callee.sym() != 0 {
                return self.value_from_name(tree, callee.sym());
            }
            return Value::Opaque;
        }

        let sym = self.tail_sym(rhs);
        if sym != 0 {
            let r = self.lookup(sym);
            if self.any_class(tree, &r) {
                Value::Type(sym)
            } else {
                Value::Alias(sym)
            }
        } else {
            Value::Opaque
        }
    }

    fn walk_branch_binding(&mut self, tree: &Tree, branch: Cursor<'_>, lhs: u32) {
        let pre = self.cur;
        let mut exits = Vec::new();

        for arm in branch.children().filter(|c| c.is(C::SsaArm)) {
            let block = self.ssa.add_sealed_successor(pre);
            self.cur = block;
            self.walk_range(tree, arm.index() + 1, arm.index() + arm.size());
            let sym = self.tail_sym(arm);
            if sym != 0 {
                let val = {
                    let r = self.lookup(sym);
                    if self.any_class(tree, &r) {
                        Value::Type(sym)
                    } else {
                        Value::Alias(sym)
                    }
                };
                self.ssa.write_variable(lhs, self.cur, val);
            }
            exits.push(self.cur);
        }

        self.cur = self.ssa.add_sealed_join(exits);
    }

    fn tail_sym(&self, node: Cursor<'_>) -> u32 {
        let last = node.children().filter(|c| c.named()).last();
        match last {
            Some(c) if c.size() == 1 && c.sym() != 0 => c.sym(),
            Some(c) if c.size() > 1 => self.tail_sym(c),
            Some(c) => c.sym(),
            None => node.sym(),
        }
    }

    fn update_branch_exit(&mut self, i: u32) {
        if let Some(br) = self.branch_stack.last_mut() {
            for (idx, &(start, end)) in br.arms.iter().enumerate() {
                if i >= start && i < end {
                    br.exits[idx] = self.cur;
                    break;
                }
            }
        }
    }

    // ── SSA resolution ──

    fn lookup(&mut self, sym: u32) -> Vec<Linked> {
        let result: Vec<Linked> = self.ssa
            .read_variable(sym, self.cur)
            .iter()
            .filter_map(|pv| match pv {
                ParseValue::LocalDef(di) => Some(Linked::Def(self.defs[*di as usize])),
                ParseValue::ImportRef(ii) => {
                    self.imports.get(*ii as usize).map(|&n| Linked::Import(n))
                }
                ParseValue::Type(ts) if *ts != 0 => Some(Linked::Type(*ts)),
                _ => None,
            })
            .collect();
        if result.is_empty() {
            let entry = BlockId(0);
            return self.ssa
                .read_variable(sym, entry)
                .iter()
                .filter_map(|pv| match pv {
                    ParseValue::LocalDef(di) => Some(Linked::Def(self.defs[*di as usize])),
                    ParseValue::ImportRef(ii) => {
                        self.imports.get(*ii as usize).map(|&n| Linked::Import(n))
                    }
                    ParseValue::Type(ts) if *ts != 0 => Some(Linked::Type(*ts)),
                    _ => None,
                })
                .collect();
        }
        result
    }

    fn emit(&self, tree: &Tree, r: &Linked, from: u32) {
        match r {
            Linked::Def(node) => {
                if crate::canonical::is_callable_def(tree.cursor(*node)) {
                    tree.add_edge(from, *node, EdgeKind::Calls);
                }
            }
            Linked::Import(node) => tree.add_edge(from, *node, EdgeKind::Imports),
            Linked::Type(_) => {}
        }
    }

    fn is_class(&self, tree: &Tree, node: u32) -> bool {
        crate::canonical::def_type_of(tree.cursor(node)) == Some(C::Class)
    }

    fn any_class(&self, tree: &Tree, resolved: &[Linked]) -> bool {
        resolved
            .iter()
            .any(|r| matches!(r, Linked::Def(n) if self.is_class(tree, *n)))
    }

    // ── Resolution ──

    fn resolve_obj(&mut self, tree: &Tree, obj: u32, method: u32, from: u32) {
        for r in self.lookup(obj) {
            match r {
                Linked::Type(ts) => self.resolve_method(tree, ts, method, from),
                Linked::Def(node) => {
                    if let Some(m) = self.find_method_in(tree, node, method) {
                        tree.add_edge(from, m, EdgeKind::Calls);
                    } else {
                        self.emit(tree, &Linked::Def(node), from);
                    }
                }
                _ => self.emit(tree, &r, from),
            }
        }
    }

    fn resolve_name(&mut self, tree: &Tree, sym: u32, from: u32) {
        let mut targets = self.lookup(sym);
        if targets.is_empty() {
            targets = self.lookup(self.wildcard);
            for r in &targets {
                self.emit(tree, r, from);
            }
        }
        for r in &targets {
            match r {
                Linked::Type(ts) => {
                    for inner in self.lookup(*ts) {
                        if let Linked::Def(target) = inner {
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
        for r in self.lookup(type_sym) {
            if let Linked::Def(cls) = r {
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
            crate::canonical::def_type_of(n)
                .is_some_and(|k| matches!(k, C::Class | C::ImplBlock | C::Trait))
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
        let container_name = tree.cursor(container).child_sym(C::DefName);
        let mut search = vec![container];
        if let Some(cn) = container_name {
            for &dn in &self.defs {
                if dn != container
                    && tree.cursor(dn).child_sym(C::DefName) == Some(cn)
                    && !search.contains(&dn)
                {
                    search.push(dn);
                }
            }
        }
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

    fn value_from_name(&mut self, tree: &Tree, sym: u32) -> Value {
        let resolved = self.lookup(sym);
        if self.any_class(tree, &resolved) {
            return Value::Type(sym);
        }
        for r in &resolved {
            if let Linked::Def(node) = r {
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
            if let Linked::Def(cls) = r {
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

fn root_object_sym(member: crate::tree::Cursor) -> u32 {
    let Some(obj) = member.child(C::Object) else {
        return 0;
    };
    if obj.child(C::Ivar).is_some() {
        return 0;
    }
    if obj.sym() != 0 {
        return obj.sym();
    }
    if let Some(inner) = obj.child(C::Member) {
        return root_object_sym(inner);
    }
    0
}

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
        loop_stack: Vec::new(),
    };

    f.walk_range(tree, 0, tree.len());

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
            .filter(|c| c.is(C::Decorator) && c.sym() != 0)
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
