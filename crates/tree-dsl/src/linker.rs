use crate::canonical::Canonical as C;
use crate::lang::Lang;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{Cursor, EdgeKind, Step, Tree, infer_return_type};

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

    fn handle_import(&mut self, tree: &Tree, i: u32) {
        for c in tree.cursor(i).children() {
            if c.is(C::Name) && c.sym() != 0 {
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
    }

    fn handle_def(&mut self, tree: &Tree, i: u32, end: u32) {
        let name = tree.cursor(i).child_sym(C::DefName).unwrap_or(0);
        if name == 0 {
            return;
        }
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
        if tree.cursor(i).has(C::Scope) {
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
                        self.resolve_type_method(tree, ts, method, from);
                    }
                }
            } else {
                let obj = member.child(C::Object).map(|o| o.sym()).unwrap_or(0);
                for pv in &self.ssa.read_variable(obj, self.cur) {
                    match pv {
                        ParseValue::Type(ts) if *ts != 0 => {
                            self.resolve_type_method(tree, *ts, method, from);
                        }
                        _ => self.emit_pv(tree, pv, from),
                    }
                }
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
        let method = tree.sym(i);
        if obj == 0 {
            return;
        }
        let from = self.enclosing();
        for pv in &self.ssa.read_variable(obj, self.cur) {
            match pv {
                ParseValue::ImportRef(ii) => {
                    if let Some(&imp) = self.imports.get(*ii as usize) {
                        tree.add_edge(from, imp, EdgeKind::Imports);
                    }
                }
                ParseValue::Type(ts) if *ts != 0 && method != 0 => {
                    self.resolve_type_method(tree, *ts, method, from);
                }
                _ => {}
            }
        }
    }

    fn handle_binding(&mut self, tree: &Tree, i: u32) {
        let lhs = tree.sym(i);
        if lhs == 0 {
            return;
        }
        if tree.cursor(i).has(C::Ivar) {
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

    // ── Helpers ──

    fn is_class_def(&self, tree: &Tree, def: u32) -> bool {
        tree.cursor(def)
            .child_sym(C::DefType)
            .is_some_and(|dt| dt == self.class_sym)
    }

    fn is_class_pv(&self, tree: &Tree, pv: &ParseValue) -> bool {
        matches!(pv, ParseValue::LocalDef(di) if self.is_class_def(tree, self.defs[*di as usize]))
    }

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
                let sn = c.sym();
                for &dn in &self.defs {
                    if tree.cursor(dn).child_sym(C::DefName) == Some(sn) && !search.contains(&dn) {
                        search.push(dn);
                    }
                }
            }
            si += 1;
        }
        None
    }

    // ── Resolution ──

    fn resolve_name(&mut self, tree: &Tree, sym: u32, from: u32) {
        let mut reaching = self.ssa.read_variable(sym, self.cur);
        if reaching.is_empty() {
            let wildcard = self.ssa.read_variable(self.wildcard, self.cur);
            if !wildcard.is_empty() {
                for pv in &wildcard {
                    self.emit_pv(tree, pv, from);
                }
                reaching = wildcard;
            }
        }
        for pv in &reaching {
            match pv {
                ParseValue::Type(ts) if *ts != 0 => {
                    for cpv in &self.ssa.read_variable(*ts, self.cur) {
                        if let ParseValue::LocalDef(cdi) = cpv {
                            let target = self.defs[*cdi as usize];
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
                _ => self.emit_pv(tree, pv, from),
            }
        }
    }

    fn resolve_type_method(&mut self, tree: &Tree, type_sym: u32, method_sym: u32, from: u32) {
        for cpv in &self.ssa.read_variable(type_sym, self.cur) {
            if let ParseValue::LocalDef(cdi) = cpv {
                if let Some(m) = self.find_method_in(tree, self.defs[*cdi as usize], method_sym) {
                    tree.add_edge(from, m, EdgeKind::Calls);
                }
            }
        }
    }

    fn emit_pv(&self, tree: &Tree, pv: &ParseValue, from: u32) {
        match pv {
            ParseValue::LocalDef(di) => {
                tree.add_edge(from, self.defs[*di as usize], EdgeKind::Calls)
            }
            ParseValue::ImportRef(ii) => {
                if let Some(&imp) = self.imports.get(*ii as usize) {
                    tree.add_edge(from, imp, EdgeKind::Imports);
                }
            }
            _ => {}
        }
    }

    // ── RHS classification ──

    fn classify_rhs(&mut self, tree: &Tree, node: u32) -> Value {
        let nr = tree.cursor(node);
        let Some(rhs) = nr.child(C::Rhs) else {
            return Value::Opaque;
        };
        if let Some(call) = rhs.child(C::Call) {
            if let Some(callee) = call.child(C::Callee) {
                if let Some(member) = callee.child(C::Member) {
                    let method = member.sym();
                    let obj_cursor = member.child(C::Object);
                    let ivar = obj_cursor.and_then(|o| o.child(C::Ivar));
                    let obj_sym = ivar
                        .map(|iv| iv.sym())
                        .or_else(|| obj_cursor.map(|o| o.sym()))
                        .unwrap_or(0);
                    let is_ivar = ivar.is_some();
                    return self.value_from_method_call(tree, obj_sym, method, node, is_ivar);
                } else if callee.child(C::Ivar).is_some() {
                    return Value::Opaque;
                } else {
                    let sym = callee.sym();
                    if sym != 0 {
                        return self.value_from_name_call(tree, sym);
                    }
                }
            }
            return Value::Opaque;
        }
        let sym = rhs.sym();
        if sym != 0 {
            if self
                .ssa
                .read_variable(sym, self.cur)
                .iter()
                .any(|pv| self.is_class_pv(tree, pv))
            {
                Value::Type(sym)
            } else {
                Value::Alias(sym)
            }
        } else {
            Value::Opaque
        }
    }

    fn value_from_name_call(&mut self, tree: &Tree, sym: u32) -> Value {
        let reaching = self.ssa.read_variable(sym, self.cur);
        if reaching.iter().any(|pv| self.is_class_pv(tree, pv)) {
            return Value::Type(sym);
        }
        let rt = reaching.iter().find_map(|pv| {
            if let ParseValue::LocalDef(di) = pv {
                infer_return_type(tree.cursor(self.defs[*di as usize]))
            } else {
                None
            }
        });
        match rt {
            Some(rt_sym) => {
                let found = self
                    .defs
                    .iter()
                    .position(|&dn| tree.cursor(dn).child_sym(C::DefName).unwrap_or(0) == rt_sym);
                match found {
                    Some(di) if self.is_class_def(tree, self.defs[di]) => Value::Type(rt_sym),
                    Some(di) => Value::LocalDef(di as u32),
                    None => Value::Type(rt_sym),
                }
            }
            None => Value::Opaque,
        }
    }

    fn value_from_method_call(
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
            self.ssa.read_variable(obj, self.cur).iter().find_map(|pv| {
                if let ParseValue::Type(ts) = pv {
                    Some(*ts)
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
        for cpv in &self.ssa.read_variable(ts, self.cur) {
            if let ParseValue::LocalDef(cdi) = cpv {
                if let Some(m) = self.find_method_in(tree, self.defs[*cdi as usize], method) {
                    if let Some(rt) = infer_return_type(tree.cursor(m)) {
                        return Value::Type(rt);
                    }
                }
            }
        }
        Value::Opaque
    }
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
        class_sym: lang.syms.intern("Class"),
        containers: vec![
            lang.syms.intern("Class"),
            lang.syms.intern("Impl"),
            lang.syms.intern("Trait"),
        ],
    };

    let mut i = 0u32;
    let len = tree.nodes.len() as u32;

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
            let pk = tree.cursor(i).parent().map_or(0, |p| p.kind());
            if pk != C::Call && pk != C::Callee {
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
        let syms: Vec<u32> = tree
            .cursor(dn)
            .children()
            .filter(|c| (c.is(C::SuperType) || c.is(C::Decorator)) && c.sym() != 0)
            .map(|c| c.sym())
            .collect();
        for sym in syms {
            for pv in &f.ssa.read_variable(sym, entry) {
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
