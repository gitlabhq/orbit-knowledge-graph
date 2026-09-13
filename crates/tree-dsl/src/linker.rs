use crate::canonical::Canonical;
use crate::lang::Lang;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{EdgeKind, Tree};

enum Target {
    Name(u32),
    Method { obj: u32, method: u32 },
    SelfMethod { attr: u32, method: u32 },
    SelfDirect(u32),
}

fn read_target(tree: &Tree, callee: u32) -> Option<Target> {
    let nr = tree.nr(callee);
    if let Some(member) = nr.child(Canonical::Member) {
        let method = member.sym();
        let obj = member.child(Canonical::Object);
        let ivar = obj.and_then(|o| o.child(Canonical::Ivar));
        let obj_sym = ivar
            .map(|iv| iv.sym())
            .or_else(|| obj.map(|o| o.sym()))
            .unwrap_or(0);
        if ivar.is_some() {
            Some(Target::SelfMethod {
                attr: obj_sym,
                method,
            })
        } else {
            Some(Target::Method {
                obj: obj_sym,
                method,
            })
        }
    } else if let Some(ivar) = nr.child(Canonical::Ivar) {
        Some(Target::SelfDirect(ivar.sym()))
    } else {
        let sym = nr.sym();
        if sym != 0 {
            Some(Target::Name(sym))
        } else {
            None
        }
    }
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

    fn handle_import(&mut self, tree: &Tree, i: u32) {
        for c in tree.nr(i).children() {
            if c.is(Canonical::Name) && c.sym() != 0 {
                let sym = c.sym();
                self.import_count += 1;
                self.imports.push(c.index());
                self.import_names.push(sym);
                self.ssa
                    .write_variable(sym, self.cur, Value::ImportRef(self.import_count - 1));
                if let Some(alias) = c.child_sym(Canonical::Alias) {
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
        let name = tree.nr(i).child_sym(Canonical::DefName).unwrap_or(0);
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
        if tree.nr(i).has(Canonical::Scope) {
            self.def_stack.push((Some(i), end, parent_block));
        }
    }

    fn handle_call(&mut self, tree: &Tree, i: u32) {
        let Some(callee) = tree.nr(i).child(Canonical::Callee) else {
            return;
        };
        let Some(target) = read_target(tree, callee.index()) else {
            return;
        };
        self.resolve_call(tree, target, self.enclosing());
    }

    fn handle_standalone_member(&mut self, tree: &Tree, i: u32) {
        let obj = tree
            .nr(i)
            .child(Canonical::Object)
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
                    self.resolve_method(tree, *ts, method, from);
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
        if tree.nr(i).has(Canonical::Ivar) {
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

    fn resolve_call(&mut self, tree: &Tree, target: Target, from: u32) {
        match target {
            Target::Name(sym) => self.resolve_name_call(tree, sym, from),
            Target::Method { obj, method } => {
                for pv in &self.ssa.read_variable(obj, self.cur) {
                    match pv {
                        ParseValue::Type(ts) if *ts != 0 => {
                            self.resolve_method(tree, *ts, method, from);
                        }
                        _ => self.emit_edge(tree, pv, from),
                    }
                }
            }
            Target::SelfMethod { attr, method } => {
                if let Some(cls) = find_enclosing_class(tree, from, &self.containers) {
                    if let Some(ts) = find_ivar_type(tree, cls, attr) {
                        self.resolve_method(tree, ts, method, from);
                    }
                }
            }
            Target::SelfDirect(method) => {
                if method != 0 {
                    if let Some(cls) = find_enclosing_class(tree, from, &self.containers) {
                        if let Some(m) = find_method(tree, &self.defs, cls, method) {
                            tree.add_edge(from, m, EdgeKind::Calls);
                        }
                    }
                }
            }
        }
    }

    fn resolve_name_call(&mut self, tree: &Tree, sym: u32, from: u32) {
        let mut reaching = self.ssa.read_variable(sym, self.cur);
        if reaching.is_empty() {
            let wildcard = self.ssa.read_variable(self.wildcard, self.cur);
            if !wildcard.is_empty() {
                for pv in &wildcard {
                    self.emit_edge(tree, pv, from);
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
                            if let Some(callable) = tree.nr(target).child_sym(Canonical::Callable) {
                                if let Some(m) = find_method(tree, &self.defs, target, callable) {
                                    tree.add_edge(from, m, EdgeKind::Calls);
                                }
                            } else {
                                tree.add_edge(from, target, EdgeKind::Calls);
                            }
                        }
                    }
                }
                _ => self.emit_edge(tree, pv, from),
            }
        }
    }

    fn resolve_method(&mut self, tree: &Tree, type_sym: u32, method_sym: u32, from: u32) {
        for cpv in &self.ssa.read_variable(type_sym, self.cur) {
            if let ParseValue::LocalDef(cdi) = cpv {
                if let Some(m) = find_method(tree, &self.defs, self.defs[*cdi as usize], method_sym)
                {
                    tree.add_edge(from, m, EdgeKind::Calls);
                }
            }
        }
    }

    fn emit_edge(&self, tree: &Tree, pv: &ParseValue, from: u32) {
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

    fn classify_rhs(&mut self, tree: &Tree, node: u32) -> Value {
        let nr = tree.nr(node);
        let Some(rhs) = nr.child(Canonical::Rhs) else {
            return Value::Opaque;
        };
        if let Some(call) = rhs.child(Canonical::Call) {
            if let Some(callee) = call.child(Canonical::Callee) {
                if let Some(target) = read_target(tree, callee.index()) {
                    return self.value_from_target(tree, target, node);
                }
            }
            return Value::Opaque;
        }
        let sym = rhs.sym();
        if sym != 0 {
            let is_class = self.ssa.read_variable(sym, self.cur).iter().any(|pv| {
                if let ParseValue::LocalDef(di) = pv {
                    tree.nr(self.defs[*di as usize])
                        .child_sym(Canonical::DefType)
                        .is_some_and(|dt| dt == self.class_sym)
                } else {
                    false
                }
            });
            if is_class {
                Value::Type(sym)
            } else {
                Value::Alias(sym)
            }
        } else {
            Value::Opaque
        }
    }

    fn value_from_target(&mut self, tree: &Tree, target: Target, binding: u32) -> Value {
        match target {
            Target::Name(sym) => {
                let reaching = self.ssa.read_variable(sym, self.cur);
                let is_class = reaching.iter().any(|pv| {
                    if let ParseValue::LocalDef(di) = pv {
                        tree.nr(self.defs[*di as usize])
                            .child_sym(Canonical::DefType)
                            .is_some_and(|dt| dt == self.class_sym)
                    } else {
                        false
                    }
                });
                if is_class {
                    return Value::Type(sym);
                }
                let rt = reaching.iter().find_map(|pv| {
                    if let ParseValue::LocalDef(di) = pv {
                        return_type_of_def(tree, self.defs[*di as usize])
                    } else {
                        None
                    }
                });
                match rt {
                    Some(rt_sym) => {
                        let found = self.defs.iter().position(|&dn| {
                            tree.nr(dn).child_sym(Canonical::DefName).unwrap_or(0) == rt_sym
                        });
                        if let Some(di) = found {
                            if tree
                                .nr(self.defs[di])
                                .child_sym(Canonical::DefType)
                                .is_some_and(|dt| dt == self.class_sym)
                            {
                                Value::Type(rt_sym)
                            } else {
                                Value::LocalDef(di as u32)
                            }
                        } else {
                            Value::Type(rt_sym)
                        }
                    }
                    None => Value::Opaque,
                }
            }
            Target::Method { obj, method } | Target::SelfMethod { attr: obj, method } => {
                let is_ivar = std::matches!(target, Target::SelfMethod { .. });
                let obj_type = if is_ivar {
                    find_enclosing_class(tree, binding, &self.containers)
                        .and_then(|cls| find_ivar_type(tree, cls, obj))
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
                        if let Some(m) =
                            find_method(tree, &self.defs, self.defs[*cdi as usize], method)
                        {
                            if let Some(rt) = return_type_of_def(tree, m) {
                                return Value::Type(rt);
                            }
                        }
                    }
                }
                Value::Opaque
            }
            Target::SelfDirect(_) => Value::Opaque,
        }
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

        if k == Canonical::Import || k == Canonical::ImportType {
            f.handle_import(tree, i);
            i += n.size.max(1);
            continue;
        }
        if tree.nr(i).child_sym(Canonical::DefType).is_some() {
            f.handle_def(tree, i, end);
            i += 1;
            continue;
        }
        if k == Canonical::Call {
            f.handle_call(tree, i);
            i += 1;
            continue;
        }
        if k == Canonical::Member {
            let pk = tree.nr(i).parent().map_or(0, |p| p.kind());
            if pk != Canonical::Call && pk != Canonical::Callee {
                f.handle_standalone_member(tree, i);
            }
            i += 1;
            continue;
        }
        if k == Canonical::Binding {
            f.handle_binding(tree, i);
            i += 1;
            continue;
        }
        if k == Canonical::Branch {
            let pre = f.cur;
            let arms: Vec<(u32, u32)> = tree
                .nr(i)
                .children()
                .filter(|c| c.is(Canonical::Arm))
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
        if k == Canonical::Loop {
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
            .nr(dn)
            .children()
            .filter(|c| (c.is(Canonical::SuperType) || c.is(Canonical::Decorator)) && c.sym() != 0)
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

fn return_type_of_def(tree: &Tree, def: u32) -> Option<u32> {
    if let Some(rt) = tree.nr(def).child_sym(Canonical::ReturnType) {
        return Some(rt);
    }
    let mut binds: Vec<(u32, u32)> = Vec::new();
    for d in tree.nr(def).descendants() {
        if d.is(Canonical::Binding) && d.sym() != 0 {
            let callee = d
                .child(Canonical::Rhs)
                .and_then(|rhs| rhs.child(Canonical::Call))
                .and_then(|c| c.child(Canonical::Callee))
                .map(|c| c.sym())
                .unwrap_or(0);
            if callee != 0 {
                binds.push((d.sym(), callee));
            }
        }
    }

    for d in tree.nr(def).descendants() {
        if !d.is(Canonical::Return) {
            continue;
        }
        for c in d.children() {
            if c.is(Canonical::Call) {
                return c
                    .child(Canonical::Callee)
                    .map(|c2| c2.sym())
                    .filter(|&v| v != 0);
            }
            let sym = c.sym();
            if sym != 0 {
                for &(lhs, callee) in &binds {
                    if lhs == sym {
                        return Some(callee);
                    }
                }
                return Some(sym);
            }
        }
    }
    None
}

fn find_method(tree: &Tree, defs: &[u32], container: u32, name: u32) -> Option<u32> {
    let mut search = vec![container];
    let mut si = 0;
    while si < search.len() {
        for d in tree.nr(search[si]).descendants() {
            if d.is(Canonical::DefType) {
                if let Some(m) = d.parent() {
                    if m.index() != search[si]
                        && m.child_sym(Canonical::DefName).unwrap_or(0) == name
                    {
                        return Some(m.index());
                    }
                }
            }
        }
        for c in tree.nr(search[si]).children() {
            if c.is(Canonical::SuperType) && c.sym() != 0 {
                let sn = c.sym();
                for &dn in defs {
                    if tree.nr(dn).child_sym(Canonical::DefName).unwrap_or(0) == sn
                        && !search.contains(&dn)
                    {
                        search.push(dn);
                    }
                }
            }
        }
        si += 1;
    }
    None
}

fn find_ivar_type(tree: &Tree, class: u32, attr: u32) -> Option<u32> {
    for d in tree.nr(class).descendants() {
        if d.is(Canonical::Binding) && d.child(Canonical::Ivar).is_some_and(|iv| iv.sym() == attr) {
            return d
                .child(Canonical::Rhs)
                .and_then(|rhs| rhs.child(Canonical::Call))
                .and_then(|c| c.child(Canonical::Callee))
                .map(|c| c.sym())
                .filter(|&v| v != 0);
        }
    }
    None
}

fn find_enclosing_class(tree: &Tree, node: u32, containers: &[u32]) -> Option<u32> {
    let check = |n| {
        tree.nr(n)
            .child_sym(Canonical::DefType)
            .is_some_and(|dt| containers.contains(&dt))
    };
    if check(node) {
        Some(node)
    } else {
        tree.nr(node)
            .enclosing(|n| {
                n.child_sym(Canonical::DefType)
                    .is_some_and(|dt| containers.contains(&dt))
            })
            .map(|n| n.index())
    }
}
