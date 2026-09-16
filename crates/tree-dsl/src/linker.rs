use crate::canonical::Canonical as C;
use crate::lang::Lang;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{Cursor, EdgeKind, Step, Tree, infer_return_type};

enum Linked {
    Def(u32),
    Import(u32),
    Type(u32),
}

enum WorkItem {
    Visit(u32),
    ExitScope,
}

struct Fold {
    ssa: SsaEngine,
    cur: BlockId,
    def_count: u32,
    import_count: u32,
    defs: Vec<u32>,
    imports: Vec<u32>,
    import_names: Vec<u32>,
    def_stack: Vec<(Option<u32>, BlockId)>,
    wildcard: u32,
}

impl Fold {
    fn enclosing(&self) -> u32 {
        self.def_stack.last().and_then(|&(d, _)| d).unwrap_or(0)
    }

    fn walk(&mut self, tree: &Tree, root: Cursor) {
        let mut stack: Vec<WorkItem> = Vec::new();

        let children: Vec<u32> = root.children().map(|c| c.index()).collect();
        for &child in children.iter().rev() {
            stack.push(WorkItem::Visit(child));
        }

        while let Some(item) = stack.pop() {
            match item {
                WorkItem::ExitScope => {
                    if self.def_stack.len() > 1 {
                        let (_, saved) = self.def_stack.pop().unwrap();
                        self.cur = saved;
                    }
                }

                WorkItem::Visit(idx) => {
                    self.dispatch(tree, idx, &mut stack);
                }
            }
        }
    }

    fn push_children(&self, tree: &Tree, idx: u32, stack: &mut Vec<WorkItem>) {
        let children: Vec<u32> = tree.cursor(idx).children().map(|ch| ch.index()).collect();
        for &child in children.iter().rev() {
            stack.push(WorkItem::Visit(child));
        }
    }

    fn dispatch(&mut self, tree: &Tree, idx: u32, stack: &mut Vec<WorkItem>) {
        let c = tree.cursor(idx);
        let k = c.kind();

        if k == C::Import || k == C::ImportType {
            self.handle_import(tree, idx);
            return;
        }
        if crate::canonical::has_def_type(c) {
            self.handle_def(tree, idx, stack);
            return;
        }
        if k == C::Call {
            self.handle_call(tree, idx);
            self.push_children(tree, idx, stack);
            return;
        }
        if k == C::Member {
            if c.parent().map_or(0, |p| p.kind()) != C::Call
                && c.parent().map_or(0, |p| p.kind()) != C::Callee
            {
                self.handle_standalone_member(tree, idx);
            }
            self.push_children(tree, idx, stack);
            return;
        }
        if k == C::Binding {
            if !self.handle_binding(tree, idx, stack) {
                self.push_children(tree, idx, stack);
            }
            return;
        }
        if k == C::SsaBranch {
            self.handle_branch(tree, idx, stack);
            return;
        }
        if k == C::SsaLoop {
            self.handle_loop(tree, idx, stack);
            return;
        }

        self.push_children(tree, idx, stack);
    }

    fn handle_branch(&mut self, tree: &Tree, idx: u32, _stack: &mut Vec<WorkItem>) {
        let branch = tree.cursor(idx);
        let non_arms: Vec<u32> = branch
            .children()
            .filter(|c| !c.is(C::SsaArm))
            .map(|c| c.index())
            .collect();
        for &child in &non_arms {
            let mut tmp = Vec::new();
            self.dispatch(tree, child, &mut tmp);
            while let Some(item) = tmp.pop() {
                if let WorkItem::Visit(i) = item {
                    self.dispatch(tree, i, &mut tmp);
                }
            }
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
            self.walk_children(tree, arm);
            exit_blocks.push(self.cur);
        }
        exit_blocks.push(pre);
        self.cur = self.ssa.add_sealed_join(exit_blocks);
    }

    fn handle_loop(&mut self, tree: &Tree, idx: u32, _stack: &mut Vec<WorkItem>) {
        let (header, body) = self.ssa.begin_loop(self.cur);
        self.cur = body;
        self.walk_children(tree, idx);
        self.cur = self.ssa.finish_loop(header, self.cur);
    }

    fn walk_children(&mut self, tree: &Tree, idx: u32) {
        let mut child_stack: Vec<WorkItem> = Vec::new();
        let children: Vec<u32> = tree.cursor(idx).children().map(|c| c.index()).collect();
        for &child in children.iter().rev() {
            child_stack.push(WorkItem::Visit(child));
        }
        while let Some(item) = child_stack.pop() {
            match item {
                WorkItem::ExitScope => {
                    if self.def_stack.len() > 1 {
                        let (_, saved) = self.def_stack.pop().unwrap();
                        self.cur = saved;
                    }
                }

                WorkItem::Visit(i) => {
                    self.dispatch(tree, i, &mut child_stack);
                }
            }
        }
    }

    fn handle_import(&mut self, tree: &Tree, idx: u32) {
        for c in tree.cursor(idx).names() {
            let sym = c.sym();
            self.import_count += 1;
            self.imports.push(c.index());
            self.import_names.push(sym);
            self.ssa
                .write_variable(sym, self.cur, Value::ImportRef(self.import_count - 1));
            for kind in [C::Alias, C::SsaHint] {
                if let Some(alias) = c.child_sym(kind)
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

    fn handle_def(&mut self, tree: &Tree, idx: u32, stack: &mut Vec<WorkItem>) {
        let c = tree.cursor(idx);
        let name = match c.child_sym(C::DefName) {
            Some(n) => n,
            None => return,
        };
        let parent_block = self.cur;
        self.cur = self.ssa.add_sealed_successor(parent_block);
        let def_idx = self.def_count;
        self.def_count += 1;
        self.defs.push(idx);
        self.ssa
            .write_variable(name, parent_block, Value::LocalDef(def_idx));
        for alias in c.children().filter(|ch| ch.is(C::Alias) && ch.sym() != 0) {
            self.ssa
                .write_variable(alias.sym(), parent_block, Value::LocalDef(def_idx));
        }
        if let Some(&(Some(parent), _)) = self.def_stack.last() {
            tree.add_edge(parent, idx, EdgeKind::Defines);
        }
        for st in c
            .children()
            .filter(|ch| ch.is(C::SuperType) && ch.sym() != 0)
        {
            let st_sym = st.sym();
            for &dn in &self.defs {
                if tree.cursor(dn).child_sym(C::DefName) == Some(st_sym) {
                    tree.add_edge(idx, dn, EdgeKind::Extends);
                    break;
                }
            }
        }
        if crate::canonical::is_scoped_def(c) {
            self.def_stack.push((Some(idx), parent_block));
            stack.push(WorkItem::ExitScope);
            let children: Vec<u32> = c.children().map(|ch| ch.index()).collect();
            for &child in children.iter().rev() {
                stack.push(WorkItem::Visit(child));
            }
        }
    }

    fn handle_call(&mut self, tree: &Tree, idx: u32) {
        let callee = match tree.cursor(idx).child(C::Callee) {
            Some(c) => c,
            None => return,
        };
        let from = self.enclosing();

        if let Some(member) = callee.child(C::Member) {
            let method = member.sym();
            if let Some(ivar) = member.child(C::Object).and_then(|o| o.child(C::Ivar)) {
                if let Some(cls) = self.enclosing_class(tree, from)
                    && let Some(ts) = self.ivar_type(tree, cls, ivar.sym())
                {
                    self.resolve_method(tree, ts, method, from);
                }
            } else {
                let obj_sym = root_object_sym(member);
                self.resolve_obj(tree, obj_sym, method, from);
            }
        } else if let Some(ivar) = callee.child(C::Ivar) {
            if ivar.sym() != 0
                && let Some(cls) = self.enclosing_class(tree, from)
                && let Some(m) = self.find_method_in(tree, cls, ivar.sym())
            {
                tree.add_edge(from, m, EdgeKind::Calls);
            }
        } else if callee.sym() != 0 {
            self.resolve_name(tree, callee.sym(), from);
        }
    }

    fn handle_standalone_member(&mut self, tree: &Tree, idx: u32) {
        let obj = tree
            .cursor(idx)
            .child(C::Object)
            .map(|o| o.sym())
            .unwrap_or(0);
        if obj == 0 {
            return;
        }
        let method = tree.cursor(idx).sym();
        let from = self.enclosing();
        for r in self.lookup(obj) {
            match r {
                Linked::Type(ts) if method != 0 => self.resolve_method(tree, ts, method, from),
                Linked::Import(node) => tree.add_edge(from, node, EdgeKind::Imports),
                _ => {}
            }
        }
    }

    fn handle_binding(&mut self, tree: &Tree, idx: u32, _stack: &mut Vec<WorkItem>) -> bool {
        let lhs = tree.cursor(idx).sym();
        if lhs == 0 || tree.cursor(idx).has(C::Ivar) {
            return false;
        }
        if self.ssa.has_variable_in_block(lhs, self.cur) {
            self.cur = self.ssa.add_sealed_successor(self.cur);
        }

        let c = tree.cursor(idx);

        if let Some(rhs) = c.child(C::Rhs) {
            if let Some(branch) = rhs.child(C::SsaBranch) {
                self.walk_branch_binding(tree, branch, lhs);
                return true;
            }

            self.walk_children(tree, rhs.index());

            let val = self.classify_rhs_value(tree, rhs, idx);
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
            self.walk_children(tree, arm.index());
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

    fn lookup(&mut self, sym: u32) -> Vec<Linked> {
        let result: Vec<Linked> = self
            .ssa
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
            return self
                .ssa
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
            if let Linked::Def(cls) = r
                && let Some(m) = self.find_method_in(tree, cls, method)
            {
                tree.add_edge(from, m, EdgeKind::Calls);
            }
        }
    }

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
            if n.is(C::Binding)
                && n.child(C::Ivar).is_some_and(|iv| iv.sym() == attr)
                && let Some(s) = n
                    .child(C::Rhs)
                    .and_then(|r| r.child(C::Call))
                    .and_then(|c| c.child_sym(C::Callee))
            {
                return Step::Out(s);
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
            if let Linked::Def(node) = r
                && let Some(rt) = infer_return_type(tree.cursor(*node))
            {
                return self.classify_return(tree, rt);
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
            if let Linked::Def(cls) = r
                && let Some(m) = self.find_method_in(tree, cls, method)
                && let Some(rt) = infer_return_type(tree.cursor(m))
            {
                return Value::Type(rt);
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

fn root_object_sym(member: Cursor) -> u32 {
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
        def_stack: vec![(None, entry)],
        wildcard: lang.syms.intern("*"),
    };

    f.walk(tree, tree.root());

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
