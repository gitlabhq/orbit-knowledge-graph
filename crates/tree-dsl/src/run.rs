//! Pipeline: parse → rewrite → classify_methods → SSA fold → prune → edges.

use crate::grammar::{self, SupportLang};
use crate::lang::Lang;
use crate::pattern;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{EdgeKind, NONE, Tree};

// ── Pipeline ──

pub struct Pipeline {
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<crate::pattern::Rewrite>>,
    pub resolve: crate::file_tree::ResolveConfig,
}

fn lang_yaml(lang_id: SupportLang) -> Option<&'static str> {
    match lang_id {
        SupportLang::Python => Some(include_str!("../langs/python.yaml")),
        SupportLang::TypeScript | SupportLang::Tsx | SupportLang::JavaScript => {
            Some(include_str!("../langs/typescript.yaml"))
        }
        SupportLang::Rust => Some(include_str!("../langs/rust.yaml")),
        _ => None,
    }
}

impl Pipeline {
    pub fn for_lang(lang_id: SupportLang) -> (Pipeline, Lang) {
        let mut lang = Lang::new();
        let (rewrite_stages, resolve) = match lang_yaml(lang_id) {
            Some(yaml) => crate::rules::load_lang(yaml, &mut lang),
            None => (vec![], crate::file_tree::ResolveConfig::default()),
        };
        (
            Pipeline {
                lang_id,
                rewrite_stages,
                resolve,
            },
            lang,
        )
    }
}

pub fn process_file(path: &str, source: &str, lang: &mut Lang, pipeline: &Pipeline) -> Tree {
    let mut tree = grammar::parse(source, pipeline.lang_id, lang, path);
    for stage in &pipeline.rewrite_stages {
        pattern::apply_rewrites(&mut tree, lang, stage);
    }
    tree.compact();
    classify_methods(&mut tree, lang);
    ssa_fold(&mut tree, lang);
    prune(&mut tree, lang);
    tree.compact();
    tree
}

fn prune(tree: &mut Tree, lang: &Lang) {
    for i in 0..tree.nodes.len() {
        if tree.nodes[i].dead || i == 0 {
            continue;
        }
        tree.nodes[i].field = 0;
        if Lang::is_synth_name(lang.kind_name(tree.nodes[i].kind)) {
            continue;
        }
        tree.nodes[i].dead = true;
        tree.nodes[i].size = 1;
    }
}

// ── Canonical tree helpers ──

fn child_sym(tree: &Tree, node: u32, kind: u16) -> Option<u32> {
    tree.children(node)
        .find(|&c| tree.kind(c) == kind)
        .map(|c| tree.sym(c))
        .filter(|&s| s != 0)
}

fn child_node(tree: &Tree, node: u32, kind: u16) -> Option<u32> {
    tree.children(node).find(|&c| tree.kind(c) == kind)
}

fn def_name(tree: &Tree, node: u32, s: &S) -> u32 {
    child_sym(tree, node, s.defname).unwrap_or(0)
}

// ── Synthetic kind IDs ──
//
// Flat struct, one u16 per canonical kind. Populated once from the
// interner at SSA-fold start. Zero means "kind not present in this
// language" and every lookup gracefully returns None.

struct S {
    import: u16,
    import_type: u16,
    name: u16,
    alias: u16,
    deftype: u16,
    defname: u16,
    supertype: u16,
    return_type: u16,
    decorator: u16,
    callable: u16,
    call: u16,
    callee: u16,
    member: u16,
    object: u16,
    ivar: u16,
    binding: u16,
    branch: u16,
    r#loop: u16,
    scope: u16,
    arm: u16,
    rhs: u16,
    ret: u16,
}

impl S {
    fn new(lang: &Lang) -> Self {
        let k = |n: &str| lang.kinds.lookup(n) as u16;
        Self {
            import: k("__import"),
            import_type: k("__import_type"),
            name: k("__name"),
            alias: k("__alias"),
            deftype: k("__deftype"),
            defname: k("__defname"),
            supertype: k("__supertype"),
            return_type: k("__return_type"),
            decorator: k("__decorator"),
            callable: k("__callable"),
            call: k("__call"),
            callee: k("__callee"),
            member: k("__member"),
            object: k("__object"),
            ivar: k("__ivar"),
            binding: k("__binding"),
            branch: k("__branch"),
            r#loop: k("__loop"),
            scope: k("__scope"),
            arm: k("__arm"),
            rhs: k("__rhs"),
            ret: k("__return"),
        }
    }
}

// ── Call target: the identity of what a call invokes ──

enum Target {
    Name(u32),
    Method { obj: u32, method: u32 },
    SelfMethod { attr: u32, method: u32 },
    SelfDirect(u32),
}

fn read_target(tree: &Tree, callee: u32, s: &S) -> Option<Target> {
    if let Some(member) = child_node(tree, callee, s.member) {
        let method = tree.sym(member);
        let obj_node = child_node(tree, member, s.object);
        let ivar = obj_node.and_then(|o| child_node(tree, o, s.ivar));
        let obj = ivar
            .map(|iv| tree.sym(iv))
            .or_else(|| obj_node.map(|o| tree.sym(o)))
            .unwrap_or(0);
        if ivar.is_some() {
            Some(Target::SelfMethod { attr: obj, method })
        } else {
            Some(Target::Method { obj, method })
        }
    } else if let Some(ivar) = child_node(tree, callee, s.ivar) {
        Some(Target::SelfDirect(tree.sym(ivar)))
    } else {
        let sym = tree.sym(callee);
        if sym != 0 {
            Some(Target::Name(sym))
        } else {
            None
        }
    }
}

// ── classify_methods: upgrade Function → Method inside Class/Impl/Trait ──

fn classify_methods(tree: &mut Tree, lang: &mut Lang) {
    let deftype_k = lang.lookup_kind("__deftype");
    let self_method_k = lang.lookup_kind("__self_method");
    let func = lang.syms.intern("Function");
    let method = lang.syms.intern("Method");
    let assoc_fn = lang.syms.intern("AssociatedFunction");
    let class = lang.syms.intern("Class");
    let impl_ = lang.syms.intern("Impl");
    let trait_ = lang.syms.intern("Trait");

    for i in 0..tree.nodes.len() as u32 {
        if tree.kind(i) != deftype_k || tree.sym(i) != func {
            continue;
        }
        let def = tree.nodes[i as usize].parent;
        if def == NONE {
            continue;
        }
        let mut p = tree.nodes[def as usize].parent;
        while p != NONE {
            if let Some(dt) = child_sym(tree, p, deftype_k) {
                if dt == class {
                    tree.nodes[i as usize].sym = method;
                    break;
                }
                if dt == impl_ || dt == trait_ {
                    let has_self = self_method_k != 0
                        && tree.children(def).any(|c| tree.kind(c) == self_method_k);
                    tree.nodes[i as usize].sym = if has_self { method } else { assoc_fn };
                    break;
                }
            }
            p = tree.nodes[p as usize].parent;
        }
    }
}

// ── SSA fold ──

struct Fold {
    s: S,
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
        for c in tree.children(i) {
            if tree.kind(c) == self.s.name && tree.sym(c) != 0 {
                let sym = tree.sym(c);
                self.import_count += 1;
                self.imports.push(c);
                self.import_names.push(sym);
                self.ssa
                    .write_variable(sym, self.cur, Value::ImportRef(self.import_count - 1));
                if let Some(alias) = child_sym(tree, c, self.s.alias) {
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

    fn handle_def(&mut self, tree: &mut Tree, i: u32, end: u32) {
        let name = def_name(tree, i, &self.s);
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
        if tree.children(i).any(|c| tree.kind(c) == self.s.scope) {
            self.def_stack.push((Some(i), end, parent_block));
        }
    }

    fn handle_call(&mut self, tree: &mut Tree, i: u32) {
        let Some(callee) = child_node(tree, i, self.s.callee) else {
            return;
        };
        let Some(target) = read_target(tree, callee, &self.s) else {
            return;
        };
        self.resolve_call(tree, target, self.enclosing());
    }

    fn handle_standalone_member(&mut self, tree: &mut Tree, i: u32) {
        let obj = child_node(tree, i, self.s.object)
            .map(|o| tree.sym(o))
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
        if child_node(tree, i, self.s.ivar).is_some() {
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

    // ── Unified call resolution ──

    fn resolve_call(&mut self, tree: &mut Tree, target: Target, from: u32) {
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
                if let Some(cls) = find_enclosing_class(tree, from, &self.s, &self.containers) {
                    if let Some(ts) = find_ivar_type(tree, cls, attr, &self.s) {
                        self.resolve_method(tree, ts, method, from);
                    }
                }
            }
            Target::SelfDirect(method) => {
                if method != 0 {
                    if let Some(cls) = find_enclosing_class(tree, from, &self.s, &self.containers) {
                        if let Some(m) = find_method(tree, &self.defs, cls, method, &self.s) {
                            tree.add_edge(from, m, EdgeKind::Calls);
                        }
                    }
                }
            }
        }
    }

    fn resolve_name_call(&mut self, tree: &mut Tree, sym: u32, from: u32) {
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
                            if let Some(callable) = child_sym(tree, target, self.s.callable) {
                                if let Some(m) =
                                    find_method(tree, &self.defs, target, callable, &self.s)
                                {
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

    fn resolve_method(&mut self, tree: &mut Tree, type_sym: u32, method_sym: u32, from: u32) {
        for cpv in &self.ssa.read_variable(type_sym, self.cur) {
            if let ParseValue::LocalDef(cdi) = cpv {
                if let Some(m) = find_method(
                    tree,
                    &self.defs,
                    self.defs[*cdi as usize],
                    method_sym,
                    &self.s,
                ) {
                    tree.add_edge(from, m, EdgeKind::Calls);
                }
            }
        }
    }

    fn emit_edge(&self, tree: &mut Tree, pv: &ParseValue, from: u32) {
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
        let Some(rhs) = child_node(tree, node, self.s.rhs) else {
            return Value::Opaque;
        };
        if let Some(call) = child_node(tree, rhs, self.s.call) {
            if let Some(callee) = child_node(tree, call, self.s.callee) {
                if let Some(target) = read_target(tree, callee, &self.s) {
                    return self.value_from_target(tree, target, node);
                }
            }
            return Value::Opaque;
        }
        let sym = tree.sym(rhs);
        if sym != 0 {
            Value::Alias(sym)
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
                        child_sym(tree, self.defs[*di as usize], self.s.deftype)
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
                        return_type_of_def(tree, self.defs[*di as usize], &self.s)
                    } else {
                        None
                    }
                });
                match rt {
                    Some(rt_sym) => {
                        let found = self
                            .defs
                            .iter()
                            .position(|&dn| def_name(tree, dn, &self.s) == rt_sym);
                        if let Some(di) = found {
                            if child_sym(tree, self.defs[di], self.s.deftype)
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
                    find_enclosing_class(tree, binding, &self.s, &self.containers)
                        .and_then(|cls| find_ivar_type(tree, cls, obj, &self.s))
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
                            find_method(tree, &self.defs, self.defs[*cdi as usize], method, &self.s)
                        {
                            if let Some(rt) = return_type_of_def(tree, m, &self.s) {
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

// ── SSA fold main loop ──

fn ssa_fold(tree: &mut Tree, lang: &mut Lang) {
    let s = S::new(lang);
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut f = Fold {
        s,
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

        // Pop finished scopes
        while f.def_stack.len() > 1 {
            let &(_, e, saved) = f.def_stack.last().unwrap();
            if i >= e {
                f.def_stack.pop();
                f.cur = saved;
            } else {
                break;
            }
        }

        // Pop finished branches
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

        // Switch SSA block when inside a branch arm
        if let Some(br) = f.branch_stack.last() {
            for (idx, &(a, b)) in br.arms.iter().enumerate() {
                if i >= a && i < b {
                    f.cur = br.entries[idx];
                    break;
                }
            }
        }

        // Dispatch
        if k == f.s.import || k == f.s.import_type {
            f.handle_import(tree, i);
            i += n.size.max(1);
            continue;
        }
        if child_sym(tree, i, f.s.deftype).is_some() {
            f.handle_def(tree, i, end);
            i += 1;
            continue;
        }
        if k == f.s.call {
            f.handle_call(tree, i);
            i += 1;
            continue;
        }
        if k == f.s.member {
            let pk = if n.parent != NONE {
                tree.nodes[n.parent as usize].kind
            } else {
                0
            };
            if pk != f.s.call && pk != f.s.callee {
                f.handle_standalone_member(tree, i);
            }
            i += 1;
            continue;
        }
        if k == f.s.binding {
            f.handle_binding(tree, i);
            i += 1;
            continue;
        }
        if k == f.s.branch {
            let pre = f.cur;
            let arms: Vec<(u32, u32)> = tree
                .children(i)
                .filter(|&c| tree.kind(c) == f.s.arm)
                .map(|c| (c, c + tree.nodes[c as usize].size))
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
        if k == f.s.r#loop {
            let (h, _) = f.ssa.begin_loop(f.cur);
            f.cur = f.ssa.finish_loop(h, f.cur);
            i += 1;
            continue;
        }
        i += 1;
    }

    // Flush remaining branches
    while !f.branch_stack.is_empty() {
        let br = f.branch_stack.pop().unwrap();
        let mut preds = br.exits;
        preds.push(br.pre);
        let _ = f.ssa.add_sealed_join(preds);
    }

    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();

    // Emit decorator/supertype edges
    let mut meta: Vec<(u32, u32)> = Vec::new();
    for &dn in &f.defs {
        let syms: Vec<u32> = tree
            .children(dn)
            .filter(|&c| {
                let ck = tree.kind(c);
                (ck == f.s.supertype || ck == f.s.decorator) && tree.sym(c) != 0
            })
            .map(|c| tree.sym(c))
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

// ── Tree-walking helpers ──

fn return_type_of_def(tree: &Tree, def: u32, s: &S) -> Option<u32> {
    if let Some(rt) = child_sym(tree, def, s.return_type) {
        return Some(rt);
    }
    if s.ret == 0 {
        return None;
    }

    // Collect local x = Foo() bindings for indirect return resolution
    let mut binds: Vec<(u32, u32)> = Vec::new();
    for d in tree.descendants(def) {
        if tree.kind(d) == s.binding && tree.sym(d) != 0 {
            let callee = child_node(tree, d, s.rhs)
                .and_then(|rhs| child_node(tree, rhs, s.call))
                .and_then(|c| child_node(tree, c, s.callee))
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if callee != 0 {
                binds.push((tree.sym(d), callee));
            }
        }
    }

    // Find __return → __call or returned variable
    for d in tree.descendants(def) {
        if tree.kind(d) != s.ret {
            continue;
        }
        for c in tree.children(d) {
            if tree.kind(c) == s.call {
                return child_node(tree, c, s.callee)
                    .map(|c2| tree.sym(c2))
                    .filter(|&v| v != 0);
            }
            let sym = tree.sym(c);
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

fn find_method(tree: &Tree, defs: &[u32], container: u32, name: u32, s: &S) -> Option<u32> {
    let mut search = vec![container];
    let mut si = 0;
    while si < search.len() {
        for d in tree.descendants(search[si]) {
            if tree.kind(d) == s.deftype {
                let m = tree.nodes[d as usize].parent;
                if m != NONE && m != search[si] && def_name(tree, m, s) == name {
                    return Some(m);
                }
            }
        }
        for c in tree.children(search[si]) {
            if tree.kind(c) == s.supertype && tree.sym(c) != 0 {
                let sn = tree.sym(c);
                for &dn in defs {
                    if def_name(tree, dn, s) == sn && !search.contains(&dn) {
                        search.push(dn);
                    }
                }
            }
        }
        si += 1;
    }
    None
}

fn find_ivar_type(tree: &Tree, class: u32, attr: u32, s: &S) -> Option<u32> {
    for d in tree.descendants(class) {
        if tree.kind(d) == s.binding
            && child_node(tree, d, s.ivar).is_some_and(|iv| tree.sym(iv) == attr)
        {
            return child_node(tree, d, s.rhs)
                .and_then(|rhs| child_node(tree, rhs, s.call))
                .and_then(|c| child_node(tree, c, s.callee))
                .map(|c| tree.sym(c))
                .filter(|&v| v != 0);
        }
    }
    None
}

fn find_enclosing_class(tree: &Tree, mut node: u32, s: &S, containers: &[u32]) -> Option<u32> {
    loop {
        if node == NONE {
            return None;
        }
        if let Some(dt) = child_sym(tree, node, s.deftype) {
            if containers.contains(&dt) {
                return Some(node);
            }
        }
        node = tree.nodes[node as usize].parent;
    }
}
