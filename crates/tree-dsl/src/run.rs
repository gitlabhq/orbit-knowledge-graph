//! Pipeline: parse → rewrite → SSA fold → edges.

use crate::grammar::{self, SupportLang};
use crate::lang::Lang;
use crate::pattern;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::{EdgeKind, NONE, Tree};

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
    tree
}

fn classify_methods(tree: &mut Tree, lang: &mut Lang) {
    let deftype_k = lang.lookup_kind("__deftype");
    let func_sym = lang.syms.intern("Function");
    let method_sym = lang.syms.intern("Method");
    let class_sym = lang.syms.intern("Class");

    for i in 0..tree.nodes.len() as u32 {
        if tree.kind(i) != deftype_k || tree.sym(i) != func_sym {
            continue;
        }
        let mut p = tree.nodes[i as usize].parent;
        while p != NONE {
            if synth_child(tree, p, deftype_k) == Some(class_sym) {
                tree.nodes[i as usize].sym = method_sym;
                break;
            }
            p = tree.nodes[p as usize].parent;
        }
    }
}

// ── Synthetic helpers ──

struct Syns {
    import: u16,
    name: u16,
    alias: u16,
    deftype: u16,
    defname: u16,
    supertype: u16,
    return_type: u16,
    decorator: u16,
    callable: u16,
    call: u16,
    member: u16,
    ivar: u16,
    binding: u16,
    branch: u16,
    r#loop: u16,
    scope: u16,
}

impl Syns {
    fn new(lang: &Lang) -> Self {
        let s = |name: &str| lang.kinds.lookup(name) as u16;
        Self {
            import: s("__import"),
            name: s("__name"),
            alias: s("__alias"),
            deftype: s("__deftype"),
            defname: s("__defname"),
            supertype: s("__supertype"),
            return_type: s("__return_type"),
            decorator: s("__decorator"),
            callable: s("__callable"),
            call: s("__call"),
            member: s("__member"),
            ivar: s("__ivar"),
            binding: s("__binding"),
            branch: s("__branch"),
            r#loop: s("__loop"),
            scope: s("__scope"),
        }
    }
}

struct Fields {
    name: u16,
    callee: u16,
    object: u16,
    member: u16,
    left: u16,
    right: u16,
    r#type: u16,
    return_type: u16,
}

impl Fields {
    fn new(lang: &Lang) -> Self {
        let f = |name: &str| lang.fields.lookup(name) as u16;
        Self {
            name: f("name"),
            callee: f("callee"),
            object: f("object"),
            member: f("member"),
            left: f("left"),
            right: f("right"),
            r#type: f("type"),
            return_type: f("return_type"),
        }
    }
}

struct BranchFrame {
    arms: Vec<(u32, u32)>,
    entry_blocks: Vec<BlockId>,
    exit_blocks: Vec<BlockId>,
    exhaustive: bool,
    pre_block: BlockId,
    end: u32,
}

fn synth_child(tree: &Tree, node: u32, kind: u16) -> Option<u32> {
    tree.children(node)
        .find(|&c| tree.kind(c) == kind)
        .map(|c| tree.sym(c))
        .filter(|&s| s != 0)
}

fn name_sym(tree: &Tree, node: u32, f: &Fields) -> u32 {
    tree.child_by_field(node, f.name)
        .or_else(|| tree.child_by_field(node, f.left))
        .map(|c| tree.sym(c))
        .unwrap_or(0)
}

fn def_name(tree: &Tree, node: u32, f: &Fields, syns: &Syns) -> u32 {
    let ns = name_sym(tree, node, f);
    if ns != 0 {
        return ns;
    }
    tree.children(node)
        .find(|&c| tree.kind(c) == syns.defname)
        .map(|c| tree.sym(c))
        .unwrap_or(0)
}

/// Resolve a type sym through SSA to a class node, then find a method on it.
fn resolve_method_on_type(
    tree: &mut Tree,
    def_nodes: &[u32],
    ssa: &mut SsaEngine,
    type_sym: u32,
    method_sym: u32,
    block: BlockId,
    from: u32,
    syns: &Syns,
    f: &Fields,
) {
    for cpv in &ssa.read_variable(type_sym, block) {
        if let ParseValue::LocalDef(cdi) = cpv {
            let class_node = def_nodes[*cdi as usize];
            if let Some(method) = find_method(tree, def_nodes, class_node, method_sym, syns, f) {
                tree.add_edge(from, method, EdgeKind::Calls);
            }
        }
    }
}

/// Emit edges for a resolved SSA value (LocalDef→E_CALLS, ImportRef→E_IMPORTS).
fn emit_edge_for_value(
    tree: &mut Tree,
    pv: &ParseValue,
    def_nodes: &[u32],
    import_nodes: &[u32],
    from: u32,
) {
    match pv {
        ParseValue::LocalDef(di) => {
            tree.add_edge(from, def_nodes[*di as usize], EdgeKind::Calls);
        }
        ParseValue::ImportRef(ii) => {
            if let Some(&imp_node) = import_nodes.get(*ii as usize) {
                tree.add_edge(from, imp_node, EdgeKind::Imports);
            }
        }
        _ => {}
    }
}

/// Get the return type of a def: __return_type synthetic, then return_type field, then infer.
fn return_type_of_def(
    tree: &Tree,
    def_node: u32,
    lang: &Lang,
    syns: &Syns,
    f: &Fields,
) -> Option<u32> {
    if let Some(rt) = synth_child(tree, def_node, syns.return_type) {
        return Some(rt);
    }
    if f.return_type != 0 {
        let rt = tree
            .child_by_field(def_node, f.return_type)
            .map(|r| tree.sym(r))
            .unwrap_or(0);
        if rt != 0 {
            return Some(rt);
        }
    }
    infer_return_type(tree, def_node, lang, syns, f)
}

fn update_branch_arm(branch_stack: &mut [BranchFrame], i: u32, cur_block: BlockId) {
    if let Some(br) = branch_stack.last_mut() {
        for (idx, &(arm_start, arm_end)) in br.arms.iter().enumerate() {
            if i >= arm_start && i < arm_end {
                br.exit_blocks[idx] = cur_block;
                break;
            }
        }
    }
}

// ── SSA fold ──

struct SsaState {
    syns: Syns,
    f: Fields,
    ssa: SsaEngine,
    cur_block: BlockId,
    def_count: u32,
    import_count: u32,
    def_nodes: Vec<u32>,
    import_nodes: Vec<u32>,
    import_names: Vec<u32>,
    def_stack: Vec<(Option<u32>, u32, BlockId)>,
    branch_stack: Vec<BranchFrame>,
    wildcard_sym: u32,
    class_sym: u32,
}

impl SsaState {
    fn handle_import(&mut self, tree: &Tree, i: u32) {
        for c in tree.children(i) {
            if tree.kind(c) == self.syns.name && tree.sym(c) != 0 {
                let name_sym = tree.sym(c);
                self.import_count += 1;
                self.import_nodes.push(c);
                self.import_names.push(name_sym);
                self.ssa.write_variable(
                    name_sym,
                    self.cur_block,
                    Value::ImportRef(self.import_count - 1),
                );
                for gc in tree.children(c) {
                    if tree.kind(gc) == self.syns.alias && tree.sym(gc) != 0 {
                        let alias = tree.sym(gc);
                        if alias != name_sym {
                            self.ssa.write_variable(
                                alias,
                                self.cur_block,
                                Value::ImportRef(self.import_count - 1),
                            );
                        }
                    }
                }
            }
        }
    }

    fn handle_def(&mut self, tree: &mut Tree, i: u32, node_end: u32) {
        let name = def_name(tree, i, &self.f, &self.syns);
        if name != 0 {
            let parent_block = self.cur_block;
            self.cur_block = self.ssa.add_sealed_successor(parent_block);
            let def_idx = self.def_count;
            self.def_count += 1;
            self.def_nodes.push(i);
            self.ssa
                .write_variable(name, parent_block, Value::LocalDef(def_idx));
            if let Some(&(Some(parent_def), _, _)) = self.def_stack.last() {
                tree.add_edge(parent_def, i, EdgeKind::Defines);
            }
            if tree.children(i).any(|c| tree.kind(c) == self.syns.scope) {
                self.def_stack.push((Some(i), node_end, parent_block));
            }
        }
    }

    fn handle_call(&mut self, tree: &mut Tree, i: u32) {
        let enclosing = self.def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
        let Some(cn) = tree.child_by_field(i, self.f.callee) else {
            return;
        };
        let callee_k = tree.kind(cn);

        if callee_k == self.syns.member {
            let obj_node = tree.child_by_field(cn, self.f.object);
            let obj_sym = obj_node.map(|c| tree.sym(c)).unwrap_or(0);
            let obj_is_ivar = obj_node.is_some_and(|c| tree.kind(c) == self.syns.ivar);
            let mem_sym = tree
                .child_by_field(cn, self.f.member)
                .map(|c| tree.sym(c))
                .unwrap_or(0);

            if obj_is_ivar && obj_sym != 0 {
                if let Some(cls) = find_enclosing_class(tree, i, &self.syns, self.class_sym)
                    && let Some(ts) = find_ivar_type(tree, cls, obj_sym, &self.syns, &self.f)
                {
                    resolve_method_on_type(
                        tree,
                        &self.def_nodes,
                        &mut self.ssa,
                        ts,
                        mem_sym,
                        self.cur_block,
                        enclosing,
                        &self.syns,
                        &self.f,
                    );
                }
            } else if obj_sym != 0 {
                for pv in &self.ssa.read_variable(obj_sym, self.cur_block) {
                    match pv {
                        ParseValue::Type(ts) if *ts != 0 => {
                            resolve_method_on_type(
                                tree,
                                &self.def_nodes,
                                &mut self.ssa,
                                *ts,
                                mem_sym,
                                self.cur_block,
                                enclosing,
                                &self.syns,
                                &self.f,
                            );
                        }
                        _ => emit_edge_for_value(
                            tree,
                            pv,
                            &self.def_nodes,
                            &self.import_nodes,
                            enclosing,
                        ),
                    }
                }
            }
        } else if callee_k == self.syns.ivar {
            let ivar_sym = tree.sym(cn);
            if ivar_sym != 0 {
                if let Some(cls) = find_enclosing_class(tree, i, &self.syns, self.class_sym)
                    && let Some(method) =
                        find_method(tree, &self.def_nodes, cls, ivar_sym, &self.syns, &self.f)
                {
                    tree.add_edge(enclosing, method, EdgeKind::Calls);
                }
            }
        } else {
            let callee_sym = tree.sym(cn);
            if callee_sym != 0 {
                let mut reaching = self.ssa.read_variable(callee_sym, self.cur_block);
                if reaching.is_empty() {
                    let wildcard = self.ssa.read_variable(self.wildcard_sym, self.cur_block);
                    if !wildcard.is_empty() {
                        for pv in &wildcard {
                            emit_edge_for_value(
                                tree,
                                pv,
                                &self.def_nodes,
                                &self.import_nodes,
                                enclosing,
                            );
                        }
                        reaching = wildcard;
                    }
                }
                for pv in &reaching {
                    match pv {
                        ParseValue::Type(ts) if *ts != 0 => {
                            let target_reaching = self.ssa.read_variable(*ts, self.cur_block);
                            for cpv in &target_reaching {
                                if let ParseValue::LocalDef(cdi) = cpv {
                                    let target = self.def_nodes[*cdi as usize];
                                    if let Some(callable_sym) =
                                        synth_child(tree, target, self.syns.callable)
                                    {
                                        if let Some(method) = find_method(
                                            tree,
                                            &self.def_nodes,
                                            target,
                                            callable_sym,
                                            &self.syns,
                                            &self.f,
                                        ) {
                                            tree.add_edge(enclosing, method, EdgeKind::Calls);
                                        }
                                    } else {
                                        tree.add_edge(enclosing, target, EdgeKind::Calls);
                                    }
                                }
                            }
                        }
                        _ => emit_edge_for_value(
                            tree,
                            pv,
                            &self.def_nodes,
                            &self.import_nodes,
                            enclosing,
                        ),
                    }
                }
            }
        }
    }

    fn handle_binding(&mut self, tree: &Tree, i: u32, lang: &Lang) {
        let lhs = tree
            .child_by_field(i, self.f.left)
            .or_else(|| tree.child_by_field(i, self.f.name))
            .or_else(|| {
                let ident_k = lang.kinds.lookup("identifier") as u16;
                if ident_k != 0 {
                    tree.children(i).find(|&c| tree.kind(c) == ident_k)
                } else {
                    None
                }
            })
            .map(|c| tree.sym(c))
            .unwrap_or(0);

        if lhs != 0 {
            let is_ivar = tree
                .child_by_field(i, self.f.left)
                .is_some_and(|n| tree.kind(n) == self.syns.ivar);
            if !is_ivar {
                if self.ssa.has_variable_in_block(lhs, self.cur_block) {
                    self.cur_block = self.ssa.add_sealed_successor(self.cur_block);
                }
                let val = classify_rhs(
                    tree,
                    i,
                    &mut self.ssa,
                    &self.def_nodes,
                    self.cur_block,
                    lang,
                    &self.syns,
                    &self.f,
                    self.class_sym,
                );
                self.ssa.write_variable(lhs, self.cur_block, val);
                update_branch_arm(&mut self.branch_stack, i, self.cur_block);
            }
        }
    }
}

fn ssa_fold(tree: &mut Tree, lang: &mut Lang) {
    let syns = Syns::new(lang);
    let f = Fields::new(lang);
    let wildcard_sym = lang.syms.intern("*");
    let class_sym = lang.syms.intern("Class");

    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut state = SsaState {
        syns,
        f,
        ssa,
        cur_block: entry,
        def_count: 0,
        import_count: 0,
        def_nodes: Vec::new(),
        import_nodes: Vec::new(),
        import_names: Vec::new(),
        def_stack: vec![(None, u32::MAX, entry)],
        branch_stack: Vec::new(),
        wildcard_sym,
        class_sym,
    };

    let mut i = 0u32;
    let len = tree.nodes.len() as u32;

    while i < len {
        let (dead, k, node_size, parent) = {
            let n = &tree.nodes[i as usize];
            (n.dead, n.kind, n.size, n.parent)
        };
        if dead {
            i += node_size.max(1);
            continue;
        }
        let node_end = i + node_size;

        while state.def_stack.len() > 1 {
            let &(_, end, saved) = state.def_stack.last().unwrap();
            if i >= end {
                state.def_stack.pop();
                state.cur_block = saved;
            } else {
                break;
            }
        }
        while let Some(br) = state.branch_stack.last() {
            if i >= br.end {
                let mut preds = br.exit_blocks.clone();
                if !br.exhaustive {
                    preds.push(br.pre_block);
                }
                let branch_end = br.end;
                state.cur_block = state.ssa.add_sealed_join(preds);
                state.branch_stack.pop();
                if let Some(outer) = state.branch_stack.last_mut() {
                    for (idx, &(arm_start, arm_end)) in outer.arms.iter().enumerate() {
                        if branch_end > arm_start && branch_end <= arm_end {
                            outer.exit_blocks[idx] = state.cur_block;
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }
        if let Some(br) = state.branch_stack.last() {
            for (idx, &(arm_start, arm_end)) in br.arms.iter().enumerate() {
                if i >= arm_start && i < arm_end {
                    state.cur_block = br.entry_blocks[idx];
                    break;
                }
            }
        }

        if k == state.syns.import {
            state.handle_import(tree, i);
            i += node_size.max(1);
            continue;
        }

        if synth_child(tree, i, state.syns.deftype).is_some() {
            state.handle_def(tree, i, node_end);
            i += 1;
            continue;
        }

        if k == state.syns.call {
            state.handle_call(tree, i);
            i += 1;
            continue;
        }

        if k == state.syns.member {
            let parent_k = if parent != NONE {
                tree.nodes[parent as usize].kind
            } else {
                0
            };
            if parent_k != state.syns.call {
                let obj_sym = tree
                    .child_by_field(i, state.f.object)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                let mem_sym = tree
                    .child_by_field(i, state.f.member)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                let enclosing = state.def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
                if obj_sym != 0 {
                    for pv in &state.ssa.read_variable(obj_sym, state.cur_block) {
                        match pv {
                            ParseValue::ImportRef(ii) => {
                                if let Some(&imp_node) = state.import_nodes.get(*ii as usize) {
                                    tree.add_edge(enclosing, imp_node, EdgeKind::Imports);
                                }
                            }
                            ParseValue::Type(ts) if *ts != 0 && mem_sym != 0 => {
                                resolve_method_on_type(
                                    tree,
                                    &state.def_nodes,
                                    &mut state.ssa,
                                    *ts,
                                    mem_sym,
                                    state.cur_block,
                                    enclosing,
                                    &state.syns,
                                    &state.f,
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
            i += 1;
            continue;
        }

        if k == state.syns.ivar
            && parent != NONE
            && tree.nodes[parent as usize].kind != state.syns.call
        {
            i += 1;
            continue;
        }

        if tree.children(i).any(|c| tree.kind(c) == state.syns.binding) {
            state.handle_binding(tree, i, lang);
            i += 1;
            continue;
        }

        if tree.children(i).any(|c| tree.kind(c) == state.syns.branch) {
            let pre = state.cur_block;
            let arm_kinds = find_arm_children(tree, i, lang);
            let arm_blocks: Vec<BlockId> = arm_kinds
                .iter()
                .map(|_| state.ssa.add_sealed_successor(pre))
                .collect();
            let arm_exits = arm_blocks.clone();
            state.branch_stack.push(BranchFrame {
                arms: arm_kinds,
                entry_blocks: arm_blocks,
                exit_blocks: arm_exits,
                exhaustive: false,
                pre_block: pre,
                end: node_end,
            });
            i += 1;
            continue;
        }
        if tree.children(i).any(|c| tree.kind(c) == state.syns.r#loop) {
            let (h, _) = state.ssa.begin_loop(state.cur_block);
            state.cur_block = state.ssa.finish_loop(h, state.cur_block);
            i += 1;
            continue;
        }
        if tree.children(i).any(|c| tree.kind(c) == state.syns.scope)
            && synth_child(tree, i, state.syns.deftype).is_none()
        {
            i += 1;
            continue;
        }
        if k == state.syns.decorator || k == state.syns.supertype {
            i += 1;
            continue;
        }

        i += 1;
    }

    while let Some(br) = state.branch_stack.last() {
        let mut preds = br.exit_blocks.clone();
        if !br.exhaustive {
            preds.push(br.pre_block);
        }
        let _ = state.ssa.add_sealed_join(preds);
        state.branch_stack.pop();
    }

    state.ssa.seal_remaining();
    state.ssa.remove_redundant_phi_sccs();

    let mut meta_edges: Vec<(u32, u32)> = Vec::new();
    for &def_node in &state.def_nodes {
        let syms: Vec<u32> = tree
            .children(def_node)
            .filter(|&c| {
                let ck = tree.kind(c);
                (ck == state.syns.supertype || ck == state.syns.decorator) && tree.sym(c) != 0
            })
            .map(|c| tree.sym(c))
            .collect();
        for sym in syms {
            for pv in &state.ssa.read_variable(sym, entry) {
                if let ParseValue::LocalDef(di) = pv {
                    meta_edges.push((def_node, state.def_nodes[*di as usize]));
                }
            }
        }
    }
    for (from, to) in meta_edges {
        tree.add_edge(from, to, EdgeKind::Calls);
    }
}

// ── Binding RHS classifier ──

/// Classify a binding's RHS to determine what SSA value to write.
fn classify_rhs(
    tree: &Tree,
    node: u32,
    ssa: &mut SsaEngine,
    def_nodes: &[u32],
    block: BlockId,
    lang: &Lang,
    syns: &Syns,
    f: &Fields,
    class_sym: u32,
) -> Value {
    let type_node = tree.child_by_field(node, f.r#type);
    if f.r#type != 0 {
        if let Some(tn) = type_node {
            let type_sym = tree.sym(tn);
            if type_sym != 0 {
                return Value::Type(type_sym);
            }
        }
    }

    let Some(rn) = tree.child_by_field(node, f.right) else {
        return Value::Opaque;
    };

    if tree.kind(rn) != syns.call {
        // Bare identifier on RHS → alias
        if !tree.nodes[rn as usize].synth && tree.sym(rn) != 0 && tree.children(rn).next().is_none()
        {
            return Value::Alias(tree.sym(rn));
        }
        return Value::Opaque;
    }

    // RHS is a call
    let callee_node = tree.child_by_field(rn, f.callee);
    let callee_is_member = callee_node.is_some_and(|c| tree.kind(c) == syns.member);

    if callee_is_member {
        return classify_member_call_rhs(
            tree,
            callee_node.unwrap(),
            node,
            ssa,
            def_nodes,
            block,
            lang,
            syns,
            f,
            class_sym,
        );
    }

    let callee_sym = callee_node.map(|c| tree.sym(c)).unwrap_or(0);
    if callee_sym == 0 {
        return Value::Opaque;
    }

    let reaching = ssa.read_variable(callee_sym, block);
    let is_class = reaching.iter().any(|pv| {
        if let ParseValue::LocalDef(di) = pv {
            synth_child(tree, def_nodes[*di as usize], syns.deftype)
                .is_some_and(|dt| lang.syms.resolve(dt) == "Class")
        } else {
            false
        }
    });
    if is_class {
        return Value::Type(callee_sym);
    }

    // Follow return type
    let ret_type = reaching.iter().find_map(|pv| {
        if let ParseValue::LocalDef(di) = pv {
            return_type_of_def(tree, def_nodes[*di as usize], lang, syns, f)
        } else {
            None
        }
    });
    match ret_type {
        Some(rt_sym) => {
            let found_def = def_nodes
                .iter()
                .position(|&dn| name_sym(tree, dn, f) == rt_sym);
            if let Some(di) = found_def {
                if synth_child(tree, def_nodes[di], syns.deftype)
                    .is_some_and(|dt| lang.syms.resolve(dt) == "Class")
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

/// Classify `x = obj.method()` — resolve method's return type.
fn classify_member_call_rhs(
    tree: &Tree,
    callee_member: u32,
    binding_node: u32,
    ssa: &mut SsaEngine,
    def_nodes: &[u32],
    block: BlockId,
    lang: &Lang,
    syns: &Syns,
    f: &Fields,
    class_sym: u32,
) -> Value {
    let obj_sym = tree
        .child_by_field(callee_member, f.object)
        .map(|c| tree.sym(c))
        .unwrap_or(0);
    let mem_sym = tree
        .child_by_field(callee_member, f.member)
        .map(|c| tree.sym(c))
        .unwrap_or(0);
    let obj_is_ivar = tree
        .child_by_field(callee_member, f.object)
        .is_some_and(|c| tree.kind(c) == syns.ivar);

    let obj_type = if obj_is_ivar {
        find_enclosing_class(tree, binding_node, syns, class_sym)
            .and_then(|cls| find_ivar_type(tree, cls, obj_sym, syns, f))
    } else if obj_sym != 0 {
        ssa.read_variable(obj_sym, block).iter().find_map(|pv| {
            if let ParseValue::Type(ts) = pv {
                Some(*ts)
            } else {
                None
            }
        })
    } else {
        None
    };

    let Some(type_sym) = obj_type else {
        return Value::Opaque;
    };

    for cpv in &ssa.read_variable(type_sym, block) {
        if let ParseValue::LocalDef(cdi) = cpv {
            let target = def_nodes[*cdi as usize];
            if let Some(method) = find_method(tree, def_nodes, target, mem_sym, syns, f) {
                if let Some(rt) = return_type_of_def(tree, method, lang, syns, f) {
                    return Value::Type(rt);
                }
            }
        }
    }
    Value::Opaque
}

// ── Tree-walking helpers ──

fn find_method(
    tree: &Tree,
    def_nodes: &[u32],
    container: u32,
    method_name: u32,
    syns: &Syns,
    f: &Fields,
) -> Option<u32> {
    let mut search = vec![container];
    let mut si = 0;
    while si < search.len() {
        let current = search[si];
        for d in tree.descendants(current) {
            if tree.kind(d) == syns.deftype {
                let method_node = tree.nodes[d as usize].parent;
                if method_node != NONE && method_node != current {
                    let mname = name_sym(tree, method_node, f);
                    if mname == method_name {
                        return Some(method_node);
                    }
                }
            }
        }
        for c in tree.children(current) {
            if tree.kind(c) == syns.supertype && tree.sym(c) != 0 {
                let super_name = tree.sym(c);
                for &dn in def_nodes {
                    let dname = tree
                        .child_by_field(dn, f.name)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if dname == super_name && !search.contains(&dn) {
                        search.push(dn);
                    }
                }
            }
        }
        si += 1;
    }
    None
}

fn infer_return_type(
    tree: &Tree,
    def_node: u32,
    lang: &Lang,
    syns: &Syns,
    f: &Fields,
) -> Option<u32> {
    let return_k = lang.kinds.lookup("return_statement") as u16;
    if return_k == 0 {
        return None;
    }

    let mut local_binds: Vec<(u32, u32)> = Vec::new();
    for d in tree.descendants(def_node) {
        if tree.children(d).any(|c| tree.kind(c) == syns.binding) {
            let lhs = tree
                .child_by_field(d, f.left)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let rhs_call = tree
                .child_by_field(d, f.right)
                .filter(|&r| tree.kind(r) == syns.call)
                .and_then(|r| tree.child_by_field(r, f.callee))
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if lhs != 0 && rhs_call != 0 {
                local_binds.push((lhs, rhs_call));
            }
        }
    }

    for d in tree.descendants(def_node) {
        if tree.nodes[d as usize].kind == return_k {
            for c in tree.children(d) {
                if tree.kind(c) == syns.call {
                    return tree
                        .child_by_field(c, f.callee)
                        .map(|c2| tree.sym(c2))
                        .filter(|&s| s != 0);
                }
                if !tree.nodes[c as usize].synth
                    && tree.sym(c) != 0
                    && tree.children(c).next().is_none()
                {
                    let sym = tree.sym(c);
                    for &(lhs, callee) in &local_binds {
                        if lhs == sym {
                            return Some(callee);
                        }
                    }
                    return Some(sym);
                }
            }
        }
    }
    None
}

fn find_ivar_type(
    tree: &Tree,
    class_node: u32,
    attr_sym: u32,
    syns: &Syns,
    f: &Fields,
) -> Option<u32> {
    for d in tree.descendants(class_node) {
        if tree.children(d).any(|c| tree.kind(c) == syns.binding) {
            let lhs_node = tree.child_by_field(d, f.left);
            if let Some(ln) = lhs_node
                && tree.kind(ln) == syns.ivar
                && tree.sym(ln) == attr_sym
            {
                let rhs = tree.child_by_field(d, f.right);
                if let Some(rn) = rhs
                    && tree.kind(rn) == syns.call
                {
                    let callee = tree
                        .child_by_field(rn, f.callee)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if callee != 0 {
                        return Some(callee);
                    }
                }
            }
        }
    }
    None
}

fn find_enclosing_class(tree: &Tree, mut node: u32, syns: &Syns, class_sym: u32) -> Option<u32> {
    loop {
        if node == NONE {
            return None;
        }
        if synth_child(tree, node, syns.deftype) == Some(class_sym) {
            return Some(node);
        }
        node = tree.nodes[node as usize].parent;
    }
}

fn find_arm_children(tree: &Tree, node: u32, lang: &Lang) -> Vec<(u32, u32)> {
    let consequence_f = lang.fields.lookup("consequence") as u16;
    let alternative_f = lang.fields.lookup("alternative") as u16;
    let body_f = lang.fields.lookup("body") as u16;

    let mut arms = Vec::new();
    for c in tree.children(node) {
        let n = &tree.nodes[c as usize];
        if n.dead {
            continue;
        }
        let fld = n.field;
        if fld == consequence_f || fld == alternative_f || (fld == body_f && arms.is_empty()) {
            arms.push((c, c + n.size));
        }
        let kname = lang.kinds.resolve(n.kind as u32);
        if kname.ends_with("_clause") && fld != consequence_f {
            arms.push((c, c + n.size));
        }
    }
    arms
}
