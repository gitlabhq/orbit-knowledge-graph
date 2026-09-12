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
    prune(&mut tree, lang);
    tree.compact();
    tree
}

// ── Post-SSA prune: strip everything non-canonical ──

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

// ── classify_methods: upgrade Function → Method inside Class/Impl/Trait ──

fn classify_methods(tree: &mut Tree, lang: &mut Lang) {
    let deftype_k = lang.lookup_kind("__deftype");
    let self_method_k = lang.lookup_kind("__self_method");
    let func_sym = lang.syms.intern("Function");
    let method_sym = lang.syms.intern("Method");
    let assoc_fn_sym = lang.syms.intern("AssociatedFunction");
    let class_sym = lang.syms.intern("Class");
    let impl_sym = lang.syms.intern("Impl");
    let trait_sym = lang.syms.intern("Trait");

    for i in 0..tree.nodes.len() as u32 {
        if tree.kind(i) != deftype_k || tree.sym(i) != func_sym {
            continue;
        }
        let def_node = tree.nodes[i as usize].parent;
        if def_node == NONE {
            continue;
        }
        let mut p = tree.nodes[def_node as usize].parent;
        while p != NONE {
            if let Some(dt) = child_sym(tree, p, deftype_k) {
                if dt == class_sym {
                    tree.nodes[i as usize].sym = method_sym;
                    break;
                }
                if dt == impl_sym || dt == trait_sym {
                    let has_self = self_method_k != 0
                        && tree
                            .children(def_node)
                            .any(|c| tree.kind(c) == self_method_k);
                    tree.nodes[i as usize].sym = if has_self { method_sym } else { assoc_fn_sym };
                    break;
                }
            }
            p = tree.nodes[p as usize].parent;
        }
    }
}

// ── Canonical tree helpers ──

/// Find a child with the given kind and return its sym (non-zero).
fn child_sym(tree: &Tree, node: u32, kind: u16) -> Option<u32> {
    tree.children(node)
        .find(|&c| tree.kind(c) == kind)
        .map(|c| tree.sym(c))
        .filter(|&s| s != 0)
}

/// Find a child with the given kind and return its node index.
fn child_node(tree: &Tree, node: u32, kind: u16) -> Option<u32> {
    tree.children(node).find(|&c| tree.kind(c) == kind)
}

/// Read the def name from a canonical __def node (__defname child sym).
fn def_name(tree: &Tree, node: u32, syns: &Syns) -> u32 {
    child_sym(tree, node, syns.defname).unwrap_or(0)
}

// ── Synthetic kind IDs ──

struct Syns {
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

impl Syns {
    fn new(lang: &Lang) -> Self {
        let s = |name: &str| lang.kinds.lookup(name) as u16;
        Self {
            import: s("__import"),
            import_type: s("__import_type"),
            name: s("__name"),
            alias: s("__alias"),
            deftype: s("__deftype"),
            defname: s("__defname"),
            supertype: s("__supertype"),
            return_type: s("__return_type"),
            decorator: s("__decorator"),
            callable: s("__callable"),
            call: s("__call"),
            callee: s("__callee"),
            member: s("__member"),
            object: s("__object"),
            ivar: s("__ivar"),
            binding: s("__binding"),
            branch: s("__branch"),
            r#loop: s("__loop"),
            scope: s("__scope"),
            arm: s("__arm"),
            rhs: s("__rhs"),
            ret: s("__return"),
        }
    }
}

// ── Branch frame for SSA ──

struct BranchFrame {
    arms: Vec<(u32, u32)>,
    entry_blocks: Vec<BlockId>,
    exit_blocks: Vec<BlockId>,
    exhaustive: bool,
    pre_block: BlockId,
    end: u32,
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

fn find_arm_children(tree: &Tree, node: u32, syns: &Syns) -> Vec<(u32, u32)> {
    tree.children(node)
        .filter(|&c| tree.kind(c) == syns.arm && !tree.nodes[c as usize].dead)
        .map(|c| (c, c + tree.nodes[c as usize].size))
        .collect()
}

// ── SSA fold state ──

struct SsaState {
    syns: Syns,
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
    container_syms: Vec<u32>,
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
        let name = def_name(tree, i, &self.syns);
        if name == 0 {
            return;
        }
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

    fn handle_call(&mut self, tree: &mut Tree, i: u32, lang: &Lang) {
        let enclosing = self.def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
        let Some(callee_node) = child_node(tree, i, self.syns.callee) else {
            return;
        };

        if let Some(member) = child_node(tree, callee_node, self.syns.member) {
            let mem_sym = tree.sym(member);
            let obj_node = child_node(tree, member, self.syns.object);
            let ivar_node = obj_node.and_then(|o| child_node(tree, o, self.syns.ivar));
            let obj_is_ivar = ivar_node.is_some();
            let obj_sym = ivar_node
                .map(|iv| tree.sym(iv))
                .or_else(|| obj_node.map(|o| tree.sym(o)))
                .unwrap_or(0);

            if obj_is_ivar && obj_sym != 0 {
                if let Some(cls) = find_enclosing_class(tree, i, &self.syns, &self.container_syms)
                    && let Some(ts) = find_ivar_type(tree, cls, obj_sym, &self.syns)
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
                            );
                        }
                        _ => emit_edge(tree, pv, &self.def_nodes, &self.import_nodes, enclosing),
                    }
                }
            }
        } else if let Some(ivar) = child_node(tree, callee_node, self.syns.ivar) {
            let ivar_sym = tree.sym(ivar);
            if ivar_sym != 0 {
                if let Some(cls) = find_enclosing_class(tree, i, &self.syns, &self.container_syms)
                    && let Some(method) =
                        find_method(tree, &self.def_nodes, cls, ivar_sym, &self.syns)
                {
                    tree.add_edge(enclosing, method, EdgeKind::Calls);
                }
            }
        } else {
            let callee_sym = tree.sym(callee_node);
            if callee_sym != 0 {
                let mut reaching = self.ssa.read_variable(callee_sym, self.cur_block);
                if reaching.is_empty() {
                    let wildcard = self.ssa.read_variable(self.wildcard_sym, self.cur_block);
                    if !wildcard.is_empty() {
                        for pv in &wildcard {
                            emit_edge(tree, pv, &self.def_nodes, &self.import_nodes, enclosing);
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
                                        child_sym(tree, target, self.syns.callable)
                                    {
                                        if let Some(method) = find_method(
                                            tree,
                                            &self.def_nodes,
                                            target,
                                            callable_sym,
                                            &self.syns,
                                        ) {
                                            tree.add_edge(enclosing, method, EdgeKind::Calls);
                                        }
                                    } else {
                                        tree.add_edge(enclosing, target, EdgeKind::Calls);
                                    }
                                }
                            }
                        }
                        _ => emit_edge(tree, pv, &self.def_nodes, &self.import_nodes, enclosing),
                    }
                }
            }
        }
    }

    fn handle_binding(&mut self, tree: &Tree, i: u32, lang: &Lang) {
        let lhs = tree.sym(i);
        if lhs == 0 {
            return;
        }
        let is_ivar = child_node(tree, i, self.syns.ivar).is_some();
        if is_ivar {
            return;
        }
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
            &self.container_syms,
        );
        self.ssa.write_variable(lhs, self.cur_block, val);
        update_branch_arm(&mut self.branch_stack, i, self.cur_block);
    }
}

// ── SSA fold main loop ──

fn ssa_fold(tree: &mut Tree, lang: &mut Lang) {
    let syns = Syns::new(lang);
    let wildcard_sym = lang.syms.intern("*");
    let container_syms = vec![
        lang.syms.intern("Class"),
        lang.syms.intern("Impl"),
        lang.syms.intern("Trait"),
    ];

    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);

    let mut state = SsaState {
        syns,
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
        container_syms,
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
        let node_end = i + n.size;

        // Pop finished scopes and branches
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

        // Switch SSA block when entering a branch arm
        if let Some(br) = state.branch_stack.last() {
            for (idx, &(arm_start, arm_end)) in br.arms.iter().enumerate() {
                if i >= arm_start && i < arm_end {
                    state.cur_block = br.entry_blocks[idx];
                    break;
                }
            }
        }

        // Dispatch on canonical node kind
        if k == state.syns.import || k == state.syns.import_type {
            state.handle_import(tree, i);
            i += n.size.max(1);
            continue;
        }
        if child_sym(tree, i, state.syns.deftype).is_some() {
            state.handle_def(tree, i, node_end);
            i += 1;
            continue;
        }
        if k == state.syns.call {
            state.handle_call(tree, i, lang);
            i += 1;
            continue;
        }
        if k == state.syns.member {
            let parent_k = if n.parent != NONE {
                tree.nodes[n.parent as usize].kind
            } else {
                0
            };
            if parent_k != state.syns.call && parent_k != state.syns.callee {
                handle_standalone_member(tree, &mut state, i);
            }
            i += 1;
            continue;
        }
        if k == state.syns.binding {
            state.handle_binding(tree, i, lang);
            i += 1;
            continue;
        }
        if k == state.syns.branch {
            let pre = state.cur_block;
            let arm_ranges = find_arm_children(tree, i, &state.syns);
            let arm_blocks: Vec<BlockId> = arm_ranges
                .iter()
                .map(|_| state.ssa.add_sealed_successor(pre))
                .collect();
            let arm_exits = arm_blocks.clone();
            state.branch_stack.push(BranchFrame {
                arms: arm_ranges,
                entry_blocks: arm_blocks,
                exit_blocks: arm_exits,
                exhaustive: false,
                pre_block: pre,
                end: node_end,
            });
            i += 1;
            continue;
        }
        if k == state.syns.r#loop {
            let (h, _) = state.ssa.begin_loop(state.cur_block);
            state.cur_block = state.ssa.finish_loop(h, state.cur_block);
            i += 1;
            continue;
        }

        i += 1;
    }

    // Flush remaining branch frames
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

    // Emit decorator/supertype call edges
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

// ── Standalone __member access (not inside __call) ──

fn handle_standalone_member(tree: &mut Tree, state: &mut SsaState, i: u32) {
    let obj_sym = child_node(tree, i, state.syns.object)
        .map(|o| tree.sym(o))
        .unwrap_or(0);
    let mem_sym = tree.sym(i);
    let enclosing = state.def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
    if obj_sym == 0 {
        return;
    }
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
                );
            }
            _ => {}
        }
    }
}

// ── Edge helpers ──

fn emit_edge(tree: &mut Tree, pv: &ParseValue, def_nodes: &[u32], import_nodes: &[u32], from: u32) {
    match pv {
        ParseValue::LocalDef(di) => tree.add_edge(from, def_nodes[*di as usize], EdgeKind::Calls),
        ParseValue::ImportRef(ii) => {
            if let Some(&imp) = import_nodes.get(*ii as usize) {
                tree.add_edge(from, imp, EdgeKind::Imports);
            }
        }
        _ => {}
    }
}

fn resolve_method_on_type(
    tree: &mut Tree,
    def_nodes: &[u32],
    ssa: &mut SsaEngine,
    type_sym: u32,
    method_sym: u32,
    block: BlockId,
    from: u32,
    syns: &Syns,
) {
    for cpv in &ssa.read_variable(type_sym, block) {
        if let ParseValue::LocalDef(cdi) = cpv {
            let class_node = def_nodes[*cdi as usize];
            if let Some(method) = find_method(tree, def_nodes, class_node, method_sym, syns) {
                tree.add_edge(from, method, EdgeKind::Calls);
            }
        }
    }
}

// ── Binding RHS classifier ──

fn classify_rhs(
    tree: &Tree,
    node: u32,
    ssa: &mut SsaEngine,
    def_nodes: &[u32],
    block: BlockId,
    lang: &Lang,
    syns: &Syns,
    container_syms: &[u32],
) -> Value {
    let rhs_node = child_node(tree, node, syns.rhs);
    let Some(rhs) = rhs_node else {
        return Value::Opaque;
    };

    // __rhs with __call child → call
    if let Some(call) = child_node(tree, rhs, syns.call) {
        let callee_node = child_node(tree, call, syns.callee);
        let callee_is_member = callee_node
            .and_then(|cn| child_node(tree, cn, syns.member))
            .is_some();

        if callee_is_member {
            let cn = callee_node.unwrap();
            let mn = child_node(tree, cn, syns.member).unwrap();
            return classify_member_call_rhs(
                tree,
                mn,
                node,
                ssa,
                def_nodes,
                block,
                lang,
                syns,
                container_syms,
            );
        }

        let callee_sym = callee_node.map(|c| tree.sym(c)).unwrap_or(0);
        if callee_sym == 0 {
            return Value::Opaque;
        }

        let reaching = ssa.read_variable(callee_sym, block);
        let is_class = reaching.iter().any(|pv| {
            if let ParseValue::LocalDef(di) = pv {
                child_sym(tree, def_nodes[*di as usize], syns.deftype)
                    .is_some_and(|dt| lang.syms.resolve(dt) == "Class")
            } else {
                false
            }
        });
        if is_class {
            return Value::Type(callee_sym);
        }

        let ret_type = reaching.iter().find_map(|pv| {
            if let ParseValue::LocalDef(di) = pv {
                return_type_of_def(tree, def_nodes[*di as usize], lang, syns)
            } else {
                None
            }
        });
        return match ret_type {
            Some(rt_sym) => {
                let found_def = def_nodes
                    .iter()
                    .position(|&dn| def_name(tree, dn, syns) == rt_sym);
                if let Some(di) = found_def {
                    if child_sym(tree, def_nodes[di], syns.deftype)
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
        };
    }

    // __rhs "name" → alias
    let alias_sym = tree.sym(rhs);
    if alias_sym != 0 {
        return Value::Alias(alias_sym);
    }
    Value::Opaque
}

fn classify_member_call_rhs(
    tree: &Tree,
    member_node: u32,
    binding_node: u32,
    ssa: &mut SsaEngine,
    def_nodes: &[u32],
    block: BlockId,
    lang: &Lang,
    syns: &Syns,
    container_syms: &[u32],
) -> Value {
    let obj_node = child_node(tree, member_node, syns.object);
    let ivar_node = obj_node.and_then(|o| child_node(tree, o, syns.ivar));
    let obj_is_ivar = ivar_node.is_some();
    let obj_sym = ivar_node
        .map(|iv| tree.sym(iv))
        .or_else(|| obj_node.map(|o| tree.sym(o)))
        .unwrap_or(0);
    let mem_sym = tree.sym(member_node);

    let obj_type = if obj_is_ivar {
        find_enclosing_class(tree, binding_node, syns, container_syms)
            .and_then(|cls| find_ivar_type(tree, cls, obj_sym, syns))
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
            if let Some(method) = find_method(tree, def_nodes, target, mem_sym, syns) {
                if let Some(rt) = return_type_of_def(tree, method, lang, syns) {
                    return Value::Type(rt);
                }
            }
        }
    }
    Value::Opaque
}

// ── Tree-walking helpers ──

fn return_type_of_def(tree: &Tree, def_node: u32, lang: &Lang, syns: &Syns) -> Option<u32> {
    if let Some(rt) = child_sym(tree, def_node, syns.return_type) {
        return Some(rt);
    }
    infer_return_type(tree, def_node, lang, syns)
}

fn infer_return_type(tree: &Tree, def_node: u32, _lang: &Lang, syns: &Syns) -> Option<u32> {
    if syns.ret == 0 {
        return None;
    }

    let mut local_binds: Vec<(u32, u32)> = Vec::new();
    for d in tree.descendants(def_node) {
        if tree.kind(d) == syns.binding && tree.sym(d) != 0 {
            let lhs = tree.sym(d);
            let rhs_call = child_node(tree, d, syns.rhs)
                .and_then(|rhs| child_node(tree, rhs, syns.call))
                .and_then(|c| child_node(tree, c, syns.callee))
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if lhs != 0 && rhs_call != 0 {
                local_binds.push((lhs, rhs_call));
            }
        }
    }

    for d in tree.descendants(def_node) {
        if tree.kind(d) == syns.ret {
            for c in tree.children(d) {
                if tree.kind(c) == syns.call {
                    return child_node(tree, c, syns.callee)
                        .map(|c2| tree.sym(c2))
                        .filter(|&s| s != 0);
                }
            }
            // Check for returned variable that was bound to a call
            let ret_sym = tree.children(d).find_map(|c| {
                let s = tree.sym(c);
                if s != 0 { Some(s) } else { None }
            });
            if let Some(sym) = ret_sym {
                for &(lhs, callee) in &local_binds {
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

fn find_method(
    tree: &Tree,
    def_nodes: &[u32],
    container: u32,
    method_name: u32,
    syns: &Syns,
) -> Option<u32> {
    let mut search = vec![container];
    let mut si = 0;
    while si < search.len() {
        let current = search[si];
        for d in tree.descendants(current) {
            if tree.kind(d) == syns.deftype {
                let method_node = tree.nodes[d as usize].parent;
                if method_node != NONE && method_node != current {
                    if def_name(tree, method_node, syns) == method_name {
                        return Some(method_node);
                    }
                }
            }
        }
        for c in tree.children(current) {
            if tree.kind(c) == syns.supertype && tree.sym(c) != 0 {
                let super_name = tree.sym(c);
                for &dn in def_nodes {
                    if def_name(tree, dn, syns) == super_name && !search.contains(&dn) {
                        search.push(dn);
                    }
                }
            }
        }
        si += 1;
    }
    None
}

fn find_ivar_type(tree: &Tree, class_node: u32, attr_sym: u32, syns: &Syns) -> Option<u32> {
    for d in tree.descendants(class_node) {
        if tree.kind(d) == syns.binding {
            let ivar_match = child_node(tree, d, syns.ivar)
                .filter(|&iv| tree.sym(iv) == attr_sym)
                .is_some();
            if ivar_match {
                let rhs_call =
                    child_node(tree, d, syns.rhs).and_then(|rhs| child_node(tree, rhs, syns.call));
                if let Some(rn) = rhs_call {
                    let callee = child_node(tree, rn, syns.callee)
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

fn find_enclosing_class(
    tree: &Tree,
    mut node: u32,
    syns: &Syns,
    container_syms: &[u32],
) -> Option<u32> {
    loop {
        if node == NONE {
            return None;
        }
        if let Some(dt) = child_sym(tree, node, syns.deftype) {
            if container_syms.contains(&dt) {
                return Some(node);
            }
        }
        node = tree.nodes[node as usize].parent;
    }
}
