//! Pipeline: parse → rewrite → SSA fold → edges.

use crate::grammar::{self, SupportLang};
use crate::lang::{DEAD, E_CALLS, E_DEFINES, E_IMPORTS, Lang, NONE, SYNTH};
use crate::pattern;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::Tree;

pub struct LangDef {
    pub rewrites: Vec<Vec<crate::pattern::Rewrite>>,
    pub resolve: crate::file_tree::ResolveConfig,
}

impl LangDef {
    pub fn empty() -> Self {
        Self {
            rewrites: vec![],
            resolve: crate::file_tree::ResolveConfig::default(),
        }
    }
}

pub struct Pipeline {
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<crate::pattern::Rewrite>>,
    pub resolve: crate::file_tree::ResolveConfig,
}

/// Embedded YAML rule files. Returns None if the language has no rules yet.
fn lang_yaml(lang_id: SupportLang) -> Option<&'static str> {
    match lang_id {
        SupportLang::Python => Some(include_str!("../langs/python.yaml")),
        // TS/JS share the same rules when they exist
        // SupportLang::TypeScript | SupportLang::Tsx | SupportLang::JavaScript =>
        //     Some(include_str!("../langs/typescript.yaml")),
        // SupportLang::Rust => Some(include_str!("../langs/rust.yaml")),
        _ => None,
    }
}

impl Pipeline {
    pub fn for_lang(lang_id: SupportLang) -> (Pipeline, Lang) {
        let mut lang = Lang::new();
        let def = match lang_yaml(lang_id) {
            Some(yaml) => {
                let (rewrites, resolve) = crate::rules::load_lang(yaml, &mut lang);
                LangDef { rewrites, resolve }
            }
            None => LangDef::empty(),
        };
        (
            Pipeline {
                lang_id,
                rewrite_stages: def.rewrites,
                resolve: def.resolve,
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
    let deftype_k = lang.kinds.lookup("__deftype") as u16 | SYNTH;
    let func_sym = lang.syms.get("Function");
    let method_sym = lang.syms.get("Method");
    let class_sym = lang.syms.get("Class");

    for i in 0..tree.nodes.len() as u32 {
        if tree.kind(i) != deftype_k || tree.sym(i) != func_sym {
            continue;
        }
        let mut p = tree.nodes[i as usize].parent;
        while p != NONE {
            if has_synth(tree, p, deftype_k) && synth_sym(tree, p, deftype_k) == class_sym {
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
        let s = |name: &str| lang.kinds.lookup(name) as u16 | SYNTH;
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

fn has_synth(tree: &Tree, node: u32, kind: u16) -> bool {
    tree.children(node).any(|c| tree.kind(c) == kind)
}

fn synth_sym(tree: &Tree, node: u32, kind: u16) -> u32 {
    tree.children(node)
        .find(|&c| tree.kind(c) == kind)
        .map(|c| tree.sym(c))
        .unwrap_or(0)
}

fn def_name(tree: &Tree, node: u32, f: &Fields, syns: &Syns) -> u32 {
    tree.child_by_field(node, f.name)
        .or_else(|| tree.child_by_field(node, f.left))
        .map(|c| tree.sym(c))
        .or_else(|| {
            tree.children(node)
                .find(|&c| tree.kind(c) == syns.defname)
                .map(|c| tree.sym(c))
        })
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
                tree.add_edge(from, method, E_CALLS);
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
            tree.add_edge(from, def_nodes[*di as usize], E_CALLS);
        }
        ParseValue::ImportRef(ii) => {
            if let Some(&imp_node) = import_nodes.get(*ii as usize) {
                tree.add_edge(from, imp_node, E_IMPORTS);
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
    let rt = synth_sym(tree, def_node, syns.return_type);
    if rt != 0 {
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

fn update_branch_arm(
    branch_stack: &mut [(
        Vec<(u32, u32)>,
        Vec<BlockId>,
        Vec<BlockId>,
        bool,
        BlockId,
        u32,
    )],
    i: u32,
    cur_block: BlockId,
) {
    if let Some(br) = branch_stack.last_mut() {
        for (idx, &(arm_start, arm_end)) in br.0.iter().enumerate() {
            if i >= arm_start && i < arm_end {
                br.2[idx] = cur_block;
                break;
            }
        }
    }
}

// ── SSA fold ──

fn ssa_fold(tree: &mut Tree, lang: &mut Lang) {
    let syns = Syns::new(lang);
    let f = Fields::new(lang);
    let wildcard_sym = lang.syms.get("*");
    let class_sym = lang.syms.get("Class");

    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);
    let mut cur_block = entry;

    let mut def_count: u32 = 0;
    let mut import_count: u32 = 0;
    let mut def_nodes: Vec<u32> = Vec::new();
    let mut import_nodes: Vec<u32> = Vec::new();
    let mut import_names: Vec<u32> = Vec::new();

    let mut def_stack: Vec<(Option<u32>, u32, BlockId)> = vec![(None, u32::MAX, entry)];

    type BranchEntry = (
        Vec<(u32, u32)>,
        Vec<BlockId>,
        Vec<BlockId>,
        bool,
        BlockId,
        u32,
    );
    let mut branch_stack: Vec<BranchEntry> = Vec::new();

    let mut i = 0u32;
    let len = tree.nodes.len() as u32;

    while i < len {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            i += n.size.max(1);
            continue;
        }
        let k = n.kind;
        let node_end = i + n.size;

        // Pop finished scopes and branches.
        while def_stack.len() > 1 {
            let &(_, end, saved) = def_stack.last().unwrap();
            if i >= end {
                def_stack.pop();
                cur_block = saved;
            } else {
                break;
            }
        }
        while let Some(br) = branch_stack.last() {
            if i >= br.5 {
                let mut preds = br.2.clone();
                if !br.3 {
                    preds.push(br.4);
                }
                let branch_end = br.5;
                cur_block = ssa.add_sealed_join(preds);
                branch_stack.pop();
                if let Some(outer) = branch_stack.last_mut() {
                    for (idx, &(arm_start, arm_end)) in outer.0.iter().enumerate() {
                        if branch_end > arm_start && branch_end <= arm_end {
                            outer.2[idx] = cur_block;
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }
        if let Some(br) = branch_stack.last() {
            for (idx, &(arm_start, arm_end)) in br.0.iter().enumerate() {
                if i >= arm_start && i < arm_end {
                    cur_block = br.1[idx];
                    break;
                }
            }
        }

        // ── import ──
        if k == syns.import {
            for c in tree.children(i) {
                if tree.kind(c) == syns.name && tree.sym(c) != 0 {
                    let name_sym = tree.sym(c);
                    import_count += 1;
                    import_nodes.push(c);
                    import_names.push(name_sym);
                    ssa.write_variable(name_sym, cur_block, Value::ImportRef(import_count - 1));
                    for gc in tree.children(c) {
                        if tree.kind(gc) == syns.alias && tree.sym(gc) != 0 {
                            let alias = tree.sym(gc);
                            if alias != name_sym {
                                ssa.write_variable(
                                    alias,
                                    cur_block,
                                    Value::ImportRef(import_count - 1),
                                );
                            }
                        }
                    }
                }
            }
            i += n.size.max(1);
            continue;
        }

        // ── def ──
        if has_synth(tree, i, syns.deftype) {
            let name = def_name(tree, i, &f, &syns);
            if name != 0 {
                let parent_block = cur_block;
                cur_block = ssa.add_sealed_successor(parent_block);
                let def_idx = def_count;
                def_count += 1;
                def_nodes.push(i);
                ssa.write_variable(name, parent_block, Value::LocalDef(def_idx));
                if let Some(&(Some(parent_def), _, _)) = def_stack.last() {
                    tree.add_edge(parent_def, i, E_DEFINES);
                }
                if has_synth(tree, i, syns.scope) {
                    def_stack.push((Some(i), node_end, parent_block));
                }
            }
            i += 1;
            continue;
        }

        // ── call ──
        if k == syns.call {
            let enclosing = def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
            if let Some(cn) = tree.child_by_field(i, f.callee) {
                let callee_k = tree.kind(cn);

                if callee_k == syns.member {
                    // a.b()
                    let obj_node = tree.child_by_field(cn, f.object);
                    let obj_sym = obj_node.map(|c| tree.sym(c)).unwrap_or(0);
                    let obj_is_ivar = obj_node.is_some_and(|c| tree.kind(c) == syns.ivar);
                    let mem_sym = tree
                        .child_by_field(cn, f.member)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);

                    if obj_is_ivar && obj_sym != 0 {
                        if let Some(cls) = find_enclosing_class(tree, i, &syns, class_sym)
                            && let Some(ts) = find_ivar_type(tree, cls, obj_sym, &syns, &f)
                        {
                            resolve_method_on_type(
                                tree, &def_nodes, &mut ssa, ts, mem_sym, cur_block, enclosing,
                                &syns, &f,
                            );
                        }
                    } else if obj_sym != 0 {
                        for pv in &ssa.read_variable(obj_sym, cur_block) {
                            match pv {
                                ParseValue::Type(ts) if *ts != 0 => {
                                    resolve_method_on_type(
                                        tree, &def_nodes, &mut ssa, *ts, mem_sym, cur_block,
                                        enclosing, &syns, &f,
                                    );
                                }
                                _ => emit_edge_for_value(
                                    tree,
                                    pv,
                                    &def_nodes,
                                    &import_nodes,
                                    enclosing,
                                ),
                            }
                        }
                    }
                } else if callee_k == syns.ivar {
                    // self.x()
                    let ivar_sym = tree.sym(cn);
                    if ivar_sym != 0 {
                        if let Some(cls) = find_enclosing_class(tree, i, &syns, class_sym)
                            && let Some(method) =
                                find_method(tree, &def_nodes, cls, ivar_sym, &syns, &f)
                        {
                            tree.add_edge(enclosing, method, E_CALLS);
                        }
                    }
                } else {
                    // f()
                    let callee_sym = tree.sym(cn);
                    if callee_sym != 0 {
                        let mut reaching = ssa.read_variable(callee_sym, cur_block);
                        // Wildcard fallback
                        if reaching.is_empty() {
                            let wildcard = ssa.read_variable(wildcard_sym, cur_block);
                            if !wildcard.is_empty() {
                                for pv in &wildcard {
                                    emit_edge_for_value(
                                        tree,
                                        pv,
                                        &def_nodes,
                                        &import_nodes,
                                        enclosing,
                                    );
                                }
                                reaching = wildcard;
                            }
                        }
                        for pv in &reaching {
                            match pv {
                                ParseValue::Type(ts) if *ts != 0 => {
                                    let target_reaching = ssa.read_variable(*ts, cur_block);
                                    for cpv in &target_reaching {
                                        if let ParseValue::LocalDef(cdi) = cpv {
                                            let target = def_nodes[*cdi as usize];
                                            let callable_sym =
                                                synth_sym(tree, target, syns.callable);
                                            if callable_sym != 0 {
                                                if let Some(method) = find_method(
                                                    tree,
                                                    &def_nodes,
                                                    target,
                                                    callable_sym,
                                                    &syns,
                                                    &f,
                                                ) {
                                                    tree.add_edge(enclosing, method, E_CALLS);
                                                }
                                            } else {
                                                tree.add_edge(enclosing, target, E_CALLS);
                                            }
                                        }
                                    }
                                }
                                _ => emit_edge_for_value(
                                    tree,
                                    pv,
                                    &def_nodes,
                                    &import_nodes,
                                    enclosing,
                                ),
                            }
                        }
                    }
                }
            }
            i += 1;
            continue;
        }

        // ── standalone member (not callee of __call) ──
        if k == syns.member {
            let parent_k = if n.parent != NONE {
                tree.nodes[n.parent as usize].kind
            } else {
                0
            };
            if parent_k != syns.call {
                let obj_sym = tree
                    .child_by_field(i, f.object)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                let mem_sym = tree
                    .child_by_field(i, f.member)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                let enclosing = def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
                if obj_sym != 0 {
                    for pv in &ssa.read_variable(obj_sym, cur_block) {
                        match pv {
                            ParseValue::ImportRef(ii) => {
                                if let Some(&imp_node) = import_nodes.get(*ii as usize) {
                                    tree.add_edge(enclosing, imp_node, E_IMPORTS);
                                }
                            }
                            ParseValue::Type(ts) if *ts != 0 && mem_sym != 0 => {
                                resolve_method_on_type(
                                    tree, &def_nodes, &mut ssa, *ts, mem_sym, cur_block, enclosing,
                                    &syns, &f,
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

        if k == syns.ivar && n.parent != NONE && tree.nodes[n.parent as usize].kind != syns.call {
            i += 1;
            continue;
        }

        // ── binding ──
        if has_synth(tree, i, syns.binding) {
            let lhs = tree
                .child_by_field(i, f.left)
                .or_else(|| tree.child_by_field(i, f.name))
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
                    .child_by_field(i, f.left)
                    .is_some_and(|n| tree.kind(n) == syns.ivar);
                if !is_ivar {
                    if ssa.has_variable_in_block(lhs, cur_block) {
                        cur_block = ssa.add_sealed_successor(cur_block);
                    }
                    let val = classify_rhs(
                        tree, i, &mut ssa, &def_nodes, cur_block, lang, &syns, &f, class_sym,
                    );
                    ssa.write_variable(lhs, cur_block, val);
                    update_branch_arm(&mut branch_stack, i, cur_block);
                }
            }
            i += 1;
            continue;
        }

        // ── branch / loop / scope / decorator ──
        if has_synth(tree, i, syns.branch) {
            let pre = cur_block;
            let arm_kinds = find_arm_children(tree, i, lang);
            let arm_blocks: Vec<BlockId> = arm_kinds
                .iter()
                .map(|_| ssa.add_sealed_successor(pre))
                .collect();
            let arm_exits = arm_blocks.clone();
            branch_stack.push((arm_kinds, arm_blocks, arm_exits, false, pre, node_end));
            i += 1;
            continue;
        }
        if has_synth(tree, i, syns.r#loop) {
            let (h, _) = ssa.begin_loop(cur_block);
            cur_block = ssa.finish_loop(h, cur_block);
            i += 1;
            continue;
        }
        if has_synth(tree, i, syns.scope) && !has_synth(tree, i, syns.deftype) {
            i += 1;
            continue;
        }
        if k == syns.decorator || k == syns.supertype {
            i += 1;
            continue;
        }

        i += 1;
    }

    // Drain remaining branches.
    while let Some(br) = branch_stack.last() {
        let mut preds = br.2.clone();
        if !br.3 {
            preds.push(br.4);
        }
        let _ = ssa.add_sealed_join(preds);
        branch_stack.pop();
    }

    ssa.seal_remaining();
    ssa.remove_redundant_phi_sccs();

    // Decorator/supertype meta-edges.
    let mut meta_edges: Vec<(u32, u32)> = Vec::new();
    for &def_node in &def_nodes {
        let syms: Vec<u32> = tree
            .children(def_node)
            .filter(|&c| {
                let ck = tree.kind(c);
                (ck == syns.supertype || ck == syns.decorator) && tree.sym(c) != 0
            })
            .map(|c| tree.sym(c))
            .collect();
        for sym in syms {
            for pv in &ssa.read_variable(sym, entry) {
                if let ParseValue::LocalDef(di) = pv {
                    meta_edges.push((def_node, def_nodes[*di as usize]));
                }
            }
        }
    }
    for (from, to) in meta_edges {
        tree.add_edge(from, to, E_CALLS);
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
        if tree.kind(rn) & SYNTH == 0 && tree.sym(rn) != 0 && tree.children(rn).next().is_none() {
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
            let dt = synth_sym(tree, def_nodes[*di as usize], syns.deftype);
            dt != 0 && lang.syms.resolve(dt) == "Class"
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
            let found_def = def_nodes.iter().position(|&dn| {
                let dname = tree
                    .child_by_field(dn, f.name)
                    .or_else(|| tree.child_by_field(dn, f.left))
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                dname == rt_sym
            });
            if let Some(di) = found_def {
                let dt = synth_sym(tree, def_nodes[di], syns.deftype);
                if dt != 0 && lang.syms.resolve(dt) == "Class" {
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
                    let mname = tree
                        .child_by_field(method_node, f.name)
                        .or_else(|| tree.child_by_field(method_node, f.left))
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
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
        if has_synth(tree, d, syns.binding) {
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
                if tree.kind(c) & SYNTH == 0
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
        if has_synth(tree, d, syns.binding) {
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
        if has_synth(tree, node, syns.deftype) && synth_sym(tree, node, syns.deftype) == class_sym {
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
        if n.flags & DEAD != 0 {
            continue;
        }
        let fld = n.field;
        if fld == consequence_f || fld == alternative_f || (fld == body_f && arms.is_empty()) {
            arms.push((c, c + n.size));
        }
        let kname = lang.kinds.resolve((n.kind & !SYNTH) as u32);
        if kname.ends_with("_clause") && fld != consequence_f {
            arms.push((c, c + n.size));
        }
    }
    arms
}
