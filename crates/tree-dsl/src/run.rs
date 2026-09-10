//! Pipeline: parse → rewrite → SSA fold → edges.
//! No coloring rules. SSA reads synthetic nodes directly.

use crate::grammar::{self, SupportLang};
use crate::lang::{DEAD, E_CALLS, E_DEFINES, E_IMPORTS, Lang, NONE, SYNTH};
use crate::langs;
use crate::pattern;
use crate::ssa::{BlockId, ParseValue, SsaEngine, Value};
use crate::tree::Tree;

/// Per-language rules.
pub struct LangDef {
    pub rewrites: Vec<Vec<crate::pattern::Rewrite>>,
    pub colorings: Vec<()>,        // placeholder — colorings removed
    pub resolver_spec: Option<()>, // placeholder — resolver spec removed
}

impl LangDef {
    pub fn empty() -> Self {
        Self {
            rewrites: vec![],
            colorings: vec![],
            resolver_spec: None,
        }
    }
}

pub struct Pipeline {
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<crate::pattern::Rewrite>>,
}

fn build_lang_def(lang_id: SupportLang, lang: &mut Lang) -> LangDef {
    match lang_id {
        SupportLang::Python => langs::python::lang_def(lang),
        SupportLang::TypeScript | SupportLang::Tsx | SupportLang::JavaScript => {
            langs::typescript::lang_def(lang)
        }
        SupportLang::Rust => langs::rust_lang::lang_def(lang),
        _ => LangDef::empty(),
    }
}

impl Pipeline {
    pub fn for_lang(lang_id: SupportLang) -> (Pipeline, Lang) {
        let mut lang = Lang::new();
        let def = build_lang_def(lang_id, &mut lang);
        (
            Pipeline {
                lang_id,
                rewrite_stages: def.rewrites,
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
    // Post-rewrite pass: upgrade Function→Method for functions inside class bodies.
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
        if tree.kind(i) != deftype_k {
            continue;
        }
        if tree.sym(i) != func_sym {
            continue;
        }
        // This is a __deftype "Function". Check if any ancestor has __deftype "Class".
        let mut p = tree.nodes[i as usize].parent;
        while p != NONE {
            if has_synth(tree, p, deftype_k) {
                let dt = synth_sym(tree, p, deftype_k);
                if dt == class_sym {
                    // Upgrade to Method
                    tree.nodes[i as usize].sym = method_sym;
                    break;
                }
            }
            p = tree.nodes[p as usize].parent;
        }
    }
}

// ── Synthetic kind cache ──

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

// ── Helpers ──

/// Check if a node has a synthetic child of the given kind.
fn has_synth(tree: &Tree, node: u32, kind: u16) -> bool {
    tree.children(node).any(|c| tree.kind(c) == kind)
}

/// Get the sym of the first synthetic child of the given kind.
fn synth_sym(tree: &Tree, node: u32, kind: u16) -> u32 {
    tree.children(node)
        .find(|&c| tree.kind(c) == kind)
        .map(|c| tree.sym(c))
        .unwrap_or(0)
}

/// Get the def name: try `name:` field, then `left:` field (for assignment defs like lambda),
/// then `__defname` synthetic.
fn def_name(tree: &Tree, node: u32, name_f: u16, left_f: u16, syns: &Syns) -> u32 {
    tree.child_by_field(node, name_f)
        .or_else(|| tree.child_by_field(node, left_f))
        .map(|c| tree.sym(c))
        .or_else(|| {
            tree.children(node)
                .find(|&c| tree.kind(c) == syns.defname)
                .map(|c| tree.sym(c))
        })
        .unwrap_or(0)
}

// ── SSA fold: walks tree, reads synthetics, produces edges ──

fn ssa_fold(tree: &mut Tree, lang: &mut Lang) {
    let syns = Syns::new(lang);
    let name_f = lang.fields.lookup("name") as u16;
    let callee_f = lang.fields.lookup("callee") as u16;
    let object_f = lang.fields.lookup("object") as u16;
    let member_f = lang.fields.lookup("member") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let right_f = lang.fields.lookup("right") as u16;
    let type_f = lang.fields.lookup("type") as u16;
    let wildcard_sym = lang.syms.get("*");

    let class_sym = lang.syms.get("Class");
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);
    let mut cur_block = entry;

    let mut def_count: u32 = 0;
    let mut import_count: u32 = 0;
    let mut def_nodes: Vec<u32> = Vec::new(); // def_index → node
    let mut import_nodes: Vec<u32> = Vec::new(); // import_index → node
    let mut import_names: Vec<u32> = Vec::new(); // import_index → name sym

    // Ref info: (node, enclosing_def_node)
    let _refs: Vec<(u32, u32)> = Vec::new();

    // Scope/def stack: (def_node_or_none, node_end, saved_block)
    let mut def_stack: Vec<(Option<u32>, u32, BlockId)> = vec![(None, u32::MAX, entry)];

    // Branch stack: (arms, arm_blocks, arm_exits, exhaustive, pre_block, node_end)
    let mut branch_stack: Vec<(
        Vec<(u32, u32)>,
        Vec<BlockId>,
        Vec<BlockId>,
        bool,
        BlockId,
        u32,
    )> = Vec::new();

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

        // Pop finished defs.
        while def_stack.len() > 1 {
            let &(_, end, saved) = def_stack.last().unwrap();
            if i >= end {
                def_stack.pop();
                cur_block = saved;
            } else {
                break;
            }
        }

        // Pop finished branches.
        while let Some(br) = branch_stack.last() {
            if i >= br.5 {
                let mut preds = br.2.clone();
                if !br.3 {
                    preds.push(br.4);
                }
                let branch_end = br.5;
                cur_block = ssa.add_sealed_join(preds);
                branch_stack.pop();
                // Update enclosing branch's arm exit to this join block
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

        // Switch to correct arm block.
        if let Some(br) = branch_stack.last() {
            for (idx, &(arm_start, arm_end)) in br.0.iter().enumerate() {
                if i >= arm_start && i < arm_end {
                    cur_block = br.1[idx];
                    break;
                }
            }
        }

        // ── __import: write import refs ──
        if k == syns.import {
            for c in tree.children(i) {
                if tree.kind(c) == syns.name && tree.sym(c) != 0 {
                    let name_sym = tree.sym(c);
                    import_count += 1;
                    import_nodes.push(c); // store the __name node, not the __import node
                    import_names.push(name_sym);
                    ssa.write_variable(name_sym, cur_block, Value::ImportRef(import_count - 1));

                    // Alias
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

        // ── def: has __deftype child ──
        if has_synth(tree, i, syns.deftype) {
            let name = def_name(tree, i, name_f, left_f, &syns);
            if name != 0 {
                let parent_block = cur_block;
                cur_block = ssa.add_sealed_successor(parent_block);
                let def_idx = def_count;
                def_count += 1;
                def_nodes.push(i);
                ssa.write_variable(name, parent_block, Value::LocalDef(def_idx));

                // E_DEFINES edge from enclosing def
                if let Some(&(Some(parent_def), _, _)) = def_stack.last() {
                    tree.add_edge(parent_def, i, E_DEFINES);
                }

                // Push scope if this def has __scope
                if has_synth(tree, i, syns.scope) {
                    def_stack.push((Some(i), node_end, parent_block));
                }
            }
            i += 1;
            continue;
        }

        // ── __call: resolve callee, collect for edge emission ──
        if k == syns.call {
            let callee = tree.child_by_field(i, callee_f);
            let enclosing = def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);

            if let Some(cn) = callee {
                let callee_k = tree.kind(cn);
                if callee_k == syns.member {
                    // a.b() — member call
                    let obj_node = tree.child_by_field(cn, object_f);
                    let obj_sym = obj_node.map(|c| tree.sym(c)).unwrap_or(0);
                    let obj_is_ivar = obj_node.is_some_and(|c| tree.kind(c) == syns.ivar);
                    let member_sym = tree
                        .child_by_field(cn, member_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);

                    if obj_is_ivar && obj_sym != 0 {
                        // self.db.execute() — instance attr chain
                        // Find enclosing class, look up attr type, resolve member
                        if let Some(class_node) = find_enclosing_class(tree, i, &syns, class_sym)
                            && let Some(type_sym) = find_ivar_type(
                                tree, class_node, obj_sym, &syns, callee_f, right_f, left_f,
                            )
                        {
                            // Resolve method on the type
                            let class_reaching = ssa.read_variable(type_sym, cur_block);
                            for cpv in &class_reaching {
                                if let ParseValue::LocalDef(cdi) = cpv {
                                    let cn = def_nodes[*cdi as usize];
                                    if let Some(method) = find_method(
                                        tree, &def_nodes, cn, member_sym, &syns, name_f, left_f,
                                    ) {
                                        tree.add_edge(enclosing, method, E_CALLS);
                                    }
                                }
                            }
                        }
                    } else if obj_sym != 0 {
                        let reaching = ssa.read_variable(obj_sym, cur_block);
                        for pv in &reaching {
                            match pv {
                                ParseValue::LocalDef(di) => {
                                    tree.add_edge(enclosing, def_nodes[*di as usize], E_CALLS);
                                }
                                ParseValue::ImportRef(ii) => {
                                    if let Some(&imp_node) = import_nodes.get(*ii as usize) {
                                        tree.add_edge(enclosing, imp_node, E_IMPORTS);
                                    }
                                }
                                ParseValue::Type(type_sym) if *type_sym != 0 => {
                                    let class_reaching = ssa.read_variable(*type_sym, cur_block);
                                    for cpv in &class_reaching {
                                        if let ParseValue::LocalDef(cdi) = cpv {
                                            let class_node = def_nodes[*cdi as usize];
                                            if let Some(method) = find_method(
                                                tree, &def_nodes, class_node, member_sym, &syns,
                                                name_f, left_f,
                                            ) {
                                                tree.add_edge(enclosing, method, E_CALLS);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                } else if callee_k == syns.ivar {
                    // self.x() — instance method call
                    let ivar_sym = tree.sym(cn);
                    if ivar_sym != 0 {
                        // Find enclosing class
                        if let Some(class_node) = find_enclosing_class(tree, i, &syns, class_sym)
                            && let Some(method) = find_method(
                                tree, &def_nodes, class_node, ivar_sym, &syns, name_f, left_f,
                            )
                        {
                            tree.add_edge(enclosing, method, E_CALLS);
                        }
                    }
                } else {
                    // bare call: f()
                    let callee_sym = tree.sym(cn);
                    if callee_sym != 0 {
                        let mut reaching = ssa.read_variable(callee_sym, cur_block);
                        // Wildcard fallback: if callee not found, check if * is in scope
                        if reaching.is_empty() {
                            let wildcard = ssa.read_variable(wildcard_sym, cur_block);
                            if !wildcard.is_empty() {
                                // Link to the wildcard import — resolver will resolve the name
                                for pv in &wildcard {
                                    if let ParseValue::ImportRef(ii) = pv
                                        && let Some(&imp_node) = import_nodes.get(*ii as usize)
                                    {
                                        tree.add_edge(enclosing, imp_node, E_IMPORTS);
                                    }
                                }
                                reaching = wildcard;
                            }
                        }
                        for pv in &reaching {
                            match pv {
                                ParseValue::LocalDef(di) => {
                                    tree.add_edge(enclosing, def_nodes[*di as usize], E_CALLS);
                                }
                                ParseValue::ImportRef(ii) => {
                                    if let Some(&imp_node) = import_nodes.get(*ii as usize) {
                                        tree.add_edge(enclosing, imp_node, E_IMPORTS);
                                    }
                                }
                                ParseValue::Type(type_sym) if *type_sym != 0 => {
                                    let type_reaching = ssa.read_variable(*type_sym, cur_block);
                                    for cpv in &type_reaching {
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
                                                    name_f,
                                                    left_f,
                                                ) {
                                                    tree.add_edge(enclosing, method, E_CALLS);
                                                }
                                            } else {
                                                tree.add_edge(enclosing, target, E_CALLS);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // Don't skip children — nested calls in args
            i += 1;
            continue;
        }

        // ── __member (not inside __call): standalone member access ──
        if k == syns.member {
            // Only process if NOT a callee of a __call (parent handles those)
            let parent_k = if n.parent != NONE {
                tree.nodes[n.parent as usize].kind
            } else {
                0
            };
            if parent_k != syns.call {
                let obj_node = tree.child_by_field(i, object_f);
                let obj_sym = obj_node.map(|c| tree.sym(c)).unwrap_or(0);
                let _obj_is_ivar = obj_node.is_some_and(|c| tree.kind(c) == syns.ivar);
                let member_sym = tree
                    .child_by_field(i, member_f)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                let enclosing = def_stack.last().and_then(|&(d, _, _)| d).unwrap_or(0);
                if obj_sym != 0 {
                    let reaching = ssa.read_variable(obj_sym, cur_block);
                    for pv in &reaching {
                        match pv {
                            ParseValue::ImportRef(ii) => {
                                if let Some(&imp_node) = import_nodes.get(*ii as usize) {
                                    tree.add_edge(enclosing, imp_node, E_IMPORTS);
                                }
                            }
                            ParseValue::Type(type_sym) if *type_sym != 0 && member_sym != 0 => {
                                let class_reaching = ssa.read_variable(*type_sym, cur_block);
                                for cpv in &class_reaching {
                                    if let ParseValue::LocalDef(cdi) = cpv {
                                        let cn = def_nodes[*cdi as usize];
                                        if let Some(member) = find_method(
                                            tree, &def_nodes, cn, member_sym, &syns, name_f, left_f,
                                        ) {
                                            tree.add_edge(enclosing, member, E_CALLS);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            i += 1;
            continue;
        }

        // ── __ivar (not inside __call): standalone self.x access ──
        if k == syns.ivar && n.parent != NONE && tree.nodes[n.parent as usize].kind != syns.call {
            i += 1;
            continue;
        }

        // ── binding: has __binding child ──
        if has_synth(tree, i, syns.binding) {
            // Name: try left: field, then name: field, then first identifier child
            let lhs = tree
                .child_by_field(i, left_f)
                .or_else(|| tree.child_by_field(i, name_f))
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
                // Check if it's an instance attr assignment (__ivar on LHS)
                let lhs_node = tree.child_by_field(i, left_f);
                let is_ivar = lhs_node.is_some_and(|n| tree.kind(n) == syns.ivar);

                if !is_ivar {
                    if ssa.has_variable_in_block(lhs, cur_block) {
                        cur_block = ssa.add_sealed_successor(cur_block);
                    }

                    // Classify RHS
                    let rhs_node = tree.child_by_field(i, right_f);
                    let type_node = tree.child_by_field(i, type_f);

                    let val = if type_f != 0 && type_node.is_some() {
                        let tn = type_node.unwrap();
                        // Type annotation: x: int = ...
                        let type_sym = tree.sym(tn);
                        if type_sym != 0 {
                            Value::Type(type_sym)
                        } else {
                            Value::Opaque
                        }
                    } else if let Some(rn) = rhs_node {
                        if tree.kind(rn) == syns.call {
                            let callee_node = tree.child_by_field(rn, callee_f);
                            let callee_is_member =
                                callee_node.is_some_and(|c| tree.kind(c) == syns.member);

                            // RHS is obj.method() — resolve method's return type
                            if callee_is_member {
                                let cn = callee_node.unwrap();
                                let obj_sym = tree
                                    .child_by_field(cn, object_f)
                                    .map(|c| tree.sym(c))
                                    .unwrap_or(0);
                                let mem_sym = tree
                                    .child_by_field(cn, member_f)
                                    .map(|c| tree.sym(c))
                                    .unwrap_or(0);
                                let obj_is_ivar = tree
                                    .child_by_field(cn, object_f)
                                    .is_some_and(|c| tree.kind(c) == syns.ivar);

                                // Find the method def, then check its return type
                                let mut method_ret = None;
                                let obj_type = if obj_is_ivar {
                                    find_enclosing_class(tree, i, &syns, class_sym).and_then(
                                        |cls| {
                                            find_ivar_type(
                                                tree, cls, obj_sym, &syns, callee_f, right_f,
                                                left_f,
                                            )
                                        },
                                    )
                                } else if obj_sym != 0 {
                                    let obj_reaching = ssa.read_variable(obj_sym, cur_block);
                                    obj_reaching.iter().find_map(|pv| {
                                        if let ParseValue::Type(ts) = pv {
                                            Some(*ts)
                                        } else {
                                            None
                                        }
                                    })
                                } else {
                                    None
                                };

                                if let Some(type_sym) = obj_type {
                                    let type_reaching = ssa.read_variable(type_sym, cur_block);
                                    for cpv in &type_reaching {
                                        if let ParseValue::LocalDef(cdi) = cpv {
                                            let cls = def_nodes[*cdi as usize];
                                            if let Some(method) = find_method(
                                                tree, &def_nodes, cls, mem_sym, &syns, name_f,
                                                left_f,
                                            ) {
                                                let ret_type_f =
                                                    lang.fields.lookup("return_type") as u16;
                                                if ret_type_f != 0 {
                                                    let rt = tree
                                                        .child_by_field(method, ret_type_f)
                                                        .map(|r| tree.sym(r))
                                                        .unwrap_or(0);
                                                    if rt != 0 {
                                                        method_ret = Some(rt);
                                                    }
                                                }
                                                if method_ret.is_none() {
                                                    method_ret = infer_return_type(
                                                        tree, method, lang, &syns, callee_f,
                                                        left_f, right_f,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }

                                let val = match method_ret {
                                    Some(rt) => Value::Type(rt),
                                    None => Value::Opaque,
                                };
                                ssa.write_variable(lhs, cur_block, val);
                                if let Some(br) = branch_stack.last_mut() {
                                    for (idx, &(arm_start, arm_end)) in br.0.iter().enumerate() {
                                        if i >= arm_start && i < arm_end {
                                            br.2[idx] = cur_block;
                                            break;
                                        }
                                    }
                                }
                                i += 1;
                                continue;
                            }
                            // RHS is f() — check if callee is a class
                            let callee_sym = callee_node.map(|c| tree.sym(c)).unwrap_or(0);
                            if callee_sym != 0 {
                                let reaching = ssa.read_variable(callee_sym, cur_block);
                                let is_class = reaching.iter().any(|pv| {
                                    if let ParseValue::LocalDef(di) = pv {
                                        let dn = def_nodes[*di as usize];
                                        let dt = synth_sym(tree, dn, syns.deftype);
                                        dt != 0 && lang.syms.resolve(dt) == "Class"
                                    } else {
                                        false
                                    }
                                });
                                if is_class {
                                    Value::Type(callee_sym)
                                } else {
                                    // Check return type: annotation, __return_type synthetic, or infer from body
                                    let ret_type_f = lang.fields.lookup("return_type") as u16;
                                    let ret_type = reaching.iter().find_map(|pv| {
                                        if let ParseValue::LocalDef(di) = pv {
                                            let dn = def_nodes[*di as usize];
                                            // 1. __return_type synthetic
                                            let rt = synth_sym(tree, dn, syns.return_type);
                                            if rt != 0 {
                                                return Some(rt);
                                            }
                                            // 2. return_type: field annotation
                                            if ret_type_f != 0 {
                                                let rt = tree
                                                    .child_by_field(dn, ret_type_f)
                                                    .map(|r| tree.sym(r))
                                                    .unwrap_or(0);
                                                if rt != 0 {
                                                    return Some(rt);
                                                }
                                            }
                                            // 3. Infer from return statement
                                            infer_return_type(
                                                tree, dn, lang, &syns, callee_f, left_f, right_f,
                                            )
                                        } else {
                                            None
                                        }
                                    });
                                    match ret_type {
                                        Some(rt_sym) => {
                                            // Check if rt_sym resolves to a known def by scanning def_nodes
                                            // (handles nested defs like `return adder` where adder is out of scope)
                                            let found_def = def_nodes.iter().position(|&dn| {
                                                let dname = tree
                                                    .child_by_field(dn, name_f)
                                                    .or_else(|| tree.child_by_field(dn, left_f))
                                                    .map(|c| tree.sym(c))
                                                    .unwrap_or(0);
                                                dname == rt_sym
                                            });
                                            if let Some(di) = found_def {
                                                let dt =
                                                    synth_sym(tree, def_nodes[di], syns.deftype);
                                                if dt != 0 && lang.syms.resolve(dt) == "Class" {
                                                    Value::Type(rt_sym)
                                                } else {
                                                    // Function/other — store as LocalDef so caller can call it
                                                    Value::LocalDef(di as u32)
                                                }
                                            } else {
                                                Value::Type(rt_sym)
                                            }
                                        }
                                        None => Value::Opaque,
                                    }
                                }
                            } else {
                                Value::Opaque
                            }
                        } else if tree.kind(rn) & SYNTH == 0
                            && tree.sym(rn) != 0
                            && tree.children(rn).next().is_none()
                        {
                            // RHS is a bare identifier — alias
                            Value::Alias(tree.sym(rn))
                        } else {
                            Value::Opaque
                        }
                    } else {
                        Value::Opaque
                    };

                    ssa.write_variable(lhs, cur_block, val);
                    // Update enclosing branch's arm exit
                    if let Some(br) = branch_stack.last_mut() {
                        for (idx, &(arm_start, arm_end)) in br.0.iter().enumerate() {
                            if i >= arm_start && i < arm_end {
                                br.2[idx] = cur_block;
                                break;
                            }
                        }
                    }
                }
            }
            i += 1;
            continue;
        }

        // ── branch: has __branch child ──
        if has_synth(tree, i, syns.branch) {
            let pre = cur_block;
            // Find arm nodes: direct children that are blocks/clauses
            let arm_kinds = find_arm_children(tree, i, lang);
            let arm_blocks: Vec<BlockId> = arm_kinds
                .iter()
                .map(|_| ssa.add_sealed_successor(pre))
                .collect();
            let arm_exits = arm_blocks.clone();
            let exhaustive = false; // TODO: detect catch-all
            branch_stack.push((arm_kinds, arm_blocks, arm_exits, exhaustive, pre, node_end));
            i += 1;
            continue;
        }

        // ── loop: has __loop child ──
        if has_synth(tree, i, syns.r#loop) {
            let (h, _) = ssa.begin_loop(cur_block);
            cur_block = ssa.finish_loop(h, cur_block);
            i += 1;
            continue;
        }

        // ── scope (module): has __scope but not __deftype ──
        if has_synth(tree, i, syns.scope) && !has_synth(tree, i, syns.deftype) {
            // Module-level scope — already in entry block
            i += 1;
            continue;
        }

        // ── decorator/supertype edges ──
        if k == syns.decorator || k == syns.supertype {
            // These are synthetic children — skip, handled during def processing
            i += 1;
            continue;
        }

        i += 1;
    }

    // Pop remaining branches.
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

    // Decorator/supertype edges: connect defs to their supertypes/decorators.
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
            let reaching = ssa.read_variable(sym, entry);
            for pv in &reaching {
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

/// Find a method in a class, following the __supertype chain.
/// Uses def_nodes to resolve supertype names by scanning all known defs.
fn find_method(
    tree: &Tree,
    def_nodes: &[u32],
    class_node: u32,
    method_name: u32,
    syns: &Syns,
    name_f: u16,
    left_f: u16,
) -> Option<u32> {
    let mut search = vec![class_node];
    let mut si = 0;
    while si < search.len() {
        let cls = search[si];
        for d in tree.descendants(cls) {
            if tree.kind(d) == syns.deftype {
                let method_node = tree.nodes[d as usize].parent;
                if method_node != NONE && method_node != cls {
                    let mname = tree
                        .child_by_field(method_node, name_f)
                        .or_else(|| tree.child_by_field(method_node, left_f))
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if mname == method_name {
                        return Some(method_node);
                    }
                }
            }
        }
        // Follow supertypes by name-matching against all known defs
        for c in tree.children(cls) {
            if tree.kind(c) == syns.supertype && tree.sym(c) != 0 {
                let super_name = tree.sym(c);
                for &dn in def_nodes {
                    let dname = tree
                        .child_by_field(dn, name_f)
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

/// Infer the return type of a function by scanning for:
///   `return X()` → X (direct constructor call)
///   `return x` where `x = X()` earlier in the body → X (indirect)
fn infer_return_type(
    tree: &Tree,
    def_node: u32,
    lang: &Lang,
    syns: &Syns,
    callee_f: u16,
    left_f: u16,
    right_f: u16,
) -> Option<u32> {
    let return_k = lang.kinds.lookup("return_statement") as u16;
    if return_k == 0 {
        return None;
    }

    // Collect local bindings: name → callee_sym (for x = Foo() patterns)
    let mut local_binds: Vec<(u32, u32)> = Vec::new();
    for d in tree.descendants(def_node) {
        if has_synth(tree, d, syns.binding) {
            let lhs = tree
                .child_by_field(d, left_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let rhs_call = tree
                .child_by_field(d, right_f)
                .filter(|&r| tree.kind(r) == syns.call)
                .and_then(|r| tree.child_by_field(r, callee_f))
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
                // return X() — direct call
                if tree.kind(c) == syns.call {
                    return tree
                        .child_by_field(c, callee_f)
                        .map(|c2| tree.sym(c2))
                        .filter(|&s| s != 0);
                }
                // return x — identifier, look up in local binds or return sym directly
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
                    // Not in local binds — return the sym directly
                    // (SSA will resolve it as a def name)
                    return Some(sym);
                }
            }
        }
    }
    None
}

/// Find the type of an instance attribute by scanning __init__ for self.attr = Type().
fn find_ivar_type(
    tree: &Tree,
    class_node: u32,
    attr_sym: u32,
    syns: &Syns,
    callee_f: u16,
    right_f: u16,
    left_f: u16,
) -> Option<u32> {
    // Search the class for assignments where LHS is __ivar with matching sym
    for d in tree.descendants(class_node) {
        if has_synth(tree, d, syns.binding) {
            let lhs_node = tree.child_by_field(d, left_f);
            if let Some(ln) = lhs_node
                && tree.kind(ln) == syns.ivar
                && tree.sym(ln) == attr_sym
            {
                // Found self.attr = ... — check RHS
                let rhs = tree.child_by_field(d, right_f);
                if let Some(rn) = rhs
                    && tree.kind(rn) == syns.call
                {
                    let callee = tree
                        .child_by_field(rn, callee_f)
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

/// Find the enclosing class for a node by walking the parent chain.
fn find_enclosing_class(tree: &Tree, mut node: u32, syns: &Syns, class_sym: u32) -> Option<u32> {
    loop {
        if node == NONE {
            return None;
        }
        if has_synth(tree, node, syns.deftype) {
            let dt = synth_sym(tree, node, syns.deftype);
            if dt == class_sym {
                return Some(node);
            }
        }
        node = tree.nodes[node as usize].parent;
    }
}

/// Find branch arm children for SSA block creation.
/// Arms are consequence/alternative/body fields and specific clause kinds.
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
        let f = n.field;
        // Arms identified by field: consequence, alternative, body
        if f == consequence_f || f == alternative_f || (f == body_f && arms.is_empty()) {
            arms.push((c, c + n.size));
        }
        // Also catch except_clause, finally_clause, case_clause by kind name
        let kname = lang.kinds.resolve((n.kind & !SYNTH) as u32);
        if kname.ends_with("_clause") && f != consequence_f {
            arms.push((c, c + n.size));
        }
    }
    arms
}
