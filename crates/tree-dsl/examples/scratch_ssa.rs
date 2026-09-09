use std::collections::HashMap;
use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::{DEAD, Lang, NAMED, NONE, SYNTH};
use tree_dsl::pattern::{Out, Rewrite, Tf};
use tree_dsl::tree::{Node, Tree};

// ── Tree dumper ──

fn dump(tree: &Tree, lang: &Lang) {
    let mut depth = vec![0u32; tree.nodes.len()];
    for (i, n) in tree.nodes.iter().enumerate() {
        if i > 0 && (n.parent as usize) < depth.len() {
            depth[i] = depth[n.parent as usize] + 1;
        }
    }
    for (i, n) in tree.nodes.iter().enumerate() {
        if n.flags & DEAD != 0 {
            continue;
        }
        if n.flags & NAMED == 0 && n.sym == 0 {
            continue;
        }
        let d = depth[i] as usize;
        let indent = "  ".repeat(d);
        let kind = lang.kinds.resolve((n.kind & !SYNTH) as u32);
        let pre = if n.kind & SYNTH != 0 { "__" } else { "" };
        let field = if n.field != 0 {
            format!("{}:", lang.fields.resolve(n.field as u32))
        } else {
            String::new()
        };
        let sym = if n.sym != 0 {
            let s = lang.syms.resolve(n.sym);
            if s.len() > 40 {
                format!(" {:?}...", &s[..40])
            } else {
                format!(" {:?}", s)
            }
        } else {
            String::new()
        };
        println!("{i:>4} {indent}{field}{pre}{kind}{sym}");
    }
}

// ── SubTree builder ──

struct SubTree {
    nodes: Vec<Node>,
    stack: Vec<usize>,
}
impl SubTree {
    fn new(kind: u16) -> Self {
        Self {
            nodes: vec![Node {
                kind,
                flags: NAMED | SYNTH,
                size: 0,
                parent: NONE,
                ..Default::default()
            }],
            stack: vec![0],
        }
    }
    fn leaf(mut self, kind: u16, sym: u32) -> Self {
        let p = *self.stack.last().unwrap() as u32;
        self.nodes.push(Node {
            kind,
            flags: NAMED | SYNTH,
            sym,
            size: 1,
            parent: p,
            ..Default::default()
        });
        self
    }
    fn open(mut self, kind: u16, sym: u32) -> Self {
        let p = *self.stack.last().unwrap() as u32;
        let idx = self.nodes.len();
        self.nodes.push(Node {
            kind,
            flags: NAMED | SYNTH,
            sym,
            size: 0,
            parent: p,
            ..Default::default()
        });
        self.stack.push(idx);
        self
    }
    fn close(mut self) -> Self {
        let idx = self.stack.pop().unwrap();
        self.nodes[idx].size = (self.nodes.len() - idx) as u32;
        self
    }
    fn build(mut self) -> Vec<Node> {
        while let Some(idx) = self.stack.pop() {
            self.nodes[idx].size = (self.nodes.len() - idx) as u32;
        }
        self.nodes
    }
}

fn leaf(kind: u16, sym: u32) -> Node {
    Node {
        kind,
        flags: NAMED | SYNTH,
        sym,
        size: 1,
        parent: NONE,
        ..Default::default()
    }
}

// ── Mini SSA: reads only synthetics, produces edges ──

#[derive(Clone, Debug)]
enum Value {
    Def(u32),    // node index of the definition
    Import(u32), // node index of the __import
    Type(u32),   // sym of a type annotation
    Opaque,
}

struct MiniSsa {
    // variable name (sym) → value in current scope
    scopes: Vec<HashMap<u32, Value>>,
    edges: Vec<(u32, u32, &'static str)>, // (from_node, to_node, kind)
}

impl MiniSsa {
    fn new() -> Self {
        Self {
            scopes: vec![HashMap::new()],
            edges: Vec::new(),
        }
    }

    fn write_var(&mut self, name: u32, val: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name, val);
        }
    }

    fn read_var(&self, name: u32) -> Option<&Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope.get(&name) {
                return Some(v);
            }
        }
        None
    }

    fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
    }
}

fn run_ssa(tree: &Tree, lang: &Lang) -> Vec<(u32, u32, &'static str)> {
    // Synthetic kind IDs
    let k_import = lang.kinds.lookup("__import") as u16 | SYNTH;
    let k_source = lang.kinds.lookup("__source") as u16 | SYNTH;
    let k_name = lang.kinds.lookup("__name") as u16 | SYNTH;
    let k_deftype = lang.kinds.lookup("__deftype") as u16 | SYNTH;
    let k_defname = lang.kinds.lookup("__defname") as u16 | SYNTH;
    let k_scope = lang.kinds.lookup("__scope") as u16 | SYNTH;
    let k_binding = lang.kinds.lookup("__binding") as u16 | SYNTH;
    let k_lhs = lang.kinds.lookup("__lhs") as u16 | SYNTH;
    let k_type_ann = lang.kinds.lookup("__type_ann") as u16 | SYNTH;
    let k_call = lang.kinds.lookup("__call") as u16 | SYNTH;
    let k_member = lang.kinds.lookup("__member") as u16 | SYNTH;
    let k_ivar = lang.kinds.lookup("__ivar") as u16 | SYNTH;
    let k_branch = lang.kinds.lookup("__branch") as u16 | SYNTH;
    let k_loop = lang.kinds.lookup("__loop") as u16 | SYNTH;

    let callee_f = lang.fields.lookup("callee") as u16;
    let object_f = lang.fields.lookup("object") as u16;
    let member_f = lang.fields.lookup("member") as u16;
    let name_f = lang.fields.lookup("name") as u16;

    let mut ssa = MiniSsa::new();

    // Single pre-order walk
    let mut i = 0u32;
    while (i as usize) < tree.nodes.len() {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            i += n.size.max(1);
            continue;
        }

        // ── __import: write each __name as an import ref ──
        if n.kind == k_import {
            for c in tree.children(i) {
                if tree.kind(c) == k_name {
                    let name_sym = tree.sym(c);
                    if name_sym != 0 {
                        ssa.write_var(name_sym, Value::Import(i));
                    }
                }
            }
            i += n.size.max(1);
            continue;
        }

        // ── __deftype child: write the def's name ──
        if n.kind == k_deftype {
            // The parent is the def node. Get its name.
            let parent = n.parent;
            if parent != NONE {
                // Try name: field on parent, or __defname child
                let def_name = tree
                    .child_by_field(parent, name_f)
                    .map(|c| tree.sym(c))
                    .or_else(|| {
                        tree.children(parent)
                            .find(|&c| tree.kind(c) == k_defname)
                            .map(|c| tree.sym(c))
                    })
                    .unwrap_or(0);
                if def_name != 0 {
                    ssa.write_var(def_name, Value::Def(parent));
                }
            }
            i += 1;
            continue;
        }

        // ── __scope: skip for now (full SSA handles push/pop via Braun) ──
        if n.kind == k_scope {
            i += 1;
            continue;
        }

        // ── __binding: write variable, classify RHS ──
        if n.kind == k_binding {
            let lhs = tree
                .children(i)
                .find(|&c| tree.kind(c) == k_lhs)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let type_ann = tree
                .children(i)
                .find(|&c| tree.kind(c) == k_type_ann)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if lhs != 0 {
                if type_ann != 0 {
                    ssa.write_var(lhs, Value::Type(type_ann));
                } else {
                    // Look at RHS (sibling after __binding in the assignment)
                    let assignment = n.parent;
                    let rhs_val = if assignment != NONE {
                        // Find __call sibling — if callee is a known class, value is Type
                        let rhs_call = tree.children(assignment).find(|&c| tree.kind(c) == k_call);
                        if let Some(call) = rhs_call {
                            let callee = tree
                                .child_by_field(call, callee_f)
                                .map(|c| tree.sym(c))
                                .unwrap_or(0);
                            if callee != 0 {
                                match ssa.read_var(callee).cloned() {
                                    Some(Value::Def(def_node)) => {
                                        // Check if the def is a class
                                        let is_class = tree
                                            .children(def_node)
                                            .find(|&c| tree.kind(c) == k_deftype)
                                            .map(|c| tree.sym(c) == lang.syms.lookup("Class"))
                                            .unwrap_or(false);
                                        if is_class {
                                            Value::Type(callee)
                                        } else {
                                            Value::Def(def_node)
                                        }
                                    }
                                    Some(v) => v,
                                    None => Value::Opaque,
                                }
                            } else {
                                Value::Opaque
                            }
                        } else {
                            // Check for bare identifier RHS (alias)
                            Value::Opaque
                        }
                    } else {
                        Value::Opaque
                    };
                    eprintln!("[binding] {} = {:?}", lang.syms.resolve(lhs), rhs_val);
                    ssa.write_var(lhs, rhs_val);
                }
            }
            i += n.size.max(1);
            continue;
        }

        // ── __call: resolve callee, emit edge ──
        if n.kind == k_call {
            let callee_node = tree.child_by_field(i, callee_f);
            if let Some(cn) = callee_node {
                let callee_kind = tree.kind(cn);
                eprintln!(
                    "[call] node={i} callee={cn} is_member={}",
                    callee_kind == k_member
                );
                if callee_kind == k_member {
                    eprintln!(
                        "[member-resolve] obj_sym={} mem_sym={} z_val={:?}",
                        lang.syms.resolve(
                            tree.child_by_field(cn, object_f)
                                .map(|c| tree.sym(c))
                                .unwrap_or(0)
                        ),
                        lang.syms.resolve(
                            tree.child_by_field(cn, member_f)
                                .map(|c| tree.sym(c))
                                .unwrap_or(0)
                        ),
                        ssa.read_var(
                            tree.child_by_field(cn, object_f)
                                .map(|c| tree.sym(c))
                                .unwrap_or(0)
                        )
                        .cloned()
                    );
                    // a.b() — resolve a, then look up b on the result type
                    let obj = tree
                        .child_by_field(cn, object_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    let member_sym = tree
                        .child_by_field(cn, member_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    if obj != 0 {
                        if let Some(val) = ssa.read_var(obj).cloned() {
                            // Resolve member: find the class, then the method
                            let target = match val {
                                Value::Type(type_sym) => {
                                    // obj has known type — find class def, then method
                                    eprintln!(
                                        "[type-resolve] type_sym={} ({}) → {:?}",
                                        type_sym,
                                        lang.syms.resolve(type_sym),
                                        ssa.read_var(type_sym)
                                    );
                                    let class_node = ssa.read_var(type_sym).cloned();
                                    if let Some(Value::Def(cn)) = class_node {
                                        // Search class + supertype chain
                                        let k_super =
                                            lang.kinds.lookup("__supertype") as u16 | SYNTH;
                                        let mut classes = vec![cn];
                                        let mut ci = 0;
                                        while ci < classes.len() {
                                            let cls = classes[ci];
                                            for c in tree.children(cls) {
                                                if tree.kind(c) == k_super && tree.sym(c) != 0 {
                                                    if let Some(Value::Def(sn)) =
                                                        ssa.read_var(tree.sym(c)).cloned()
                                                    {
                                                        if !classes.contains(&sn) {
                                                            classes.push(sn);
                                                        }
                                                    }
                                                }
                                            }
                                            ci += 1;
                                        }
                                        let mut found = None;
                                        for &cls in &classes {
                                            for d in tree.descendants(cls) {
                                                if tree.kind(d) == k_deftype {
                                                    let mn = tree.nodes[d as usize].parent;
                                                    if mn != NONE {
                                                        let mname = tree
                                                            .child_by_field(mn, name_f)
                                                            .map(|c| tree.sym(c))
                                                            .unwrap_or(0);
                                                        if mname == member_sym {
                                                            found = Some(mn);
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            if found.is_some() {
                                                break;
                                            }
                                        }
                                        found.map(|n| (n, "Calls"))
                                    } else {
                                        None
                                    }
                                }
                                Value::Def(def_node) => Some((def_node, "Calls")),
                                Value::Import(imp_node) => Some((imp_node, "CallsImport")),
                                _ => None,
                            };
                            if let Some((target_node, kind)) = target {
                                ssa.edges.push((i, target_node, kind));
                            }
                        }
                    }
                } else {
                    // bare call: f()
                    let callee_sym = tree.sym(cn);
                    if callee_sym != 0 {
                        if let Some(val) = ssa.read_var(callee_sym) {
                            match val {
                                Value::Def(def_node) => {
                                    ssa.edges.push((i, *def_node, "Calls"));
                                }
                                Value::Import(imp_node) => {
                                    ssa.edges.push((i, *imp_node, "CallsImport"));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // Don't skip children — there may be nested calls in args
            i += 1;
            continue;
        }

        i += 1;
    }

    // Pop remaining scopes (for completeness)
    ssa.edges
}

fn main() {
    let src = r#"
from models import User
import os

class Animal:
    sound = "generic"
    def speak(self):
        return self.sound

class Dog(Animal):
    sound = "woof"

def greet(name: str) -> str:
    return f"hello {name}"

x = 42
y = greet("world")
z = Dog()
z.speak()
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::Python, &mut lang, "test.py");

    // ── Rewrite stages (same as scratch.rs) ──

    // Stage 1: aliases, self.x → __ivar, supertypes, decorators
    let stage1 = vec![
        Rewrite::new(&mut lang, "(aliased_import name: $N)", |c| Out::SetText {
            target: 0,
            from: c.slot("N"),
            tf: Tf::Id,
        }),
        Rewrite::new(&mut lang, "(aliased_import alias: $A)", |c| Out::Append {
            under: 0,
            each: c.slot("A"),
            kind: c.kind("__alias"),
            tf: Tf::Id,
        }),
        Rewrite::new(
            &mut lang,
            r#"(attribute object: (identifier "self") attribute: $A)"#,
            |c| Out::Replace(c.template("(__ivar @$A)")),
        ),
        Rewrite::new(
            &mut lang,
            r#"(attribute object: (identifier "cls") attribute: $A)"#,
            |c| Out::Replace(c.template("(__ivar @$A)")),
        ),
        Rewrite::new(
            &mut lang,
            "(class_definition superclasses: (argument_list $$$SUPERS:identifier|attribute|call))",
            |c| Out::Append {
                under: 0,
                each: c.slot("SUPERS"),
                kind: c.kind("__supertype"),
                tf: Tf::Field(c.field("function")),
            },
        ),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // Stage 2: a.b → __member, f() → __call
    let stage2 = vec![
        Rewrite::new(&mut lang, "(attribute object: $O attribute: $M)", |c| {
            Out::Retag {
                kind: c.kind("__member"),
                fields: vec![
                    (c.slot("O"), c.field("object")),
                    (c.slot("M"), c.field("member")),
                ],
            }
        }),
        Rewrite::new(&mut lang, "(call function: $F arguments: $A)", |c| {
            Out::Retag {
                kind: c.kind("__call"),
                fields: vec![
                    (c.slot("F"), c.field("callee")),
                    (c.slot("A"), c.field("args")),
                ],
            }
        }),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage2);
    tree.compact();

    // Stage 3: pop-and-rebuild imports
    let import_from = lang.kinds.lookup("import_from_statement") as u16;
    let import_bare = lang.kinds.lookup("import_statement") as u16;
    let future_imp = lang.kinds.lookup("future_import_statement") as u16;
    let wildcard_k = lang.kinds.lookup("wildcard_import") as u16;
    let alias_synth = lang.kinds.lookup("__alias") as u16 | SYNTH;
    let mname_f = lang.fields.lookup("module_name") as u16;
    let name_f = lang.fields.lookup("name") as u16;
    let k_import = lang.kind("__import");
    let k_source = lang.kind("__source");
    let k_name = lang.kind("__name");
    let k_alias = lang.kind("__alias");

    let imports: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| {
            let k = tree.nodes[i as usize].kind;
            k == import_from || k == import_bare || k == future_imp
        })
        .collect();
    for &imp in &imports {
        let kind = tree.nodes[imp as usize].kind;
        let source_sym = if kind == future_imp {
            lang.syms.get("__future__")
        } else if kind == import_from {
            tree.child_by_field(imp, mname_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0)
        } else {
            tree.child_by_field(imp, name_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0)
        };
        let mut names: Vec<(u32, u32)> = Vec::new();
        for c in tree.children(imp) {
            let cn = &tree.nodes[c as usize];
            let ck = cn.kind & !SYNTH;
            if ck == wildcard_k {
                names.push((lang.syms.get("*"), 0));
            } else if cn.field == name_f {
                let alias = tree
                    .children(c)
                    .find(|&gc| tree.kind(gc) == alias_synth)
                    .map(|gc| tree.sym(gc))
                    .unwrap_or(0);
                names.push((tree.sym(c), alias));
            }
        }
        if names.is_empty() && kind == import_bare {
            names.push((source_sym, 0));
        }
        tree.remove(imp);
        let mut b = SubTree::new(k_import).leaf(k_source, source_sym);
        for &(ns, als) in &names {
            if als != 0 {
                b = b.open(k_name, ns).leaf(k_alias, als).close();
            } else {
                b = b.leaf(k_name, ns);
            }
        }
        tree.insert_before(imp, &b.build());
    }
    tree.compact();

    // Stage 4: annotate defs
    let class_def = lang.kinds.lookup("class_definition") as u16;
    let func_def = lang.kinds.lookup("function_definition") as u16;
    let block_k = lang.kinds.lookup("block") as u16;
    let body_f = lang.fields.lookup("body") as u16;
    let k_deftype = lang.kind("__deftype");
    let k_scope = lang.kind("__scope");
    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            continue;
        }
        let k = n.kind;
        if k == class_def {
            tree.append(i, leaf(k_deftype, lang.syms.get("Class")));
            tree.append(i, leaf(k_scope, 0));
        } else if k == func_def {
            let is_method = {
                let p = n.parent;
                p != NONE
                    && tree
                        .nodes
                        .get(p as usize)
                        .map_or(false, |pn| pn.kind == block_k && pn.field == body_f)
                    && tree.nodes.get(p as usize).map_or(false, |pn| {
                        pn.parent != NONE
                            && tree
                                .nodes
                                .get(pn.parent as usize)
                                .map_or(false, |gp| gp.kind == class_def)
                    })
            };
            let label = if is_method { "Method" } else { "Function" };
            tree.append(i, leaf(k_deftype, lang.syms.get(label)));
            tree.append(i, leaf(k_scope, 0));
        }
    }
    tree.compact();

    // Stage 5: bindings
    let assign_k = lang.kinds.lookup("assignment") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let type_f = lang.fields.lookup("type") as u16;
    let k_binding = lang.kind("__binding");
    let k_lhs = lang.kind("__lhs");
    let k_type_ann = lang.kind("__type_ann");
    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == assign_k {
            let lhs = tree
                .child_by_field(i, left_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let ty = tree
                .child_by_field(i, type_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let mut b = SubTree::new(k_binding).leaf(k_lhs, lhs);
            if ty != 0 {
                b = b.leaf(k_type_ann, ty);
            }
            tree.insert_before(i + 1, &b.build());
        }
    }
    tree.compact();

    // ── Show the rewritten tree ──
    println!("=== Rewritten tree ===");
    dump(&tree, &lang);

    // ── Run mini SSA ──
    let edges = run_ssa(&tree, &lang);

    println!("\n=== Edges from SSA ===");
    for (from, to, kind) in &edges {
        let from_label = lang.syms.resolve(
            tree.child_by_field(*from, lang.fields.lookup("callee") as u16)
                .map(|c| tree.sym(c))
                .unwrap_or(tree.sym(*from)),
        );
        let to_label = tree
            .child_by_field(*to, lang.fields.lookup("name") as u16)
            .map(|n| lang.syms.resolve(tree.sym(n)))
            .unwrap_or_else(|| {
                // For imports, get the source
                let k_src = lang.kinds.lookup("__source") as u16 | SYNTH;
                tree.children(*to)
                    .find(|&c| tree.kind(c) == k_src)
                    .map(|c| lang.syms.resolve(tree.sym(c)))
                    .unwrap_or("?")
            });
        println!("  {kind}: {from_label} [node {from}] → {to_label} [node {to}]");
    }
}
