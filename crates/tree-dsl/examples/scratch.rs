use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::{DEAD, Lang, NAMED, NONE, SYNTH};
use tree_dsl::pattern::{Out, Rewrite, Tf};
use tree_dsl::tree::{Node, Tree};

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
            if s.len() > 60 {
                format!(" {:?}...", &s[..60])
            } else {
                format!(" {:?}", s)
            }
        } else {
            String::new()
        };
        let tag = if n.tag != 0 {
            format!(" [tag={}]", n.tag)
        } else {
            String::new()
        };
        println!("{indent}{field}{pre}{kind}{sym}{tag}");
    }
}

// ── SubTree builder ──

struct SubTree {
    nodes: Vec<Node>,
    stack: Vec<usize>, // open node indices
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
        let parent = *self.stack.last().unwrap() as u32;
        self.nodes.push(Node {
            kind,
            flags: NAMED | SYNTH,
            sym,
            size: 1,
            parent,
            ..Default::default()
        });
        self
    }

    fn open(mut self, kind: u16, sym: u32) -> Self {
        let parent = *self.stack.last().unwrap() as u32;
        let idx = self.nodes.len();
        self.nodes.push(Node {
            kind,
            flags: NAMED | SYNTH,
            sym,
            size: 0,
            parent,
            ..Default::default()
        });
        self.stack.push(idx);
        self
    }

    fn close(mut self) -> Self {
        let idx = self.stack.pop().expect("close without open");
        self.nodes[idx].size = (self.nodes.len() - idx) as u32;
        self
    }

    fn build(mut self) -> Vec<Node> {
        // Close any remaining open nodes (root at minimum)
        while let Some(idx) = self.stack.pop() {
            self.nodes[idx].size = (self.nodes.len() - idx) as u32;
        }
        self.nodes
    }
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
y: int = greet("world")
z = Dog()
z.speak()

if x > 10:
    y = 1
elif x > 5:
    y = 2
else:
    y = 3

for item in [1, 2, 3]:
    greet(str(item))

while x > 0:
    x = x - 1

try:
    z.speak()
except Exception as e:
    print(e)
finally:
    print("done")

with open("file.txt") as f:
    data = f.read()

match x:
    case 1:
        y = "one"
    case 2:
        y = "two"
    case _:
        y = "other"
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::Python, &mut lang, "test.py");

    println!("=== CST ===");
    dump(&tree, &lang);

    // ── Stage 1: low-level rewrites ──
    let stage1 = vec![
        // aliased_import: fix sym, add __alias
        Rewrite::new(&mut lang, "(aliased_import name: $N)", |c| Out::SetText {
            target: 0,
            from: c.slot("N"),
            tf: Tf::Id,
        }),
        Rewrite::new(&mut lang, "(aliased_import alias: $A)", |c| Out::Append {
            under: 0,
            each: c.slot("A"),
            kind: c.intern_kind("__alias"),
            tf: Tf::Id,
        }),
        // self.x / cls.x → __ivar
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
        // superclass list → __supertype children on class
        Rewrite::new(
            &mut lang,
            "(class_definition superclasses: (argument_list $$$SUPERS:identifier|attribute|call))",
            |c| Out::Append {
                under: 0,
                each: c.slot("SUPERS"),
                kind: c.intern_kind("__supertype"),
                tf: Tf::Field(c.intern_field("function")),
            },
        ),
        // decorated_definition: hoist decorators onto the inner def/class
        Rewrite::new(
            &mut lang,
            "(decorated_definition $$$DECOS:decorator definition: $D)",
            |c| Out::Append {
                under: c.slot("D"),
                each: c.slot("DECOS"),
                kind: c.intern_kind("__decorator"),
                tf: Tf::Strip("@".into()),
            },
        ),
        // __callable: class with __call__ method
        Rewrite::new(
            &mut lang,
            r#"(class_definition name: $N body: (block (function_definition name: (identifier "__call__"))))"#,
            |c| Out::Append {
                under: 0,
                each: c.slot("N"),
                kind: c.intern_kind("__callable"),
                tf: Tf::Const("__call__"),
            },
        ),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // ── Stage 2: structural retag (a.b → __member, f() → __call) ──
    let stage2 = vec![
        Rewrite::new(&mut lang, "(attribute object: $O attribute: $M)", |c| {
            Out::Retag {
                kind: c.intern_kind("__member"),
                fields: vec![
                    (c.slot("O"), c.intern_field("object")),
                    (c.slot("M"), c.intern_field("member")),
                ],
            }
        }),
        Rewrite::new(&mut lang, "(call function: $F arguments: $A)", |c| {
            Out::Retag {
                kind: c.intern_kind("__call"),
                fields: vec![
                    (c.slot("F"), c.intern_field("callee")),
                    (c.slot("A"), c.intern_field("args")),
                ],
            }
        }),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage2);
    tree.compact();

    println!("\n=== After stage 1+2 (low-level rewrites) ===");
    dump(&tree, &lang);

    // ── Stage 3: pop-and-rebuild imports ──
    let import_from = lang.kinds.lookup("import_from_statement") as u16;
    let import_bare = lang.kinds.lookup("import_statement") as u16;
    let future_imp = lang.kinds.lookup("future_import_statement") as u16;
    let wildcard_k = lang.kinds.lookup("wildcard_import") as u16;
    let alias_synth = lang.kinds.lookup("__alias") as u16 | SYNTH;
    let mname_f = lang.fields.lookup("module_name") as u16;
    let name_f = lang.fields.lookup("name") as u16;

    let k_import = lang.intern_kind("__import");
    let k_source = lang.intern_kind("__source");
    let k_name = lang.intern_kind("__name");
    let k_alias = lang.intern_kind("__alias");

    let imports: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| {
            let k = tree.nodes[i as usize].kind;
            k == import_from || k == import_bare || k == future_imp
        })
        .collect();

    for &imp in &imports {
        let kind = tree.nodes[imp as usize].kind;
        let source_sym = if kind == future_imp {
            lang.syms.intern("__future__")
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
                names.push((lang.syms.intern("*"), 0));
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
        for &(name_sym, alias_sym) in &names {
            if alias_sym != 0 {
                b = b.open(k_name, name_sym).leaf(k_alias, alias_sym).close();
            } else {
                b = b.leaf(k_name, name_sym);
            }
        }
        tree.insert_before(imp, &b.build());
    }
    tree.compact();

    // ── Stage 4: annotate defs ──
    // Classify def types, attach return type annotations
    let class_def = lang.kinds.lookup("class_definition") as u16;
    let func_def = lang.kinds.lookup("function_definition") as u16;
    let block_k = lang.kinds.lookup("block") as u16;
    let body_f = lang.fields.lookup("body") as u16;
    let return_type_f = lang.fields.lookup("return_type") as u16;
    let name_f2 = lang.fields.lookup("name") as u16;
    let decorator_k = lang.kinds.lookup("__decorator") as u16 | SYNTH;

    let k_deftype = lang.intern_kind("__deftype");
    let k_rettype = lang.intern_kind("__return_type");

    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            continue;
        }
        let k = n.kind;

        if k == class_def {
            let dt_sym = lang.syms.intern("Class");
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );
        } else if k == func_def {
            // Method if parent chain goes through block → class_definition
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
            let has_decorator = tree.children(i).any(|c| tree.kind(c) == decorator_k);
            let label = if is_method { "Method" } else { "Function" };
            let dt_sym = lang.syms.intern(label);
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );

            // Return type annotation
            if let Some(rt) = tree.child_by_field(i, return_type_f) {
                let rt_sym = tree.sym(rt);
                if rt_sym != 0 {
                    tree.append(
                        i,
                        Node {
                            kind: k_rettype,
                            flags: NAMED | SYNTH,
                            sym: rt_sym,
                            size: 1,
                            ..Default::default()
                        },
                    );
                }
            }
        }
    }
    tree.compact();

    // ── Stage 5: stamp __branch, __loop, __scope, __binding ──
    let if_k = lang.kinds.lookup("if_statement") as u16;
    let try_k = lang.kinds.lookup("try_statement") as u16;
    let match_k = lang.kinds.lookup("match_statement") as u16;
    let for_k = lang.kinds.lookup("for_statement") as u16;
    let while_k = lang.kinds.lookup("while_statement") as u16;
    let with_k = lang.kinds.lookup("with_statement") as u16;
    let assign_k = lang.kinds.lookup("assignment") as u16;

    let elif_k = lang.kinds.lookup("elif_clause") as u16;
    let else_k = lang.kinds.lookup("else_clause") as u16;
    let except_k = lang.kinds.lookup("except_clause") as u16;
    let finally_k = lang.kinds.lookup("finally_clause") as u16;
    let case_k = lang.kinds.lookup("case_clause") as u16;

    let k_branch = lang.intern_kind("__branch");
    let k_loop = lang.intern_kind("__loop");
    let k_scope = lang.intern_kind("__scope");
    let k_binding = lang.intern_kind("__binding");
    let k_arm = lang.intern_kind("__arm");
    let k_exhaustive = lang.intern_kind("__exhaustive");
    let k_lhs = lang.intern_kind("__lhs");
    let k_rhs = lang.intern_kind("__rhs");
    let k_type = lang.intern_kind("__type_ann");

    let left_f = lang.fields.lookup("left") as u16;
    let right_f = lang.fields.lookup("right") as u16;
    let type_f = lang.fields.lookup("type") as u16;

    let leaf = |kind: u16, sym: u32| Node {
        kind,
        flags: NAMED | SYNTH,
        sym,
        size: 1,
        ..Default::default()
    };

    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if tree.nodes[i as usize].flags & DEAD != 0 {
            continue;
        }

        // Branches
        if k == if_k {
            let has_else = tree.children(i).any(|c| tree.kind(c) == else_k);
            let mut b = SubTree::new(k_branch);
            b = b.leaf(k_arm, lang.syms.intern("body"));
            // elif and else arms
            for c in tree.children(i) {
                let ck = tree.kind(c);
                if ck == elif_k {
                    b = b.leaf(k_arm, lang.syms.intern("elif"));
                }
                if ck == else_k {
                    b = b.leaf(k_arm, lang.syms.intern("else"));
                }
            }
            if has_else {
                b = b.leaf(k_exhaustive, lang.syms.intern("true"));
            }
            tree.insert_before(i + 1, &b.build());
        }

        if k == try_k {
            let mut b = SubTree::new(k_branch).leaf(k_arm, lang.syms.intern("body"));
            for c in tree.children(i) {
                let ck = tree.kind(c);
                if ck == except_k {
                    b = b.leaf(k_arm, lang.syms.intern("except"));
                }
                if ck == finally_k {
                    b = b.leaf(k_arm, lang.syms.intern("finally"));
                }
            }
            b = b.leaf(k_exhaustive, lang.syms.intern("true"));
            tree.insert_before(i + 1, &b.build());
        }

        if k == match_k {
            let mut b = SubTree::new(k_branch);
            let mut has_wildcard = false;
            for c in tree.children(i) {
                if tree.kind(c) == case_k {
                    b = b.leaf(k_arm, lang.syms.intern("case"));
                    // Check for wildcard pattern (_)
                    for gc in tree.children(c) {
                        if lang.kinds.resolve((tree.kind(gc) & !SYNTH) as u32) == "case_pattern" {
                            for ggc in tree.children(gc) {
                                if tree.sym(ggc) != 0 && lang.syms.resolve(tree.sym(ggc)) == "_" {
                                    has_wildcard = true;
                                }
                            }
                        }
                    }
                }
            }
            if has_wildcard {
                b = b.leaf(k_exhaustive, lang.syms.intern("true"));
            }
            tree.insert_before(i + 1, &b.build());
        }

        // Loops
        if k == for_k || k == while_k {
            tree.append(i, leaf(k_loop, 0));
        }

        // Scopes (function and class bodies already have __deftype; also mark them as scopes)
        if k == class_def || k == func_def {
            tree.append(i, leaf(k_scope, 0));
        }

        // Bindings
        if k == assign_k {
            let lhs = tree
                .child_by_field(i, left_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let has_type = tree
                .child_by_field(i, type_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let mut b = SubTree::new(k_binding).leaf(k_lhs, lhs);
            if has_type != 0 {
                b = b.leaf(k_type, has_type);
            }
            tree.insert_before(i + 1, &b.build());
        }

        // with statement → scope + binding
        if k == with_k {
            tree.append(i, leaf(k_scope, 0));
        }

        // for loop target → binding
        if k == for_k {
            let left = tree
                .child_by_field(i, left_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if left != 0 {
                let b = SubTree::new(k_binding).leaf(k_lhs, left);
                tree.insert_before(i + 1, &b.build());
            }
        }
    }
    tree.compact();

    println!("\n=== After all stages ===");
    dump(&tree, &lang);
}
