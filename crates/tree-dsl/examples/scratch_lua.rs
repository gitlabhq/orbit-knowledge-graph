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
        println!("{indent}{field}{pre}{kind}{sym}");
    }
}

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
    fn build(mut self) -> Vec<Node> {
        while let Some(idx) = self.stack.pop() {
            self.nodes[idx].size = (self.nodes.len() - idx) as u32;
        }
        self.nodes
    }
}

fn main() {
    let src = std::fs::read_to_string("/tmp/test.lua").expect("read /tmp/test.lua");
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Lua, &mut lang, "test.lua");

    // ── Stage 1: structural retag ──
    let stage1 = vec![
        // table.field → __member
        Rewrite::new(
            &mut lang,
            "(dot_index_expression table: $T field: $F)",
            |c| Out::Retag {
                kind: c.kind("__member"),
                fields: vec![
                    (c.slot("T"), c.field("object")),
                    (c.slot("F"), c.field("member")),
                ],
            },
        ),
        // obj:method(args) → __call (method call with implicit self)
        Rewrite::new(
            &mut lang,
            "(method_index_expression table: $T method: $M)",
            |c| Out::Retag {
                kind: c.kind("__member"),
                fields: vec![
                    (c.slot("T"), c.field("object")),
                    (c.slot("M"), c.field("member")),
                ],
            },
        ),
        // function_call → __call
        Rewrite::new(&mut lang, "(function_call name: $N arguments: $A)", |c| {
            Out::Retag {
                kind: c.kind("__call"),
                fields: vec![
                    (c.slot("N"), c.field("callee")),
                    (c.slot("A"), c.field("args")),
                ],
            }
        }),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // ── Stage 2: pop-and-rebuild require() as __import ──
    let call_k = lang.kinds.lookup("__call") as u16 | SYNTH;
    let callee_f = lang.fields.lookup("callee") as u16;
    let args_f = lang.fields.lookup("args") as u16;
    let string_content_k = lang.kinds.lookup("string_content") as u16;

    let k_import = lang.kind("__import");
    let k_source = lang.kind("__source");
    let k_name = lang.kind("__name");

    let require_sym = lang.syms.lookup("require");

    // Find require() calls — they might be nested in variable_declaration
    let var_decl_k = lang.kinds.lookup("variable_declaration") as u16;
    let assign_k = lang.kinds.lookup("assignment_statement") as u16;

    // Find all require() calls
    let require_nodes: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| {
            let n = &tree.nodes[i as usize];
            if n.kind != call_k {
                return false;
            }
            tree.child_by_field(i, callee_f)
                .map(|c| tree.sym(c) == require_sym)
                .unwrap_or(false)
        })
        .collect();

    for &node in &require_nodes {
        let source_sym = tree
            .child_by_field(node, args_f)
            .and_then(|a| {
                tree.descendants(a)
                    .find(|&d| tree.kind(d) == string_content_k)
            })
            .map(|d| tree.sym(d))
            .unwrap_or(0);

        let source_str = lang.syms.resolve(source_sym).to_string();
        let name = source_str.rsplit('.').next().unwrap_or(&source_str);
        let name_sym = lang.syms.get(name);

        // Find the enclosing variable_declaration to replace the whole thing
        let parent = tree.nodes[node as usize].parent;
        let grandparent = if parent != NONE {
            tree.nodes[parent as usize].parent
        } else {
            NONE
        };
        let replace_node =
            if grandparent != NONE && tree.nodes[grandparent as usize].kind == var_decl_k {
                grandparent
            } else {
                node
            };

        tree.remove(replace_node);
        let b = SubTree::new(k_import)
            .leaf(k_source, source_sym)
            .leaf(k_name, name_sym);
        tree.insert_before(replace_node, &b.build());
    }
    tree.compact();

    // ── Stage 3: annotate defs ──
    let func_decl_k = lang.kinds.lookup("function_declaration") as u16;
    let member_k = lang.kinds.lookup("__member") as u16 | SYNTH;
    let name_f = lang.fields.lookup("name") as u16;
    let object_f = lang.fields.lookup("object") as u16;
    let member_f = lang.fields.lookup("member") as u16;

    let k_deftype = lang.kind("__deftype");
    let k_defname = lang.kind("__defname");
    let k_receiver = lang.kind("__receiver");

    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 || n.kind != func_decl_k {
            continue;
        }

        let name_node = tree.child_by_field(i, name_f);
        if let Some(nn) = name_node {
            if tree.kind(nn) == member_k {
                let table_sym = tree
                    .child_by_field(nn, object_f)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                let method_sym = tree
                    .child_by_field(nn, member_f)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                tree.append(
                    i,
                    Node {
                        kind: k_deftype,
                        flags: NAMED | SYNTH,
                        sym: lang.syms.get("Method"),
                        size: 1,
                        ..Default::default()
                    },
                );
                tree.append(
                    i,
                    Node {
                        kind: k_defname,
                        flags: NAMED | SYNTH,
                        sym: method_sym,
                        size: 1,
                        ..Default::default()
                    },
                );
                if table_sym != 0 {
                    tree.append(
                        i,
                        Node {
                            kind: k_receiver,
                            flags: NAMED | SYNTH,
                            sym: table_sym,
                            size: 1,
                            ..Default::default()
                        },
                    );
                }
            } else {
                let defname = tree.sym(nn);
                tree.append(
                    i,
                    Node {
                        kind: k_deftype,
                        flags: NAMED | SYNTH,
                        sym: lang.syms.get("Function"),
                        size: 1,
                        ..Default::default()
                    },
                );
                tree.append(
                    i,
                    Node {
                        kind: k_defname,
                        flags: NAMED | SYNTH,
                        sym: defname,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
        }
    }
    tree.compact();

    println!("=== After all stages ===");
    dump(&tree, &lang);
}
