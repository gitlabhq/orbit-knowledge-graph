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
        while let Some(idx) = self.stack.pop() {
            self.nodes[idx].size = (self.nodes.len() - idx) as u32;
        }
        self.nodes
    }
}

fn main() {
    let src = r#"
package main

import (
    "fmt"
    "os"
    myalias "github.com/user/repo/pkg"
)

type Animal struct {
    Sound string
}

func (a *Animal) Speak() string {
    return a.Sound
}

type Speaker interface {
    Speak() string
}

func greet(name string) string {
    return fmt.Sprintf("hello %s", name)
}

func main() {
    z := &Animal{Sound: "woof"}
    z.Speak()
    var x int = 42
    y := greet("world")
    _ = y
}
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::Go, &mut lang, "main.go");

    // ── Stage 1: structural retag ──
    let stage1 = vec![
        // selector_expression → __member (Go's a.b)
        Rewrite::new(
            &mut lang,
            "(selector_expression operand: $O field: $F)",
            |c| Out::Retag {
                kind: c.kind("__member"),
                fields: vec![
                    (c.slot("O"), c.field("object")),
                    (c.slot("F"), c.field("member")),
                ],
            },
        ),
        // call_expression → __call
        Rewrite::new(
            &mut lang,
            "(call_expression function: $F arguments: $A)",
            |c| Out::Retag {
                kind: c.kind("__call"),
                fields: vec![
                    (c.slot("F"), c.field("callee")),
                    (c.slot("A"), c.field("args")),
                ],
            },
        ),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // ── Stage 2: pop-and-rebuild imports ──
    let import_decl = lang.kinds.lookup("import_declaration") as u16;
    let import_spec = lang.kinds.lookup("import_spec") as u16;
    let import_spec_list = lang.kinds.lookup("import_spec_list") as u16;
    let string_content = lang.kinds.lookup("interpreted_string_literal_content") as u16;
    let path_f = lang.fields.lookup("path") as u16;
    let name_f = lang.fields.lookup("name") as u16;

    let k_import = lang.kind("__import");
    let k_source = lang.kind("__source");
    let k_name = lang.kind("__name");
    let k_alias = lang.kind("__alias");

    let import_decls: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| tree.nodes[i as usize].kind == import_decl)
        .collect();

    for &imp in &import_decls {
        // Collect all import_spec nodes
        let specs: Vec<u32> = tree
            .descendants(imp)
            .filter(|&d| tree.kind(d) == import_spec)
            .collect();

        // Each spec becomes a separate __import
        let mut replacements: Vec<Vec<Node>> = Vec::new();
        for &spec in &specs {
            // Path: string content inside path field
            let source_sym = tree
                .child_by_field(spec, path_f)
                .and_then(|p| tree.children(p).find(|&c| tree.kind(c) == string_content))
                .map(|c| tree.sym(c))
                .unwrap_or(0);

            // Alias: name field (package_identifier)
            let alias_sym = tree
                .child_by_field(spec, name_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);

            // Go import name = last segment of path
            let source_str = lang.syms.resolve(source_sym).to_string();
            let pkg_name = source_str.rsplit('/').next().unwrap_or(&source_str);
            let name_sym = lang.syms.get(pkg_name);

            let mut b = SubTree::new(k_import).leaf(k_source, source_sym);
            if alias_sym != 0 {
                b = b.open(k_name, name_sym).leaf(k_alias, alias_sym).close();
            } else {
                b = b.leaf(k_name, name_sym);
            }
            replacements.push(b.build());
        }

        tree.remove(imp);
        // Insert all __import nodes where the import_declaration was
        for nodes in replacements.iter().rev() {
            tree.insert_before(imp, nodes);
        }
    }
    tree.compact();

    // ── Stage 3: annotate defs ──
    let type_spec = lang.kinds.lookup("type_spec") as u16;
    let struct_type = lang.kinds.lookup("struct_type") as u16;
    let interface_type = lang.kinds.lookup("interface_type") as u16;
    let func_decl = lang.kinds.lookup("function_declaration") as u16;
    let method_decl = lang.kinds.lookup("method_declaration") as u16;
    let var_spec = lang.kinds.lookup("var_spec") as u16;
    let receiver_f = lang.fields.lookup("receiver") as u16;
    let result_f = lang.fields.lookup("result") as u16;
    let type_f = lang.fields.lookup("type") as u16;

    let k_deftype = lang.kind("__deftype");
    let k_rettype = lang.kind("__return_type");
    let k_receiver = lang.kind("__receiver");

    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            continue;
        }
        let kind = n.kind;

        if kind == type_spec {
            let inner_kind = tree
                .child_by_field(i, type_f)
                .map(|c| tree.kind(c))
                .unwrap_or(0);
            let label = if inner_kind == struct_type {
                "Struct"
            } else if inner_kind == interface_type {
                "Interface"
            } else {
                "Type"
            };
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: lang.syms.get(label),
                    size: 1,
                    ..Default::default()
                },
            );
        }

        if kind == func_decl {
            let rt = tree
                .child_by_field(i, result_f)
                .map(|r| tree.sym(r))
                .unwrap_or(0);
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
            if rt != 0 {
                tree.append(
                    i,
                    Node {
                        kind: k_rettype,
                        flags: NAMED | SYNTH,
                        sym: rt,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
        }

        if kind == method_decl {
            let rt = tree
                .child_by_field(i, result_f)
                .map(|r| tree.sym(r))
                .unwrap_or(0);
            let recv_type = tree
                .child_by_field(i, receiver_f)
                .and_then(|rl| tree.children(rl).next())
                .and_then(|pd| tree.child_by_field(pd, type_f))
                .map(|t| {
                    let mut target = t;
                    for d in tree.descendants(t) {
                        if lang.kinds.resolve((tree.kind(d) & !SYNTH) as u32) == "type_identifier" {
                            target = d;
                        }
                    }
                    tree.sym(target)
                })
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
            if rt != 0 {
                tree.append(
                    i,
                    Node {
                        kind: k_rettype,
                        flags: NAMED | SYNTH,
                        sym: rt,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
            if recv_type != 0 {
                tree.append(
                    i,
                    Node {
                        kind: k_receiver,
                        flags: NAMED | SYNTH,
                        sym: recv_type,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
        }

        if kind == var_spec {
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: lang.syms.get("Variable"),
                    size: 1,
                    ..Default::default()
                },
            );
        }
    }
    tree.compact();

    println!("=== After all stages ===");
    dump(&tree, &lang);
}
