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
use std::collections::HashMap;
use crate::models::User;
use super::utils::helper;
use std::io::{Read, Write};

struct Animal {
    sound: String,
}

impl Animal {
    fn speak(&self) -> &str {
        &self.sound
    }
}

trait Greetable {
    fn greet(&self) -> String;
}

impl Greetable for Animal {
    fn greet(&self) -> String {
        format!("hello from {}", self.speak())
    }
}

fn main() {
    let x: i32 = 42;
    let z = Animal { sound: "woof".into() };
    z.speak();
}

enum Status {
    Active,
    Inactive,
}

type UserId = u64;

mod submodule {
    pub fn helper() {}
}
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::Rust, &mut lang, "test.rs");

    println!("=== CST ===");
    dump(&tree, &lang);

    // ── Stage 1: self.x → __ivar ──
    let stage1 = vec![Rewrite::new(
        &mut lang,
        "(field_expression value: (self) field: $F)",
        |c| Out::Replace(c.template("(__ivar @$F)")),
    )];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // ── Stage 2: structural retag ──
    let stage2 = vec![
        // field_expression → __member (for non-self cases)
        Rewrite::new(&mut lang, "(field_expression value: $V field: $F)", |c| {
            Out::Retag {
                kind: c.intern_kind("__member"),
                fields: vec![
                    (c.slot("V"), c.intern_field("object")),
                    (c.slot("F"), c.intern_field("member")),
                ],
            }
        }),
        // call_expression → __call
        Rewrite::new(
            &mut lang,
            "(call_expression function: $F arguments: $A)",
            |c| Out::Retag {
                kind: c.intern_kind("__call"),
                fields: vec![
                    (c.slot("F"), c.intern_field("callee")),
                    (c.slot("A"), c.intern_field("args")),
                ],
            },
        ),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage2);
    tree.compact();

    // ── Stage 3: pop-and-rebuild use declarations as __import ──
    let use_decl = lang.kinds.lookup("use_declaration") as u16;
    let scoped_id = lang.kinds.lookup("scoped_identifier") as u16;
    let use_list = lang.kinds.lookup("use_list") as u16;
    let use_as = lang.kinds.lookup("use_as_clause") as u16;
    let use_wildcard = lang.kinds.lookup("use_wildcard") as u16;
    let arg_f = lang.fields.lookup("argument") as u16;
    let path_f = lang.fields.lookup("path") as u16;
    let name_f = lang.fields.lookup("name") as u16;
    let alias_f = lang.fields.lookup("alias") as u16;

    let k_import = lang.intern_kind("__import");
    let k_source = lang.intern_kind("__source");
    let k_name = lang.intern_kind("__name");
    let k_alias = lang.intern_kind("__alias");

    let uses: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| tree.nodes[i as usize].kind == use_decl)
        .collect();

    for &u in &uses {
        let arg = tree.child_by_field(u, arg_f);
        if arg.is_none() {
            continue;
        }
        let arg = arg.unwrap();

        // Collect the full path and final names
        // use std::collections::HashMap  →  source="std::collections", name="HashMap"
        // use std::io::{Read, Write}     →  source="std::io", names=["Read", "Write"]
        // use crate::models::User        →  source="crate::models", name="User"

        fn collect_path(
            tree: &Tree,
            lang: &Lang,
            node: u32,
            scoped_id: u16,
            path_f: u16,
        ) -> String {
            let n = &tree.nodes[node as usize];
            if n.kind == scoped_id {
                if let Some(p) = tree.child_by_field(node, path_f) {
                    let prefix = collect_path(tree, lang, p, scoped_id, path_f);
                    let name_part = tree
                        .children(node)
                        .filter(|&c| tree.field_of(c) != path_f)
                        .find(|&c| tree.sym(c) != 0)
                        .map(|c| lang.syms.resolve(tree.sym(c)).to_string())
                        .unwrap_or_default();
                    if prefix.is_empty() {
                        name_part
                    } else {
                        format!("{prefix}::{name_part}")
                    }
                } else {
                    lang.syms.resolve(tree.sym(node)).to_string()
                }
            } else {
                lang.syms.resolve(tree.sym(node)).to_string()
            }
        }

        fn extract_source_and_names(
            tree: &Tree,
            lang: &mut Lang,
            node: u32,
            scoped_id: u16,
            use_list: u16,
            use_as: u16,
            use_wildcard: u16,
            path_f: u16,
            name_f: u16,
            alias_f: u16,
        ) -> (String, Vec<(u32, u32)>) {
            let n = &tree.nodes[node as usize];

            // scoped_identifier with a use_list child: use std::io::{Read, Write}
            if n.kind == scoped_id {
                // Check if name child is a use_list
                let name_child = tree
                    .children(node)
                    .find(|&c| tree.field_of(c) != path_f && tree.field_of(c) != 0);
                if let Some(nc) = name_child {
                    if tree.kind(nc) == use_list {
                        let source = tree
                            .child_by_field(node, path_f)
                            .map(|p| collect_path(tree, lang, p, scoped_id, path_f))
                            .unwrap_or_default();
                        let mut names = Vec::new();
                        for c in tree.children(nc) {
                            let ck = tree.kind(c);
                            if ck == use_as {
                                let n = tree
                                    .child_by_field(c, path_f)
                                    .or_else(|| {
                                        tree.children(c).find(|&x| {
                                            tree.field_of(x) != alias_f && tree.sym(x) != 0
                                        })
                                    })
                                    .map(|x| tree.sym(x))
                                    .unwrap_or(0);
                                let a = tree
                                    .child_by_field(c, alias_f)
                                    .map(|x| tree.sym(x))
                                    .unwrap_or(0);
                                names.push((n, a));
                            } else if ck == use_wildcard {
                                names.push((lang.syms.intern("*"), 0));
                            } else if tree.sym(c) != 0 {
                                names.push((tree.sym(c), 0));
                            }
                        }
                        return (source, names);
                    }
                }

                // Simple: use std::collections::HashMap
                let path = tree
                    .child_by_field(node, path_f)
                    .map(|p| collect_path(tree, lang, p, scoped_id, path_f))
                    .unwrap_or_default();
                let name_sym = tree
                    .children(node)
                    .find(|&c| tree.field_of(c) == name_f)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                return (path, vec![(name_sym, 0)]);
            }

            // Bare identifier: use HashMap; (unlikely but handle)
            (String::new(), vec![(tree.sym(node), 0)])
        }

        let scoped_use = lang.kinds.lookup("scoped_use_list") as u16;
        let list_f = lang.fields.lookup("list") as u16;

        let (source, names) = if tree.kind(arg) == scoped_use {
            // use std::io::{Read, Write}
            let source = tree
                .child_by_field(arg, path_f)
                .map(|p| collect_path(&tree, &lang, p, scoped_id, path_f))
                .unwrap_or_default();
            let list_node = tree.child_by_field(arg, list_f);
            let mut names = Vec::new();
            if let Some(ln) = list_node {
                for c in tree.children(ln) {
                    let ck = tree.kind(c);
                    if ck == use_as {
                        let n = tree
                            .children(c)
                            .find(|&x| tree.field_of(x) != alias_f && tree.sym(x) != 0)
                            .map(|x| tree.sym(x))
                            .unwrap_or(0);
                        let a = tree
                            .child_by_field(c, alias_f)
                            .map(|x| tree.sym(x))
                            .unwrap_or(0);
                        names.push((n, a));
                    } else if ck == use_wildcard {
                        names.push((lang.syms.intern("*"), 0));
                    } else if tree.sym(c) != 0 {
                        names.push((tree.sym(c), 0));
                    }
                }
            }
            (source, names)
        } else {
            extract_source_and_names(
                &tree,
                &mut lang,
                arg,
                scoped_id,
                use_list,
                use_as,
                use_wildcard,
                path_f,
                name_f,
                alias_f,
            )
        };

        tree.remove(u);
        let source_sym = lang.syms.intern(&source);
        let mut b = SubTree::new(k_import).leaf(k_source, source_sym);
        for &(name_sym, alias_sym) in &names {
            if alias_sym != 0 {
                b = b.open(k_name, name_sym).leaf(k_alias, alias_sym).close();
            } else {
                b = b.leaf(k_name, name_sym);
            }
        }
        tree.insert_before(u, &b.build());
    }
    tree.compact();

    // ── Stage 4: annotate defs ──
    let struct_item = lang.kinds.lookup("struct_item") as u16;
    let enum_item = lang.kinds.lookup("enum_item") as u16;
    let func_item = lang.kinds.lookup("function_item") as u16;
    let trait_item = lang.kinds.lookup("trait_item") as u16;
    let impl_item = lang.kinds.lookup("impl_item") as u16;
    let type_item = lang.kinds.lookup("type_item") as u16;
    let mod_item = lang.kinds.lookup("mod_item") as u16;
    let func_sig = lang.kinds.lookup("function_signature_item") as u16;

    let k_deftype = lang.intern_kind("__deftype");
    let k_rettype = lang.intern_kind("__return_type");
    let k_super = lang.intern_kind("__supertype");
    let return_type_f = lang.fields.lookup("return_type") as u16;
    let trait_f = lang.fields.lookup("trait") as u16;
    let type_f = lang.fields.lookup("type") as u16;

    let def_kinds: &[(u16, &str)] = &[
        (struct_item, "Struct"),
        (enum_item, "Enum"),
        (func_item, "Function"),
        (trait_item, "Trait"),
        (type_item, "TypeAlias"),
        (mod_item, "Module"),
        (func_sig, "FunctionSignature"),
    ];

    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            continue;
        }
        let kind = n.kind;

        for &(dk, label) in def_kinds {
            if kind == dk {
                let rt = tree
                    .child_by_field(i, return_type_f)
                    .map(|r| tree.sym(r))
                    .unwrap_or(0);
                tree.append(
                    i,
                    Node {
                        kind: k_deftype,
                        flags: NAMED | SYNTH,
                        sym: lang.syms.intern(label),
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
                break;
            }
        }

        if kind == impl_item {
            let trait_sym = tree
                .child_by_field(i, trait_f)
                .map(|t| tree.sym(t))
                .unwrap_or(0);
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: lang.syms.intern("Impl"),
                    size: 1,
                    ..Default::default()
                },
            );
            if trait_sym != 0 {
                tree.append(
                    i,
                    Node {
                        kind: k_super,
                        flags: NAMED | SYNTH,
                        sym: trait_sym,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
        }
    }
    tree.compact();

    println!("\n=== After all stages ===");
    dump(&tree, &lang);
}
