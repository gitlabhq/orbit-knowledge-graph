use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::{DEAD, Lang, NAMED, NONE, SYNTH};
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
    let src = std::fs::read_to_string("/tmp/test.ex").expect("read /tmp/test.ex");
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Elixir, &mut lang, "test.ex");

    let call_k = lang.kinds.lookup("call") as u16;
    let dot_k = lang.kinds.lookup("dot") as u16;
    let alias_k = lang.kinds.lookup("alias") as u16;
    let do_block_k = lang.kinds.lookup("do_block") as u16;
    let target_f = lang.fields.lookup("target") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let right_f = lang.fields.lookup("right") as u16;
    let args_k = lang.kinds.lookup("arguments") as u16;
    let keywords_k = lang.kinds.lookup("keywords") as u16;
    let pair_k = lang.kinds.lookup("pair") as u16;
    let key_f = lang.fields.lookup("key") as u16;
    let value_f = lang.fields.lookup("value") as u16;

    let k_import = lang.intern_kind("__import");
    let k_source = lang.intern_kind("__source");
    let k_name = lang.intern_kind("__name");
    let k_deftype = lang.intern_kind("__deftype");
    let k_defname = lang.intern_kind("__defname");
    let k_supertype = lang.intern_kind("__supertype");
    let k_call = lang.intern_kind("__call");

    fn get_target_sym(tree: &Tree, node: u32, target_f: u16) -> u32 {
        tree.child_by_field(node, target_f)
            .map(|t| tree.sym(t))
            .unwrap_or(0)
    }

    fn get_first_alias(tree: &Tree, node: u32, args_k: u16, alias_k: u16) -> u32 {
        tree.children(node)
            .find(|&c| tree.kind(c) == args_k)
            .and_then(|a| tree.children(a).find(|&c| tree.kind(c) == alias_k))
            .map(|c| tree.sym(c))
            .unwrap_or(0)
    }

    fn get_def_name(tree: &Tree, node: u32, args_k: u16, call_k: u16, target_f: u16) -> u32 {
        tree.children(node)
            .find(|&c| tree.kind(c) == args_k)
            .and_then(|a| tree.children(a).find(|&c| tree.kind(c) == call_k))
            .and_then(|inner| tree.child_by_field(inner, target_f))
            .map(|t| tree.sym(t))
            .unwrap_or_else(|| {
                tree.children(node)
                    .find(|&c| tree.kind(c) == args_k)
                    .and_then(|a| tree.children(a).next())
                    .map(|c| tree.sym(c))
                    .unwrap_or(0)
            })
    }

    fn get_impl_for(
        tree: &Tree,
        lang: &Lang,
        node: u32,
        args_k: u16,
        keywords_k: u16,
        pair_k: u16,
        key_f: u16,
        value_f: u16,
    ) -> u32 {
        tree.children(node)
            .find(|&c| tree.kind(c) == args_k)
            .and_then(|a| tree.children(a).find(|&c| tree.kind(c) == keywords_k))
            .and_then(|kw| tree.children(kw).find(|&c| tree.kind(c) == pair_k))
            .and_then(|p| {
                let key = tree
                    .child_by_field(p, key_f)
                    .map(|k| lang.syms.resolve(tree.sym(k)).to_string())
                    .unwrap_or_default();
                if key.starts_with("for") {
                    tree.child_by_field(p, value_f).map(|v| tree.sym(v))
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    // Intern known target names
    let s_defmodule = lang.syms.intern("defmodule");
    let s_def = lang.syms.intern("def");
    let s_defp = lang.syms.intern("defp");
    let s_alias = lang.syms.intern("alias");
    let s_import = lang.syms.intern("import");
    let s_require = lang.syms.intern("require");
    let s_defstruct = lang.syms.intern("defstruct");
    let s_defprotocol = lang.syms.intern("defprotocol");
    let s_defimpl = lang.syms.intern("defimpl");

    // Collect all top-level-ish call nodes and classify
    let calls: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| tree.nodes[i as usize].kind == call_k)
        .collect();

    for &node in &calls {
        let tsym = get_target_sym(&tree, node, target_f);

        // ── Imports: alias, import, require ──
        if tsym == s_alias || tsym == s_import || tsym == s_require {
            let source_sym = get_first_alias(&tree, node, args_k, alias_k);
            let source_str = lang.syms.resolve(source_sym).to_string();
            let name = source_str.rsplit('.').next().unwrap_or(&source_str);
            let name_sym = lang.syms.intern(name);

            tree.remove(node);
            let b = SubTree::new(k_import)
                .leaf(k_source, source_sym)
                .leaf(k_name, name_sym);
            tree.insert_before(node, &b.build());
            continue;
        }

        // ── Module defs: defmodule ──
        if tsym == s_defmodule {
            let module_name = get_first_alias(&tree, node, args_k, alias_k);
            let dt_sym = lang.syms.intern("Module");
            tree.append(
                node,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );
            tree.append(
                node,
                Node {
                    kind: k_defname,
                    flags: NAMED | SYNTH,
                    sym: module_name,
                    size: 1,
                    ..Default::default()
                },
            );
            continue;
        }

        // ── Function defs: def, defp ──
        if tsym == s_def || tsym == s_defp {
            let fname = get_def_name(&tree, node, args_k, call_k, target_f);
            let label = if tsym == s_defp {
                "PrivateFunction"
            } else {
                "Function"
            };
            let dt_sym = lang.syms.intern(label);
            tree.append(
                node,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );
            tree.append(
                node,
                Node {
                    kind: k_defname,
                    flags: NAMED | SYNTH,
                    sym: fname,
                    size: 1,
                    ..Default::default()
                },
            );
            continue;
        }

        // ── Struct: defstruct ──
        if tsym == s_defstruct {
            let dt_sym = lang.syms.intern("Struct");
            tree.append(
                node,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );
            continue;
        }

        // ── Protocol: defprotocol ──
        if tsym == s_defprotocol {
            let proto_name = get_first_alias(&tree, node, args_k, alias_k);
            let dt_sym = lang.syms.intern("Protocol");
            tree.append(
                node,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );
            tree.append(
                node,
                Node {
                    kind: k_defname,
                    flags: NAMED | SYNTH,
                    sym: proto_name,
                    size: 1,
                    ..Default::default()
                },
            );
            continue;
        }

        // ── Protocol impl: defimpl ──
        if tsym == s_defimpl {
            let proto_name = get_first_alias(&tree, node, args_k, alias_k);
            let for_type = get_impl_for(
                &tree, &lang, node, args_k, keywords_k, pair_k, key_f, value_f,
            );
            let dt_sym = lang.syms.intern("Impl");
            tree.append(
                node,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: dt_sym,
                    size: 1,
                    ..Default::default()
                },
            );
            if proto_name != 0 {
                tree.append(
                    node,
                    Node {
                        kind: k_supertype,
                        flags: NAMED | SYNTH,
                        sym: proto_name,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
            if for_type != 0 {
                tree.append(
                    node,
                    Node {
                        kind: k_defname,
                        flags: NAMED | SYNTH,
                        sym: for_type,
                        size: 1,
                        ..Default::default()
                    },
                );
            }
            continue;
        }

        // ── Qualified calls: Enum.map(...) → __call ──
        let target_node = tree.child_by_field(node, target_f);
        if let Some(tn) = target_node {
            if tree.kind(tn) == dot_k {
                // dot(left: Enum, right: map) → module.function call
                // Leave as-is for now, SSA will handle
            }
        }
    }
    tree.compact();

    println!("=== After all stages ===");
    dump(&tree, &lang);
}
