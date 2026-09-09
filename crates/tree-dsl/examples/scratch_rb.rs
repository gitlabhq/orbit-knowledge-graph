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
    let src = r#"
require 'json'
require_relative './helpers'

module Greetable
  def greet
    "hello from #{name}"
  end
end

class Animal
  include Greetable
  attr_accessor :sound

  def initialize(sound)
    @sound = sound
  end

  def speak
    @sound
  end

  def self.create(sound)
    new(sound)
  end
end

class Dog < Animal
  def speak
    "woof: #{@sound}"
  end
end

def standalone_function(x)
  x * 2
end

z = Dog.new("bark")
z.speak
y = standalone_function(21)
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::Ruby, &mut lang, "test.rb");

    // ── Stage 1: @x → __ivar, receiver.method → __call ──
    let stage1 = vec![
        // @sound → __ivar (Ruby instance variables)
        Rewrite::new(&mut lang, "(instance_variable)", |c| {
            Out::SetKind(c.kind("__ivar"))
        }),
        // obj.method(args) → __call(callee: __member(object, member), args)
        Rewrite::new(
            &mut lang,
            "(call receiver: $R method: $M arguments: $A)",
            |c| Out::Retag {
                kind: c.kind("__call"),
                fields: vec![
                    (c.slot("R"), c.field("object")),
                    (c.slot("M"), c.field("callee")),
                    (c.slot("A"), c.field("args")),
                ],
            },
        ),
        // method call without args: obj.method
        Rewrite::new(&mut lang, "(call receiver: $R method: $M)", |c| {
            Out::Retag {
                kind: c.kind("__call"),
                fields: vec![
                    (c.slot("R"), c.field("object")),
                    (c.slot("M"), c.field("callee")),
                ],
            }
        }),
        // bare function call: method(args)
        Rewrite::new(&mut lang, "(call method: $M arguments: $A)", |c| {
            Out::Retag {
                kind: c.kind("__call"),
                fields: vec![
                    (c.slot("M"), c.field("callee")),
                    (c.slot("A"), c.field("args")),
                ],
            }
        }),
        // class inheritance: superclass field → __supertype
        Rewrite::new(&mut lang, "(class superclass: (superclass $S))", |c| {
            Out::Append {
                under: 0,
                each: c.slot("S"),
                kind: c.kind("__supertype"),
                tf: Tf::Id,
            }
        }),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // ── Stage 2: pop-and-rebuild imports (require/require_relative) ──
    let call_k = lang.kinds.lookup("__call") as u16 | SYNTH;
    let callee_f = lang.fields.lookup("callee") as u16;
    let args_f = lang.fields.lookup("args") as u16;
    let string_content_k = lang.kinds.lookup("string_content") as u16;

    let k_import = lang.kind("__import");
    let k_source = lang.kind("__source");
    let k_name = lang.kind("__name");

    let require_sym = lang.syms.lookup("require");
    let require_rel_sym = lang.syms.lookup("require_relative");
    let include_sym = lang.syms.lookup("include");

    // Find require/require_relative calls
    let import_calls: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| {
            let n = &tree.nodes[i as usize];
            if n.kind != call_k {
                return false;
            }
            tree.child_by_field(i, callee_f)
                .map(|c| {
                    let s = tree.sym(c);
                    s == require_sym || s == require_rel_sym
                })
                .unwrap_or(false)
        })
        .collect();

    for &imp in &import_calls {
        // Source: string_content inside arguments
        let source_sym = tree
            .child_by_field(imp, args_f)
            .and_then(|a| {
                tree.descendants(a)
                    .find(|&d| tree.kind(d) == string_content_k)
            })
            .map(|d| tree.sym(d))
            .unwrap_or(0);

        // Name: last segment of path
        let source_str = lang.syms.resolve(source_sym).to_string();
        let name = source_str.rsplit('/').next().unwrap_or(&source_str);
        let name = name.strip_prefix("./").unwrap_or(name);
        let name_sym = lang.syms.get(name);

        tree.remove(imp);
        let b = SubTree::new(k_import)
            .leaf(k_source, source_sym)
            .leaf(k_name, name_sym);
        tree.insert_before(imp, &b.build());
    }

    // include Greetable → __supertype on the enclosing class
    // (include was already retagged to __call, find those)
    let include_calls: Vec<(u32, u32)> = (0..tree.nodes.len() as u32)
        .filter_map(|i| {
            let n = &tree.nodes[i as usize];
            if n.kind != call_k {
                return None;
            }
            let callee = tree.child_by_field(i, callee_f)?;
            if tree.sym(callee) != include_sym {
                return None;
            }
            // The included module name is in args
            let arg = tree
                .child_by_field(i, args_f)
                .and_then(|a| tree.children(a).next())?;
            Some((i, tree.sym(arg)))
        })
        .collect();

    // Find enclosing class for each include call and add __supertype
    let class_k = lang.kinds.lookup("class") as u16;
    let body_stmt_k = lang.kinds.lookup("body_statement") as u16;
    let supertype_k = lang.kind("__supertype");
    for &(call_node, module_sym) in &include_calls {
        // Walk up: call → body_statement → class
        let mut p = tree.nodes[call_node as usize].parent;
        while p != NONE {
            let pn = &tree.nodes[p as usize];
            if pn.kind == class_k {
                tree.append(
                    p,
                    Node {
                        kind: supertype_k,
                        flags: NAMED | SYNTH,
                        sym: module_sym,
                        size: 1,
                        ..Default::default()
                    },
                );
                break;
            }
            p = pn.parent;
        }
        tree.remove(call_node);
    }
    tree.compact();

    // ── Stage 3: annotate defs ──
    let class_k2 = lang.kinds.lookup("class") as u16;
    let module_k = lang.kinds.lookup("module") as u16;
    let method_k = lang.kinds.lookup("method") as u16;
    let singleton_method_k = lang.kinds.lookup("singleton_method") as u16;

    let k_deftype = lang.kind("__deftype");

    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            continue;
        }
        let kind = n.kind;
        let parent = n.parent;

        if kind == class_k2 {
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: lang.syms.get("Class"),
                    size: 1,
                    ..Default::default()
                },
            );
        } else if kind == module_k {
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: lang.syms.get("Module"),
                    size: 1,
                    ..Default::default()
                },
            );
        } else if kind == method_k {
            let is_method = {
                let mut p = parent;
                while p != NONE {
                    let pk = tree.nodes[p as usize].kind;
                    if pk == class_k2 || pk == module_k {
                        break;
                    }
                    p = tree.nodes[p as usize].parent;
                }
                p != NONE && tree.nodes[p as usize].kind == class_k2
            };
            let label = if is_method { "Method" } else { "Function" };
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
        } else if kind == singleton_method_k {
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: lang.syms.get("ClassMethod"),
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
