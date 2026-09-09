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
import { User, Admin } from './models';
import * as ns from './utils';
import Default from './config';
import type { UserId } from './types';

interface Serializable {
    toJSON(): string;
}

class Animal {
    sound: string = "generic";
    speak(): string { return this.sound; }
}

class Dog extends Animal {
    sound = "woof";
}

abstract class Base {
    abstract run(): void;
    static create(): Base { return new Base(); }
}

function greet(name: string): string {
    return `hello ${name}`;
}

async function fetchData(url: string): Promise<void> {
    await fetch(url);
}

const x: number = 42;
let y = greet("world");
const z = new Dog();
z.speak();

type AliasId = string;
enum Status { Active, Inactive }

export function exported(): void {}
export default class DefaultExport {}
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::TypeScript, &mut lang, "test.ts");

    println!("=== CST ===");
    dump(&tree, &lang);

    // ── Stage 1: low-level rewrites ──
    let stage1 = vec![
        // this.x → __ivar
        Rewrite::new(
            &mut lang,
            r#"(member_expression object: (this) property: $P)"#,
            |c| Out::Replace(c.template("(__ivar @$P)")),
        ),
        // extends clause → __supertype
        Rewrite::new(
            &mut lang,
            "(class_declaration (class_heritage (extends_clause value: $V)))",
            |c| Out::Append {
                under: 0,
                each: c.slot("V"),
                kind: c.kind("__supertype"),
                tf: Tf::Id,
            },
        ),
        Rewrite::new(
            &mut lang,
            "(abstract_class_declaration (class_heritage (extends_clause value: $V)))",
            |c| Out::Append {
                under: 0,
                each: c.slot("V"),
                kind: c.kind("__supertype"),
                tf: Tf::Id,
            },
        ),
    ];
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage1);
    tree.compact();

    // ── Stage 2: structural retag ──
    let stage2 = vec![
        Rewrite::new(
            &mut lang,
            "(member_expression object: $O property: $P)",
            |c| Out::Retag {
                kind: c.kind("__member"),
                fields: vec![
                    (c.slot("O"), c.field("object")),
                    (c.slot("P"), c.field("member")),
                ],
            },
        ),
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
    tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, &stage2);
    tree.compact();

    println!("\n=== After stage 1+2 (low-level rewrites) ===");
    dump(&tree, &lang);

    // ── Stage 3: pop-and-rebuild imports ──
    let import_stmt = lang.kinds.lookup("import_statement") as u16;
    let import_spec = lang.kinds.lookup("import_specifier") as u16;
    let namespace_imp = lang.kinds.lookup("namespace_import") as u16;
    let import_clause = lang.kinds.lookup("import_clause") as u16;
    let named_imports = lang.kinds.lookup("named_imports") as u16;
    let string_frag = lang.kinds.lookup("string_fragment") as u16;
    let source_f = lang.fields.lookup("source") as u16;
    let name_f = lang.fields.lookup("name") as u16;
    let alias_f = lang.fields.lookup("alias") as u16;

    let k_import = lang.kind("__import");
    let k_source = lang.kind("__source");
    let k_name = lang.kind("__name");
    let k_alias = lang.kind("__alias");

    let imports: Vec<u32> = (0..tree.nodes.len() as u32)
        .filter(|&i| tree.nodes[i as usize].kind == import_stmt)
        .collect();

    for &imp in &imports {
        // Source: string_fragment inside source field
        let source_sym = tree
            .child_by_field(imp, source_f)
            .and_then(|s| tree.children(s).find(|&c| tree.kind(c) == string_frag))
            .map(|c| tree.sym(c))
            .unwrap_or(0);

        // Check for `type` keyword (import type { ... })
        let is_type_only = tree
            .children(imp)
            .any(|c| lang.kinds.resolve((tree.kind(c) & !SYNTH) as u32) == "type");

        // Find import_clause
        let clause = tree.children(imp).find(|&c| tree.kind(c) == import_clause);

        let mut names: Vec<(u32, u32, &str)> = Vec::new(); // (name, alias, label)

        if let Some(cl) = clause {
            // Check for namespace_import: import * as ns
            let has_namespace = tree.descendants(cl).any(|d| tree.kind(d) == namespace_imp);
            // Check for named_imports: import { a, b }
            let has_named = tree.descendants(cl).any(|d| tree.kind(d) == named_imports);

            if has_namespace {
                // import * as ns — name is the identifier in namespace_import
                let ns_name = tree
                    .descendants(cl)
                    .find(|&d| tree.kind(d) == namespace_imp)
                    .and_then(|ns| {
                        tree.children(ns)
                            .find(|&c| tree.node(c).flags & NAMED != 0 && tree.sym(c) != 0)
                    })
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                names.push((lang.syms.get("*"), ns_name, "NamespaceImport"));
            } else if has_named {
                // import { User, Admin as A }
                for spec in tree
                    .descendants(cl)
                    .filter(|&d| tree.kind(d) == import_spec)
                {
                    let n = tree
                        .child_by_field(spec, name_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    let a = tree
                        .child_by_field(spec, alias_f)
                        .map(|c| tree.sym(c))
                        .unwrap_or(0);
                    names.push((n, a, "NamedImport"));
                }
            } else {
                // default import: import Default from '...'
                let default_name = tree
                    .children(cl)
                    .find(|&c| tree.node(c).flags & NAMED != 0 && tree.sym(c) != 0)
                    .map(|c| tree.sym(c))
                    .unwrap_or(0);
                names.push((lang.syms.get("default"), default_name, "DefaultImport"));
            }
        }

        let label = names.first().map(|n| n.2).unwrap_or("Import");

        tree.remove(imp);
        let mut b = SubTree::new(k_import)
            .leaf(k_source, source_sym)
            .leaf(lang.kind("__import_type"), lang.syms.get(label));
        if is_type_only {
            b = b.leaf(lang.kind("__type_only"), lang.syms.get("true"));
        }
        for &(name_sym, alias_sym, _) in &names {
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
    let class_decl = lang.kinds.lookup("class_declaration") as u16;
    let abstract_class = lang.kinds.lookup("abstract_class_declaration") as u16;
    let func_decl = lang.kinds.lookup("function_declaration") as u16;
    let method_def = lang.kinds.lookup("method_definition") as u16;
    let abstract_method = lang.kinds.lookup("abstract_method_signature") as u16;
    let iface_decl = lang.kinds.lookup("interface_declaration") as u16;
    let enum_decl = lang.kinds.lookup("enum_declaration") as u16;
    let type_alias = lang.kinds.lookup("type_alias_declaration") as u16;
    let var_declarator = lang.kinds.lookup("variable_declarator") as u16;
    let class_body = lang.kinds.lookup("class_body") as u16;
    let iface_body = lang.kinds.lookup("interface_body") as u16;
    let body_f = lang.fields.lookup("body") as u16;
    let return_type_f = lang.fields.lookup("return_type") as u16;

    let k_deftype = lang.kind("__deftype");
    let k_rettype = lang.kind("__return_type");

    let def_kinds: &[(u16, &str)] = &[
        (class_decl, "Class"),
        (abstract_class, "AbstractClass"),
        (func_decl, "Function"),
        (iface_decl, "Interface"),
        (enum_decl, "Enum"),
        (type_alias, "TypeAlias"),
    ];

    let static_k = lang.kinds.lookup("static") as u16;

    for i in 0..tree.nodes.len() as u32 {
        let n = &tree.nodes[i as usize];
        if n.flags & DEAD != 0 {
            continue;
        }

        let mut deftype = 0u32;
        let mut rettype = 0u32;

        for &(dk, label) in def_kinds {
            if n.kind == dk {
                deftype = lang.syms.get(label);
                rettype = tree
                    .child_by_field(i, return_type_f)
                    .map(|r| tree.sym(r))
                    .unwrap_or(0);
                break;
            }
        }

        if n.kind == method_def || n.kind == abstract_method {
            let is_static = tree.children(i).any(|c| tree.kind(c) == static_k);
            let label = if n.kind == abstract_method {
                "AbstractMethod"
            } else if is_static {
                "StaticMethod"
            } else {
                "Method"
            };
            deftype = lang.syms.get(label);
            rettype = tree
                .child_by_field(i, return_type_f)
                .map(|r| tree.sym(r))
                .unwrap_or(0);
        }

        if n.kind == var_declarator {
            deftype = lang.syms.get("Variable");
        }

        if deftype != 0 {
            tree.append(
                i,
                Node {
                    kind: k_deftype,
                    flags: NAMED | SYNTH,
                    sym: deftype,
                    size: 1,
                    ..Default::default()
                },
            );
            if rettype != 0 {
                tree.append(
                    i,
                    Node {
                        kind: k_rettype,
                        flags: NAMED | SYNTH,
                        sym: rettype,
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
