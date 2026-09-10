use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::{DEAD, Lang, NAMED, NONE, SYNTH};
use tree_dsl::tree::{Node, Tree};

fn dump(tree: &Tree, lang: &Lang, filter: &str) {
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
        let line = format!("{indent}{pre}{kind}{sym}");
        if filter.is_empty() || line.contains(filter) || line.contains("__") {
            println!("{line}");
        }
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

fn main() {
    println!("========== RUST ==========");
    stamp_rust();
    println!("\n========== GO ==========");
    stamp_go();
    println!("\n========== PYTHON ==========");
    stamp_python();
    println!("\n========== RUBY ==========");
    stamp_ruby();
    println!("\n========== HASKELL ==========");
    stamp_haskell();
    println!("\n========== C ==========");
    stamp_c();
}

fn stamp_rust() {
    let src = std::fs::read_to_string("/tmp/test_vis.rs").unwrap();
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Rust, &mut lang, "test.rs");

    let vis_mod = lang.kinds.lookup("visibility_modifier") as u16;
    let k_visibility = lang.intern_kind("__visibility");
    let k_deftype = lang.intern_kind("__deftype");
    let struct_k = lang.kinds.lookup("struct_item") as u16;
    let func_k = lang.kinds.lookup("function_item") as u16;

    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == struct_k || k == func_k {
            // Check for visibility_modifier child
            let vis = tree
                .children(i)
                .find(|&c| tree.kind(c) == vis_mod)
                .map(|c| tree.sym(c));
            let vis_sym = match vis {
                Some(s) => s,
                None => lang.syms.intern("private"),
            };
            tree.append(i, leaf(k_visibility, vis_sym));
            let dt = if k == struct_k { "Struct" } else { "Function" };
            tree.append(i, leaf(k_deftype, lang.syms.intern(dt)));
        }
    }
    tree.compact();
    dump(&tree, &lang, "__");
}

fn stamp_go() {
    let src = std::fs::read_to_string("/tmp/test_vis.go").unwrap();
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Go, &mut lang, "test.go");

    let func_decl = lang.kinds.lookup("function_declaration") as u16;
    let type_spec = lang.kinds.lookup("type_spec") as u16;
    let name_f = lang.fields.lookup("name") as u16;
    let k_visibility = lang.intern_kind("__visibility");
    let k_deftype = lang.intern_kind("__deftype");

    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == func_decl || k == type_spec {
            let name_sym = tree
                .child_by_field(i, name_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let name_str = lang.syms.resolve(name_sym);
            let exported = name_str.chars().next().map_or(false, |c| c.is_uppercase());
            let vis_sym = if exported {
                lang.syms.intern("exported")
            } else {
                lang.syms.intern("unexported")
            };
            tree.append(i, leaf(k_visibility, vis_sym));
            let dt = if k == type_spec { "Struct" } else { "Function" };
            tree.append(i, leaf(k_deftype, lang.syms.intern(dt)));
        }
    }
    tree.compact();
    dump(&tree, &lang, "__");
}

fn stamp_python() {
    let src = std::fs::read_to_string("/tmp/test_vis.py").unwrap();
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Python, &mut lang, "test.py");

    let assign_k = lang.kinds.lookup("assignment") as u16;
    let class_k = lang.kinds.lookup("class_definition") as u16;
    let func_k = lang.kinds.lookup("function_definition") as u16;
    let string_content_k = lang.kinds.lookup("string_content") as u16;
    let left_f = lang.fields.lookup("left") as u16;
    let right_f = lang.fields.lookup("right") as u16;
    let name_f = lang.fields.lookup("name") as u16;
    let k_exports = lang.intern_kind("__exports");
    let k_name = lang.intern_kind("__name");
    let k_visibility = lang.intern_kind("__visibility");
    let k_deftype = lang.intern_kind("__deftype");

    let all_sym = lang.syms.intern("__all__");

    // Detect __all__ assignment, extract names, stamp on module root
    let mut exported_names: Vec<u32> = Vec::new();
    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == assign_k {
            let lhs = tree
                .child_by_field(i, left_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            if lhs == all_sym {
                // Extract string contents from the list
                if let Some(rhs) = tree.child_by_field(i, right_f) {
                    for d in tree.descendants(rhs) {
                        if tree.kind(d) == string_content_k {
                            exported_names.push(tree.sym(d));
                        }
                    }
                }
            }
        }
    }
    if !exported_names.is_empty() {
        let mut b = vec![Node {
            kind: k_exports,
            flags: NAMED | SYNTH,
            size: 0,
            parent: NONE,
            ..Default::default()
        }];
        for &n in &exported_names {
            b.push(Node {
                kind: k_name,
                flags: NAMED | SYNTH,
                sym: n,
                size: 1,
                parent: 0,
                ..Default::default()
            });
        }
        b[0].size = b.len() as u32;
        tree.append(0, b[0]); // append __exports to module root
        for n in &b[1..] {
            tree.append(0, *n);
        }
    }

    // Stamp defs with visibility based on _ prefix
    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == class_k || k == func_k {
            let name_sym = tree
                .child_by_field(i, name_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let name_str = lang.syms.resolve(name_sym);
            let vis = if name_str.starts_with('_') {
                "private"
            } else {
                "public"
            };
            tree.append(i, leaf(k_visibility, lang.syms.intern(vis)));
            let dt = if k == class_k { "Class" } else { "Function" };
            tree.append(i, leaf(k_deftype, lang.syms.intern(dt)));
        }
    }
    tree.compact();
    dump(&tree, &lang, "__");
}

fn stamp_ruby() {
    let src = std::fs::read_to_string("/tmp/test_vis.rb").unwrap();
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Ruby, &mut lang, "test.rb");

    let method_k = lang.kinds.lookup("method") as u16;
    let ident_k = lang.kinds.lookup("identifier") as u16;
    let body_stmt_k = lang.kinds.lookup("body_statement") as u16;
    let k_visibility = lang.intern_kind("__visibility");
    let k_deftype = lang.intern_kind("__deftype");

    let public_sym = lang.syms.intern("public");
    let private_sym = lang.syms.intern("private");
    let protected_sym = lang.syms.intern("protected");

    // Ruby visibility is stack-based: public/private/protected keywords affect all methods after them
    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k != body_stmt_k {
            continue;
        }

        let mut current_vis = public_sym; // Ruby default is public
        for c in tree.children(i) {
            let ck = tree.kind(c);
            if ck == ident_k {
                let s = tree.sym(c);
                if s == public_sym {
                    current_vis = public_sym;
                } else if s == private_sym {
                    current_vis = private_sym;
                } else if s == protected_sym {
                    current_vis = protected_sym;
                }
            }
            if ck == method_k {
                tree.append(c, leaf(k_visibility, current_vis));
                tree.append(c, leaf(k_deftype, lang.syms.intern("Method")));
            }
        }
    }
    tree.compact();
    dump(&tree, &lang, "__");
}

fn stamp_haskell() {
    let src = std::fs::read_to_string("/tmp/test_vis.hs").unwrap();
    let mut lang = Lang::new();
    let mut tree = grammar::parse(&src, SupportLang::Haskell, &mut lang, "test.hs");

    let header_k = lang.kinds.lookup("header") as u16;
    let exports_k = lang.kinds.lookup("exports") as u16;
    let export_k = lang.kinds.lookup("export") as u16;
    let data_type_k = lang.kinds.lookup("data_type") as u16;
    let function_k = lang.kinds.lookup("function") as u16;
    let signature_k = lang.kinds.lookup("signature") as u16;
    let exports_f = lang.fields.lookup("exports") as u16;
    let name_f = lang.fields.lookup("name") as u16;
    let variable_f = lang.fields.lookup("variable") as u16;
    let type_f = lang.fields.lookup("type") as u16;

    let k_exports_synth = lang.intern_kind("__exports");
    let k_name = lang.intern_kind("__name");
    let k_visibility = lang.intern_kind("__visibility");
    let k_deftype = lang.intern_kind("__deftype");

    // Extract export list from module header
    let mut exported_names: Vec<u32> = Vec::new();
    for i in 0..tree.nodes.len() as u32 {
        if tree.nodes[i as usize].kind == header_k {
            if let Some(exports) = tree.child_by_field(i, exports_f) {
                for c in tree.children(exports) {
                    if tree.kind(c) == export_k {
                        // Export can have variable: or type: field
                        let sym = tree
                            .child_by_field(c, variable_f)
                            .or_else(|| tree.child_by_field(c, type_f))
                            .map(|n| tree.sym(n))
                            .unwrap_or(0);
                        if sym != 0 {
                            exported_names.push(sym);
                        }
                    }
                }
            }
        }
    }

    // Stamp __exports on the module root
    if !exported_names.is_empty() {
        for &n in &exported_names {
            tree.append(0, leaf(k_name, n));
        }
        // Wrap later — for now just show them
    }

    // Stamp defs with visibility
    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == data_type_k {
            let name_sym = tree
                .child_by_field(i, name_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let vis = if exported_names.contains(&name_sym) {
                "exported"
            } else {
                "private"
            };
            tree.append(i, leaf(k_visibility, lang.syms.intern(vis)));
            tree.append(i, leaf(k_deftype, lang.syms.intern("DataType")));
        }
        if k == function_k || k == signature_k {
            let name_sym = tree
                .child_by_field(i, name_f)
                .map(|c| tree.sym(c))
                .unwrap_or(0);
            let vis = if exported_names.contains(&name_sym) {
                "exported"
            } else {
                "private"
            };
            tree.append(i, leaf(k_visibility, lang.syms.intern(vis)));
            if k == function_k {
                tree.append(i, leaf(k_deftype, lang.syms.intern("Function")));
            }
        }
    }
    tree.compact();
    dump(&tree, &lang, "__");
}

fn stamp_c() {
    let src = r#"
static void internal_func(void) {}
void public_func(void) {}
static int file_local = 42;
int global_var = 0;
"#;
    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::C, &mut lang, "test.c");

    let func_def = lang.kinds.lookup("function_definition") as u16;
    let decl_k = lang.kinds.lookup("declaration") as u16;
    let storage_class_k = lang.kinds.lookup("storage_class_specifier") as u16;
    let k_visibility = lang.intern_kind("__visibility");
    let k_deftype = lang.intern_kind("__deftype");

    let static_sym = lang.syms.intern("static");

    for i in 0..tree.nodes.len() as u32 {
        let k = tree.nodes[i as usize].kind;
        if k == func_def || k == decl_k {
            let is_static = tree
                .children(i)
                .any(|c| tree.kind(c) == storage_class_k && tree.sym(c) == static_sym);
            let vis = if is_static { "static" } else { "external" };
            tree.append(i, leaf(k_visibility, lang.syms.intern(vis)));
            let dt = if k == func_def {
                "Function"
            } else {
                "Variable"
            };
            tree.append(i, leaf(k_deftype, lang.syms.intern(dt)));
        }
    }
    tree.compact();
    dump(&tree, &lang, "__");
}
