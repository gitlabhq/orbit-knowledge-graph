use std::path::Path;
use treesitter_visit::{LanguageExt, SupportLang};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: ast <file>");
        std::process::exit(1);
    }
    let path = Path::new(&args[1]);
    let code = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("failed to read {}: {e}", path.display());
        std::process::exit(1);
    });
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let lang = match ext {
        "c" | "h" => SupportLang::C,
        "cpp" | "cc" | "cxx" | "hpp" | "hh" | "hxx" => SupportLang::Cpp,
        "py" => SupportLang::Python,
        "rb" => SupportLang::Ruby,
        "rs" => SupportLang::Rust,
        "java" => SupportLang::Java,
        "kt" | "kts" => SupportLang::Kotlin,
        "go" => SupportLang::Go,
        "cs" => SupportLang::CSharp,
        "ts" | "tsx" => SupportLang::TypeScript,
        "js" | "jsx" => SupportLang::JavaScript,
        "php" | "phtml" => SupportLang::Php,
        "tf" | "tfvars" | "hcl" => SupportLang::Hcl,
        _ => {
            eprintln!("unknown extension: {ext}");
            std::process::exit(1);
        }
    };

    let ts_lang = lang.get_ts_language();
    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&ts_lang).expect("set language");
    let tree = parser.parse(&code, None).expect("parse");
    print_node(tree.root_node(), &code, 0);
}

fn print_node(node: tree_sitter::Node, src: &str, depth: usize) {
    let indent = "  ".repeat(depth);
    let text = if node.child_count() == 0 {
        format!(" {:?}", &src[node.byte_range()])
    } else {
        String::new()
    };
    if node.is_named() {
        println!(
            "{indent}{} [{}:{}]{text}",
            node.kind(),
            node.start_position().row,
            node.start_position().column
        );
    }
    for i in 0..node.child_count() as u32 {
        if let Some(child) = node.child(i) {
            if !child.is_named() {
                continue;
            }
            let field = node.field_name_for_child(i).unwrap_or("");
            if !field.is_empty() {
                print!("{indent}  {field}: ");
            }
            print_node(child, src, depth + 1);
        }
    }
}
