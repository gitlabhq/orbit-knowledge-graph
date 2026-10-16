use crate::intern::Lang;

use super::Cursor;

const BOLD_CYAN: &str = "\x1b[1;36m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";
const GREEN: &str = "\x1b[32m";
const SYMBOL_BYTES: usize = 50;

pub fn pretty_print<S: super::Storage<Node = super::Node>>(
    tree: &super::Tree<S>,
    lang: &Lang,
    color: bool,
) -> String {
    if tree.is_empty() {
        return String::from("(empty)");
    }
    let root = tree.root();
    let mut out = label(root, lang, color);
    out.push('\n');
    let mut indent = String::new();
    let mut stack = Vec::new();
    push_children(&mut stack, root, 0);
    while let Some((node, last, depth)) = stack.pop() {
        indent.truncate(depth);
        out.push_str(&indent);
        out.push_str(if last { "└── " } else { "├── " });
        out.push_str(&label(node, lang, color));
        out.push('\n');
        indent.push_str(if last { "    " } else { "│   " });
        push_children(&mut stack, node, indent.len());
    }
    out
}

fn push_children<'t, S: super::Storage<Node = super::Node>>(
    stack: &mut Vec<(Cursor<'t, S>, bool, usize)>,
    node: Cursor<'t, S>,
    depth: usize,
) {
    let children: Vec<_> = node.children().collect();
    let count = children.len();
    stack.extend(
        children
            .into_iter()
            .enumerate()
            .rev()
            .map(|(i, child)| (child, i + 1 == count, depth)),
    );
}

fn label<S: super::Storage<Node = super::Node>>(
    cursor: Cursor<'_, S>,
    lang: &Lang,
    color: bool,
) -> String {
    let kind = lang.kind_name(cursor.kind());
    let field_prefix = match (cursor.field(), color) {
        (0, _) => String::new(),
        (field, true) => format!("{DIM}{}:{RESET}", lang.field_name(field)),
        (field, false) => format!("{}:", lang.field_name(field)),
    };
    let sym_suffix = match cursor.sym() {
        0 => String::new(),
        sym => {
            let s = lang.syms.resolve(sym);
            let shown = match s.len() > SYMBOL_BYTES {
                true => format!("{:?}...", &s[..s.floor_char_boundary(SYMBOL_BYTES)]),
                false => format!("{s:?}"),
            };
            match color {
                true => format!(" {GREEN}{shown}{RESET}"),
                false => format!(" {shown}"),
            }
        }
    };
    let kind = match (color, kind.starts_with("__")) {
        (true, true) => format!("{BOLD_CYAN}{kind}{RESET}"),
        (true, false) => format!("{DIM}{kind}{RESET}"),
        (false, _) => kind.to_string(),
    };
    format!("{field_prefix}{kind}{sym_suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::treesitter::{SupportLang, parse};

    fn termtree_print<S: super::super::Storage<Node = super::super::Node>>(
        cursor: Cursor<'_, S>,
        lang: &Lang,
        color: bool,
    ) -> termtree::Tree<String> {
        let mut tree = termtree::Tree::new(label(cursor, lang, color));
        for child in cursor.children() {
            tree.push(termtree_print(child, lang, color));
        }
        tree
    }

    #[test]
    fn a_small_tree_prints_as_the_termtree_printer_did() {
        let lang = Lang::default();
        let source = "class A:\n    def f(self, x):\n        return g(x, [1, 2])\n";
        let tree = parse(source, SupportLang::Python, &lang, "a.py").unwrap();
        for color in [false, true] {
            assert_eq!(
                pretty_print(&tree, &lang, color),
                termtree_print(tree.root(), &lang, color).to_string()
            );
        }
    }

    #[test]
    fn a_symbol_with_a_multibyte_character_at_the_cut_prints() {
        let lang = Lang::default();
        let path = format!("{}é{}.py", "a".repeat(49), "b".repeat(10));
        let tree = parse("x = 1\n", SupportLang::Python, &lang, &path).unwrap();
        let printed = pretty_print(&tree, &lang, false);
        assert!(printed.starts_with(&format!("module {:?}...", "a".repeat(49))));
    }

    #[test]
    fn a_deep_tree_prints_on_a_small_stack() {
        let lang = Lang::default();
        let levels = 5_000;
        let source = format!("x = {}1{}\n", "[".repeat(levels), "]".repeat(levels));
        let tree = parse(&source, SupportLang::Python, &lang, "deep.py").unwrap();
        let lines = std::thread::scope(|scope| {
            std::thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn_scoped(scope, || pretty_print(&tree, &lang, false).lines().count())
                .unwrap()
                .join()
                .unwrap()
        });
        assert_eq!(lines, tree.len() as usize);
    }
}
