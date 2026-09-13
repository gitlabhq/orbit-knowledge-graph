use super::types::Tree;

pub fn pretty_print(tree: &Tree, lang: &crate::lang::Lang, color: bool) -> String {
    use termtree::Tree as TTree;

    const BOLD_CYAN: &str = "\x1b[1;36m";
    const DIM: &str = "\x1b[2m";
    const RESET: &str = "\x1b[0m";
    const GREEN: &str = "\x1b[32m";

    fn build(tree: &Tree, lang: &crate::lang::Lang, idx: u32, color: bool) -> TTree<String> {
        let n = &tree.nodes[idx as usize];
        let kind = lang.kind_name(n.kind);
        let is_canonical = kind.starts_with("__");
        let field_prefix = if n.field != 0 {
            let f = lang.field_name(n.field);
            if color {
                format!("{DIM}{f}:{RESET}")
            } else {
                format!("{f}:")
            }
        } else {
            String::new()
        };
        let sym_suffix = if n.sym != 0 {
            let s = lang.syms.resolve(n.sym);
            let truncated = if s.len() > 50 {
                format!("{:?}...", &s[..50])
            } else {
                format!("{s:?}")
            };
            if color {
                format!(" {GREEN}{truncated}{RESET}")
            } else {
                format!(" {truncated}")
            }
        } else {
            String::new()
        };
        let kind_str = if color {
            if is_canonical {
                format!("{BOLD_CYAN}{kind}{RESET}")
            } else {
                format!("{DIM}{kind}{RESET}")
            }
        } else {
            kind.to_string()
        };
        let label = format!("{field_prefix}{kind_str}{sym_suffix}");
        let mut tt = TTree::new(label);
        for c in tree.children(idx) {
            if !tree.nodes[c as usize].dead {
                tt.push(build(tree, lang, c, color));
            }
        }
        tt
    }

    if tree.nodes.is_empty() {
        return String::from("(empty)");
    }
    build(tree, lang, 0, color).to_string()
}
