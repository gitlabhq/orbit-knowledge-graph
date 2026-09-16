use crate::intern::Lang;

use super::walk::Cursor;

pub fn pretty_print(tree: &super::Tree, lang: &Lang, color: bool) -> String {
    use termtree::Tree as TTree;

    const BOLD_CYAN: &str = "\x1b[1;36m";
    const DIM: &str = "\x1b[2m";
    const RESET: &str = "\x1b[0m";
    const GREEN: &str = "\x1b[32m";

    fn build(cursor: Cursor, lang: &Lang, color: bool) -> TTree<String> {
        let kind = lang.kind_name(cursor.kind());
        let is_canonical = kind.starts_with("__");
        let field_prefix = if cursor.field() != 0 {
            let f = lang.field_name(cursor.field());
            if color {
                format!("{DIM}{f}:{RESET}")
            } else {
                format!("{f}:")
            }
        } else {
            String::new()
        };
        let sym_suffix = if cursor.sym() != 0 {
            let s = lang.syms.resolve(cursor.sym());
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
        for child in cursor.children() {
            tt.push(build(child, lang, color));
        }
        tt
    }

    if tree.len() == 0 {
        return String::from("(empty)");
    }
    build(tree.root(), lang, color).to_string()
}
