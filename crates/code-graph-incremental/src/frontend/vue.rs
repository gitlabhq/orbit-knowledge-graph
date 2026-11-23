//! Reads a Vue single-file component: each `<script>` block, with its
//! language, for the script's own frontend to parse in place.

use std::sync::LazyLock;

use orbit_utils::markdown::LineIndex;

use crate::intern::Lang;
use crate::tree::{Node, Tree};

static SCRIPT: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?s)<script\b([^>]*)>(.*?)</script>").expect("a valid pattern")
});

static LANG: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r#"\blang\s*=\s*["']?([A-Za-z]+)"#).expect("a valid pattern")
});

/// A component's tree: the document, and a `vue_script` node over each
/// script block's contents holding a `vue_lang` node that names its
/// language, `js` when the block names none.
pub fn parse(source: &str, lang: &Lang, label: &str) -> Tree {
    let lines = LineIndex::new(source);
    let node = |kind: u16, sym: u32, start: usize, end: usize| {
        let (start_row, start_col) = lines.position(start);
        let (end_row, end_col) = lines.end_position(source, end);
        Node {
            kind,
            named: true,
            sym,
            start: start as u32,
            end: end as u32,
            start_row: start_row as u32,
            start_col: start_col as u32,
            end_row: end_row as u32,
            end_col: end_col as u32,
            ..Default::default()
        }
    };
    let document = lang.intern_kind("vue_document");
    let mut tree = Tree::new(node(document, lang.syms.intern(label), 0, source.len()));
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);
    for block in SCRIPT.captures_iter(source) {
        let (Some(attrs), Some(body)) = (block.get(1), block.get(2)) else {
            continue;
        };
        let name = LANG
            .captures(attrs.as_str())
            .and_then(|c| c.get(1))
            .map_or("js", |m| m.as_str());
        let (start, end) = (body.start(), body.end());
        let root = tree.root;
        let script = tree.append(root, node(lang.intern_kind("vue_script"), 0, start, end));
        let name = lang.syms.intern(name);
        tree.append(
            script,
            node(lang.intern_kind("vue_lang"), name, start, start),
        );
    }
    tree
}
