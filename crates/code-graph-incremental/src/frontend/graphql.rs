//! Reads a GraphQL document: JavaScript imports a `.graphql` file as its
//! default export, so the reader keeps only the named operations and
//! fragments the document declares.

use std::sync::LazyLock;

use orbit_utils::markdown::LineIndex;

use crate::intern::Lang;
use crate::tree::{Node, Tree};

static OPERATION: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r"(?m)^\s*(?:query|mutation|subscription|fragment)\s+([_A-Za-z][_0-9A-Za-z]*)",
    )
    .expect("a valid pattern")
});

/// A document's tree: a `graphql_document` node over the file holding a
/// `graphql_operation` node named by each operation or fragment.
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
    let document = lang.intern_kind("graphql_document");
    let mut tree = Tree::new(node(document, lang.syms.intern(label), 0, source.len()));
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);
    let operation = lang.intern_kind("graphql_operation");
    for found in OPERATION.captures_iter(source) {
        let Some(name) = found.get(1) else {
            continue;
        };
        let root = tree.root;
        let sym = lang.syms.intern(name.as_str());
        tree.append(root, node(operation, sym, name.start(), name.end()));
    }
    tree
}
