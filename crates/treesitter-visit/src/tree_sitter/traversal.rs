//! Tree traversal algorithms for tree-sitter nodes.
//!
//! This module provides efficient pre-order traversal using tree-sitter's cursor API
//! without extra heap allocation.

use tree_sitter as ts;

/// Represents a pre-order traversal of tree-sitter nodes.
pub struct TsPre<'tree> {
    cursor: ts::TreeCursor<'tree>,
    start_id: Option<usize>,
    current_depth: usize,
}

impl<'tree> TsPre<'tree> {
    pub fn new(node: &ts::Node<'tree>) -> Self {
        Self {
            cursor: node.walk(),
            start_id: Some(node.id()),
            current_depth: 0,
        }
    }

    fn step_down(&mut self) -> bool {
        if self.cursor.goto_first_child() {
            self.current_depth += 1;
            true
        } else {
            false
        }
    }

    fn trace_up(&mut self, start: usize) {
        let cursor = &mut self.cursor;
        while cursor.node().id() != start {
            if cursor.goto_next_sibling() {
                return;
            }
            self.current_depth -= 1;
            if !cursor.goto_parent() {
                break;
            }
        }
        self.start_id = None;
    }

    /// Get the current depth of traversal
    pub fn current_depth(&self) -> usize {
        self.current_depth
    }
}

/// Amortized time complexity is O(NlgN), depending on branching factor.
impl<'tree> Iterator for TsPre<'tree> {
    type Item = ts::Node<'tree>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.start_id?;
        let cursor = &mut self.cursor;
        let inner = cursor.node();
        let ret = Some(inner);

        if self.step_down() {
            return ret;
        }

        self.trace_up(start);
        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tree-sitter-typescript")]
    fn parse_typescript(src: &str) -> ts::Tree {
        let mut parser = ts::Parser::new();
        let language: ts::Language = tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into();
        parser.set_language(&language).unwrap();
        parser.parse(src, None).unwrap()
    }

    #[test]
    #[cfg(feature = "tree-sitter-typescript")]
    fn test_pre_order_traversal() {
        let tree = parse_typescript("let a = 1");
        let root = tree.root_node();

        let nodes: Vec<_> = TsPre::new(&root).map(|n| n.kind()).collect();
        assert!(!nodes.is_empty());
        assert_eq!(nodes[0], "program");
    }

    #[test]
    #[cfg(feature = "tree-sitter-typescript")]
    fn test_fused_traversal() {
        let tree = parse_typescript("let a = 1");
        let root = tree.root_node();

        let mut pre = TsPre::new(&root);
        while pre.next().is_some() {}
        assert!(pre.next().is_none());
        assert!(pre.next().is_none());
    }
}
