//! Tree traversal algorithms for tree-sitter nodes.
//!
//! This module provides efficient pre-order traversal using tree-sitter's cursor API
//! without extra heap allocation.

use tree_sitter as ts;

/// Pre-order (depth-first) traversal of tree-sitter nodes using a cursor.
///
/// Zero heap allocation. Each node is visited exactly once going down and at
/// most once going up (via `goto_parent`), giving O(N) amortized complexity
/// where N is the number of nodes in the subtree.
pub struct TsPre<'tree> {
    cursor: ts::TreeCursor<'tree>,
    current_depth: usize,
    done: bool,
}

impl<'tree> TsPre<'tree> {
    pub fn new(node: &ts::Node<'tree>) -> Self {
        Self {
            cursor: node.walk(),
            current_depth: 0,
            done: false,
        }
    }

    /// Get the current depth of traversal relative to the starting node.
    pub fn current_depth(&self) -> usize {
        self.current_depth
    }
}

impl<'tree> Iterator for TsPre<'tree> {
    type Item = ts::Node<'tree>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let node = self.cursor.node();

        // Try to descend into the first child.
        if self.cursor.goto_first_child() {
            self.current_depth += 1;
            return Some(node);
        }

        // No children — walk up until we find a sibling or return to start.
        loop {
            if self.cursor.goto_next_sibling() {
                return Some(node);
            }
            if self.current_depth == 0 {
                self.done = true;
                return Some(node);
            }
            self.current_depth -= 1;
            self.cursor.goto_parent();
        }
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
