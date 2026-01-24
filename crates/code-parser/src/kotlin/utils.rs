use crate::kotlin::types::AstNode;

/// Get a child node by following a path of node kinds
/// The path is specified as a comma-separated string of node kinds
pub(in crate::kotlin) fn get_child_by_kind<'a>(
    node: &AstNode<'a>,
    node_kind: &str,
) -> Option<AstNode<'a>> {
    node.children()
        .find(|child| child.kind().as_ref() == node_kind)
}

/// Get a child node by any of the given node kinds
pub(in crate::kotlin) fn get_child_by_any_kind<'a>(
    node: &AstNode<'a>,
    node_kinds: &[&str],
) -> Option<AstNode<'a>> {
    node.children()
        .find(|child| node_kinds.contains(&child.kind().as_ref()))
}

pub(in crate::kotlin) fn get_children_by_kind<'a>(
    node: &AstNode<'a>,
    node_kind: &str,
) -> Vec<AstNode<'a>> {
    node.children()
        .filter(|child| child.kind().as_ref() == node_kind)
        .collect()
}
