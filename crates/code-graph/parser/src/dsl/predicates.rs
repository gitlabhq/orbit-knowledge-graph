use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// A predicate evaluates a tree-sitter node and returns `true` if the node
/// matches the condition.
///
/// Predicates are composable via `.and()`, `.or()`, and `.not()`.
pub trait Predicate: Send + Sync {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool;

    fn and(self, other: impl Predicate + 'static) -> And
    where
        Self: Sized + 'static,
    {
        And(Box::new(self), Box::new(other))
    }

    fn or(self, other: impl Predicate + 'static) -> Or
    where
        Self: Sized + 'static,
    {
        Or(Box::new(self), Box::new(other))
    }

    fn not(self) -> Not
    where
        Self: Sized + 'static,
    {
        Not(Box::new(self))
    }
}

pub struct And(Box<dyn Predicate>, Box<dyn Predicate>);

impl Predicate for And {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        self.0.test(node) && self.1.test(node)
    }
}

pub struct Or(Box<dyn Predicate>, Box<dyn Predicate>);

impl Predicate for Or {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        self.0.test(node) || self.1.test(node)
    }
}

pub struct Not(Box<dyn Predicate>);

impl Predicate for Not {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        !self.0.test(node)
    }
}

/// Matches if the node's kind is one of the given strings.
pub struct KindAny {
    kinds: Vec<&'static str>,
}

impl Predicate for KindAny {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        let kind = node.kind();
        self.kinds.iter().any(|k| *k == kind)
    }
}

/// Shorthand constructor.
pub fn kind_any(kinds: Vec<&'static str>) -> KindAny {
    KindAny { kinds }
}

/// Matches if the node's kind equals the given string.
pub fn kind_eq(kind: &'static str) -> KindAny {
    KindAny { kinds: vec![kind] }
}

/// Matches if the node's **parent** has one of the given kinds.
pub struct ParentKindAny {
    kinds: Vec<&'static str>,
}

impl Predicate for ParentKindAny {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        if let Some(parent) = node.parent() {
            let kind = parent.kind();
            self.kinds.iter().any(|k| *k == kind)
        } else {
            false
        }
    }
}

pub fn parent_kind_any(kinds: Vec<&'static str>) -> ParentKindAny {
    ParentKindAny { kinds }
}

pub fn parent_kind(kind: &'static str) -> ParentKindAny {
    ParentKindAny { kinds: vec![kind] }
}

/// Matches if the node's **grandparent** has one of the given kinds.
pub struct GrandparentKindAny {
    kinds: Vec<&'static str>,
}

impl Predicate for GrandparentKindAny {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        if let Some(parent) = node.parent()
            && let Some(grandparent) = parent.parent()
        {
            let kind = grandparent.kind();
            return self.kinds.iter().any(|k| *k == kind);
        }
        false
    }
}

pub fn grandparent_kind_any(kinds: Vec<&'static str>) -> GrandparentKindAny {
    GrandparentKindAny { kinds }
}

pub fn grandparent_kind(kind: &'static str) -> GrandparentKindAny {
    GrandparentKindAny { kinds: vec![kind] }
}

/// Matches if any ancestor (parent, grandparent, ...) has one of the given kinds.
pub struct AncestorKindAny {
    kinds: Vec<&'static str>,
}

impl Predicate for AncestorKindAny {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        let mut current = node.parent();
        while let Some(ancestor) = current {
            let kind = ancestor.kind();
            if self.kinds.iter().any(|k| *k == kind) {
                return true;
            }
            current = ancestor.parent();
        }
        false
    }
}

pub fn ancestor_kind_any(kinds: Vec<&'static str>) -> AncestorKindAny {
    AncestorKindAny { kinds }
}

/// Matches if the node has a child with one of the given kinds.
pub struct HasChildKind {
    kinds: Vec<&'static str>,
}

impl Predicate for HasChildKind {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        node.children().any(|child| {
            let kind = child.kind();
            self.kinds.iter().any(|k| *k == kind)
        })
    }
}

pub fn has_child_kind(kinds: Vec<&'static str>) -> HasChildKind {
    HasChildKind { kinds }
}

/// Matches if the node has a field child whose kind is one of the given kinds.
pub struct HasFieldWithKind {
    field_name: &'static str,
    kinds: Vec<&'static str>,
}

impl Predicate for HasFieldWithKind {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        if let Some(field_node) = node.field(self.field_name) {
            let kind = field_node.kind();
            self.kinds.iter().any(|k| *k == kind)
        } else {
            false
        }
    }
}

pub fn has_field_with_kind(field_name: &'static str, kinds: Vec<&'static str>) -> HasFieldWithKind {
    HasFieldWithKind { field_name, kinds }
}

/// Matches if the parent node has one of the given kinds AND the current node
/// is accessed via one of the specified field names on that parent.
pub struct ParentFieldAny {
    entries: Vec<(&'static str, &'static str)>,
}

impl Predicate for ParentFieldAny {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        let Some(parent) = node.parent() else {
            return false;
        };
        let parent_kind = parent.kind();
        for &(expected_parent_kind, field_name) in &self.entries {
            if parent_kind == expected_parent_kind
                && let Some(field_node) = parent.field(field_name)
                && field_node.node_id() == node.node_id()
            {
                return true;
            }
        }
        false
    }
}

/// Each entry is `(parent_kind, field_name)`.
pub fn parent_field_any(entries: Vec<(&'static str, &'static str)>) -> ParentFieldAny {
    ParentFieldAny { entries }
}

/// Matches if the node has a `name` field (i.e. its name can be extracted).
pub struct HasNameField;

impl Predicate for HasNameField {
    fn test(&self, node: &Node<StrDoc<SupportLang>>) -> bool {
        node.field("name").is_some()
    }
}

pub fn has_name_field() -> HasNameField {
    HasNameField
}

/// Always returns true.
pub struct Always;

impl Predicate for Always {
    fn test(&self, _node: &Node<StrDoc<SupportLang>>) -> bool {
        true
    }
}

pub fn always() -> Always {
    Always
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::LanguageExt;

    fn parse_python(code: &str) -> treesitter_visit::Root<StrDoc<SupportLang>> {
        SupportLang::Python.ast_grep(code)
    }

    #[test]
    fn test_kind_any() {
        let root = parse_python("def foo(): pass");
        let node = root.root();

        let pred = kind_eq("module");
        assert!(pred.test(&node));

        let pred = kind_eq("class_definition");
        assert!(!pred.test(&node));
    }

    #[test]
    fn test_combinators() {
        let root = parse_python("def foo(): pass");
        let func_node = root.root().children().next().unwrap();

        let pred = kind_eq("function_definition").and(has_name_field());
        assert!(pred.test(&func_node));

        let pred = kind_eq("class_definition").or(kind_eq("function_definition"));
        assert!(pred.test(&func_node));

        let pred = kind_eq("function_definition").not();
        assert!(!pred.test(&func_node));
    }
}
