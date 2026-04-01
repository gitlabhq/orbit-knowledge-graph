use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::utils::{Range, node_to_range};

/// Extracts the name string from a tree-sitter node.
pub trait NameExtractor: Send + Sync {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String>;
}

/// Default: look for a `name` field child, then fall back to the first named child.
pub struct DefaultNameExtractor;

impl NameExtractor for DefaultNameExtractor {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        if let Some(name_node) = node.field("name") {
            return Some(name_node.text().to_string());
        }
        // Fallback: first named child that looks like an identifier
        for child in node.children() {
            if child.is_named() && is_identifier_kind(&child.kind()) {
                return Some(child.text().to_string());
            }
        }
        None
    }
}

fn is_identifier_kind(kind: &str) -> bool {
    matches!(
        kind,
        "identifier"
            | "type_identifier"
            | "simple_identifier"
            | "name"
            | "field_identifier"
            | "property_identifier"
    )
}

/// Extract the name from a specific field.
pub struct FieldNameExtractor {
    pub field_name: &'static str,
}

impl NameExtractor for FieldNameExtractor {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        node.field(self.field_name).map(|n| n.text().to_string())
    }
}

pub fn extract_from_field(field_name: &'static str) -> FieldNameExtractor {
    FieldNameExtractor { field_name }
}

/// Extract the name from a field on the **parent** node.
///
/// Each entry is `(parent_kind, field_name)`. The first matching parent is used.
pub struct ParentFieldNameExtractor {
    pub entries: Vec<(&'static str, &'static str)>,
}

impl NameExtractor for ParentFieldNameExtractor {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        let parent = node.parent()?;
        let parent_kind = parent.kind();
        for &(expected_kind, field_name) in &self.entries {
            if parent_kind == expected_kind
                && let Some(field_node) = parent.field(field_name)
            {
                return Some(field_node.text().to_string());
            }
        }
        None
    }
}

pub fn extract_from_parent_fields(
    entries: Vec<(&'static str, &'static str)>,
) -> ParentFieldNameExtractor {
    ParentFieldNameExtractor { entries }
}

/// Extract the name by looking at the `declarator` field, then the `name`
/// field inside it. Common in C for patterns like
/// `int my_func(int x) { ... }` where the tree is:
/// ```text
/// function_definition
///   type: primitive_type "int"
///   declarator: function_declarator
///     declarator: identifier "my_func"
///     parameters: ...
/// ```
pub struct DeclaratorNameExtractor;

impl NameExtractor for DeclaratorNameExtractor {
    fn extract_name(&self, node: &Node<StrDoc<SupportLang>>) -> Option<String> {
        let declarator = node.field("declarator")?;
        // function_declarator -> declarator (identifier)
        if let Some(inner) = declarator.field("declarator") {
            return Some(inner.text().to_string());
        }
        // Fallback: the declarator itself might be the identifier
        if is_identifier_kind(&declarator.kind()) {
            return Some(declarator.text().to_string());
        }
        // struct/enum: check for `name` field
        if let Some(name) = declarator.field("name") {
            return Some(name.text().to_string());
        }
        None
    }
}

pub fn extract_from_declarator() -> DeclaratorNameExtractor {
    DeclaratorNameExtractor
}

/// Extracts the range from a tree-sitter node.
pub trait RangeExtractor: Send + Sync {
    fn extract_range(&self, node: &Node<StrDoc<SupportLang>>) -> Range;
}

/// Default: use the node's own range.
pub struct DefaultRangeExtractor;

impl RangeExtractor for DefaultRangeExtractor {
    fn extract_range(&self, node: &Node<StrDoc<SupportLang>>) -> Range {
        node_to_range(node)
    }
}

/// Use the `name` field child's range instead of the whole node.
pub struct NameFieldRangeExtractor;

impl RangeExtractor for NameFieldRangeExtractor {
    fn extract_range(&self, node: &Node<StrDoc<SupportLang>>) -> Range {
        if let Some(name_node) = node.field("name") {
            node_to_range(&name_node)
        } else {
            node_to_range(node)
        }
    }
}

pub fn range_from_name_field() -> NameFieldRangeExtractor {
    NameFieldRangeExtractor
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::LanguageExt;

    #[test]
    fn test_default_name_extractor() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();

        let extractor = DefaultNameExtractor;
        assert_eq!(extractor.extract_name(&func), Some("foo".to_string()));
    }

    #[test]
    fn test_field_name_extractor() {
        let root = SupportLang::Python.ast_grep("def bar(): pass");
        let func = root.root().children().next().unwrap();

        let extractor = extract_from_field("name");
        assert_eq!(extractor.extract_name(&func), Some("bar".to_string()));
    }
}
