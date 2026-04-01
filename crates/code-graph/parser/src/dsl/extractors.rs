use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// How to extract a name from a tree-sitter node.
pub enum Extract {
    /// Look for a `name` field, then fall back to first identifier child.
    Default,
    /// Extract text from a specific field.
    Field(&'static str),
    /// Follow a chain of fields and extract the final node's text.
    FieldChain(&'static [&'static str]),
    /// C-style: follow `declarator` -> `declarator` chain to find the name.
    Declarator,
}

impl Extract {
    pub fn extract_name(&self, node: &N<'_>) -> Option<String> {
        match self {
            Extract::Default => default_name(node),
            Extract::Field(name) => node.field(name).map(|n| n.text().to_string()),
            Extract::FieldChain(fields) => {
                let mut current = node.clone();
                for f in *fields {
                    current = current.field(f)?;
                }
                Some(current.text().to_string())
            }
            Extract::Declarator => declarator_name(node),
        }
    }
}

fn default_name(node: &N<'_>) -> Option<String> {
    if let Some(name_node) = node.field("name") {
        return Some(name_node.text().to_string());
    }
    for child in node.children() {
        if child.is_named() && is_identifier_kind(&child.kind()) {
            return Some(child.text().to_string());
        }
    }
    None
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

fn declarator_name(node: &N<'_>) -> Option<String> {
    let declarator = node.field("declarator")?;
    if let Some(inner) = declarator.field("declarator") {
        return Some(inner.text().to_string());
    }
    if is_identifier_kind(&declarator.kind()) {
        return Some(declarator.text().to_string());
    }
    if let Some(name) = declarator.field("name") {
        return Some(name.text().to_string());
    }
    None
}

pub fn field(name: &'static str) -> Extract {
    Extract::Field(name)
}

pub fn field_chain(fields: &'static [&'static str]) -> Extract {
    Extract::FieldChain(fields)
}

pub fn declarator() -> Extract {
    Extract::Declarator
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::LanguageExt;

    #[test]
    fn test_default_extract() {
        let root = SupportLang::Python.ast_grep("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(
            Extract::Default.extract_name(&func),
            Some("foo".to_string())
        );
    }

    #[test]
    fn test_field_extract() {
        let root = SupportLang::Python.ast_grep("def bar(): pass");
        let func = root.root().children().next().unwrap();
        assert_eq!(field("name").extract_name(&func), Some("bar".to_string()));
    }
}
