//! Domain-specific metadata extraction built on `treesitter_visit::extract`.

use treesitter_visit::Node;
use treesitter_visit::syntax_tree::SyntaxTree;

type N<'a> = Node<'a, SyntaxTree>;

pub use treesitter_visit::extract::{
    Extract, child_of_kind, default_name, descendant, field, field_chain, name_or_ident,
    no_extract, text,
};

/// Single-value fields use `Extract`. Multi-value fields use `fn(&Node) -> Vec<String>`.
pub struct MetadataRule {
    pub super_types: Option<fn(&N<'_>) -> Vec<String>>,
    pub return_type: Option<Extract>,
    pub type_annotation: Option<Extract>,
    pub receiver_type: Option<Extract>,
    pub decorators: Option<fn(&N<'_>) -> Vec<String>>,
    pub companion_of: Option<Extract>,
}

impl Default for MetadataRule {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataRule {
    pub fn new() -> Self {
        Self {
            super_types: None,
            return_type: None,
            type_annotation: None,
            receiver_type: None,
            decorators: None,
            companion_of: None,
        }
    }

    pub fn super_types(mut self, f: fn(&N<'_>) -> Vec<String>) -> Self {
        self.super_types = Some(f);
        self
    }

    /// Read supertypes from the `__supertype` virtual children that a rewrite
    /// rule inserted. The common case for every language whose `rewrite()`
    /// emits `__supertype` nodes.
    pub fn supertypes(self) -> Self {
        self.super_types(supertype_children)
    }
    pub fn return_type(mut self, extract: Extract) -> Self {
        self.return_type = Some(extract);
        self
    }
    pub fn type_annotation(mut self, extract: Extract) -> Self {
        self.type_annotation = Some(extract);
        self
    }
    pub fn receiver_type(mut self, extract: Extract) -> Self {
        self.receiver_type = Some(extract);
        self
    }
    pub fn decorators(mut self, f: fn(&N<'_>) -> Vec<String>) -> Self {
        self.decorators = Some(f);
        self
    }
    pub fn companion_of(mut self, extract: Extract) -> Self {
        self.companion_of = Some(extract);
        self
    }

    /// The `resolve` closure transforms bare type names into FQNs using tree context + import map.
    pub fn extract_metadata(
        &self,
        node: &N<'_>,
        resolve: impl Fn(String, &N<'_>) -> String,
    ) -> Option<Box<crate::v2::types::DefinitionMetadata>> {
        let super_types: Vec<String> = self
            .super_types
            .map(|f| f(node).into_iter().map(|s| resolve(s, node)).collect())
            .unwrap_or_default();
        let return_type = self
            .return_type
            .as_ref()
            .and_then(|e| e.apply_with(node, &resolve));
        let type_annotation = self
            .type_annotation
            .as_ref()
            .and_then(|e| e.apply_with(node, &resolve));
        // receiver_type uses bare apply (no import resolution) so it
        // stores the source-level type name. The lookup_by_receiver_type
        // matches against bare_type extracted from the chain's FQN.
        let receiver_type = self.receiver_type.as_ref().and_then(|e| e.apply(node));
        let decorators = self.decorators.map(|f| f(node)).unwrap_or_default();
        let companion_of = self.companion_of.as_ref().and_then(|e| e.apply(node));

        let has_data = !super_types.is_empty()
            || return_type.is_some()
            || type_annotation.is_some()
            || receiver_type.is_some()
            || !decorators.is_empty()
            || companion_of.is_some();

        if !has_data {
            return None;
        }

        Some(Box::new(crate::v2::types::DefinitionMetadata {
            super_types,
            return_type,
            type_annotation,
            receiver_type,
            decorators,
            companion_of,
            is_exported: false,
        }))
    }
}

pub fn metadata() -> MetadataRule {
    MetadataRule::new()
}

/// Text of every `__supertype` virtual child a rewrite rule inserted.
pub fn supertype_children(node: &N<'_>) -> Vec<String> {
    virtual_children(node, "__supertype")
}

/// Text of every `__decorator` virtual child a rewrite rule inserted.
pub fn decorator_children(node: &N<'_>) -> Vec<String> {
    virtual_children(node, "__decorator")
}

/// Text of every direct child of the given kind.
pub fn virtual_children(node: &N<'_>, kind: &str) -> Vec<String> {
    node.children()
        .filter(|c| c.kind().as_ref() == kind)
        .map(|c| c.text().to_string())
        .collect()
}
