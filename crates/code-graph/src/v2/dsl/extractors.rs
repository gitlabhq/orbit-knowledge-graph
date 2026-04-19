//! Domain-specific metadata extraction built on `treesitter_visit::extract`.
//!
//! Re-exports the core `Extract` type and constructors from treesitter-visit.
//! Adds `MetadataRule` (scope metadata), `ExtractList` (multi-value),
//! and `resolve_type_via_map` (import-map FQN resolution).

use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Match, Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

// Re-export Extract type and constructors for lang specs
pub use treesitter_visit::extract::{
    Extract, child_of_kind, default_name, descendant, field, field_chain, name_or_ident,
    no_extract, text,
};

// ── Multi-value extraction ──────────────────────────────────────

/// How to extract a list of strings from a tree-sitter node.
/// Used for super_types, decorators, etc.
pub enum ExtractList {
    /// Extract all children of a specific kind from a named field.
    ChildrenOfField(&'static str, &'static [&'static str]),
    /// Extract text of all children matching these kinds directly on the node.
    ChildrenOfKind(&'static [&'static str]),
    /// Extract text from a named field, then split on a separator.
    FieldSplit(&'static str, &'static str),
    /// Walk up to parent, collect children of this kind.
    Decorators(&'static str),
    /// Custom function for complex extraction.
    Fn(fn(&N<'_>) -> Vec<String>),
}

impl ExtractList {
    pub fn extract(&self, node: &N<'_>) -> Vec<String> {
        match self {
            ExtractList::ChildrenOfField(field_name, kinds) => {
                let Some(field_node) = node.field(field_name) else {
                    return vec![];
                };
                field_node
                    .children_matching(Match::AnyKind(kinds))
                    .map(|c| c.text().to_string())
                    .collect()
            }
            ExtractList::ChildrenOfKind(kinds) => node
                .children_matching(Match::AnyKind(kinds))
                .map(|c| c.text().to_string())
                .collect(),
            ExtractList::FieldSplit(field_name, sep) => {
                let Some(field_node) = node.field(field_name) else {
                    return vec![];
                };
                field_node
                    .text()
                    .split(sep)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
            ExtractList::Decorators(decorator_kind) => {
                if let Some(parent) = node.find(
                    treesitter_visit::Axis::Parent,
                    Match::Kind("decorated_definition"),
                ) {
                    parent
                        .children_matching(Match::Kind(decorator_kind))
                        .map(|c| c.text().trim_start_matches('@').trim().to_string())
                        .collect()
                } else {
                    vec![]
                }
            }
            ExtractList::Fn(f) => f(node),
        }
    }
}

// ── Metadata extraction ─────────────────────────────────────────

/// Declarative metadata extraction rules for a scope definition.
pub struct MetadataRule {
    pub super_types: Option<ExtractList>,
    pub return_type: Option<Extract>,
    pub type_annotation: Option<Extract>,
    pub receiver_type: Option<Extract>,
    pub decorators: Option<ExtractList>,
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

    pub fn super_types(mut self, extract: ExtractList) -> Self {
        self.super_types = Some(extract);
        self
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
    pub fn decorators(mut self, extract: ExtractList) -> Self {
        self.decorators = Some(extract);
        self
    }
    pub fn companion_of(mut self, extract: Extract) -> Self {
        self.companion_of = Some(extract);
        self
    }

    /// Extract metadata from a node. Type names are resolved against
    /// the file's imports to produce FQNs where possible.
    pub fn extract_metadata(
        &self,
        node: &N<'_>,
        import_map: &rustc_hash::FxHashMap<String, String>,
        sep: &'static str,
    ) -> Option<Box<crate::v2::types::DefinitionMetadata>> {
        let super_types: Vec<String> = self
            .super_types
            .as_ref()
            .map(|e| {
                e.extract(node)
                    .into_iter()
                    .map(|s| resolve_type_via_map(&s, import_map, sep))
                    .collect()
            })
            .unwrap_or_default();
        let return_type = self
            .return_type
            .as_ref()
            .and_then(|e| e.apply(node))
            .map(|s| resolve_type_via_map(&s, import_map, sep));
        let type_annotation = self
            .type_annotation
            .as_ref()
            .and_then(|e| e.apply(node))
            .map(|s| resolve_type_via_map(&s, import_map, sep));
        let receiver_type = self
            .receiver_type
            .as_ref()
            .and_then(|e| e.apply(node))
            .map(|s| resolve_type_via_map(&s, import_map, sep));
        let decorators = self
            .decorators
            .as_ref()
            .map(|e| e.extract(node))
            .unwrap_or_default();
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
        }))
    }
}

pub fn metadata() -> MetadataRule {
    MetadataRule::new()
}

/// Resolve a bare type name to a full FQN using the pre-built import map.
pub fn resolve_type_via_map(
    bare_name: &str,
    import_map: &rustc_hash::FxHashMap<String, String>,
    _sep: &str,
) -> String {
    import_map
        .get(bare_name)
        .cloned()
        .unwrap_or_else(|| bare_name.to_string())
}
