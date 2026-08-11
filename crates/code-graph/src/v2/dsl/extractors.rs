use crate::v2::linker::state::{GraphDefMeta, StringPool};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

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
        pool: &StringPool,
        resolve: impl Fn(String, &N<'_>) -> String,
    ) -> Option<Box<GraphDefMeta>> {
        let super_types: smallvec::SmallVec<[gkg_utils::strings::StrId; 2]> = self
            .super_types
            .map(|f| {
                f(node)
                    .into_iter()
                    .map(|s| pool.alloc(&resolve(s, node)))
                    .collect()
            })
            .unwrap_or_default();
        let return_type = self
            .return_type
            .as_ref()
            .and_then(|e| e.apply_with(node, &resolve))
            .map(|s| pool.alloc(&s));
        let type_annotation = self
            .type_annotation
            .as_ref()
            .and_then(|e| e.apply_with(node, &resolve))
            .map(|s| pool.alloc(&s));
        let receiver_type = self
            .receiver_type
            .as_ref()
            .and_then(|e| e.apply(node))
            .map(|s| pool.alloc(&s));
        let decorators: smallvec::SmallVec<[gkg_utils::strings::StrId; 2]> = self
            .decorators
            .map(|f| f(node).into_iter().map(|s| pool.alloc(&s)).collect())
            .unwrap_or_default();
        let companion_of = self
            .companion_of
            .as_ref()
            .and_then(|e| e.apply(node))
            .map(|s| pool.alloc(&s));

        let has_data = !super_types.is_empty()
            || return_type.is_some()
            || type_annotation.is_some()
            || receiver_type.is_some()
            || !decorators.is_empty()
            || companion_of.is_some();

        if !has_data {
            return None;
        }

        Some(Box::new(GraphDefMeta {
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
