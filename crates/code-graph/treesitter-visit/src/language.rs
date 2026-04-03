//! Language trait for tree-sitter languages.

/// Trait to abstract tree-sitter language usage.
pub trait Language: Clone + 'static {
    fn kind_to_id(&self, kind: &str) -> u16;
    fn field_to_id(&self, field: &str) -> Option<u16>;
}
