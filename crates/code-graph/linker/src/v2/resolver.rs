/// Language-agnostic reference resolver.
///
/// Resolves `CanonicalReference` entries against the full definition
/// index to produce call edges. Uses `ScopeIndex` for enclosing-scope
/// lookup.
///
/// Implementation deferred — will support local-first and global
/// backtracking resolution strategies.
pub struct ReferenceResolver;

impl ReferenceResolver {
    pub fn new() -> Self {
        Self
    }
}
