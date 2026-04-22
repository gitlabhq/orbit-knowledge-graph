use crate::v2::types::ImportBindingKind;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

/// Resolve a bare or dotted type name to its FQN using import_map,
/// separator-based splitting, and module_prefix fallback.
///
/// Resolution order:
/// 1. Direct import_map lookup for the full name
/// 2. Split on separator, resolve first segment via imports, append rest
/// 3. Prepend module_prefix (same-package/module fallback)
/// 4. Return bare name unchanged
pub fn resolve_type_name(
    name: &str,
    import_map: &rustc_hash::FxHashMap<String, String>,
    module_prefix: Option<&str>,
    sep: &str,
) -> String {
    if let Some(fqn) = import_map.get(name) {
        return fqn.clone();
    }
    if name.contains(sep)
        && let Some((first, rest)) = name.split_once(sep)
        && let Some(fqn) = import_map.get(first)
    {
        return format!("{fqn}{sep}{rest}");
    }
    if let Some(prefix) = module_prefix {
        return format!("{prefix}{sep}{name}");
    }
    name.to_string()
}

pub fn infer_import_binding_kind(
    name: Option<&str>,
    alias: Option<&str>,
    wildcard: bool,
) -> ImportBindingKind {
    if wildcard {
        ImportBindingKind::Named
    } else if name.is_none() && alias.is_none() {
        ImportBindingKind::SideEffect
    } else {
        ImportBindingKind::Named
    }
}

pub fn canonical_range(r: &crate::utils::Range) -> crate::v2::types::Range {
    crate::v2::types::Range::new(
        crate::v2::types::Position::new(r.start.line, r.start.column),
        crate::v2::types::Position::new(r.end.line, r.end.column),
        r.byte_offset,
    )
}

/// Find the first identifier node in an expression tree (DFS).
/// Uses the language's `ident_kinds` from chain config to detect identifiers
/// generically across languages.
pub fn find_first_ident(node: &Node<StrDoc<SupportLang>>, ident_kinds: &[&str]) -> Option<String> {
    node.find_descendant(|n| n.is_named() && ident_kinds.contains(&n.kind().as_ref()))
        .map(|n| n.text().to_string())
}
