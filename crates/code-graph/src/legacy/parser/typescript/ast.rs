use crate::legacy::parser::typescript::types::{TypeScriptFqn, TypeScriptNodeFqnMap};
use crate::utils::Range;

pub const TS_FQN_SEPARATOR: &str = "::";

/// Converts a TypeScript FQN to a string by joining node names with '::'
pub fn typescript_fqn_to_string(fqn: &TypeScriptFqn) -> String {
    if fqn.is_empty() {
        return String::new();
    }
    fqn.as_ref()
        .iter()
        .map(|part| part.node_name().to_string())
        .collect::<Vec<_>>()
        .join(TS_FQN_SEPARATOR)
}

/// Find TypeScript FQN with metadata
pub fn find_typescript_fqn_for_node<'a>(
    range: &Range,
    node_fqn_map: &TypeScriptNodeFqnMap<'a>,
) -> Option<TypeScriptFqn> {
    node_fqn_map
        .get(range)
        .map(|(_, fqn_parts)| fqn_parts.clone())
}
