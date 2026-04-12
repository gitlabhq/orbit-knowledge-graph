use code_graph_types::{CanonicalFqn, DefKind, FqnPart, Language, Range, ToCanonical};
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::fqn::FQNPart;
use smallvec::SmallVec;
use std::hash::Hash;

use super::types::DefinitionNode;
use internment::ArcIntern;

/// Convert any FQNPart-based FQN to CanonicalFqn.
pub fn fqn_parts_to_canonical<T, M>(parts: &[FQNPart<T, M>], lang: Language) -> CanonicalFqn
where
    T: DefinitionTypeInfo + Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    let canonical_parts: SmallVec<[FqnPart; 8]> = parts
        .iter()
        .map(|part| FqnPart {
            part_type: part.node_type().as_str(),
            name: part.node_name().to_string(),
            range: convert_range(part.range()),
        })
        .collect();
    CanonicalFqn::new(canonical_parts, lang.fqn_separator())
}

/// Convert a simple string-based FQN (like Fqn<String>) to CanonicalFqn.
pub fn string_fqn_to_canonical(parts: &[String], lang: Language) -> CanonicalFqn {
    let canonical_parts: SmallVec<[FqnPart; 8]> = parts
        .iter()
        .map(|name| FqnPart {
            part_type: "Unknown",
            name: name.clone(),
            range: Range::empty(),
        })
        .collect();
    CanonicalFqn::new(canonical_parts, lang.fqn_separator())
}

/// Convert parser_core::utils::Range to code_graph_types::Range.
/// For now these are the same type re-exported, but this handles the case
/// where they diverge.
fn convert_range(range: parser_core::utils::Range) -> Range {
    Range::new(
        code_graph_types::Position::new(range.start.line, range.start.column),
        code_graph_types::Position::new(range.end.line, range.end.column),
        range.byte_offset,
    )
}
