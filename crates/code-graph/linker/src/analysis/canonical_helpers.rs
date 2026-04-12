use crate::graph::RelationshipType;
use code_graph_types::{CanonicalFqn, DefKind, FqnPart, Language, Range};
use parser_core::definitions::DefinitionTypeInfo;
use parser_core::fqn::FQNPart;
use smallvec::SmallVec;
use std::hash::Hash;

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

/// Language-agnostic relationship type determination from DefKind pairs.
pub fn determine_relationship_type(parent: DefKind, child: DefKind) -> Option<RelationshipType> {
    match (parent, child) {
        (DefKind::Module, _) => Some(RelationshipType::ModuleToSingletonMethod),
        (DefKind::Class, DefKind::Class) => Some(RelationshipType::ClassToClass),
        (DefKind::Class, DefKind::Interface) => Some(RelationshipType::ClassToInterface),
        (DefKind::Class, DefKind::Method | DefKind::Function) => {
            Some(RelationshipType::ClassToMethod)
        }
        (DefKind::Class, DefKind::Constructor) => Some(RelationshipType::ClassToConstructor),
        (DefKind::Class, DefKind::Property | DefKind::EnumEntry) => {
            Some(RelationshipType::ClassToProperty)
        }
        (DefKind::Class, DefKind::Lambda) => Some(RelationshipType::ClassToLambda),
        (DefKind::Interface, DefKind::Interface) => Some(RelationshipType::InterfaceToInterface),
        (DefKind::Interface, DefKind::Class) => Some(RelationshipType::InterfaceToClass),
        (DefKind::Interface, DefKind::Method) => Some(RelationshipType::InterfaceToMethod),
        (DefKind::Interface, DefKind::Property) => Some(RelationshipType::InterfaceToProperty),
        (DefKind::Method | DefKind::Function, DefKind::Method | DefKind::Function) => {
            Some(RelationshipType::MethodToMethod)
        }
        (DefKind::Method | DefKind::Function, DefKind::Lambda) => {
            Some(RelationshipType::MethodToLambda)
        }
        (DefKind::Other, DefKind::Method | DefKind::Function) => {
            Some(RelationshipType::ClassToMethod)
        }
        _ => None,
    }
}
