use compiler::{AccessLevel, AuthorizedPath, SecurityContext};
use ontology::{DataType, Ontology};
use std::sync::Arc;

pub fn test_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).unwrap()
}

pub fn admin_ctx() -> SecurityContext {
    SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap()
    .with_role(true, Some(AccessLevel::Owner as u32))
}

pub fn test_ontology() -> Arc<Ontology> {
    Arc::new(
        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("state", DataType::String),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_fields(
                "Note",
                [
                    ("confidential", DataType::Bool),
                    ("created_at", DataType::DateTime),
                    ("traversal_path", DataType::String),
                ],
            )
            .with_fields(
                "Project",
                [
                    ("name", DataType::String),
                    ("traversal_path", DataType::String),
                ],
            )
            .with_fields(
                "Group",
                [
                    ("name", DataType::String),
                    ("traversal_path", DataType::String),
                ],
            ),
    )
}

pub fn embedded_ontology() -> Arc<Ontology> {
    Arc::new(Ontology::load_embedded().expect("Failed to load embedded ontology"))
}

/// Compiles two spellings of one query and asserts they produce the same SQL,
/// parameters, and hydration plan.
pub fn compile_equivalent(
    expected: &str,
    query: &str,
    ontology: &Arc<Ontology>,
    context: &SecurityContext,
) -> compiler::Result<compiler::CompiledQueryContext> {
    match (
        compiler::compile(expected, compiler::Frontend::Gql, ontology, context),
        compiler::compile(query, compiler::Frontend::Gql, ontology, context),
    ) {
        (Ok(expected), Ok(actual)) => {
            assert_eq!(
                expected.base.sql, actual.base.sql,
                "SQL differs for {query}"
            );
            assert_eq!(
                expected.base.params, actual.base.params,
                "parameters differ for {query}"
            );
            assert_eq!(expected.query_type, actual.query_type);
            assert_eq!(
                expected.hydration, actual.hydration,
                "hydration differs for {query}"
            );
            Ok(actual)
        }
        (Err(expected), Err(actual)) => {
            assert_eq!(
                std::mem::discriminant(&expected),
                std::mem::discriminant(&actual),
                "rejection differs for {query}: expected={expected}; actual={actual}"
            );
            Err(actual)
        }
        (expected, actual) => {
            panic!("acceptance differs for {query}: expected={expected:?}; actual={actual:?}")
        }
    }
}
