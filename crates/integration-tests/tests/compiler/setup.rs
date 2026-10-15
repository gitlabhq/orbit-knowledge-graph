use compiler::SecurityContext;
use ontology::{DataType, Ontology};
use std::sync::Arc;

pub fn test_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).unwrap()
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
