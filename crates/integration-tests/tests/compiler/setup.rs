//! Shared test setup for compiler tests.

use compiler::{Node, SecurityContext, Validator, lower, normalize};
use ontology::{DataType, Ontology};

pub fn test_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).unwrap()
}

pub fn admin_ctx() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()])
        .unwrap()
        .with_role(true, None)
}

pub fn test_ontology() -> Ontology {
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
        )
}

pub fn embedded_ontology() -> Ontology {
    Ontology::load_embedded().expect("Failed to load embedded ontology")
}

pub fn compile_to_ast(json_input: &str, ontology: &Ontology) -> compiler::Result<Node> {
    let v = Validator::new(ontology);
    let value = v.check_json(json_input)?;
    v.check_ontology(&value)?;
    let input: compiler::Input = serde_json::from_value(value)?;
    v.check_references(&input)?;
    let mut input = normalize(input, ontology)?;
    let node = lower(&mut input)?;
    Ok(node)
}
