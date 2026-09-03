use compiler::passes::lower::lower;
use compiler::passes::validate::Validator;
use compiler::{AccessLevel, AuthorizedPath, Node, SecurityContext, normalize};
use ontology::{DataType, Ontology};

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

pub fn compile_both(
    json: &str,
    gql: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> compiler::passes::codegen::CompiledQueryContext {
    let from_json = compiler::compile(json, ontology, ctx).unwrap();
    let from_gql = orbit_gql::compile_gql(gql, &orbit_gql::Params::new(), ontology, ctx)
        .unwrap_or_else(|e| panic!("GQL twin failed to compile: {e}\n{gql}"));
    assert_eq!(
        from_gql.base.sql, from_json.base.sql,
        "SQL differs for GQL twin:\n{gql}"
    );
    assert_eq!(
        from_gql.base.params, from_json.base.params,
        "params differ for GQL twin:\n{gql}"
    );
    from_json
}

pub fn gql_error(gql: &str, ontology: &Ontology) -> compiler::QueryError {
    orbit_gql::compile_gql(gql, &orbit_gql::Params::new(), ontology, &test_ctx())
        .err()
        .unwrap_or_else(|| panic!("expected GQL to be rejected:\n{gql}"))
}
