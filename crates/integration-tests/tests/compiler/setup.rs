use compiler::passes::lower::lower;
use compiler::passes::validate::Validator;
use compiler::{
    AccessLevel, AuthorizedPath, CompiledQueryContext, Node, QueryError, SecurityContext, compile,
    normalize,
};
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

fn no_params() -> opencypher::Parameters {
    opencypher::Parameters::new()
}

/// Compile a JSON query and its openCypher twin and assert they produce the
/// same SQL, parameters, result context, settings, query type, and hydration
/// plan. Both frontends enter the pipeline at `validate_input`, so any
/// difference can only come from the text frontend building a different
/// `Input`. Returns the openCypher result for further assertions.
pub fn compile_both(
    json: &str,
    statement: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> CompiledQueryContext {
    let expected =
        compile(json, ontology, ctx).unwrap_or_else(|e| panic!("JSON failed: {e}\n{json}"));
    let actual = opencypher::compile(statement, &no_params(), ontology, ctx)
        .unwrap_or_else(|e| panic!("openCypher failed: {e}\n{statement}"));
    assert_eq!(
        actual.base.sql, expected.base.sql,
        "SQL differs\nstatement: {statement}\njson: {json}"
    );
    assert_eq!(
        actual.base, expected.base,
        "compiled query differs for {statement}"
    );
    assert_eq!(actual.query_type, expected.query_type);
    assert_eq!(
        actual.hydration, expected.hydration,
        "hydration plan differs for {statement}"
    );
    actual
}

/// `compile_to_ast` for both frontends, asserting the lowered ASTs match.
pub fn compile_to_ast_both(json: &str, statement: &str, ontology: &Ontology) -> Node {
    let expected = compile_to_ast(json, ontology).unwrap_or_else(|e| panic!("JSON failed: {e}"));
    let mut input = opencypher::lower(statement, &no_params(), ontology)
        .unwrap_or_else(|e| panic!("openCypher failed: {e}\n{statement}"));
    let v = Validator::new(ontology);
    v.check_references(&input).unwrap();
    v.annotate_filter_types(&mut input);
    let mut input = normalize(input, ontology).unwrap();
    let actual = lower(&mut input).unwrap();
    assert_eq!(actual, expected, "lowered AST differs for {statement}");
    actual
}

/// Both frontends reject. Returns `(json_error, opencypher_error)`; the
/// openCypher error must be client-safe.
pub fn reject_both(
    json: &str,
    statement: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> (QueryError, QueryError) {
    let json_err = compile(json, ontology, ctx)
        .err()
        .unwrap_or_else(|| panic!("JSON should be rejected: {json}"));
    let cypher_err = opencypher::compile(statement, &no_params(), ontology, ctx)
        .err()
        .unwrap_or_else(|| panic!("openCypher should be rejected: {statement}"));
    assert!(cypher_err.is_client_safe(), "{cypher_err:?}");
    (json_err, cypher_err)
}

pub fn reject_opencypher(statement: &str, ontology: &Ontology) -> QueryError {
    opencypher::compile(statement, &no_params(), ontology, &test_ctx())
        .err()
        .unwrap_or_else(|| panic!("openCypher should be rejected: {statement}"))
}
