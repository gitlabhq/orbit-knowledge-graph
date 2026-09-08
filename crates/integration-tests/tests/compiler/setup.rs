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

pub fn compile_pair(
    json: &str,
    orbit_query: &str,
    ontology: &Ontology,
    context: &SecurityContext,
) -> compiler::Result<compiler::CompiledQueryContext> {
    compile_pair_with_parameters(
        json,
        orbit_query,
        &orbit_query::Parameters::new(),
        ontology,
        context,
    )
}

pub fn compile_pair_with_parameters(
    json: &str,
    orbit_query: &str,
    parameters: &orbit_query::Parameters,
    ontology: &Ontology,
    context: &SecurityContext,
) -> compiler::Result<compiler::CompiledQueryContext> {
    let json_result = compiler::compile(json, ontology, context);
    let orbit_query_result = orbit_query::compile(orbit_query, parameters, ontology, context);
    match (json_result, orbit_query_result) {
        (Ok(json), Ok(orbit_query_result)) => {
            assert_eq!(
                json.base.sql, orbit_query_result.base.sql,
                "SQL differs for {orbit_query}"
            );
            assert_eq!(
                json.base.params, orbit_query_result.base.params,
                "parameters differ for {orbit_query}"
            );
            assert_eq!(json.query_type, orbit_query_result.query_type);
            assert_eq!(
                json.hydration, orbit_query_result.hydration,
                "hydration differs for {orbit_query}"
            );
            Ok(json)
        }
        (Err(json), Err(orbit_query_result)) => {
            assert_eq!(
                std::mem::discriminant(&json),
                std::mem::discriminant(&orbit_query_result),
                "rejection differs for {orbit_query}: JSON={json}; Orbit={orbit_query_result}"
            );
            Err(json)
        }
        (json, orbit_query_result) => panic!(
            "frontend acceptance differs for {orbit_query}: JSON={json:?}; Orbit={orbit_query_result:?}"
        ),
    }
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
