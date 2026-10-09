#[test]
fn yaml_plan_shapes() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/compiler/plan_shape/fixtures");
    integration_testkit::plan_shape::run_dir(&directory, super::setup::embedded_ontology());
}

#[test]
fn native_predicate_bounds() {
    use compiler::input::{
        BooleanExpression, FilterOp, InputFilter, PredicateTarget, PropertyPredicate,
    };
    let ontology = super::setup::embedded_ontology();
    let model = query_data_model::ClickHouseDataModel::derive(ontology).unwrap();
    let validator = compiler::passes::validate::Validator::new(&model);
    let mut input =
        compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let leaf = BooleanExpression::Leaf(PropertyPredicate {
        target: PredicateTarget::Node("u".into()),
        property: "id".into(),
        filter: InputFilter {
            op: Some(FilterOp::Eq),
            value: Some(1.into()),
            ..Default::default()
        },
    });
    let mut deep = leaf.clone();
    for _ in 0..1024 {
        deep = BooleanExpression::Not(Box::new(deep));
    }
    input.predicates = vec![deep];
    assert!(
        validator
            .check_shape(&input)
            .unwrap_err()
            .to_string()
            .contains("expression bounds")
    );
    input.predicates = vec![BooleanExpression::And(vec![leaf; 257])];
    assert!(
        validator
            .check_shape(&input)
            .unwrap_err()
            .to_string()
            .contains("256 leaves")
    );
}
