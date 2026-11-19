use crate::compiler_legacy::setup::{test_ctx, test_ontology};
use compiler::{Frontend, compile};

#[test]
fn invalid_json_rejected() {
    assert!(
        compile(
            "not valid json",
            Frontend::JsonDsl,
            &test_ontology(),
            &test_ctx()
        )
        .is_err()
    );
}

#[test]
fn orbit_query_rejects_input_past_the_byte_limit() {
    let oversized = format!(
        "MATCH (u:User {{username: '{}'}}) RETURN u",
        "x".repeat(40_000)
    );
    assert!(compiler::passes::frontend::gql::parse(&oversized).is_err());
}

#[test]
fn native_predicate_trees_are_bounded_before_planning() {
    use compiler::input::{BooleanExpression, Condition, FilterOp, InputFilter, PredicateTarget};
    let model = query_data_model::ClickHouseDataModel::derive(
        crate::compiler_legacy::setup::embedded_ontology(),
    )
    .unwrap();
    let validator = compiler::passes::validate::Validator::new(&model);
    let mut input =
        compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let leaf = |target| {
        BooleanExpression::Leaf(Condition {
            target,
            property: "id".into(),
            filter: InputFilter {
                op: Some(FilterOp::In),
                value: Some(serde_json::json!([1])),
                ..Default::default()
            },
        })
    };
    let mut deep = leaf(PredicateTarget::Node("u".into()));
    for _ in 0..1024 {
        deep = BooleanExpression::Not(Box::new(deep));
    }
    input.predicates = vec![deep];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("must not nest deeper than 32"),
        "{error}"
    );
    input.predicates = vec![BooleanExpression::Not(Box::new(leaf(
        PredicateTarget::Relationship(7),
    )))];
    let error = validator.check_shape(&input).unwrap_err();
    assert!(
        error.to_string().contains("undefined relationship"),
        "{error}"
    );
}

#[test]
fn native_conditions_need_a_column() {
    use compiler::input::{BooleanExpression, InputFilter};
    let base = compiler::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
    let mut empty_group = base.clone();
    empty_group.predicates = vec![BooleanExpression::Not(Box::new(BooleanExpression::And(
        vec![],
    )))];
    let mut empty_property = base;
    empty_property.nodes[0].filters.insert(
        String::new(),
        vec![InputFilter {
            value: Some(serde_json::json!(1)),
            ..Default::default()
        }],
    );
    for input in [empty_group, empty_property] {
        let error = compiler::compile_input(
            input,
            Default::default(),
            &crate::compiler_legacy::setup::embedded_ontology(),
            &test_ctx(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("every condition must compare a property"),
            "{error}"
        );
    }
}
