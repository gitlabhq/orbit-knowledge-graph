use crate::compiler_legacy::setup::{embedded_ontology, test_ctx, test_ontology};
use compiler::gql::{PreparedStatement, prepare};
use compiler::{Frontend, compile};
use ontology::introspection::{
    IntrospectionScope::{All, Local},
    build_compact_schema_response,
};

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

#[test]
fn gql_prepares_queries_and_scoped_schema() {
    let ontology = embedded_ontology();
    let ctx = test_ctx();
    assert!(matches!(
        prepare("MATCH (n:User {id: 1}) RETURN n", &ontology, &ctx, All),
        Ok(PreparedStatement::Query(_))
    ));
    for (scope, call, node) in [
        (All, "CALL db.schema()", None),
        (Local, "CALL db.schema()", None),
        (All, "CALL db.schema('MergeRequest')", Some("MergeRequest")),
        (Local, "CALL db.schema('File')", Some("File")),
    ] {
        let PreparedStatement::Schema(response) = prepare(call, &ontology, &ctx, scope).unwrap()
        else {
            panic!("expected schema: {call}");
        };
        if let Some(name) = node {
            assert_eq!(response.domains.len(), 1);
            assert_eq!(response.domains[0].nodes.len(), 1);
            let node = serde_json::to_value(&response.domains[0].nodes[0]).unwrap();
            assert_eq!(node["name"], name);
            assert!(!node["props"].as_array().unwrap().is_empty());
            assert!(response.edges.is_empty());
        } else {
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(build_compact_schema_response(&ontology, scope, None))
                    .unwrap(),
            );
        }
    }
    for (scope, call) in [
        (All, "CALL db.schema('Missing')"),
        (All, "CALL db.schema('*')"),
        (All, "CALL db.schema('File', 'User')"),
        (All, "CALL db.schema() YIELD name"),
        (Local, "CALL db.schema('MergeRequest')"),
    ] {
        assert!(
            prepare(call, &ontology, &ctx, scope)
                .unwrap_err()
                .is_client_safe()
        );
    }
    assert!(compile("CALL db.schema()", Frontend::Gql, &ontology, &ctx).is_err());
}

#[test]
fn cte_statements_carry_the_ctes_they_read() {
    let compiled = compile(
        "MATCH (g:Group)-[:CONTAINS]->(p:Project)<-[:IN_PROJECT]-(mr:MergeRequest)-[:HAS_LATEST_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile) WHERE g.full_path = 'g701' RETURN p, count(f) AS c LIMIT 10",
        Frontend::Gql,
        &embedded_ontology(),
        &test_ctx(),
    )
    .unwrap();
    let statements = compiler::passes::codegen::clickhouse::cte_statements(&compiled.ast).unwrap();
    assert!(
        statements
            .iter()
            .any(|(_, sql, _)| sql.starts_with("WITH ")),
        "expected a CTE body that reads another CTE"
    );
    for (own, sql, _) in &statements {
        for (position, _) in sql.match_indices("_orbit_relation_") {
            let digits: String = sql[position + "_orbit_relation_".len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            let name = format!("_orbit_relation_{digits}");
            assert!(
                &name == own || sql.contains(&format!("{name} AS ")),
                "{name} is read but not defined in: {sql}"
            );
        }
    }
}
