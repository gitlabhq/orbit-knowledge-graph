use crate::query::NamedQuery;
use crate::{BindingValues, Language, NamedQueryError};
use serde_json::{Map, Value, json};

const VALID: &str = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  json:
    query_type: traversal
    nodes:
      - id: n
        entity: User
        node_ids: [{ $binding: current_user_id }]
  gql: |
    MATCH (n:User {id: {{ binding("current_user_id") }}}) RETURN n
"#;

const WITH_PARAMS: &str = r#"
name: q
description: A query.
parameters:
  entity:
    schema: {type: string}
    example: User
  node_ids:
    schema: {type: array, items: {type: integer}, minItems: 1, maxItems: 500}
    example: [1]
query:
  json:
    query_type: traversal
    nodes:
      - id: n
        entity: { $param: entity }
        node_ids: { $param: node_ids }
  gql: |
    MATCH (n:{{ identifier("entity") }}) WHERE n.id IN {{ param("node_ids") }} RETURN n
"#;

fn parse(yaml: &str) -> Result<NamedQuery, NamedQueryError> {
    NamedQuery::from_yaml("q.yaml", yaml)
}

fn values() -> BindingValues {
    BindingValues {
        current_user_id: 42,
    }
}

fn params(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn query_render_substitutes_current_user_id() {
    let query = parse(VALID).unwrap();
    assert_eq!(
        query.render(Language::Gql, &values(), &Map::new()).unwrap(),
        "MATCH (n:User {id: 42}) RETURN n"
    );
}

#[test]
fn query_render_rejects_missing_parameter_and_lists_valid() {
    let query = parse(WITH_PARAMS).unwrap();
    let error = query
        .render(Language::Gql, &values(), &params(json!({"entity": "User"})))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("missing required parameter `node_ids`"),
        "{error}"
    );
    assert!(error.contains("entity, node_ids"), "{error}");
}

#[test]
fn query_render_rejects_unknown_parameter_and_lists_valid() {
    let query = parse(WITH_PARAMS).unwrap();
    let error = query
        .render(
            Language::Gql,
            &values(),
            &params(json!({"entity": "User", "node_ids": [1], "extra": 2})),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown parameter `extra`"), "{error}");
    assert!(error.contains("entity, node_ids"), "{error}");
}

#[test]
fn query_render_rejects_parameter_violating_schema() {
    let query = parse(WITH_PARAMS).unwrap();
    let error = query
        .render(
            Language::Gql,
            &values(),
            &params(json!({"entity": "User", "node_ids": "[1]"})),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("parameter `node_ids` is invalid"), "{error}");
}

#[test]
fn query_render_example_uses_declared_examples() {
    assert_eq!(
        parse(WITH_PARAMS)
            .unwrap()
            .render_example(Language::Gql)
            .unwrap(),
        "MATCH (n:`User`) WHERE n.id IN [1] RETURN n"
    );
}

#[test]
fn query_example_must_satisfy_parameter_schema() {
    let error = parse(&WITH_PARAMS.replace("example: [1]", "example: nope"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not satisfy its own schema"), "{error}");
}

#[test]
fn query_rejects_invalid_parameter_schema() {
    let error = parse(&WITH_PARAMS.replace("type: string", "type: nope"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("invalid schema"), "{error}");
}

#[test]
fn query_rejects_undeclared_parameters() {
    let error = parse(&VALID.replace("binding(\"current_user_id\")", "param(\"node_id\")"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("undeclared parameter `node_id`"), "{error}");
}

#[test]
fn query_rejects_unused_parameters() {
    let error = parse(&WITH_PARAMS.replace("{{ identifier(\"entity\") }}", "User"))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("parameter `entity` but never uses it"),
        "{error}"
    );
}

#[test]
fn query_name_must_match_file_stem() {
    assert!(
        parse(&VALID.replace("name: q", "name: other"))
            .unwrap_err()
            .to_string()
            .contains("file stem")
    );
}

#[test]
fn query_description_must_be_non_empty() {
    assert!(
        parse(&VALID.replace("A query.", "''"))
            .unwrap_err()
            .to_string()
            .contains("description")
    );
}

#[test]
fn query_rejects_unknown_bindings_even_if_unused() {
    for yaml in [
        VALID.replace("current_user_id", "current_project_id"),
        VALID.replace(
            "bindings: [current_user_id]",
            "bindings: [current_user_id, current_project_id]",
        ),
    ] {
        assert!(
            parse(&yaml)
                .unwrap_err()
                .to_string()
                .contains("unknown binding")
        );
    }
}

#[test]
fn query_rejects_undeclared_bindings() {
    let error = parse(&VALID.replace("bindings: [current_user_id]", ""))
        .unwrap_err()
        .to_string();
    assert!(error.contains("undeclared binding"), "{error}");
}

#[test]
fn query_rejects_unused_bindings() {
    let error = parse(&VALID.replace("{{ binding(\"current_user_id\") }}", "1"))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("binding `current_user_id` but never uses it"),
        "{error}"
    );
}

#[test]
fn query_rejects_unknown_yaml_keys() {
    assert!(matches!(
        parse(&format!("{VALID}extra_key: 1\n")),
        Err(NamedQueryError::Parse { .. })
    ));
}

#[test]
fn query_rejects_non_string_and_hostile_identifiers() {
    for example in [
        "''",
        "'User`)'",
        "'User\\\\'",
        "'User/*'",
        "'User--'",
        "'用户'",
        "'$binding'",
        "'1User'",
    ] {
        assert!(
            parse(&WITH_PARAMS.replace("example: User", &format!("example: {example}"))).is_err(),
            "{example}"
        );
    }
    let yaml = WITH_PARAMS
        .replace("schema: {type: string}", "schema: {type: integer}")
        .replace("example: User", "example: 1");
    assert!(
        parse(&yaml)
            .unwrap_err()
            .to_string()
            .contains("must be a string")
    );
}

#[test]
fn query_rejects_raw_parameter_context_access() {
    for expression in [
        "{{ entity }}",
        "{{ parameters.entity }}",
        "{{ current_user_id }}",
    ] {
        assert!(parse(&WITH_PARAMS.replace("{{ identifier(\"entity\") }}", expression)).is_err());
    }
}

#[test]
fn query_binding_namespace_cannot_be_spoofed_by_parameter() {
    let yaml = VALID
        .replace(
            "query:\n",
            "parameters:\n  current_user_id:\n    schema: {type: integer}\n    example: 999\nquery:\n",
        )
        .replace(
            "node_ids: [{ $binding: current_user_id }]",
            "node_ids: [{ $binding: current_user_id }]\n        filters:\n          project_id: { $param: current_user_id }",
        )
        .replace(
            "RETURN n",
            "WHERE n.project_id = {{ param(\"current_user_id\") }} RETURN n",
        );
    let query = parse(&yaml).unwrap();
    let spoof = params(json!({"current_user_id": 999}));
    assert_eq!(
        query.render(Language::Gql, &values(), &spoof).unwrap(),
        "MATCH (n:User {id: 42}) WHERE n.project_id = 999 RETURN n"
    );
    let rendered = query.render(Language::Json, &values(), &spoof).unwrap();
    assert!(rendered.contains("\"node_ids\":[42]"), "{rendered}");
    assert!(rendered.contains("\"project_id\":999"), "{rendered}");
}

#[test]
fn query_render_json_substitutes_values_and_keys() {
    let yaml = WITH_PARAMS.replace(
        "node_ids: { $param: node_ids }",
        "node_ids: { $param: node_ids }\n        filters:\n          \"$param:entity\": { eq: true }",
    );
    let query = parse(&yaml).unwrap();
    let rendered = query
        .render(
            Language::Json,
            &values(),
            &params(json!({"entity": "Project", "node_ids": [7, 9]})),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&rendered).unwrap(),
        json!({
            "nodes": [{
                "entity": "Project",
                "filters": {"Project": {"eq": true}},
                "id": "n",
                "node_ids": [7, 9]
            }],
            "query_type": "traversal"
        })
    );
    for placeholder in [
        "{ $param: node_ids, extra: 1 }",
        "{ $param: 1 }",
        "{ $param: other }",
    ] {
        let yaml = WITH_PARAMS.replace("{ $param: node_ids }", placeholder);
        assert!(parse(&yaml).is_err(), "{placeholder}");
    }
}

#[test]
fn query_rejects_a_spelling_that_skips_a_declared_name() {
    let error = parse(&WITH_PARAMS.replace("node_ids: { $param: node_ids }", "node_ids: [1]"))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("parameter `node_ids` but never uses it"),
        "{error}"
    );
}

#[test]
fn query_string_id_formatter_preserves_unsigned_decimal_values() {
    let yaml = WITH_PARAMS
        .replace("example: User", "example: '9007199254740993'")
        .replace("identifier(\"entity\")", "integer(\"entity\")");
    let query = parse(&yaml).unwrap();
    assert!(
        query
            .render_example(Language::Gql)
            .unwrap()
            .contains("9007199254740993")
    );
    for value in [
        "1) RETURN n //",
        "1e2",
        "1.0",
        "-1",
        "9223372036854775808",
        "18446744073709551616",
    ] {
        assert!(
            query
                .render(
                    Language::Gql,
                    &values(),
                    &params(json!({"entity": value, "node_ids": [1]}))
                )
                .is_err()
        );
    }
}

#[test]
fn query_strings_and_arrays_are_encoded_not_re_evaluated() {
    let yaml = WITH_PARAMS
        .replace("items: {type: integer}", "items: {type: string}")
        .replace("example: [1]", "example: ['abc']");
    let query = parse(&yaml).unwrap();
    let strings = json!([
        "\"'] /* */ // \\",
        "雪🙂",
        "{{ binding('current_user_id') }}"
    ]);
    let rendered = query
        .render(
            Language::Gql,
            &values(),
            &params(json!({"entity": "User", "node_ids": strings})),
        )
        .unwrap();
    assert!(rendered.contains(&strings.to_string()), "{rendered}");
}
