use std::sync::{Arc, LazyLock};

use compiler::{
    AccessLevel, AuthorizedPath, CompiledQueryContext, Frontend, Ontology, SecurityContext,
};
use named_queries::{BindingValues, Language, NamedQueries, NamedQuery};
use serde_json::{Map, Value, json};

static ONTOLOGY: LazyLock<Arc<Ontology>> =
    LazyLock::new(|| Arc::new(Ontology::load_embedded().unwrap()));

const BINDINGS: BindingValues = BindingValues {
    current_user_id: 42,
};

fn security_context() -> SecurityContext {
    SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap()
    .with_role(true, Some(AccessLevel::Owner as u32))
}

fn compile(
    query: &NamedQuery,
    language: Language,
    params: &Map<String, Value>,
) -> CompiledQueryContext {
    let rendered = query.render(language, &BINDINGS, params).unwrap();
    let frontend = match language {
        Language::Json => Frontend::JsonDsl,
        Language::Gql => Frontend::Gql,
    };
    compiler::compile(&rendered, frontend, &ONTOLOGY, &security_context())
        .unwrap_or_else(|error| panic!("{} ({language:?}): {error}\n{rendered}", query.name))
}

fn assert_parity(query: &NamedQuery, params: &Map<String, Value>) -> CompiledQueryContext {
    let json = compile(query, Language::Json, params);
    let gql = compile(query, Language::Gql, params);
    let name = &query.name;
    assert_eq!(json.base.sql, gql.base.sql, "SQL for {name}");
    assert_eq!(json.base.params, gql.base.params, "parameters for {name}");
    assert_eq!(json.query_type, gql.query_type, "query type for {name}");
    let before = &json.base.result_context;
    let after = &gql.base.result_context;
    assert_eq!(before.query_type, after.query_type);
    let nodes = |context: &compiler::ResultContext| {
        context
            .nodes()
            .map(|node| (node.alias.clone(), node.clone()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(nodes(before), nodes(after), "result nodes for {name}");
    let auth = |context: &compiler::ResultContext| {
        context
            .entity_auth()
            .map(|(entity, config)| (entity.to_string(), format!("{config:?}")))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(auth(before), auth(after), "entity auth for {name}");
    assert_eq!(
        format!("{:?}", before.edges()),
        format!("{:?}", after.edges()),
        "edges for {name}"
    );
    assert_eq!(json.hydration, gql.hydration, "hydration for {name}");
    gql
}

fn with_params(query: &NamedQuery, overrides: Value) -> Map<String, Value> {
    let mut params = query.example_parameters();
    params.extend(overrides.as_object().unwrap().clone());
    params
}

#[test]
fn every_named_query_compiles_identically_in_both_languages() {
    let queries = NamedQueries::load_embedded().unwrap();
    assert_eq!(queries.iter().count(), 12);
    for query in queries.iter() {
        assert_parity(query, &query.example_parameters());
    }
}

#[test]
fn hostile_strings_are_exact_parameter_data_not_query_structure() {
    let queries = NamedQueries::load_embedded().unwrap();
    for text in [
        "\"}) RETURN n //",
        "' OR true //",
        "\\\"'`[]{}(); /* */ // -- #",
        "\\u0027\\n\\\\",
        "100%_literal\\",
        "quote\" apostrophe' slash/ backslash\\ newline\n\r\t\u{0000}\u{0008}\u{000c}",
        "雪🙂 café \u{2028}\u{2029}",
        "{{ binding(\"current_user_id\") }} {% set x = 1 %}",
        "__orbit_named_value_0__",
    ] {
        for (name, param) in [
            ("search_nodes", "text"),
            ("file_definitions", "file_path"),
            ("file_definition_callers", "file_path"),
        ] {
            let query = queries.get(name).unwrap();
            let compiled = assert_parity(query, &with_params(query, json!({param: text})));
            assert!(
                !compiled.base.sql.contains(text),
                "client value leaked into SQL"
            );
        }
    }
}

#[test]
fn dynamic_entities_properties_and_integer_ids_preserve_semantics() {
    let queries = NamedQueries::load_embedded().unwrap();
    let search = queries.get("search_nodes").unwrap();
    for (entity, field) in [
        ("User", "username"),
        ("MergeRequest", "title"),
        ("Definition", "name"),
    ] {
        assert_parity(
            search,
            &with_params(search, json!({"entity": entity, "field": field})),
        );
    }
    for name in ["definition_callees", "definition_references"] {
        let query = queries.get(name).unwrap();
        for id in ["1", "9007199254740993", "9223372036854775807"] {
            assert_parity(query, &with_params(query, json!({"node_id": id})));
        }
    }
    let expand = queries.get("expand_neighbors").unwrap();
    for ids in [
        json!([7, 9]),
        json!([9007199254740993_u64, 9223372036854775807_u64]),
    ] {
        assert_parity(
            expand,
            &with_params(expand, json!({"node_ids": ids, "limit": 500})),
        );
    }
}

#[test]
fn identifier_array_id_and_binding_attacks_are_rejected_in_both_languages() {
    let queries = NamedQueries::load_embedded().unwrap();
    let mut requests = Vec::new();
    for entity in [
        "",
        "User`) RETURN n //",
        "User)-[:AUTHORED]->(x",
        "User/*",
        "Üser",
        "User\n",
        "`User`",
        "User.Project",
        "User;MATCH",
        "User\\",
    ] {
        requests.push(json!({"name": "list_nodes", "parameters": {"entity": entity}}));
    }
    for field in [
        "name` CONTAINS 'x' //",
        "name\n",
        "name/*",
        "name.path",
        "$param",
        "名字",
    ] {
        requests.push(json!({"name": "search_nodes", "parameters": {"entity": "Project", "field": field, "text": "gitlab"}}));
    }
    for ids in [
        json!(["1) RETURN n //"]),
        json!(["1"]),
        json!([[1]]),
        json!([1, null]),
        json!([]),
        json!([1.5]),
        json!([9223372036854775808_u64]),
    ] {
        requests.push(json!({"name": "expand_neighbors", "parameters": {"entity": "User", "node_ids": ids, "limit": 50}}));
    }
    for id in ["9223372036854775808", "9999999999999999999"] {
        requests.push(json!({"name": "definition_callees", "parameters": {"node_id": id}}));
    }
    requests.push(json!({"name": "my_neighbors", "parameters": {"current_user_id": 999}}));
    requests.push(json!({"name": "my_neighbors", "bindings": {"current_user_id": 999}}));
    requests.push(json!({"name": "my_neighbors", "current_user_id": 999}));

    for request in requests {
        for (language, frontend) in [
            (Language::Json, Frontend::JsonDsl),
            (Language::Gql, Frontend::Gql),
        ] {
            let rejected = match queries.render_request(&request.to_string(), language, &BINDINGS) {
                Err(_) => true,
                Ok(rendered) => {
                    compiler::compile(&rendered, frontend, &ONTOLOGY, &security_context()).is_err()
                }
            };
            assert!(rejected, "{language:?} accepted {request}");
        }
    }
}
