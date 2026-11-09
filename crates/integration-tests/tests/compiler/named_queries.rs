use std::sync::{Arc, LazyLock};

use compiler::{AccessLevel, AuthorizedPath, Frontend, SecurityContext};
use named_queries::{BindingValues, Language, NamedQueries, NamedQuery};
use query_data_model::{Catalog, ClickHouse};
use serde_json::{Map, Value};

static DATA_MODEL: LazyLock<Arc<Catalog<ClickHouse>>> =
    LazyLock::new(|| Arc::new(Catalog::<ClickHouse>::embedded().unwrap()));

const BINDINGS: BindingValues = BindingValues {
    current_user_id: 42,
};

fn compile(
    query: &NamedQuery,
    language: Language,
    params: &Map<String, Value>,
) -> compiler::CompiledQueryContext {
    let rendered = query.render_language(language, &BINDINGS, params).unwrap();
    let frontend = match language {
        Language::Json => Frontend::JsonDsl,
        Language::Gql => Frontend::Gql,
    };
    let security = SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap()
    .with_role(true, Some(AccessLevel::Owner as u32));
    compiler::compile(&rendered, frontend, &DATA_MODEL, &security)
        .unwrap_or_else(|error| panic!("{} ({language:?}): {error}\n{rendered}", query.name))
}

#[test]
fn every_named_query_compiles_identically_in_both_languages() {
    let queries = NamedQueries::load_embedded().unwrap();
    for query in queries.iter() {
        let params = query.example_parameters();
        let json = compile(query, Language::Json, &params);
        let gql = compile(query, Language::Gql, &params);
        assert_eq!(
            (&json.base.sql, &json.base.params, &json.query_type),
            (&gql.base.sql, &gql.base.params, &gql.query_type),
            "{}",
            query.name
        );
    }
}
