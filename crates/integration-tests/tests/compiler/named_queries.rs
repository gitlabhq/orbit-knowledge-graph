use std::sync::{Arc, LazyLock};

use compiler::{AccessLevel, AuthorizedPath, Frontend, Ontology, SecurityContext};
use named_queries::{BindingValues, Language, NamedQueries, NamedQuery};
use serde_json::{Map, Value};

static ONTOLOGY: LazyLock<Arc<Ontology>> =
    LazyLock::new(|| Arc::new(Ontology::load_embedded().unwrap()));

const BINDINGS: BindingValues = BindingValues {
    current_user_id: 42,
};

fn compile(
    query: &NamedQuery,
    language: Language,
    params: &Map<String, Value>,
) -> compiler::CompiledQueryContext {
    let rendered = query.render(language, &BINDINGS, params).unwrap();
    let frontend = match language {
        Language::Gql => Frontend::Gql,
    };
    let security = SecurityContext::new_with_roles(
        1,
        vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
    )
    .unwrap()
    .with_role(true, Some(AccessLevel::Owner as u32));
    compiler::compile(&rendered, frontend, &ONTOLOGY, &security)
        .unwrap_or_else(|error| panic!("{} ({language:?}): {error}\n{rendered}", query.name))
}

#[test]
fn every_named_query_compiles_in_every_language() {
    let queries = NamedQueries::load_embedded().unwrap();
    for query in queries.iter() {
        let params = query.example_parameters();
        let [first, rest @ ..] = Language::ALL.map(|language| compile(query, language, &params));
        for other in rest {
            assert_eq!(
                (&first.base.sql, &first.base.params, &first.query_type),
                (&other.base.sql, &other.base.params, &other.query_type),
                "{}",
                query.name
            );
        }
    }
}
