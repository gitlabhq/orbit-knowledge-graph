use named_queries::{BindingValues, Language, NamedQueries};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Generate {
    NamedQueries,
    Repeat {
        placeholder: String,
        text: String,
        count: usize,
    },
}

const BINDINGS: BindingValues = BindingValues {
    current_user_id: 42,
};

pub(super) fn expand(mut case: Value) -> Result<Vec<Value>, String> {
    let Some(generate) = case
        .as_object_mut()
        .and_then(|case| case.remove("generate"))
    else {
        return Ok(vec![case]);
    };
    let name = case
        .get("name")
        .and_then(Value::as_str)
        .ok_or("a generated case needs a name")?
        .to_string();
    let generate: Generate =
        serde_json::from_value(generate).map_err(|error| format!("{name}: generate: {error}"))?;
    match generate {
        Generate::NamedQueries => {
            if case.get("query").is_some() {
                return Err(format!("{name}: named_queries supplies the query"));
            }
            let queries = NamedQueries::load_embedded().map_err(|error| error.to_string())?;
            queries
                .iter()
                .map(|query| {
                    let params = query.example_parameters();
                    let render = |language| {
                        query
                            .render_language(language, &BINDINGS, &params)
                            .map(Value::String)
                            .map_err(|error| format!("{name}: {}: {error}", query.name))
                    };
                    let mut generated = case.clone();
                    let fields = generated.as_object_mut().unwrap();
                    fields.insert("name".into(), format!("{name}: {}", query.name).into());
                    fields.insert(
                        "query".into(),
                        Value::Object(Map::from_iter([
                            ("json".into(), render(Language::Json)?),
                            ("gql".into(), render(Language::Gql)?),
                        ])),
                    );
                    Ok(generated)
                })
                .collect()
        }
        Generate::Repeat {
            placeholder,
            text,
            count,
        } => {
            let queries = case
                .get_mut("query")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| format!("{name}: repeat needs a query"))?;
            let mut found = false;
            for query in queries.values_mut() {
                if let Value::String(query) = query
                    && query.contains(&placeholder)
                {
                    *query = query.replace(&placeholder, &text.repeat(count));
                    found = true;
                }
            }
            if !found {
                return Err(format!("{name}: no query contains {placeholder}"));
            }
            Ok(vec![case])
        }
    }
}

#[test]
fn generators_expand_cases_and_reject_misuse() {
    let case = |extra: &str| -> Value {
        orbit_utils::yaml::from_str(&format!("name: generated\n{extra}")).unwrap()
    };
    let repeated = expand(case(
        "generate: {kind: repeat, placeholder: LONG, text: ab, count: 3}\nquery: {gql: \"x LONG y\"}",
    ))
    .unwrap();
    assert_eq!(repeated[0]["query"]["gql"], "x ababab y");
    assert!(repeated[0].get("generate").is_none());
    let named = expand(case("generate: {kind: named_queries}")).unwrap();
    assert!(named.len() > 1);
    assert!(named.iter().all(|case| case["query"]["json"].is_string()
        && case["query"]["gql"].is_string()
        && case["name"].as_str().unwrap().starts_with("generated: ")));
    for (extra, expected) in [
        (
            "generate: {kind: repeat, placeholder: LONG, text: a, count: 2}\nquery: {gql: x}",
            "no query contains LONG",
        ),
        (
            "generate: {kind: named_queries}\nquery: {gql: x}",
            "named_queries supplies the query",
        ),
        ("generate: {kind: sweep}", "unknown variant `sweep`"),
    ] {
        let error = expand(case(extra)).err().unwrap_or_default();
        assert!(error.contains(expected), "{extra}: {error}");
    }
}
