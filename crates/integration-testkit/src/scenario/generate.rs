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
    Rows {
        into: String,
        count: usize,
        #[serde(default)]
        start: i64,
        row: Value,
    },
}

impl Generate {
    fn kind(&self) -> &'static str {
        match self {
            Self::NamedQueries => "named_queries",
            Self::Repeat { .. } => "repeat",
            Self::Rows { .. } => "rows",
        }
    }
}

const BINDINGS: BindingValues = BindingValues {
    current_user_id: 42,
};

pub(crate) fn expand(mut case: Value, kinds: &[&str]) -> Result<Vec<Value>, String> {
    let Some(generate) = case
        .as_object_mut()
        .and_then(|case| case.remove("generate"))
    else {
        return Ok(vec![case]);
    };
    let name = case
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let generators = match generate {
        Value::Array(items) => items
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<Generate>, _>>(),
        generate => serde_json::from_value(generate).map(|generator| vec![generator]),
    }
    .map_err(|error| format!("{name}: generate: {error}"))?;
    let mut cases = vec![case];
    for generator in generators {
        if !kinds.contains(&generator.kind()) {
            return Err(format!(
                "{name}: generate kind {} is not one of {kinds:?}",
                generator.kind()
            ));
        }
        cases = cases
            .into_iter()
            .map(|case| apply(&generator, case, &name))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
    }
    Ok(cases)
}

fn apply(generator: &Generate, mut case: Value, name: &str) -> Result<Vec<Value>, String> {
    match generator {
        Generate::NamedQueries => {
            if name.is_empty() {
                return Err("named_queries needs a case name".into());
            }
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
            let replacement = text.repeat(*count);
            if !replace_strings(&mut case, &mut |text| {
                text.contains(placeholder.as_str())
                    .then(|| Value::String(text.replace(placeholder.as_str(), &replacement)))
            }) {
                return Err(format!("{name}: no string contains {placeholder}"));
            }
            Ok(vec![case])
        }
        Generate::Rows {
            into,
            count,
            start,
            row,
        } => {
            let rows = (*start..*start + *count as i64).map(|index| {
                let mut row = row.clone();
                replace_strings(&mut row, &mut |text| match text {
                    "$i" => Some(Value::from(index)),
                    text if text.contains("$i") => {
                        Some(Value::String(text.replace("$i", &index.to_string())))
                    }
                    _ => None,
                });
                row
            });
            let mut target = &mut case;
            for key in into.split('.') {
                target = target
                    .as_object_mut()
                    .ok_or_else(|| format!("{name}: {into} must lead through mappings"))?
                    .entry(key)
                    .or_insert_with(|| Value::Object(Map::new()));
            }
            if target.as_object().is_some_and(Map::is_empty) {
                *target = Value::Array(vec![]);
            }
            let Value::Array(list) = target else {
                return Err(format!("{name}: {into} must be a list"));
            };
            list.extend(rows);
            Ok(vec![case])
        }
    }
}

fn replace_strings(value: &mut Value, replace: &mut impl FnMut(&str) -> Option<Value>) -> bool {
    match value {
        Value::String(text) => match replace(text) {
            Some(replacement) => {
                *value = replacement;
                true
            }
            None => false,
        },
        Value::Array(items) => items
            .iter_mut()
            .fold(false, |found, item| replace_strings(item, replace) | found),
        Value::Object(fields) => fields
            .values_mut()
            .fold(false, |found, item| replace_strings(item, replace) | found),
        _ => false,
    }
}

#[test]
fn generators_expand_cases_and_reject_misuse() {
    let case = |extra: &str| -> Value {
        orbit_utils::yaml::from_str(&format!("name: generated\n{extra}")).unwrap()
    };
    let all = ["named_queries", "repeat", "rows"];
    let repeated = expand(
        case("generate: {kind: repeat, placeholder: LONG, text: ab, count: 3}\nquery: {gql: \"x LONG y\"}\nexpect: {v: LONG}"),
        &all,
    )
    .unwrap();
    assert_eq!(repeated[0]["query"]["gql"], "x ababab y");
    assert_eq!(repeated[0]["expect"]["v"], "ababab");
    assert!(repeated[0].get("generate").is_none());
    let named = expand(case("generate: {kind: named_queries}"), &all).unwrap();
    assert!(named.len() > 1);
    assert!(named.iter().all(|case| case["query"]["json"].is_string()
        && case["query"]["gql"].is_string()
        && case["name"].as_str().unwrap().starts_with("generated: ")));
    let rows = expand(
        case("generate:\n  - {kind: rows, into: config.extra_seed.gl_note, count: 2, start: 7, row: {id: $i, traversal_path: 1/$i/}}\n  - {kind: rows, into: config.extra_seed.gl_note, count: 1, row: {id: $i}}\n  - {kind: rows, into: config.redaction.allow.project, count: 2, start: 5, row: $i}\nconfig: {extra_seed: {gl_note: [{id: 1}]}}"),
        &all,
    )
    .unwrap();
    assert_eq!(
        rows[0]["config"]["extra_seed"]["gl_note"],
        serde_json::json!([{"id": 1}, {"id": 7, "traversal_path": "1/7/"}, {"id": 8, "traversal_path": "1/8/"}, {"id": 0}])
    );
    assert_eq!(
        rows[0]["config"]["redaction"]["allow"]["project"],
        serde_json::json!([5, 6])
    );
    for (extra, kinds, expected) in [
        (
            "generate: {kind: repeat, placeholder: LONG, text: a, count: 2}\nquery: {gql: x}",
            &all[..],
            "no string contains LONG",
        ),
        (
            "generate: {kind: named_queries}\nquery: {gql: x}",
            &all[..],
            "named_queries supplies the query",
        ),
        (
            "generate: {kind: sweep}",
            &all[..],
            "unknown variant `sweep`",
        ),
        (
            "generate: {kind: rows, into: t, count: 1, row: {}}",
            &["repeat"][..],
            "generate kind rows is not one of",
        ),
    ] {
        let error = expand(case(extra), kinds).err().unwrap_or_default();
        assert!(error.contains(expected), "{extra}: {error}");
    }
}
