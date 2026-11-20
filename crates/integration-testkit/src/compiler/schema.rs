use std::path::Path;

use ontology::introspection::IntrospectionScope;
use query_engine::compiler::gql::{PreparedStatement, prepare};
use serde::Deserialize;

use crate::query_scenario::shown_private_detail;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaCase {
    name: String,
    call: String,
    scope: Scope,
    expect: Option<serde_json::Value>,
    rejected: Option<SchemaRejection>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Scope {
    All,
    Local,
}

impl From<Scope> for IntrospectionScope {
    fn from(scope: Scope) -> Self {
        match scope {
            Scope::All => Self::All,
            Scope::Local => Self::Local,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaRejection {
    contains: Vec<String>,
}

pub fn run_schema_dir(directory: &Path, ontology: &ontology::Ontology) {
    let failures: Vec<String> = super::fixture_paths(directory)
        .iter()
        .flat_map(|path| {
            crate::scenario::read_cases::<SchemaCase>(path)
                .into_iter()
                .filter_map(|(_, case)| {
                    check(&case, ontology)
                        .err()
                        .map(|error| format!("{}: {}: {error}", path.display(), case.name))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} schema fixture failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

fn check(case: &SchemaCase, ontology: &ontology::Ontology) -> Result<(), String> {
    let security = crate::query_scenario::build_security(&None);
    let scope = case.scope.into();
    let prepared = prepare(&case.call, ontology, &security, scope);
    match (&case.expect, &case.rejected, prepared) {
        (Some(expect), None, Ok(prepared)) => check_prepared(expect, prepared, "expect"),
        (None, Some(rejection), Err(error)) => {
            let message = error.to_string();
            if !error.is_client_safe() {
                return Err(format!("rejection is not client safe: {message}"));
            }
            if let Some(detail) = shown_private_detail(&message) {
                return Err(format!("rejection shows {detail}: {message}"));
            }
            if rejection.contains.is_empty() {
                return Err("rejected needs contains".into());
            }
            match rejection
                .contains
                .iter()
                .find(|fragment| !message.contains(fragment.as_str()))
            {
                Some(fragment) => Err(format!(
                    "rejection '{message}' does not contain '{fragment}'"
                )),
                None => Ok(()),
            }
        }
        (Some(_), None, Err(error)) => Err(format!("rejected: {error}")),
        (None, Some(_), Ok(_)) => Err("expected a rejection, but the call prepared".into()),
        _ => Err("set exactly one of expect and rejected".into()),
    }
}

fn check_prepared(
    expect: &serde_json::Value,
    prepared: PreparedStatement,
    label: &str,
) -> Result<(), String> {
    let actual = match prepared {
        PreparedStatement::Query(_) => serde_json::json!("query"),
        PreparedStatement::Schema(response) => serde_json::json!({"schema": response}),
    };
    if let Some(path) = super::empty_map_path(expect, label) {
        return Err(format!("{path} is an empty map, which matches anything"));
    }
    match crate::scenario::subset_mismatch(label, expect, &actual) {
        Some(mismatch) => Err(mismatch),
        None => Ok(()),
    }
}
