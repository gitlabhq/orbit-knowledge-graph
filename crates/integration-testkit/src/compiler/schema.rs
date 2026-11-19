use std::path::Path;

use ontology::introspection::{IntrospectionScope, SchemaNode, build_compact_schema_response};
use query_engine::compiler::gql::{PreparedStatement, prepare};
use serde::Deserialize;

use crate::query_scenario::shown_private_detail;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaCase {
    name: String,
    call: String,
    scope: Scope,
    expect: Option<SchemaExpect>,
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
#[serde(rename_all = "lowercase")]
enum SchemaExpect {
    Query,
    Overview,
    Node(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaRejection {
    contains: Vec<String>,
}

pub fn run_schema_dir(directory: &Path, ontology: &ontology::Ontology) {
    let mut paths = Vec::new();
    crate::scenario::discover(directory, &mut paths);
    paths.sort();
    assert!(!paths.is_empty());
    let failures: Vec<String> = paths
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
        (Some(expect), None, Ok(prepared)) => check_prepared(expect, prepared, ontology, scope),
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
    expect: &SchemaExpect,
    prepared: PreparedStatement,
    ontology: &ontology::Ontology,
    scope: IntrospectionScope,
) -> Result<(), String> {
    match (expect, prepared) {
        (SchemaExpect::Query, PreparedStatement::Query(_)) => Ok(()),
        (SchemaExpect::Query, PreparedStatement::Schema(_)) => {
            Err("prepared a schema response, expected a query".into())
        }
        (_, PreparedStatement::Query(_)) => {
            Err("prepared a query, expected a schema response".into())
        }
        (SchemaExpect::Overview, PreparedStatement::Schema(response)) => {
            let expected = build_compact_schema_response(ontology, scope, None);
            if to_json(&response) == to_json(&expected) {
                Ok(())
            } else {
                Err("response differs from the compact schema overview".into())
            }
        }
        (SchemaExpect::Node(name), PreparedStatement::Schema(response)) => {
            let nodes: Vec<_> = response
                .domains
                .iter()
                .flat_map(|domain| &domain.nodes)
                .collect();
            let expanded = matches!(
                nodes.as_slice(),
                [SchemaNode::Expanded { name: found, props, .. }] if found == name && !props.is_empty()
            );
            if expanded && response.domains.len() == 1 && response.edges.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "expected one domain with only {name} and its properties, and no edges: {}",
                    to_json(&response)
                ))
            }
        }
    }
}

fn to_json(value: &impl serde::Serialize) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}
