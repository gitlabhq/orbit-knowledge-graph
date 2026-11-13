//! The loader reads backend capabilities and field lists, never a backend kind's name: a store
//! sets only the fragment fields its backend reads, only writer stores take a writer, `via` is
//! for backends without joins, and a delegated check needs a delegating store.

use std::collections::BTreeMap;
use std::sync::Arc;

use query_data_model::catalog::Check;
use query_data_model::{DataModelError, Model, Packages, testkit};

/// The demo deployment with each `(file, find, replace)` edit applied to the one occurrence of
/// `find` in the package file `file`.
fn demo(edits: &[(&str, &str, &str)]) -> Result<Arc<Model>, DataModelError> {
    let packages = testkit::with_demo();
    let mut sources = packages.sources().clone();
    for (file, find, replace) in edits {
        let source = sources.get_mut(*file).unwrap();
        assert_eq!(
            source.matches(find).count(),
            1,
            "{file} holds {find:?} once"
        );
        *source = source.replacen(find, replace, 1);
    }
    let mut tiers = BTreeMap::new();
    for (path, text) in sources {
        let tier = packages.tiers()[path.split('/').next().unwrap()];
        tiers
            .entry(tier)
            .or_insert_with(BTreeMap::new)
            .insert(path, text);
    }
    tiers
        .into_iter()
        .map(|(tier, sources)| Packages::new(tier, sources))
        .reduce(Packages::with)
        .unwrap()
        .deployment("demo")
}

fn errors(edits: &[(&str, &str, &str)]) -> Vec<String> {
    match demo(edits) {
        Err(DataModelError::Spec(errors)) => errors.iter().map(ToString::to_string).collect(),
        other => panic!("expected spec errors, got {other:?}"),
    }
}

const CODE: &str = "gitlab.source_code/storage/orbit_code.yaml";
const LIVE: &str = "integration.slack/storage/live.yaml";
const SLACK_AUTHORIZATION: &str = "integration.slack/authorization.yaml";
const INCIDENTS: &str = "integration.slack/sources/incident_sync.yaml";

#[test]
fn a_code_store_column_with_a_selectivity_fails_the_load() {
    assert_eq!(
        errors(&[(CODE, "      path: {}", "      path: {selectivity: low}")]),
        [
            "gitlab.source_code: store code sets selectivity on File.path, which orbit_code stores do not read"
        ]
    );
}

#[test]
fn a_live_store_with_access_hints_fails_the_load() {
    assert_eq!(
        errors(&[(
            LIVE,
            "    backend:\n      operation: search.messages",
            "    access: {order: [id]}\n    backend:\n      operation: search.messages",
        )]),
        [
            "integration.slack: store slack sets access on SlackMessage, which live stores do not read"
        ]
    );
}

#[test]
fn a_source_that_writes_an_entity_only_a_live_store_holds_fails_the_load() {
    assert_eq!(
        errors(&[(
            INCIDENTS,
            "entities: [Incident]",
            "entities: [Incident, SlackMessage]"
        )]),
        [
            "integration.slack: incident_sync writes entity SlackMessage, whose stores slack take no writer"
        ]
    );
}

#[test]
fn an_entity_of_a_writer_store_without_a_writer_fails_the_load() {
    let errors = errors(&[(INCIDENTS, "entities: [Incident]", "entities: []")]);
    assert!(
        errors.contains(&"integration.slack: stored entity Incident has no writer".to_string()),
        "{errors:?}"
    );
}

#[test]
fn via_fails_the_load_on_a_backend_with_joins() {
    let errors = errors(&[(
        SLACK_AUTHORIZATION,
        "enforce: {clickhouse: {scope_key: traversal_path}}\n    properties:\n      open: [id, title, message_id]",
        "enforce: {clickhouse: {scope_key: {via: RAISED_IN}}}\n    properties:\n      open: [id, title, message_id]",
    )]);
    assert!(
        errors.contains(
            &"integration.slack: clickhouse cannot enforce Incident's Via policy".to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn a_delegated_check_on_an_entity_no_delegating_store_holds_fails_the_load() {
    assert_eq!(
        errors(&[(
            SLACK_AUTHORIZATION,
            "    properties:\n      open: [id, title, message_id]",
            "    checks: [{provider: slack, kind: delegated}]\n    properties:\n      open: [id, title, message_id]",
        )]),
        [
            "integration.slack: Incident has a delegated check, but no store on a backend that reads with the caller's sign-in holds it"
        ]
    );
}

#[test]
fn a_delegated_check_is_answered_only_on_the_delegating_connection() {
    let model = demo(&[]).unwrap();
    let message = model.names().entity("SlackMessage").unwrap();
    let delegated = |connection: &str| {
        let connection = model.names().connection(connection).unwrap();
        model
            .checks(message, connection)
            .any(|check| matches!(check, Check::Delegated { .. }))
    };
    assert!(delegated("data_extraction"));
    assert!(!delegated("main"));
}

#[test]
fn a_push_down_or_search_names_a_declared_fact() {
    assert_eq!(
        errors(&[(
            LIVE,
            "filters: [id, channel, author, sent_at, merge_request_id]",
            "filters: [id, channel, author, sent_at, merge_request_id, thread]",
        )]),
        ["integration.slack: store slack pushes down SlackMessage.thread, which it does not store"]
    );
    assert_eq!(
        errors(&[(CODE, "path: [trigram]}", "body: [trigram]}")]),
        ["gitlab.source_code: store code searches unknown property File.body"]
    );
}
