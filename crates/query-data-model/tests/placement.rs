//! Placement: a read runs on the entity's complete store when that serves every need, else on
//! the one partial store that does, and the loader rejects two partial stores that could both
//! serve what the complete store cannot.

use std::sync::Arc;

use query_data_model::catalog::{Needs, Operation, SearchMode, Unplaced};
use query_data_model::{Model, Packages, PropertyId, Tier, testkit};

fn demo() -> Arc<Model> {
    testkit::with_demo().deployment("demo").unwrap()
}

fn file(model: &Model, name: &str) -> PropertyId {
    let file = model.names().entity("File").unwrap();
    model.names().property(file, name).unwrap()
}

/// A read of `File` that filters on its project, reads `read`, and searches `searched`.
fn place(model: &Model, read: &str, searched: Option<&str>) -> Result<String, Unplaced> {
    let mut needs = Needs::default();
    needs.apply(file(model, "project_id"), Operation::Equal);
    needs.read(file(model, read));
    if let Some(searched) = searched {
        needs.apply(
            file(model, searched),
            Operation::Search(SearchMode::Structural),
        );
    }
    let entity = model.names().entity("File").unwrap();
    let store = model.get_store(entity, &needs)?;
    Ok(model.store(store).name().to_string())
}

#[test]
fn a_read_of_rows_runs_on_the_complete_store() {
    assert_eq!(place(&demo(), "path", None).unwrap(), "graph");
}

#[test]
fn a_structural_match_runs_on_the_partial_store_that_searches() {
    assert_eq!(place(&demo(), "path", Some("content")).unwrap(), "code");
}

#[test]
fn a_need_no_store_serves_names_the_entity_and_the_need() {
    let model = demo();
    assert_eq!(
        place(&model, "path", Some("name")).unwrap_err().to_string(),
        "no store serves a structural search on File.name"
    );
    assert_eq!(
        place(&model, "name", Some("content"))
            .unwrap_err()
            .to_string(),
        "no one store of File serves both a structural search on File.content and a read of File.name"
    );
}

#[test]
fn an_entity_no_served_store_holds_is_unstored() {
    let model = testkit::with_demo().deployment("server").unwrap();
    let message = model.names().entity("SlackMessage").unwrap();
    assert_eq!(
        model
            .get_store(message, &Needs::default())
            .unwrap_err()
            .to_string(),
        "entity 'SlackMessage' is not available in this data model"
    );
}

/// The demo deployment with a second code store on the code connection, which searches `search`.
fn with_mirror(search: &str) -> Result<Arc<Model>, String> {
    let mut sources = std::collections::BTreeMap::new();
    sources.insert(
        "gitlab.extra/package.yaml".into(),
        "kind: package\nname: gitlab.extra\nrequires: [gitlab, gitlab.core, gitlab.source_code]\n"
            .into(),
    );
    sources.insert(
        "gitlab.extra/storage/orbit_code.yaml".into(),
        format!(
            "kind: storage\npackage: gitlab.extra\nstore: mirror\nengine:\n  kind: orbit_code\n  connection: code_indexing\nentities:\n  File:\n    accepts: [project_id, branch]\n    columns:\n      project_id: {{immutable: true}}\n      branch: {{immutable: true}}\n      path: {{immutable: true}}\n    backend:\n      repository: {{project: project_id, branch: branch}}\n      search: {search}\n"
        ),
    );
    testkit::with_demo()
        .with(Packages::new(Tier::Platform, sources))
        .deployment("demo")
        .map_err(|error| error.to_string())
}

#[test]
fn two_partial_stores_that_both_serve_a_search_fail_the_load() {
    let error = with_mirror("{content: [structural]}").unwrap_err();
    assert!(
        error.contains(
            "both serve a structural search on File.content, which the complete store graph does not; placement cannot choose between them"
        ),
        "{error}"
    );
    let error = with_mirror("{path: [trigram]}").unwrap_err();
    assert!(
        error.contains(
            "both serve a trigram search on File.path, which the complete store graph does not"
        ),
        "{error}"
    );
}

#[test]
fn two_partial_stores_that_serve_different_searches_load() {
    let model = with_mirror("{name: [structural]}").unwrap();
    assert_eq!(place(&model, "path", Some("name")).unwrap(), "mirror");
    assert_eq!(place(&model, "path", Some("content")).unwrap(), "code");
}
