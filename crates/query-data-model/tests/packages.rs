use std::sync::Arc;

use query_data_model::catalog::{RowFilter, Scope};
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::{Endpoint, Model, Packages};

fn model() -> Arc<Model> {
    Packages::embedded().load().unwrap()
}

#[test]
fn graph_store_entities_filter_on_their_own_path_column() {
    let model = model();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    for entity in model.graph().entities() {
        let guard = store.guard(entity.id).unwrap();
        let expected = match model.access().entity(entity.id).unwrap().scope {
            Scope::Global => RowFilter::None,
            Scope::Namespace => RowFilter::Path {
                column: "traversal_path".into(),
            },
        };
        assert_eq!(guard.filter, expected, "{}", entity.name);
    }
}

#[test]
fn code_edges_copy_endpoint_project_and_branch_in_the_graph_store_only() {
    let model = model();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let copy = |relationship: &str, source: &str, target: &str, endpoint: Endpoint| {
        let variant = graph.variant_named(relationship, source, target).unwrap();
        let entity = match endpoint {
            Endpoint::Source => variant.source,
            Endpoint::Target => variant.target,
        };
        let branch = graph.property_id(entity, "branch")?;
        store.copy(variant.id, endpoint, branch)
    };
    assert_eq!(
        copy("CALLS", "Definition", "Definition", Endpoint::Source),
        Some("branch")
    );
    assert_eq!(
        copy("CALLS", "Definition", "Definition", Endpoint::Target),
        Some("branch")
    );
    assert_eq!(
        copy("CONTAINS", "Directory", "File", Endpoint::Target),
        Some("branch")
    );
    assert_eq!(copy("CONTAINS", "Branch", "File", Endpoint::Source), None);
    assert!(
        graph
            .relationships()
            .all(|relationship| relationship.attributes.is_empty())
    );
    let copies_nothing = |store: &query_data_model::catalog::Store| {
        store
            .bindings()
            .all(|(_, _, binding)| binding.copies.is_empty())
    };
    assert!(
        model
            .stores()
            .iter()
            .filter(|store| store.name != "graph")
            .all(copies_nothing)
    );
}

fn load_error(entry: &str) -> String {
    let mut sources = Packages::embedded().sources().clone();
    let yaml = format!("kind: source\npackage: gitlab.extra\nsource: siphon\n{entry}");
    sources.insert("gitlab.extra/source.yaml".into(), yaml);
    Packages::new(sources).load().unwrap_err().to_string()
}

#[test]
fn source_fragments_reject_unknown_names() {
    let entity = "entities:\n  Nope: {tables: [t], order_by: [id], query: generated}\n";
    assert!(load_error(entity).contains("unknown entity Nope"));
    let property =
        "entities:\n  File: {tables: [t], order_by: [id], query: generated, columns: {nope: x}}\n";
    assert!(load_error(property).contains("unknown property File.nope"));
    let template = "entities:\n  File: {tables: [t], order_by: [id], query: missing.sql.j2}\n";
    assert!(load_error(template).contains("unknown template missing.sql.j2"));
    let variant = "relationships:\n  AUTHORED:\n    pipelines:\n      AUTHORED_t:\n        \
                   tables: [t]\n        order_by: [id]\n        query: generated\n        \
                   from: {field: a, kind: Note}\n        to: {field: b, kind: Note}\n";
    assert!(load_error(variant).contains("unknown variant Note -[AUTHORED]-> Note"));
}
