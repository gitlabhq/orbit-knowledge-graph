use std::path::Path;

use query_data_model::catalog::{Engine, IndexKind, RowFilter};
use query_data_model::{Catalog, DataModelError, Direction, Endpoint};

fn package_sources(packages: &[&str]) -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/packages");
    let mut sources = Vec::new();
    for package in packages {
        let mut files: Vec<_> = std::fs::read_dir(root.join(package))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        files.sort();
        for file in files {
            let name = format!("{package}/{}", file.file_name().unwrap().to_string_lossy());
            sources.push((name, std::fs::read_to_string(file).unwrap()));
        }
    }
    sources
}

fn load(packages: &[&str]) -> Result<Catalog, DataModelError> {
    load_with(packages, |_, yaml| yaml)
}

fn load_with(
    packages: &[&str],
    edit: impl Fn(&str, String) -> String,
) -> Result<Catalog, DataModelError> {
    let sources: Vec<_> = package_sources(packages)
        .into_iter()
        .map(|(name, yaml)| {
            let edited = edit(&name, yaml);
            (name, edited)
        })
        .collect();
    Catalog::from_yaml(
        sources
            .iter()
            .map(|(name, yaml)| (name.as_str(), yaml.as_str())),
    )
}

fn spec_errors(result: Result<Catalog, DataModelError>) -> Vec<String> {
    match result {
        Err(DataModelError::Spec(errors)) => errors.iter().map(ToString::to_string).collect(),
        other => panic!("expected spec errors, got {other:?}"),
    }
}

#[test]
fn platform_integration_and_custom_packages_compose_into_one_graph() {
    let catalog = load(&["gitlab", "linear", "agent_memory"]).unwrap();
    let graph = catalog.graph();
    let in_project = catalog.relationship("IN_PROJECT").unwrap();
    let memory = catalog.entity("AgentMemory").unwrap();
    let project = catalog.entity("Project").unwrap();

    assert_eq!(graph.entity(memory).package, "tenant.agent_memory");
    let hops = catalog
        .connect(memory, &[in_project], Some(project), Direction::Outgoing)
        .unwrap();
    assert_eq!(hops.len(), 1);

    let issue = catalog.entity("LinearIssue").unwrap();
    let request = catalog.entity("MergeRequest").unwrap();
    let both = catalog
        .connect(issue, &[], Some(request), Direction::Both)
        .unwrap();
    let names: Vec<_> = both
        .iter()
        .map(|hop| graph.relationship(hop.relationship).name.as_str())
        .collect();
    assert_eq!(names, ["RELATES_TO", "CLOSES"]);
}

#[test]
fn storage_bindings_resolve_tables_columns_and_routes_at_load() {
    let catalog = load(&["gitlab", "linear", "agent_memory"]).unwrap();
    let graph = catalog.graph();
    let store = catalog.store("graph").unwrap();
    let request = catalog.entity("MergeRequest").unwrap();
    let title = catalog.property(request, "title").unwrap().id;
    let diff = catalog.property(request, "diff").unwrap().id;

    assert_eq!(store.engine, Engine::Clickhouse);
    assert_eq!(store.source(request).unwrap().table, "gl_merge_request");
    assert_eq!(store.column(title).unwrap().indexes, [IndexKind::Text]);
    assert!(store.column(diff).is_none());

    let gitaly = catalog.store("gitaly").unwrap();
    let lookup = gitaly.lookup(diff).unwrap();
    assert_eq!(lookup.name, "mr_raw_patch");
    let keys: Vec<_> = lookup
        .keys
        .iter()
        .map(|key| graph.property(*key).name.as_str())
        .collect();
    assert_eq!(keys, ["project_id", "iid"]);
    let stores: Vec<_> = catalog
        .realizations(diff)
        .map(|(store, _)| store.name.as_str())
        .collect();
    assert_eq!(stores, ["gitaly"]);

    let authored = graph
        .variant_named("AUTHORED", "User", "MergeRequest")
        .unwrap();
    let route = store.route(authored.id).unwrap();
    let key = route.foreign_key.unwrap();
    assert_eq!(route.table, "gl_edge");
    assert_eq!(key.holder, Endpoint::Target);
    assert_eq!(graph.property(key.property).name, "author_id");
}

#[test]
fn third_party_rows_keep_their_own_column_names_in_the_lake() {
    let catalog = load(&["gitlab", "linear"]).unwrap();
    let issue = catalog.entity("LinearIssue").unwrap();
    let body = catalog.property(issue, "body").unwrap().id;
    let lake = catalog.store("lake").unwrap();
    let source = lake.source(issue).unwrap();

    assert_eq!(source.table, "linear_tickets");
    assert_eq!(source.key, "ticket_id");
    assert_eq!(lake.column(body).unwrap().name, "ticket_body");
    let tables: Vec<_> = catalog
        .stores()
        .iter()
        .filter_map(|store| Some((store.name.as_str(), store.source(issue)?.table.as_str())))
        .collect();
    assert_eq!(
        tables,
        [("graph", "gl_linear_issue"), ("lake", "linear_tickets")]
    );
}

#[test]
fn guards_follow_path_columns_or_reach_one_through_the_graph() {
    let catalog = load(&["gitlab", "linear", "agent_memory"]).unwrap();
    let graph = catalog.graph();
    let store = catalog.store("graph").unwrap();

    let issue = store.guard(catalog.entity("LinearIssue").unwrap()).unwrap();
    assert_eq!(
        issue.filter,
        RowFilter::Path {
            column: "traversal_path".into()
        }
    );
    assert_eq!(issue.min_role, Some(40));
    assert!(!issue.redacted);

    let lake = catalog.store("lake").unwrap();
    let issue = lake.guard(catalog.entity("LinearIssue").unwrap()).unwrap();
    assert_eq!(
        issue.filter,
        RowFilter::Path {
            column: "trav_path".into()
        }
    );

    let memory = store.guard(catalog.entity("AgentMemory").unwrap()).unwrap();
    let RowFilter::PathThrough { hops, column } = &memory.filter else {
        panic!("expected a path through the graph, got {memory:?}");
    };
    assert_eq!(column, "traversal_path");
    assert_eq!(hops.len(), 1);
    assert_eq!(graph.entity(hops[0].to).name, "Project");
    assert_eq!(memory.min_role, Some(30));

    let user = store.guard(catalog.entity("User").unwrap()).unwrap();
    assert_eq!(user.filter, RowFilter::None);
}

#[test]
fn an_entity_that_cannot_be_authorized_fails_the_load() {
    let errors = spec_errors(load_with(&["gitlab", "agent_memory"], |name, yaml| {
        if name == "gitlab/storage.yaml" {
            yaml.replace("  IN_PROJECT: { fk: project_id }\n", "")
        } else {
            yaml
        }
    }));
    assert_eq!(
        errors,
        [
            "tenant.agent_memory: AgentMemory in store graph has no namespace path column, reaches no entity that has one, and has no redaction rule"
        ]
    );
}

#[test]
fn every_entity_needs_an_authorization_entry() {
    let errors = spec_errors(load_with(&["gitlab", "linear"], |name, yaml| {
        if name == "linear/authorization.yaml" {
            yaml.replace(
                "entities:\n  LinearIssue: { scope: namespace, role: maintainer }\n",
                "",
            )
        } else {
            yaml
        }
    }));
    assert_eq!(
        errors,
        ["integration.linear: entity LinearIssue has no authorization entry"]
    );
}

#[test]
fn packages_cannot_redeclare_another_package_entity() {
    let errors = spec_errors(load_with(&["gitlab", "linear"], |name, yaml| {
        if name == "linear/graph.yaml" {
            yaml.replace("  LinearIssue:\n", "  MergeRequest:\n")
        } else {
            yaml
        }
    }));
    assert!(
        errors.contains(
            &"integration.linear: entity MergeRequest is already declared by gitlab".to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn every_bad_reference_is_reported_in_one_load() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/storage.yaml" => yaml.replace("title: text", "titel: text"),
        "gitlab/graph.yaml" => yaml.replace("User -> Note]", "User -> Notes]"),
        "gitlab/authorization.yaml" => yaml.replace(
            "role: reporter, redaction: { resource: note",
            "role: viewer, redaction: { resource: note",
        ),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "gitlab: AUTHORED variant `User -> Notes` names an unknown entity",
            "gitlab: Note has unknown role `viewer`",
            "gitlab: store graph names unknown property MergeRequest.titel",
        ]
    );
}

#[test]
fn frontend_errors_name_the_alternatives() {
    let catalog = load(&["gitlab", "linear"]).unwrap();
    let user = catalog.entity("User").unwrap();
    let project = catalog.entity("Project").unwrap();
    let authored = catalog.relationship("AUTHORED").unwrap();

    let error = catalog
        .connect(user, &[authored], Some(project), Direction::Outgoing)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "no AUTHORED relationship connects User to Project; these do: "
    );
    let request = catalog.entity("MergeRequest").unwrap();
    let has_note = catalog.relationship("HAS_NOTE").unwrap();
    let error = catalog
        .connect(user, &[has_note], Some(request), Direction::Outgoing)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "no HAS_NOTE relationship connects User to MergeRequest; these do: AUTHORED"
    );
    let error = catalog.entity("Linear").unwrap_err();
    assert_eq!(
        error.to_string(),
        "unknown entity Linear; similar: LinearIssue"
    );
}

#[test]
fn catalog_identity_changes_with_the_spec() {
    let first = load(&["gitlab"]).unwrap();
    let again = load(&["gitlab"]).unwrap();
    let extended = load(&["gitlab", "linear"]).unwrap();
    assert_eq!(first.id(), again.id());
    assert_ne!(first.id(), extended.id());
}

#[test]
fn a_search_store_binds_only_the_fields_it_indexes() {
    let catalog = load(&["gitlab"]).unwrap();
    let note = catalog.entity("Note").unwrap();
    let search = catalog.store("note_search").unwrap();
    let body = catalog.property(note, "body").unwrap().id;
    let author = catalog.property(note, "author_id").unwrap().id;

    assert_eq!(search.engine, Engine::Search);
    assert_eq!(search.source(note).unwrap().table, "notes");
    assert_eq!(search.column(body).unwrap().indexes, [IndexKind::Text]);
    assert!(search.column(author).is_none());
    assert_eq!(
        search.guard(note).unwrap().filter,
        RowFilter::Path {
            column: "traversal_path".into()
        }
    );
}

#[test]
fn redaction_alone_authorizes_an_entity_without_a_path() {
    let catalog = load_with(&["gitlab", "agent_memory"], |name, yaml| match name {
        "gitlab/storage.yaml" => yaml.replace("  IN_PROJECT: { fk: project_id }\n", ""),
        "agent_memory/authorization.yaml" => yaml.replace(
            "{ scope: namespace, role: developer }",
            "{ scope: namespace, role: developer, redaction: { resource: agent_memory, ability: read_agent_memory } }",
        ),
        _ => yaml,
    })
    .unwrap();
    let memory = catalog.entity("AgentMemory").unwrap();
    let guard = catalog.store("graph").unwrap().guard(memory).unwrap();
    assert_eq!(guard.filter, RowFilter::None);
    assert!(guard.redacted);
    assert_eq!(guard.min_role, Some(30));
}

#[test]
fn only_namespace_anchors_carry_a_path_to_another_entity() {
    let errors = spec_errors(load_with(&["gitlab", "agent_memory"], |name, yaml| {
        if name == "gitlab/authorization.yaml" {
            yaml.replace("IN_PROJECT: namespace_anchor", "IN_PROJECT: same_namespace")
        } else {
            yaml
        }
    }));
    assert_eq!(
        errors,
        [
            "tenant.agent_memory: AgentMemory in store graph has no namespace path column, reaches no entity that has one, and has no redaction rule"
        ]
    );
}

#[test]
fn a_package_cannot_loosen_another_package_access_rule() {
    let errors = spec_errors(load_with(&["gitlab", "linear"], |name, yaml| {
        if name == "linear/authorization.yaml" {
            yaml.replace("properties:\n", "properties:\n  User.email: {}\n")
                .replace(
                    "relationships:\n",
                    "relationships:\n  AUTHORED: same_namespace\n",
                )
        } else {
            yaml
        }
    }));
    assert_eq!(
        errors,
        [
            "integration.linear: AUTHORED has two scope entries",
            "integration.linear: User.email has two access entries",
        ]
    );
}

#[test]
fn string_enums_keep_their_declared_values() {
    let catalog = load(&["gitlab", "linear"]).unwrap();
    let issue = catalog.entity("LinearIssue").unwrap();
    let state = catalog.property(issue, "state").unwrap();
    assert_eq!(state.enum_type, ontology::EnumType::String);
    assert_eq!(state.enum_values.as_ref().unwrap()[&3], "done");
}
