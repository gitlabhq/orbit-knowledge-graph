use std::path::Path;

use query_data_model::catalog::RowFilter;
use query_data_model::implementations::clickhouse::backend::IndexKind;
use query_data_model::implementations::clickhouse::layout::ClickHouse;
use query_data_model::implementations::duckdb::layout::DuckDb;
use query_data_model::implementations::search::Search;
use query_data_model::{DataModelError, Direction, Endpoint, Model};

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

fn load(packages: &[&str]) -> Result<std::sync::Arc<Model>, DataModelError> {
    load_with(packages, |_, yaml| yaml)
}

fn load_with(
    packages: &[&str],
    edit: impl Fn(&str, String) -> String,
) -> Result<std::sync::Arc<Model>, DataModelError> {
    let sources: Vec<_> = package_sources(packages)
        .into_iter()
        .map(|(name, yaml)| {
            let edited = edit(&name, yaml);
            (name, edited)
        })
        .collect();
    Model::from_yaml(
        sources
            .iter()
            .map(|(name, yaml)| (name.as_str(), yaml.as_str())),
    )
    .map(std::sync::Arc::new)
}

fn spec_errors<T: std::fmt::Debug>(result: Result<T, DataModelError>) -> Vec<String> {
    match result {
        Err(DataModelError::Spec(errors)) => errors.iter().map(ToString::to_string).collect(),
        other => panic!("expected spec errors, got {other:?}"),
    }
}

#[test]
fn platform_integration_and_custom_packages_compose_into_one_graph() {
    let model = load(&["gitlab", "linear", "agent_memory"]).unwrap();
    let graph = model.graph();
    let in_project = model.relationship("IN_PROJECT").unwrap();
    let memory = model.entity("AgentMemory").unwrap();
    let project = model.entity("Project").unwrap();

    assert_eq!(graph.entity(memory).package, "tenant.agent_memory");
    let hops = model
        .connect(memory, &[in_project], Some(project), Direction::Outgoing)
        .unwrap();
    assert_eq!(hops.len(), 1);

    let issue = model.entity("LinearIssue").unwrap();
    let request = model.entity("MergeRequest").unwrap();
    let both = model
        .connect(issue, &[], Some(request), Direction::Both)
        .unwrap();
    let names: Vec<_> = both
        .iter()
        .map(|hop| graph.relationship(hop.relationship).name.as_str())
        .collect();
    assert_eq!(names, ["RELATES_TO", "CLOSES"]);
}

#[test]
fn a_clickhouse_catalog_resolves_tables_columns_and_routes() {
    let model = load(&["gitlab", "linear", "agent_memory"]).unwrap();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let facts = catalog.backend("graph").unwrap();
    let request = model.entity("MergeRequest").unwrap();
    let title = model.property(request, "title").unwrap().id;
    let diff = model.property(request, "diff").unwrap().id;

    assert_eq!(store.source(request).unwrap().table, "gl_merge_request");
    assert_eq!(facts.column(title).unwrap().indexes, [IndexKind::Text]);
    assert_eq!(
        facts.relational.versioning.as_ref().unwrap().version,
        "_version"
    );
    assert!(store.column(diff).is_none());

    let authored = graph
        .variant_named("AUTHORED", "User", "MergeRequest")
        .unwrap();
    let routes: Vec<_> = catalog.routes(authored.id).collect();
    assert_eq!(routes.len(), 1);
    let edge = routes[0].edge.unwrap();
    let key = routes[0].foreign_key.unwrap();
    assert_eq!((edge.store, edge.table), ("graph", "gl_edge"));
    assert_eq!(key.store, "graph");
    assert_eq!(key.key.holder, Endpoint::Target);
    assert_eq!(graph.property(key.key.property).name, "author_id");
}

#[test]
fn virtual_columns_come_from_the_lookup_registry() {
    let model = load(&["gitlab"]).unwrap();
    let request = model.entity("MergeRequest").unwrap();
    let diff = model.property(request, "diff").unwrap().id;
    let lookup = model.lookup(diff).unwrap();
    let keys: Vec<_> = lookup
        .keys
        .iter()
        .map(|key| model.graph().property(*key).name.as_str())
        .collect();

    assert_eq!(
        (lookup.service.as_str(), lookup.operation.as_str()),
        ("gitaly", "mr_raw_patch")
    );
    assert_eq!(keys, ["project_id", "iid"]);
    assert!(
        model
            .stores()
            .iter()
            .all(|store| store.column(diff).is_none())
    );
}

#[test]
fn one_query_can_join_stores_on_the_same_connection() {
    let model = load(&["gitlab", "linear"]).unwrap();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let issue = model.entity("LinearIssue").unwrap();
    let body = model.property(issue, "body").unwrap().id;

    let tables: Vec<_> = catalog
        .sources(issue)
        .map(|(store, source)| (store.name.as_str(), source.table.as_str()))
        .collect();
    assert_eq!(
        tables,
        [("graph", "gl_linear_issue"), ("lake", "linear_tickets")]
    );
    let lake = catalog.store("lake").unwrap();
    assert_eq!(lake.source(issue).unwrap().key, "ticket_id");
    assert_eq!(lake.column(body), Some("ticket_body"));

    let relates = graph
        .variant_named("RELATES_TO", "LinearIssue", "MergeRequest")
        .unwrap();
    let routes: Vec<_> = catalog.routes(relates.id).collect();
    let edges: Vec<_> = routes.iter().filter_map(|route| route.edge).collect();
    let keys: Vec<_> = routes
        .iter()
        .filter_map(|route| route.foreign_key)
        .collect();
    assert_eq!(edges.len(), 1);
    assert_eq!((edges[0].store, edges[0].table), ("graph", "gl_edge"));
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].store, "lake");
    assert_eq!(keys[0].key.holder, Endpoint::Source);
    assert_eq!(
        graph.property(keys[0].key.property).name,
        "merge_request_id"
    );
}

#[test]
fn stores_on_another_connection_are_not_joinable() {
    let model = load_with(&["gitlab", "linear"], |name, yaml| {
        if name == "linear/storage.yaml" {
            yaml.replace("connection: main", "connection: lake")
        } else {
            yaml
        }
    })
    .unwrap();
    let relates = model
        .graph()
        .variant_named("RELATES_TO", "LinearIssue", "MergeRequest")
        .unwrap();
    let main = model.physical::<ClickHouse>("main").unwrap();
    assert!(main.store("lake").is_none());
    assert!(
        main.routes(relates.id)
            .all(|route| route.foreign_key.is_none())
    );
    let lake = model.physical::<ClickHouse>("lake").unwrap();
    assert_eq!(lake.routes(relates.id).count(), 0);
}

#[test]
fn a_catalog_rejects_stores_of_another_backend() {
    let model = load(&["gitlab"]).unwrap();
    let errors = spec_errors(model.physical::<DuckDb>("main"));
    assert_eq!(
        errors,
        ["gitlab: connection main is clickhouse, not duckdb"]
    );
}

#[test]
fn backends_reject_unknown_options() {
    let model = load_with(&["gitlab"], |name, yaml| {
        if name == "gitlab/storage.yaml" {
            yaml.replace("deleted_column:", "delete_column:")
        } else {
            yaml
        }
    })
    .unwrap();
    let errors = spec_errors(model.physical::<ClickHouse>("main"));
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].starts_with("gitlab: store graph: unknown field `delete_column`"),
        "{errors:?}"
    );
}

#[test]
fn a_search_catalog_binds_only_the_fields_it_indexes() {
    let model = load(&["gitlab"]).unwrap();
    let note = model.entity("Note").unwrap();
    let body = model.property(note, "body").unwrap().id;
    let author = model.property(note, "author_id").unwrap().id;
    let catalog = model.physical::<Search>("search").unwrap();
    let store = catalog.store("note_search").unwrap();

    assert_eq!(store.source(note).unwrap().table, "notes");
    assert!(catalog.backend("note_search").unwrap().is_text(body));
    assert!(store.column(author).is_none());
    assert_eq!(
        store.guard(note).unwrap().filter,
        RowFilter::Path {
            column: "traversal_path".into()
        }
    );
}

#[test]
fn guards_follow_path_columns_or_reach_one_through_the_graph() {
    let model = load(&["gitlab", "linear", "agent_memory"]).unwrap();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let issue = model.entity("LinearIssue").unwrap();

    let guard = store.guard(issue).unwrap();
    assert_eq!(
        guard.filter,
        RowFilter::Path {
            column: "traversal_path".into()
        }
    );
    assert_eq!(guard.min_role, Some(40));
    let lake = catalog.store("lake").unwrap().guard(issue).unwrap();
    assert_eq!(
        lake.filter,
        RowFilter::Path {
            column: "trav_path".into()
        }
    );

    let memory = store.guard(model.entity("AgentMemory").unwrap()).unwrap();
    let RowFilter::PathThrough {
        hops,
        store: home,
        column,
    } = &memory.filter
    else {
        panic!("expected a path through the graph, got {memory:?}");
    };
    assert_eq!(
        (home.as_str(), column.as_str()),
        ("graph", "traversal_path")
    );
    assert_eq!(hops.len(), 1);
    assert_eq!(graph.entity(hops[0].hop.to).name, "Project");
    assert_eq!(hops[0].store, "graph");
    assert_eq!(memory.min_role, Some(30));

    let user = store.guard(model.entity("User").unwrap()).unwrap();
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
fn redaction_alone_authorizes_an_entity_without_a_path() {
    let model = load_with(&["gitlab", "agent_memory"], |name, yaml| match name {
        "gitlab/storage.yaml" => yaml.replace("  IN_PROJECT: { fk: project_id }\n", ""),
        "agent_memory/authorization.yaml" => yaml.replace(
            "{ scope: namespace, role: developer }",
            "{ scope: namespace, role: developer, redaction: { resource: agent_memory, ability: read_agent_memory } }",
        ),
        _ => yaml,
    })
    .unwrap();
    let memory = model.entity("AgentMemory").unwrap();
    let store = model
        .stores()
        .iter()
        .find(|store| store.name == "graph")
        .unwrap();
    let guard = store.guard(memory).unwrap();
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
        "gitlab/storage.yaml" => yaml.replace(
            "  User: { path_column: null, backend: { sort_key: [id] } }",
            "  User: { path_column: null, columns: { usernam: login } }",
        ),
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
            "gitlab: store graph names unknown property User.usernam",
        ]
    );
}

#[test]
fn frontend_errors_name_the_alternatives() {
    let model = load(&["gitlab", "linear"]).unwrap();
    let user = model.entity("User").unwrap();
    let request = model.entity("MergeRequest").unwrap();
    let has_note = model.relationship("HAS_NOTE").unwrap();

    let error = model
        .connect(user, &[has_note], Some(request), Direction::Outgoing)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "no HAS_NOTE relationship connects User to MergeRequest; these do: AUTHORED"
    );
    let error = model.entity("Linear").unwrap_err();
    assert_eq!(
        error.to_string(),
        "unknown entity Linear; similar: LinearIssue"
    );
}

#[test]
fn string_enums_keep_their_declared_values() {
    let model = load(&["gitlab", "linear"]).unwrap();
    let issue = model.entity("LinearIssue").unwrap();
    let state = model.property(issue, "state").unwrap();
    assert_eq!(state.enum_type, ontology::EnumType::String);
    assert_eq!(state.enum_values.as_ref().unwrap()[&3], "done");
}

#[test]
fn model_identity_changes_with_the_spec() {
    let first = load(&["gitlab"]).unwrap();
    let again = load(&["gitlab"]).unwrap();
    let extended = load(&["gitlab", "linear"]).unwrap();
    assert_eq!(first.id(), again.id());
    assert_ne!(first.id(), extended.id());
}

fn edit_one(
    target: &'static str,
    from: &'static str,
    to: &'static str,
) -> impl Fn(&str, String) -> String {
    move |name, yaml| {
        if name == target {
            yaml.replace(from, to)
        } else {
            yaml
        }
    }
}

#[test]
fn a_misspelled_relationship_key_fails_the_load() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one("gitlab/storage.yaml", "fk: project_id", "fk: projet_id"),
    ));
    assert_eq!(
        errors,
        ["gitlab: IN_PROJECT foreign key projet_id is on no endpoint of any variant"]
    );
}

#[test]
fn a_misspelled_shared_field_fails_the_load() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/storage.yaml",
            "  connection: main",
            "  conection: main",
        ),
    ));
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("unknown field `conection`"),
        "{errors:?}"
    );
}

#[test]
fn trusted_and_untrusted_stores_cannot_share_a_connection() {
    let errors = spec_errors(load_with(
        &["gitlab", "linear"],
        edit_one(
            "linear/storage.yaml",
            "  connection: main\n",
            "  connection: main\n  trusted: true\n",
        ),
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: store lake and store graph share connection main but only one is trusted"
        ]
    );
}

#[test]
fn one_connection_has_one_backend() {
    let errors = spec_errors(load_with(
        &["gitlab", "linear"],
        edit_one(
            "linear/storage.yaml",
            "  kind: clickhouse",
            "  kind: duckdb",
        ),
    ));
    assert_eq!(
        errors,
        ["integration.linear: store lake is duckdb but connection main is clickhouse"]
    );
}

#[test]
fn a_lookup_property_cannot_also_be_a_column() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/storage.yaml",
            "  MergeRequest:\n",
            "  MergeRequest:\n    columns: { diff: patch }\n",
        ),
    ));
    assert_eq!(
        errors,
        ["gitlab: store graph stores MergeRequest.diff, which a lookup provides"]
    );
}

#[test]
fn backend_annotations_need_a_stored_column() {
    let model = load_with(
        &["gitlab"],
        edit_one(
            "gitlab/storage.yaml",
            "indexes: { title: text,",
            "indexes: { diff: text, title: text,",
        ),
    )
    .unwrap();
    let errors = spec_errors(model.physical::<ClickHouse>("main"));
    assert_eq!(
        errors,
        ["gitlab: store graph MergeRequest indexes unknown column diff"]
    );
}

const MANAGED_STORE: &str = r#"    sort_key: [traversal_path, id]
    engine: ReplacingMergeTree(_version, _deleted)
    columns:
      _version: { type: "DateTime64(6, 'UTC')", default: now64(6), codec: [Delta(8), ZSTD(1)] }
      _deleted: { type: Bool, default: "false" }
    settings: { index_granularity: "1024" }
"#;

fn managed(extra: &'static str) -> impl Fn(&str, String) -> String {
    move |name, yaml| {
        if name == "gitlab/storage.yaml" {
            yaml.replace(
                "    sort_key: [traversal_path, id]\n",
                &format!("{MANAGED_STORE}{extra}"),
            )
        } else {
            yaml
        }
    }
}

#[test]
fn a_managed_store_resolves_its_physical_schema() {
    let model = load_with(&["gitlab"], managed("")).unwrap();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let schema = catalog.backend("graph").unwrap().schema().unwrap();
    let requests = schema.table("gl_merge_request").unwrap();
    let columns: Vec<_> = requests
        .columns
        .iter()
        .map(|column| format!("{} {}", column.name, column.storage_type))
        .collect();
    assert_eq!(
        columns,
        [
            "id Int64",
            "iid Int64",
            "title String",
            "state LowCardinality(String)",
            "project_id Int64",
            "author_id Nullable(Int64)",
            "traversal_path String",
            "_version DateTime64(6, 'UTC')",
            "_deleted Bool",
        ]
    );
    let indexes: Vec<_> = requests
        .indexes
        .iter()
        .map(|index| index.name.as_str())
        .collect();
    assert_eq!(
        indexes,
        [
            "idx_title",
            "idx_title_ngram",
            "idx_state",
            "idx_project_id",
            "idx_id",
            "idx_id_minmax"
        ]
    );
    let sql = requests.to_create_sql("v1_");
    assert!(sql.contains("INDEX idx_title lower(title) TYPE text(tokenizer = splitByNonAlpha)"));
    assert!(sql.contains("ENGINE = ReplacingMergeTree(_version, _deleted)"));
    assert!(sql.ends_with("ORDER BY (traversal_path, id)\nSETTINGS index_granularity = 1024"));
    assert_eq!(
        schema
            .writers("gl_merge_request")
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        ["MergeRequest"]
    );
}

#[test]
fn a_managed_store_rejects_unknown_physical_references() {
    let model = load_with(
        &["gitlab"],
        managed(
            r#"    copies: { gl_note_by_id: { source: gl_missing, sort_key: [id] } }
    tables:
      audit: { engine: MergeTree, columns: { at: {} }, indexes: { actor: minmax } }
    views:
      broken: { select: "SELECT 1" }
      misrouted: { select: "SELECT 1", to: gl_missing }
"#,
        ),
    )
    .unwrap();
    let errors = spec_errors(model.physical::<ClickHouse>("main"));
    assert_eq!(
        errors,
        [
            "gitlab: copy gl_note_by_id names unknown table gl_missing",
            "gitlab: store graph table audit column at declares no type",
            "gitlab: store graph table audit indexes unknown column actor",
            "gitlab: store graph view broken names neither a destination table nor an engine",
            "gitlab: store graph view misrouted writes unknown table gl_missing",
        ]
    );
}
