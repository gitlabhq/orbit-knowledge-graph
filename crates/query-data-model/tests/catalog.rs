mod support;

use query_data_model::EdgeField;
use query_data_model::catalog::Rule;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::implementations::relational::Target;
use query_data_model::{DataModelError, Direction, Endpoint, Model, Packages};
use support::{demo, load_with};

fn load(packages: &[&str]) -> Result<std::sync::Arc<Model>, DataModelError> {
    load_with(packages, |_, yaml| yaml)
}

/// The gitlab fixture with `drop` removed from its siphon source, and `writer` added as a
/// source that lists the rows it writes.
fn with_writers(drop: &[&str], writer: &str) -> Result<std::sync::Arc<Model>, DataModelError> {
    let mut sources = support::packages(&["gitlab"], |name, yaml| match name {
        "gitlab/sources/siphon.yaml" => drop
            .iter()
            .fold(yaml, |yaml, line| yaml.replace(&format!("{line}\n"), "")),
        _ => yaml,
    })
    .sources()
    .clone();
    if !writer.is_empty() {
        let fragment = format!("kind: source\npackage: gitlab\n{writer}");
        sources.insert("gitlab/sources/writer.yaml".into(), fragment);
    }
    Packages::new(query_data_model::Tier::Platform, sources).deployment("server")
}

const USER_PIPELINE: &str = "  User: {tables: [users], order_by: [id], query: generated}";
const NOTE_IN_PROJECT: &str = "    - {label: IN_PROJECT, from: {field: id, kind: Note}, to: {field: project_id, kind: Project}}";
const NOTE_HAS_NOTE: &str = "    - {label: HAS_NOTE, from: {field: noteable_id, kind: MergeRequest}, to: {field: id, kind: Note}}";

#[test]
fn every_stored_entity_and_edge_table_variant_needs_a_writer() {
    let errors = spec_errors(with_writers(
        &[USER_PIPELINE, NOTE_IN_PROJECT, NOTE_HAS_NOTE],
        "",
    ));
    assert_eq!(
        errors,
        [
            "gitlab: stored entity User has no writer",
            "gitlab: IN_PROJECT(Note -> Project) in edge table gl_edge has no writer",
            "gitlab: HAS_NOTE(MergeRequest -> Note) in edge table gl_edge has no writer",
        ]
    );
}

#[test]
fn an_entity_or_edge_table_variant_has_at_most_one_writer() {
    let errors = spec_errors(with_writers(
        &[],
        "source: indexer\nentities: [User]\nrelationships:\n  HAS_NOTE: [MergeRequest -> Note]\n",
    ));
    assert_eq!(
        errors,
        [
            "gitlab: entity User has two writers: User and indexer",
            "gitlab: HAS_NOTE(MergeRequest -> Note) has two writers: Note and indexer",
        ]
    );
}

#[test]
fn a_foreign_key_variant_is_written_by_the_writer_of_its_node() {
    let errors = spec_errors(with_writers(
        &[NOTE_IN_PROJECT],
        "source: indexer\nrelationships:\n  IN_PROJECT: [Note -> Project]\n",
    ));
    assert_eq!(
        errors,
        [
            "gitlab: foreign-key variant IN_PROJECT(Note -> Project) is written by indexer, not by the writer of Note"
        ]
    );
}

#[test]
fn a_source_role_names_a_declared_entity_relationship_or_lake_table() {
    let errors = spec_errors(with_writers(
        &[],
        "source: indexer\nroles:\n  entities: {person: Persn}\n  relationships: {wrote: AUTHORED}\n  tables: {rows: users}\n",
    ));
    assert_eq!(
        errors,
        [
            "gitlab: source indexer declares table roles but no connection",
            "gitlab: source indexer role person names unknown Persn",
            "gitlab: source indexer is never declared",
        ]
    );
}

#[test]
fn a_source_that_lists_its_rows_declares_no_pipelines() {
    let errors = spec_errors(with_writers(
        &[],
        "source: siphon\nentities: [Wiki]\nrelationships:\n  CONTAINS: [Group -> Wiki]\n",
    ));
    assert_eq!(
        errors,
        [
            "gitlab: source siphon lists the rows it writes, so it declares no pipelines",
            "gitlab: source siphon writes unknown entity Wiki",
            "gitlab: source siphon writes unknown variant CONTAINS(Group -> Wiki)",
        ]
    );
}

fn spec_errors<T: std::fmt::Debug>(result: Result<T, DataModelError>) -> Vec<String> {
    match result {
        Err(DataModelError::Spec(errors)) => errors.iter().map(ToString::to_string).collect(),
        other => panic!("expected spec errors, got {other:?}"),
    }
}

#[test]
fn platform_and_tenant_packages_compose_into_one_graph() {
    let model = load(&["gitlab", "tenant.agent_memory"]).unwrap();
    let graph = model.graph();
    let in_project = model.names().relationship("IN_PROJECT").unwrap();
    let memory = model.names().entity("AgentMemory").unwrap();
    let project = model.names().entity("Project").unwrap();

    assert_eq!(graph.entity(memory).package, "tenant.agent_memory");
    let hops = model
        .connect(memory, &[in_project], Some(project), Direction::Outgoing)
        .unwrap();
    assert_eq!(hops.len(), 1);
}

#[test]
fn a_clickhouse_catalog_resolves_tables_columns_and_routes() {
    let model = load(&["gitlab", "tenant.agent_memory"]).unwrap();
    let graph = model.graph();
    let names = model.names();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let store = catalog
        .model()
        .store(catalog.names().store("graph").unwrap());
    let facts = catalog
        .facts(catalog.names().store("graph").unwrap())
        .unwrap();
    let request = model.names().entity("MergeRequest").unwrap();
    let title = model.names().property(request, "title").unwrap();
    let diff = model.names().property(request, "diff").unwrap();

    assert_eq!(
        store.source(request).unwrap().table.as_deref(),
        Some("gl_merge_request")
    );
    assert!(catalog.text_index(title));
    assert_eq!(
        facts.relational.versioning.as_ref().unwrap().version,
        "_version"
    );
    assert!(store.column(diff).is_none());

    let authored = graph.variant(names.variant("AUTHORED", "User", "MergeRequest").unwrap());
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
    let request = model.names().entity("MergeRequest").unwrap();
    let diff = model.names().property(request, "diff").unwrap();
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
    assert!(query_data_model::testkit::stores(&model).all(|store| store.column(diff).is_none()));
}

/// A partial store of merge requests on the graph store's connection, under its own names.
const LAKE: &str = "kind: storage
package: gitlab
store: lake
engine:
  kind: clickhouse
  connection: main
  path_column: traversal_path
  backend:
    edge_columns: {relationship_kind: relationship_kind, source_id: source_id, source_kind: source_kind, target_id: target_id, target_kind: target_kind}
    edge_tables:
      mr_note_export:
        sort_key: [traversal_path, source_id, target_id]
        columns: { source_id: { type: Int64 }, source_kind: { type: String }, relationship_kind: { type: String }, target_kind: { type: String }, target_id: { type: Int64 } }
entities:
  MergeRequest:
    accepts: [id, project_id]
    table: mr_export
    path_column: trav_path
    columns:
      id: { immutable: true, column: mr_id, backend: { type: Int64 } }
      title: { column: mr_title, backend: { type: String } }
      state: { backend: { type: LowCardinality(String) } }
      project_id: { immutable: true, backend: { type: Int64 } }
      traversal_path: { column: trav_path, backend: { type: String } }
    access: { order: [id] }
relationships:
  IN_PROJECT:
    MergeRequest -> Project: { fk: { on: source, column: project_id } }
  HAS_NOTE:
    MergeRequest -> Note: { edge_table: mr_note_export }
";

const LAKE_FILE: &str = "gitlab/storage/lake.yaml";

/// The gitlab fixture plus `LAKE`, each file passed through `edit`.
fn with_lake(
    edit: impl Fn(&str, String) -> String,
) -> Result<std::sync::Arc<Model>, DataModelError> {
    let mut sources = support::packages(&["gitlab"], &edit).sources().clone();
    sources.insert(LAKE_FILE.into(), edit(LAKE_FILE, LAKE.into()));
    Packages::new(query_data_model::Tier::Platform, sources).deployment("server")
}

#[test]
fn one_query_can_join_stores_on_the_same_connection() {
    let model = with_lake(|_, yaml| yaml).unwrap();
    let graph = model.graph();
    let names = model.names();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let request = model.names().entity("MergeRequest").unwrap();
    let title = model.names().property(request, "title").unwrap();

    let tables: Vec<_> = catalog
        .sources(request)
        .map(|(store, source)| (store.name(), source.table.as_deref().unwrap_or_default()))
        .collect();
    assert_eq!(
        tables,
        [("graph", "gl_merge_request"), ("lake", "mr_export")]
    );
    let lake = catalog
        .model()
        .store(catalog.names().store("lake").unwrap());
    assert_eq!(lake.source(request).unwrap().key, ["mr_id"]);
    assert_eq!(lake.column(title), Some("mr_title"));
    assert_eq!(
        lake.guard(request).unwrap().rule,
        Rule::Column("trav_path".into())
    );

    let in_project = graph.variant(
        names
            .variant("IN_PROJECT", "MergeRequest", "Project")
            .unwrap(),
    );
    let routes: Vec<_> = catalog.routes(in_project.id).collect();
    let edges: Vec<_> = routes.iter().filter_map(|route| route.edge).collect();
    let keys: Vec<_> = routes
        .iter()
        .filter_map(|route| route.foreign_key)
        .collect();
    assert_eq!(edges.len(), 1);
    assert_eq!((edges[0].store, edges[0].table), ("graph", "gl_edge"));
    let stores: Vec<_> = keys.iter().map(|key| key.store).collect();
    assert_eq!(stores, ["graph", "lake"]);
    for key in keys {
        assert_eq!(key.key.holder, Endpoint::Source);
        assert_eq!(graph.property(key.key.property).name, "project_id");
    }
}

#[test]
fn a_catalog_rejects_stores_of_another_backend() {
    let model = load(&["gitlab"]).unwrap();
    let errors = spec_errors(model.physical::<DuckDb>(model.names().connection("main").unwrap()));
    assert_eq!(
        errors,
        ["gitlab: connection main is clickhouse, not duckdb"]
    );
}

#[test]
fn backends_reject_unknown_options() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| {
        if name == "gitlab/storage/clickhouse.yaml" {
            yaml.replace("deleted_column:", "delete_column:")
        } else {
            yaml
        }
    }));
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].starts_with("gitlab/storage/clickhouse.yaml: unknown field `delete_column`"),
        "{errors:?}"
    );
}

#[test]
fn guards_read_the_declared_scope_key_of_each_store() {
    let model = load(&["gitlab", "tenant.agent_memory"]).unwrap();
    let access = model.access();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let store = catalog
        .model()
        .store(catalog.names().store("graph").unwrap());
    let request = model.names().entity("MergeRequest").unwrap();

    let guard = store.guard(request).unwrap();
    assert_eq!(guard.rule, Rule::Column("traversal_path".into()));
    assert_eq!(access.permission(guard.permission).name, "read");
    assert_eq!(access.scope(guard.scope).name, "namespace");

    let memory = store
        .guard(model.names().entity("AgentMemory").unwrap())
        .unwrap();
    assert_eq!(memory.rule, Rule::Column("traversal_path".into()));
    assert_eq!(access.permission(memory.permission).name, "read_developer");

    let user = store.guard(model.names().entity("User").unwrap()).unwrap();
    assert_eq!(user.rule, Rule::Global);
    assert_eq!(
        store.edge_guard("gl_edge").unwrap().rule,
        Rule::Column("traversal_path".into())
    );
}

#[test]
fn an_entity_without_a_policy_for_its_store_fails_the_load() {
    let errors = spec_errors(load_with(
        &["gitlab", "tenant.agent_memory"],
        |name, yaml| match name {
            "tenant.agent_memory/authorization.yaml" => yaml.replace(
                "    enforce: {clickhouse: {scope_key: traversal_path}}\n",
                "",
            ),
            _ => yaml,
        },
    ));
    assert_eq!(
        errors,
        ["tenant.agent_memory: AgentMemory is stored in graph but enforces no clickhouse policy"]
    );
}

#[test]
fn a_relational_store_cannot_enforce_a_via_chain() {
    let errors = spec_errors(load_with(
        &["gitlab", "tenant.agent_memory"],
        |name, yaml| match name {
            "tenant.agent_memory/authorization.yaml" => yaml.replace(
                "{clickhouse: {scope_key: traversal_path}}",
                "{clickhouse: {scope_key: {via: IN_PROJECT}}}",
            ),
            _ => yaml,
        },
    ));
    assert_eq!(
        errors,
        ["tenant.agent_memory: clickhouse cannot enforce AgentMemory's Via policy"]
    );
}

#[test]
fn a_via_step_needs_one_target() {
    let errors = spec_errors(load_with(
        &["gitlab", "tenant.agent_memory"],
        |name, yaml| match name {
            "tenant.agent_memory/authorization.yaml" => yaml.replace(
                "{clickhouse: {scope_key: traversal_path}}",
                "{clickhouse: {scope_key: {via: REMEMBERS}}}",
            ),
            _ => yaml,
        },
    ));
    assert_eq!(
        errors,
        [
            "tenant.agent_memory: AgentMemory reaches 2 entities from AgentMemory through REMEMBERS; a step needs one target",
            "tenant.agent_memory: AgentMemory is stored in graph but enforces no clickhouse policy",
        ]
    );
}

#[test]
fn every_row_check_of_an_entity_keys_rows_its_own_way() {
    let checks = |second: &str| {
        let second = second.to_string();
        load_with(&["gitlab"], move |name, yaml| {
            match name {
            "gitlab/authorization.yaml" => yaml.replace(
                "    checks: [{provider: rails, permission: read_note, resource: note}]",
                &format!("    checks: [{{provider: rails, permission: read_note, resource: note}}, {second}]"),
            ),
            _ => yaml,
        }
        })
    };
    for second in [
        "{provider: cedar, permission: read, resource: note}",
        "{provider: rails, permission: read_project, resource: project, object: Project, id: project_id}",
    ] {
        let model = checks(second).unwrap();
        let note = model.names().entity("Note").unwrap();
        assert_eq!(model.access().entity(note).checks.len(), 2);
    }
}

#[test]
fn a_checked_entity_needs_its_checks() {
    let checked = |checks: &str| {
        let checks = checks.to_string();
        load_with(&["gitlab"], move |name, yaml| {
            match name {
            "gitlab/authorization.yaml" => yaml.replace(
                "    enforce: {clickhouse: {scope_key: traversal_path}}\n    checks: [{provider: rails, permission: read_note, resource: note}]",
                &format!("    enforce: {{clickhouse: none}}\n{checks}"),
            ),
            _ => yaml,
        }
        })
    };
    let model =
        checked("    checks: [{provider: rails, permission: read_note, resource: note}]").unwrap();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let guard = catalog
        .guard(model.names().entity("Note").unwrap())
        .unwrap();
    assert_eq!(guard.rule, Rule::Checked);
    assert_eq!(
        spec_errors(checked("")),
        ["gitlab: Note enforces none and declares no check"]
    );
}

#[test]
fn packages_below_the_platform_tier_declare_no_checks_or_none() {
    let errors = spec_errors(load_with(
        &["gitlab", "tenant.agent_memory"],
        |name, yaml| {
            match name {
            "tenant.agent_memory/authorization.yaml" => yaml.replace(
                "    enforce: {clickhouse: {scope_key: traversal_path}}\n",
                "    enforce: {clickhouse: none}\n    checks: [{provider: rails, permission: read, resource: memory}]\n",
            ),
            _ => yaml,
        }
        },
    ));
    assert_eq!(
        errors,
        [
            "tenant.agent_memory: AgentMemory enforces none, which only a platform package may",
            "tenant.agent_memory: AgentMemory declares a check; a tenant package declares none",
        ]
    );
}

#[test]
fn a_global_entity_declares_its_checks_or_open() {
    let model = load(&["gitlab"]).unwrap();
    let user = model.names().entity("User").unwrap();
    assert!(model.access().is_global(user));
    assert!(model.access().entity(user).checks.is_empty());

    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/authorization.yaml" => yaml.replace("    checks: open\n", ""),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        ["gitlab: User is in global scope global, so it declares checks or checks: open"]
    );
}

#[test]
fn only_a_global_entity_declares_checks_open() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| {
        match name {
        "gitlab/authorization.yaml" => yaml.replace(
            "    checks: [{provider: rails, permission: read_merge_request, resource: merge_request}]\n",
            "    checks: open\n",
        ),
        _ => yaml,
    }
    }));
    assert_eq!(
        errors,
        ["gitlab: MergeRequest declares checks: open, which only a global entity may"]
    );
}

#[test]
fn only_a_platform_package_declares_checks_open() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/authorization.yaml" => yaml.replace(
            "  JiraIssue:\n    scope: namespace\n",
            "  JiraIssue:\n    scope: namespace\n    checks: open\n",
        ),
        _ => yaml,
    }));
    assert!(
        errors.contains(
            &"integration.jira: JiraIssue declares checks: open, which only a platform package may"
                .to_string()
        ),
        "{errors:?}"
    );

    let errors = spec_errors(load_with(
        &["gitlab", "tenant.agent_memory"],
        |name, yaml| match name {
            "tenant.agent_memory/authorization.yaml" => yaml.replace(
                "    permission: read_developer\n",
                "    permission: read_developer\n    checks: open\n",
            ),
            _ => yaml,
        },
    ));
    assert!(
        errors.contains(
            &"tenant.agent_memory: AgentMemory declares checks: open, which only a platform package may"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn every_entity_needs_an_authorization_entry() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/authorization.yaml" => {
            yaml.replace("  JiraIssue:\n", "  JiraIssueRemoved:\n")
        }
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "integration.jira: authorization names unknown entity JiraIssueRemoved",
            "integration.jira: entity JiraIssue has no authorization entry",
        ]
    );
}

#[test]
fn a_package_cannot_loosen_another_package_access_rule() {
    let errors = spec_errors(demo(|name, yaml| {
        match name {
        "integration.jira/authorization.yaml" => yaml
            .replace(
                "entities:\n",
                "entities:\n  User: {scope: global, permission: read, properties: {open: [id, username, email]}}\n",
            )
            .replace(
                "relationships:\n",
                "relationships:\n  AUTHORED: {scope: namespace, permission: read, enforce: {}}\n",
            ),
        _ => yaml,
    }
    }));
    assert_eq!(
        errors,
        [
            "integration.jira: User.id has two access classes",
            "integration.jira: User.username has two access classes",
            "integration.jira: User.email has two access classes",
            "integration.jira: User is in global scope global, so it declares checks or checks: open",
            "integration.jira: User has two authorization entries",
            "integration.jira: AUTHORED has two authorization entries",
            "integration.jira: User is in Global scope global, which a Integration package may not use",
        ]
    );
}

#[test]
fn every_property_has_one_access_class() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/authorization.yaml" => yaml.replace("      admin_exact: [email]\n", ""),
        _ => yaml,
    }));
    assert_eq!(errors, ["gitlab: User.email has no access class"]);
}

#[test]
fn a_package_names_only_what_it_requires() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/package.yaml" => yaml.replace(
            "requires: [gitlab, gitlab.core, gitlab.code_review]",
            "requires: []",
        ),
        _ => yaml,
    }));
    assert!(
        errors.contains(
            &"integration.jira: integration.jira names namespace from package gitlab, which it does not require"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn a_package_directory_holds_its_manifest() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/package.yaml" => yaml.replace("name: integration.jira", "name: jira"),
        _ => yaml,
    }));
    assert!(
        errors.contains(
            &"integration.jira/package.yaml: package.yaml names package jira, but sits in directory integration.jira"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn an_integration_package_declares_no_vocabulary() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/authorization.yaml" => yaml.replace(
            "package: integration.jira\n",
            "package: integration.jira\npermissions: [read_jira]\n",
        ),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "integration.jira: integration.jira is a Integration package and declares permissions or scopes, which only a platform package may"
        ]
    );
}

#[test]
fn packages_cannot_redeclare_another_package_entity() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/graph.yaml" => yaml.replace("  JiraIssue:\n", "  MergeRequest:\n"),
        _ => yaml,
    }));
    assert!(
        errors.contains(
            &"integration.jira: entity MergeRequest is already declared by gitlab.code_review"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn every_bad_reference_is_reported_in_one_load() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/storage/clickhouse.yaml" => yaml.replace(
            "      username: { backend: { type: String } }",
            "      usernam: { backend: { type: String } }",
        ),
        "gitlab/graph.yaml" => yaml.replace("User -> Note: {", "User -> Notes: {"),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "gitlab: AUTHORED variant `User -> Notes` names an unknown entity",
            "gitlab: store graph names unknown property User.usernam",
            "gitlab: store graph binds unknown variant AUTHORED(User -> Note)",
            "gitlab: store graph is complete for User but neither stores nor excludes User.username",
            "gitlab: pipeline Note emits unknown variant User -[AUTHORED]-> Note",
            "gitlab: pipeline Note emits no declared AUTHORED variant",
        ]
    );
}

#[test]
fn an_unknown_permission_or_scope_fails_the_load() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/authorization.yaml" => yaml
            .replace("permission: read_note,", "permission: read_notes,")
            .replace(
                "  Group:\n    scope: namespace",
                "  Group:\n    scope: namespaces",
            ),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "gitlab: Group names unknown scope namespaces",
            "gitlab: Note checks unknown permission read_notes",
        ]
    );
}

#[test]
fn frontend_errors_name_the_alternatives() {
    let model = load(&["gitlab"]).unwrap();
    let user = model.names().entity("User").unwrap();
    let request = model.names().entity("MergeRequest").unwrap();
    let has_note = model.names().relationship("HAS_NOTE").unwrap();

    let error = model
        .connect(user, &[has_note], Some(request), Direction::Outgoing)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "no HAS_NOTE relationship connects User to MergeRequest; these do: AUTHORED"
    );
    let error = demo(|_, yaml| yaml)
        .unwrap()
        .names()
        .entity("JiraIssu")
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "unknown entity JiraIssu; candidates: JiraIssue"
    );
}

const ATTRIBUTE_COLUMNS: &str = "thread: { type: String }, position: { type: Nullable(Int64) }";

const HAS_NOTE_VARIANT: &str = "      MergeRequest -> Note: { cardinality: one_to_many }\n";

const HAS_NOTE_ATTRIBUTES: &str = "      MergeRequest -> Note: { cardinality: one_to_many }
    attributes:
      position: int64?
      thread: { type: string, description: Discussion the note belongs to }\n";

#[test]
fn relationships_declare_typed_attributes() {
    let model = load_with(&["gitlab"], attribute_storage(ATTRIBUTE_COLUMNS)).unwrap();
    let graph = model.graph();
    let names = model.names();
    let has_note = names.relationship("HAS_NOTE").unwrap();
    let names: Vec<_> = graph
        .relationship(has_note)
        .attributes
        .iter()
        .map(|id| graph.attribute(*id))
        .map(|attribute| {
            (
                attribute.name.as_str(),
                attribute.data_type,
                attribute.nullable,
            )
        })
        .collect();
    assert_eq!(
        names,
        [
            ("position", query_data_model::DataType::Int, true),
            ("thread", query_data_model::DataType::String, false),
        ]
    );
    let thread = model.names().attribute(has_note, "thread").unwrap();
    assert_eq!(
        graph.attribute(thread).description.as_deref(),
        Some("Discussion the note belongs to")
    );
    assert_eq!(model.names().attribute(has_note, "missing"), None);
}

#[test]
fn access_entries_apply_to_relationship_attributes() {
    let storage = attribute_storage(ATTRIBUTE_COLUMNS);
    let model = load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/authorization.yaml" => {
            attribute_access(yaml, "{open: [position], hidden: [thread, mood]}")
        }
        _ => storage(name, yaml),
    });
    assert_eq!(
        spec_errors(model),
        ["gitlab: authorization names unknown property HAS_NOTE.mood"]
    );
    let model = load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/authorization.yaml" => {
            attribute_access(yaml, "{open: [position], hidden: [thread]}")
        }
        _ => storage(name, yaml),
    })
    .unwrap();
    let names = model.names();
    let has_note = names.relationship("HAS_NOTE").unwrap();
    let access = |name| {
        model
            .access()
            .attribute(names.attribute(has_note, name).unwrap())
    };
    assert!(access("thread").hidden && !access("thread").filterable);
    assert!(!access("position").hidden && access("position").filterable);
}

#[test]
fn bad_relationship_attributes_fail_the_load() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/graph.yaml",
            HAS_NOTE_VARIANT,
            "      MergeRequest -> Note: { cardinality: one_to_many }
    attributes: { mood: feeling, state: enum }\n",
        ),
    ));
    assert_eq!(
        errors,
        [
            "gitlab: attribute HAS_NOTE.mood has unsupported type `feeling`",
            "gitlab: attribute HAS_NOTE.state has unsupported type `enum`",
        ]
    );
}

#[test]
fn attributes_never_take_a_declared_edge_column() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| {
        match name {
        "gitlab/graph.yaml" => yaml.replace(
            HAS_NOTE_VARIANT,
            "      MergeRequest -> Note: { cardinality: one_to_many }\n    attributes: { source_id: int64 }\n",
        ),
        "gitlab/authorization.yaml" => attribute_access(yaml, "{open: [source_id]}"),
        _ => yaml,
    }
    }));
    assert_eq!(
        errors,
        [
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) declares attribute source_id on an edge column the store reserves"
        ]
    );
}

#[test]
fn edge_tables_need_their_store_edge_columns() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/storage/clickhouse.yaml" => yaml.replace("source_kind: { type: String }, ", ""),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        ["gitlab: store graph edge table gl_edge has no edge column source_kind"]
    );
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/storage/clickhouse.yaml" => {
            let start = yaml.find("    edge_columns:").unwrap();
            let end = start + yaml[start..].find('\n').unwrap() + 1;
            format!("{}{}", &yaml[..start], &yaml[end..])
        }
        _ => yaml,
    }));
    assert_eq!(
        errors,
        ["gitlab: store graph declares edge tables but no edge_columns"]
    );
}

#[test]
fn the_physical_catalog_answers_column_facts_by_handle() {
    let model = load_with(&["gitlab"], |_, yaml| yaml).unwrap();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let names = model.names();
    let edge = catalog.edge_table_ids(&[]).pop().unwrap();
    let kind = catalog
        .column_of(Target::Edge(EdgeField::Kind), edge)
        .unwrap();
    assert_eq!(catalog.column_name(kind), "relationship_kind");
    assert!(catalog.in_sort_key(kind));
    let source_kind = catalog
        .column_of(Target::Edge(EdgeField::SourceKind), edge)
        .unwrap();
    assert!(!catalog.in_sort_key(source_kind));
    assert_eq!(
        catalog.column_type(source_kind),
        Some(query_data_model::DataType::String)
    );
    let group = names.entity("Group").unwrap();
    let id = Target::Property(names.property(group, "id").unwrap());
    let table = catalog.entity_table(group).unwrap();
    assert!(catalog.column_of(id, table).is_some());
    assert_eq!(catalog.column_of(id, edge), None);
    assert_eq!(
        catalog.column_of(Target::Edge(EdgeField::Kind), table),
        None
    );
}

const HAS_NOTE_ACCESS: &str = "  HAS_NOTE:\n    scope: namespace\n    permission: read\n    enforce: {clickhouse: {scope_key: traversal_path, of: both}}\n";

fn attribute_access(yaml: String, classes: &str) -> String {
    yaml.replace(
        HAS_NOTE_ACCESS,
        &format!("{HAS_NOTE_ACCESS}    attributes: {classes}\n"),
    )
}

fn attribute_storage(edge_columns: &'static str) -> impl Fn(&str, String) -> String {
    move |name, yaml| match name {
        "gitlab/graph.yaml" => yaml.replace(HAS_NOTE_VARIANT, HAS_NOTE_ATTRIBUTES),
        "gitlab/authorization.yaml" => attribute_access(yaml, "{open: [position, thread]}"),
        "gitlab/storage/clickhouse.yaml" => yaml.replace(
            "target_id: { type: Int64 } }",
            &format!("target_id: {{ type: Int64 }}, {edge_columns} }}"),
        ),
        _ => yaml,
    }
}

#[test]
fn edge_tables_hold_a_column_for_every_relationship_attribute() {
    let model = load_with(
        &["gitlab"],
        attribute_storage(
            "thread: { type: LowCardinality(String) }, position: { type: Nullable(Int32) }",
        ),
    )
    .unwrap();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let edge = catalog.physical_table("gl_edge").unwrap();
    assert!(edge.has_column("thread") && edge.has_column("position"));
}

#[test]
fn attributes_need_a_matching_edge_column() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        attribute_storage("position: { type: String }"),
    ));
    assert_eq!(
        errors,
        [
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) stores attribute position as String, not Int",
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) has no edge column for attribute thread",
        ]
    );
}

#[test]
fn string_enums_keep_their_declared_values() {
    let model = demo(|_, yaml| yaml).unwrap();
    let issue = model.names().entity("JiraIssue").unwrap();
    let category = model.names().property(issue, "status_category").unwrap();
    assert_eq!(
        model.enum_type(category),
        query_data_model::EnumType::String
    );
    assert_eq!(
        model
            .graph()
            .property(category)
            .enum_values
            .as_ref()
            .unwrap()[&2],
        "done"
    );
}

#[test]
fn model_identity_changes_with_the_spec() {
    let first = load(&["gitlab"]).unwrap();
    let again = load(&["gitlab"]).unwrap();
    let extended = load(&["gitlab", "tenant.agent_memory"]).unwrap();
    assert_eq!(first.id(), again.id());
    assert_ne!(first.id(), extended.id());
    let fragments = || Packages::embedded().fragments().unwrap();
    let prefixed =
        query_data_model::implementations::clickhouse::versioned(fragments(), "v1_").unwrap();
    assert_ne!(prefixed.id(), fragments().id());
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
fn packages_sharing_a_domain_agree_on_its_description() {
    let errors = spec_errors(demo(|name, yaml| match name {
        "integration.jira/graph.yaml" => yaml.replace(
            "domain: plan\n",
            "domain: plan\ndomain_description: Jira planning\n",
        ),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        ["integration.jira: domain plan already has another description"]
    );
}

#[test]
fn a_misspelled_relationship_key_fails_the_load() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/storage/clickhouse.yaml",
            "MergeRequest -> Project: { fk: { on: source, column: project_id }",
            "MergeRequest -> Project: { fk: { on: source, column: projet_id }",
        ),
    ));
    assert_eq!(
        errors,
        [
            "gitlab: store graph IN_PROJECT(MergeRequest -> Project) foreign key projet_id is not a column of MergeRequest"
        ]
    );
}

#[test]
fn a_misspelled_shared_field_fails_the_load() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/storage/clickhouse.yaml",
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
    let errors = spec_errors(with_lake(edit_one(
        LAKE_FILE,
        "  connection: main\n",
        "  connection: main\n  trusted: true\n",
    )));
    assert_eq!(
        errors,
        ["gitlab: store lake and store graph share connection main but only one is trusted"]
    );
}

#[test]
fn one_connection_has_one_backend() {
    let errors = spec_errors(with_lake(|name, yaml| {
        match name {
        LAKE_FILE => yaml
            .replace("  kind: clickhouse", "  kind: duckdb")
            .replace(
                "{ source_id: { type: Int64 }, source_kind: { type: String }, relationship_kind: { type: String }, target_kind: { type: String }, target_id: { type: Int64 } }",
                "{ source_id: int64, source_kind: string, relationship_kind: string, target_kind: string, target_id: int64 }",
            ),
        _ => yaml,
    }
    }));
    assert_eq!(
        errors,
        [
            "gitlab: store lake is duckdb but connection main is clickhouse",
            "gitlab: MergeRequest is stored in lake but enforces no duckdb policy",
            "gitlab: IN_PROJECT is stored in lake but enforces no duckdb policy",
            "gitlab: HAS_NOTE is stored in lake but enforces no duckdb policy",
        ]
    );
}

#[test]
fn a_lookup_property_cannot_also_be_a_column() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/storage/clickhouse.yaml",
            "      iid: { immutable: true, backend: { type: Int64 } }\n",
            "      iid: { immutable: true, backend: { type: Int64 } }\n      diff: { backend: { type: String } }\n",
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
            "gitlab/storage/clickhouse.yaml",
            "indexes: { title: text,",
            "indexes: { diff: text, title: text,",
        ),
    );
    let errors = spec_errors(model);
    assert_eq!(
        errors,
        ["gitlab: store graph MergeRequest indexes unknown column diff"]
    );
}

const MANAGED_STORE: &str = r#"    engine: ReplacingMergeTree(_version, _deleted)
    columns:
      _version: { type: "DateTime64(6, 'UTC')", default: now64(6), codec: [Delta(8), ZSTD(1)] }
      _deleted: { type: Bool, default: "false" }
    settings: { index_granularity: "1024" }
"#;

const RANK_JOIN: &str = r#"    joins:
      gl_rank_group:
        sort_key: [t0_group_id]
        sources:
        - table: gl_rank
          columns: { group_id: t0_group_id, rank: t0_rank }
        nodes:
        - { entity: Rank, source: 0, key: t0_group_id }
"#;

fn managed(extra: &'static str) -> impl Fn(&str, String) -> String {
    move |name, yaml| {
        if name == "gitlab/storage/clickhouse.yaml" {
            yaml.replace(
                "    deleted_column: _deleted\n",
                &format!("    deleted_column: _deleted\n{MANAGED_STORE}{extra}"),
            )
        } else {
            yaml
        }
    }
}

#[test]
fn a_managed_store_resolves_its_physical_schema() {
    let model = load_with(&["gitlab"], managed("")).unwrap();
    let catalog = model
        .physical::<ClickHouse>(model.names().connection("main").unwrap())
        .unwrap();
    let schema = catalog
        .facts(catalog.names().store("graph").unwrap())
        .unwrap()
        .schema()
        .unwrap();
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
      audit: { engine: MergeTree, columns: { at: { type: DateTime } }, indexes: { actor: minmax } }
    views:
      broken: { select: "SELECT 1" }
      misrouted: { select: "SELECT 1", to: gl_missing }
"#,
        ),
    );
    let errors = spec_errors(model);
    assert_eq!(
        errors,
        [
            "gitlab: copy gl_note_by_id names unknown table gl_missing",
            "gitlab: store graph table audit indexes unknown column actor",
            "gitlab: store graph view broken names neither a destination table nor an engine",
            "gitlab: store graph view misrouted writes unknown table gl_missing",
        ]
    );
    let untyped = load_with(
        &["gitlab"],
        managed("    tables:\n      audit: { engine: MergeTree, columns: { at: {} } }\n"),
    );
    assert_eq!(
        spec_errors(untyped),
        ["gitlab/storage/clickhouse.yaml: missing field `type`"]
    );
}

#[test]
fn join_filters_reject_values_that_are_not_plain_words() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        managed(
            r#"    joins:
      gl_named_project:
        sort_key: [t0_id]
        sources:
        - table: gl_project
          columns: { id: t0_id }
          filter: { name: "x' OR 1 = 1 --" }
        nodes:
        - { entity: Project, source: 0, key: t0_id }
"#,
        ),
    ));
    assert_eq!(
        errors,
        [
            "gitlab: store graph join gl_named_project filters gl_project.name on `x' OR 1 = 1 --`, which is not a plain word"
        ]
    );
}

#[test]
fn every_guard_carries_the_declared_scope_and_permission() {
    use query_data_model::testkit;

    for (packages, shape) in [
        (Packages::embedded(), "server"),
        (testkit::with_demo(), "demo"),
    ] {
        let model = packages.deployment(shape).unwrap();
        for store in query_data_model::testkit::stores(&model) {
            for source in store.sources() {
                let entity = model.graph().entity(source.entity);
                let declared = model.access().entity(source.entity);
                let guard = store.guard(source.entity).unwrap();
                assert_eq!(
                    (guard.scope, guard.permission),
                    (declared.scope, declared.permission),
                    "{} {}",
                    store.name(),
                    entity.name
                );
                assert_eq!(store.trusted(), guard.rule == Rule::Trusted);
            }
        }
    }
}

fn gitlab_errors(find: &'static str, replace: &'static str) -> Vec<String> {
    spec_errors(load_with(
        &["gitlab"],
        edit_one("gitlab/storage/clickhouse.yaml", find, replace),
    ))
}

#[test]
fn storage_must_hold_or_exclude_every_property() {
    let errors = gitlab_errors(
        "      full_path: { backend: { type: String } }\n      traversal_path: { backend: { type: String } }\n    access: { order: [id] }\n  User:",
        "      traversal_path: { backend: { type: String } }\n    access: { order: [id] }\n  User:",
    );
    assert_eq!(
        errors,
        [
            "gitlab: store graph is complete for Project but neither stores nor excludes Project.full_path"
        ]
    );
}

#[test]
fn relational_columns_and_bindings_are_explicit() {
    let errors = gitlab_errors(
        "      body: { backend: { type: String } }",
        "      body: {}",
    );
    assert_eq!(
        errors,
        ["gitlab: store graph column Note.body declares no storage type"]
    );
    let errors = gitlab_errors("    table: gl_note\n", "");
    assert_eq!(
        errors,
        [
            "gitlab: store graph holds Note in a clickhouse store, which a source writes, but names no table"
        ]
    );
}

#[test]
fn every_variant_is_bound_where_its_endpoints_live() {
    let errors = gitlab_errors("    Group -> Group: { edge_table: gl_edge }\n", "");
    assert_eq!(
        errors,
        [
            "gitlab: store graph holds both endpoints of CONTAINS(Group -> Group) but does not bind it",
            "gitlab: CONTAINS(Group -> Group) is bound in no store",
        ]
    );
    let errors = gitlab_errors(
        "    MergeRequest -> Note: { edge_table: gl_edge }",
        "    MergeRequest -> Note: {}",
    );
    assert_eq!(
        errors,
        [
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) names neither a foreign key nor an edge table"
        ]
    );
}

#[test]
fn foreign_keys_follow_the_variant_cardinality() {
    let errors = gitlab_errors(
        "User -> Note: { fk: { on: target, column: author_id }",
        "User -> Note: { fk: { on: source, column: author_id }",
    );
    assert_eq!(
        errors,
        [
            "gitlab: store graph AUTHORED(User -> Note) is OneToMany and cannot hold a foreign key on its Source"
        ]
    );
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/graph.yaml" => yaml.replace(
            "MergeRequest -> Note: { cardinality: one_to_many }",
            "MergeRequest -> Note: { cardinality: many_to_many }",
        ),
        "gitlab/storage/clickhouse.yaml" => yaml.replace(
            "MergeRequest -> Note: { edge_table: gl_edge }",
            "MergeRequest -> Note: { fk: { on: target, column: project_id }, edge_table: gl_edge }",
        ),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) is ManyToMany and cannot hold a foreign key on its Target"
        ]
    );
}

#[test]
fn an_edge_copy_names_an_endpoint_property() {
    let errors = gitlab_errors(
        "  HAS_NOTE:\n    MergeRequest -> Note: { edge_table: gl_edge }",
        "  HAS_NOTE:\n    MergeRequest -> Note: { edge_table: gl_edge, copies: { project_id: target.nope } }",
    );
    assert_eq!(
        errors,
        [
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) copy project_id names no property of an endpoint"
        ]
    );
}

#[test]
fn graph_defaults_match_the_property_type() {
    let errors = spec_errors(load_with(
        &["gitlab"],
        edit_one(
            "gitlab/graph.yaml",
            "title: { type: string, default: \"\" }",
            "title: { type: string, default: 3 }",
        ),
    ));
    assert_eq!(
        errors,
        ["gitlab: property title default Int(3) is not a String"]
    );
}

#[test]
fn stores_that_hold_no_rows_name_no_table() {
    let packages = query_data_model::testkit::patched([(
        "gitlab.source_code/storage/orbit_code.yaml",
        "  File:\n    accepts: [project_id, branch]\n    columns:",
        "  File:\n    accepts: [project_id, branch]\n    table: gl_file\n    columns:\n      id: {}",
    )]);
    let errors = spec_errors(packages.deployment("demo"));
    assert_eq!(
        errors,
        ["gitlab.source_code: store code sets table on File, which orbit_code stores do not read"]
    );
}

#[test]
fn variants_that_share_a_key_column_name_distinct_types() {
    let packages = query_data_model::testkit::patched([(
        "gitlab/storage/clickhouse.yaml",
        "type_value: [Epic, Issue]",
        "type_value: [Epic, MergeRequest]",
    )]);
    let errors = spec_errors(packages.deployment("server"));
    assert_eq!(
        errors,
        [
            "gitlab: store graph keys HAS_NOTE(MergeRequest -> Note) and HAS_NOTE(WorkItem -> Note) on noteable_id without distinct type values"
        ]
    );
}

#[test]
fn every_entity_declares_a_typed_identity_and_default_columns() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| match name {
        "gitlab/graph.yaml" => yaml
            .replace("  Group:\n    identity: id\n", "  Group:\n")
            .replace(
                "  User:\n    identity: id\n",
                "  User:\n    identity: [email, mail]\n",
            )
            .replace("    default_columns: [id, body]\n", ""),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "gitlab: entity Group declares no identity",
            "gitlab: entity Note declares no default_columns",
            "gitlab: User identity email is String?; an identity is a non-null integer, string or uuid",
            "gitlab: User identity mail is not a property",
            "gitlab: Group check keys rows by its identity, which is not one property; name an object and id",
            "gitlab: store graph CONTAINS(Group -> Project) names an edge table but the store does not hold its source",
            "gitlab: store graph CONTAINS(Group -> Group) names an edge table but the store does not hold its source",
            "gitlab: store graph AUTHORED(User -> MergeRequest) foreign key author_id is Int but User.email is String; keys are never cast",
            "gitlab: store graph AUTHORED(User -> Note) foreign key author_id is Int but User.email is String; keys are never cast",
            "gitlab: Group writes entity Group, which no store holds",
        ]
    );
}

#[test]
fn exactly_one_store_is_complete_and_partial_stores_list_what_they_hold() {
    let errors = spec_errors(with_lake(|name, yaml| match name {
        "gitlab/storage/clickhouse.yaml" => {
            yaml.replace("  User:\n    complete: true\n", "  User:\n")
        }
        LAKE_FILE => yaml
            .replace("  MergeRequest:\n", "  MergeRequest:\n    complete: true\n")
            .replace(
                "mr_title, backend",
                "mr_title, stored_as_text: true, backend",
            )
            .replace("state: { backend", "state: { stored_as_text: true, backend"),
        _ => yaml,
    }));
    assert_eq!(
        errors,
        [
            "gitlab: store lake stores MergeRequest.title as text, but it is not an enum",
            "gitlab: stores graph and lake both mark MergeRequest complete: true; exactly one store is complete",
            "gitlab: stores graph and lake disagree on whether MergeRequest.title is stored as text",
            "gitlab: stores graph and lake disagree on whether MergeRequest.state is stored as text",
            "gitlab: User is stored in graph but no store marks it complete: true",
        ]
    );
}

#[test]
fn edge_tables_hold_ids_of_their_endpoint_identity_types() {
    let errors = spec_errors(load_with(&["gitlab"], |name, yaml| {
        match name {
        "gitlab/graph.yaml" => yaml.replace(
            "    default_columns: [id, body]\n    properties:\n      id: int64",
            "    default_columns: [id, body]\n    properties:\n      id: string",
        ),
        "gitlab/storage/clickhouse.yaml" => yaml.replace(
            "  Note:\n    complete: true\n    table: gl_note\n    columns:\n      id: { immutable: true, backend: { type: Int64 } }",
            "  Note:\n    complete: true\n    table: gl_note\n    columns:\n      id: { immutable: true, backend: { type: String } }",
        ),
        _ => yaml,
    }
    }));
    assert_eq!(
        errors,
        [
            "gitlab: store graph IN_PROJECT(Note -> Project) binds edge table gl_edge whose source_id is Int, but Note's identity is String; keys are never cast",
            "gitlab: store graph AUTHORED(User -> Note) binds edge table gl_edge whose target_id is Int, but Note's identity is String; keys are never cast",
            "gitlab: store graph HAS_NOTE(MergeRequest -> Note) binds edge table gl_edge whose target_id is Int, but Note's identity is String; keys are never cast",
        ]
    );
}

#[test]
fn an_engine_table_role_is_held_by_one_table() {
    let packages = query_data_model::testkit::patched([(
        "gitlab/storage/clickhouse.yaml",
        "role: code_progress",
        "role: plan_progress",
    )]);
    let errors = spec_errors(packages.deployment("server"));
    assert!(
        errors
            .iter()
            .any(|error| error.ends_with("both hold the PlanProgress role")),
        "{errors:?}"
    );
}

/// Two variants keyed on the same composite columns reach different entities, and a composite
/// key carries no type value to tell them apart.
#[test]
fn variants_on_the_same_composite_key_columns_clash() {
    let mut sources = Packages::embedded().sources().clone();
    let files = [
        (
            "overlay/package.yaml",
            "kind: package\nname: overlay\nrequires: [gitlab, gitlab.core, gitlab.code_review, gitlab.source_code]\n",
        ),
        (
            "overlay/graph.yaml",
            "kind: graph\npackage: overlay\nentities:\n  Head:\n    identity: [project_id, branch]\n    label: branch\n    default_columns: [project_id, branch]\n    properties: {project_id: int64, branch: string}\n  Tip:\n    identity: [project_id, branch]\n    label: branch\n    default_columns: [project_id, branch]\n    properties: {project_id: int64, branch: string}\nrelationships:\n  SOURCE_HEAD: {variants: {MergeRequest -> Head: {cardinality: many_to_one}}}\n  SOURCE_TIP: {variants: {MergeRequest -> Tip: {cardinality: many_to_one}}}\n",
        ),
        (
            "overlay/authorization.yaml",
            "kind: authorization\npackage: overlay\nentities:\n  Head: {scope: namespace, permission: read, enforce: {orbit_code: none}, checks: [{provider: rails, permission: read_code, resource: project, object: Project, id: project_id}], properties: {open: [project_id, branch]}}\n  Tip: {scope: namespace, permission: read, enforce: {orbit_code: none}, checks: [{provider: rails, permission: read_code, resource: project, object: Project, id: project_id}], properties: {open: [project_id, branch]}}\nrelationships:\n  SOURCE_HEAD: {scope: namespace, permission: read, enforce: {clickhouse: {scope_key: traversal_path, of: source}}}\n  SOURCE_TIP: {scope: namespace, permission: read, enforce: {clickhouse: {scope_key: traversal_path, of: source}}}\n",
        ),
        (
            "overlay/storage/orbit_code.yaml",
            "kind: storage\npackage: overlay\nstore: code\nentities:\n  Head: {complete: true, accepts: [project_id, branch], columns: {project_id: {}, branch: {}}, backend: {repository: {project: project_id, branch: branch}}}\n  Tip: {complete: true, accepts: [project_id, branch], columns: {project_id: {}, branch: {}}, backend: {repository: {project: project_id, branch: branch}}}\n",
        ),
        (
            "overlay/storage/clickhouse.yaml",
            "kind: storage\npackage: overlay\nstore: graph\nrelationships:\n  SOURCE_HEAD:\n    MergeRequest -> Head: {fk: {on: source, column: [project_id, source_branch]}}\n  SOURCE_TIP:\n    MergeRequest -> Tip: {fk: {on: source, column: [project_id, source_branch]}}\n",
        ),
    ];
    for (path, text) in files {
        sources.insert(path.into(), text.into());
    }
    let errors =
        spec_errors(Packages::new(query_data_model::Tier::Platform, sources).deployment("demo"));
    assert_eq!(
        errors,
        [
            "overlay: store graph keys SOURCE_HEAD(MergeRequest -> Head) and SOURCE_TIP(MergeRequest -> Tip) on project_id, source_branch without distinct type values"
        ]
    );
}

/// A table store keys rows by every part of a composite identity, and a foreign key reaches it
/// on one column per part; an edge table endpoint, an edge index the writer derives and a
/// materialized join node, each keyed by one column, cannot hold it.
#[test]
fn a_composite_identity_sits_on_a_table_but_not_in_a_single_key_slot() {
    let load = |slots: bool| {
        let (graph, authorization, storage, written, index) = match slots {
            true => (
                "  RANKED_BY: {variants: {User -> Rank: {cardinality: many_to_many}}}\n",
                "  RANKED_BY: {scope: namespace, permission: read, enforce: {clickhouse: {scope_key: traversal_path, of: target}}}\n",
                "  RANKED_BY:\n    User -> Rank: {edge_table: gl_edge}\n",
                "relationships:\n  RANKED_BY: [User -> Rank]\n  RANK_OF: [Rank -> Group]\n",
                ", index: {edge_table: gl_edge}",
            ),
            false => ("", "", "", "", ""),
        };
        let mut sources = Packages::embedded().sources().clone();
        let files = [
            (
                "overlay/package.yaml",
                "kind: package\nname: overlay\nrequires: [gitlab, gitlab.core]\n".to_string(),
            ),
            (
                "overlay/graph.yaml",
                format!(
                    "kind: graph\npackage: overlay\nentities:\n  Rank:\n    identity: [group_id, rank]\n    label: rank\n    default_columns: [group_id, rank]\n    properties: {{group_id: int64, rank: string, traversal_path: string}}\nrelationships:\n  RANK_OF: {{variants: {{Rank -> Group: {{cardinality: many_to_one}}}}}}\n{graph}"
                ),
            ),
            (
                "overlay/authorization.yaml",
                format!(
                    "kind: authorization\npackage: overlay\nentities:\n  Rank: {{scope: namespace, permission: read, enforce: {{clickhouse: {{scope_key: traversal_path}}}}, properties: {{open: [group_id, rank], hidden: [traversal_path]}}}}\nrelationships:\n  RANK_OF: {{scope: namespace, permission: read, enforce: {{clickhouse: {{scope_key: traversal_path, of: source}}}}}}\n{authorization}"
                ),
            ),
            (
                "overlay/sources/rank_sync.yaml",
                format!(
                    "kind: source\npackage: overlay\nsource: rank_sync\nentities: [Rank]\n{written}"
                ),
            ),
            (
                "overlay/storage/clickhouse.yaml",
                format!(
                    "kind: storage\npackage: overlay\nstore: graph\nentities:\n  Rank:\n    complete: true\n    accepts: [group_id, rank]\n    table: gl_rank\n    columns:\n      group_id: {{immutable: true, backend: {{type: Int64}}}}\n      rank: {{immutable: true, backend: {{type: String}}}}\n      traversal_path: {{backend: {{type: String}}}}\n    backend:\n      primary_key: [traversal_path, group_id, rank]\nrelationships:\n  RANK_OF:\n    Rank -> Group: {{fk: {{on: source, column: group_id}}{index}}}\n{storage}"
                ),
            ),
        ];
        for (path, text) in files {
            sources.insert(path.into(), text);
        }
        if slots {
            let engine = sources.get_mut("gitlab/storage/clickhouse.yaml").unwrap();
            *engine = engine.replace(
                "    deleted_column: _deleted\n",
                &format!("    deleted_column: _deleted\n{RANK_JOIN}"),
            );
        }
        Packages::new(query_data_model::Tier::Platform, sources).deployment("server")
    };
    load(false).unwrap();
    assert_eq!(
        spec_errors(load(true)),
        [
            "gitlab: store graph join gl_rank_group keys Rank by one column t0_group_id, but Rank has a composite identity",
            "overlay: store graph RANK_OF(Rank -> Group) keeps its edges in gl_edge, whose source_id holds one key, but Rank has a composite identity",
            "overlay: store graph RANKED_BY(User -> Rank) keeps its edges in gl_edge, whose target_id holds one key, but Rank has a composite identity",
        ]
    );
}

/// No read maps an enum's codes to its labels, so a ClickHouse column stores the labels.
#[test]
fn an_enum_column_stores_its_labels() {
    let packages = query_data_model::testkit::patched([(
        "gitlab.core/storage/clickhouse.yaml",
        "table: gl_group\n    columns:\n      id: {immutable: true, backend: {type: Int64, codec: [T64, ZSTD(1)]}}\n      name: {backend: {type: String, codec: [ZSTD(1)]}}\n      description: {backend: {type: String, codec: [ZSTD(3)]}}\n      visibility_level: {backend: {type: LowCardinality(String), codec: [LZ4]}}",
        "table: gl_group\n    columns:\n      id: {immutable: true, backend: {type: Int64, codec: [T64, ZSTD(1)]}}\n      name: {backend: {type: String, codec: [ZSTD(1)]}}\n      description: {backend: {type: String, codec: [ZSTD(3)]}}\n      visibility_level: {backend: {type: Int8}}",
    )]);
    assert_eq!(
        spec_errors(packages.deployment("server")),
        [
            "gitlab.core: store graph stores Group.visibility_level, an enum, as Int8; an enum column holds its labels"
        ]
    );
}

#[test]
fn every_store_names_the_stored_columns_it_takes_key_sets_on() {
    let group = |accepts: &str| {
        query_data_model::testkit::patched([(
            "gitlab.core/storage/clickhouse.yaml",
            "  Group:\n    complete: true\n    accepts: [id]\n",
            accepts,
        )])
        .deployment("server")
    };
    assert_eq!(
        spec_errors(group("  Group:\n    complete: true\n")),
        ["gitlab.core: store graph declares no accepts for Group"]
    );
    assert_eq!(
        spec_errors(group(
            "  Group:\n    complete: true\n    accepts: [visibility]\n"
        )),
        ["gitlab.core: store graph accepts keys on Group.visibility, which it does not store"]
    );
    let mut sources = query_data_model::testkit::with_demo().sources().clone();
    let live = sources
        .get_mut("integration.slack/storage/live.yaml")
        .unwrap();
    *live = live.replace(
        "accepts: [id, merge_request_id]",
        "accepts: [id, workspace]",
    );
    assert_eq!(
        spec_errors(Packages::new(query_data_model::Tier::Platform, sources).deployment("demo")),
        [
            "integration.slack: store slack accepts keys on SlackMessage.workspace, which its operation does not filter on"
        ]
    );
}
