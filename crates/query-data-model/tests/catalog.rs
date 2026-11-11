mod support;

use query_data_model::catalog::Rule;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::{DataModelError, Direction, Endpoint, Model, Packages};
use support::load_with;

fn load(packages: &[&str]) -> Result<std::sync::Arc<Model>, DataModelError> {
    load_with(packages, |_, yaml| yaml)
}

fn spec_errors<T: std::fmt::Debug>(result: Result<T, DataModelError>) -> Vec<String> {
    match result {
        Err(DataModelError::Spec(errors)) => errors.iter().map(ToString::to_string).collect(),
        other => panic!("expected spec errors, got {other:?}"),
    }
}

#[test]
fn platform_integration_and_custom_packages_compose_into_one_graph() {
    let model = load(&["gitlab", "integration.linear", "tenant.agent_memory"]).unwrap();
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
    let model = load(&["gitlab", "integration.linear", "tenant.agent_memory"]).unwrap();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let facts = catalog.backend("graph").unwrap();
    let request = model.entity("MergeRequest").unwrap();
    let title = model.property(request, "title").unwrap().id;
    let diff = model.property(request, "diff").unwrap().id;

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
    let model = load(&["gitlab", "integration.linear"]).unwrap();
    let graph = model.graph();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let issue = model.entity("LinearIssue").unwrap();
    let body = model.property(issue, "body").unwrap().id;

    let tables: Vec<_> = catalog
        .sources(issue)
        .map(|(store, source)| {
            (
                store.name.as_str(),
                source.table.as_deref().unwrap_or_default(),
            )
        })
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
fn a_foreign_key_must_join_within_its_connection() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| {
            if name == "integration.linear/storage/clickhouse.lake.yaml" {
                yaml.replace("connection: main", "connection: lake")
            } else {
                yaml
            }
        },
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: store lake binds RELATES_TO(LinearIssue -> MergeRequest), whose other endpoint is not on connection lake"
        ]
    );
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
    let model = load(&["gitlab", "integration.linear", "tenant.agent_memory"]).unwrap();
    let access = model.access();
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let store = catalog.store("graph").unwrap();
    let issue = model.entity("LinearIssue").unwrap();

    let guard = store.guard(issue).unwrap();
    assert_eq!(guard.rule, Rule::Column("traversal_path".into()));
    assert_eq!(access.permission(guard.permission).name, "read_maintainer");
    assert_eq!(access.scope(guard.scope).name, "namespace");
    let lake = catalog.store("lake").unwrap().guard(issue).unwrap();
    assert_eq!(lake.rule, Rule::Column("trav_path".into()));

    let memory = store.guard(model.entity("AgentMemory").unwrap()).unwrap();
    assert_eq!(memory.rule, Rule::Column("traversal_path".into()));
    assert_eq!(access.permission(memory.permission).name, "read_developer");

    let user = store.guard(model.entity("User").unwrap()).unwrap();
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
        let note = model.entity("Note").unwrap();
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
    let catalog = model.physical::<ClickHouse>("main").unwrap();
    let guard = catalog.guard(model.entity("Note").unwrap()).unwrap();
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
fn every_entity_needs_an_authorization_entry() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| {
            if name == "integration.linear/authorization.yaml" {
                yaml.replace("  LinearIssue:\n", "  LinearIssueRemoved:\n")
            } else {
                yaml
            }
        },
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: authorization names unknown entity LinearIssueRemoved",
            "integration.linear: entity LinearIssue has no authorization entry",
        ]
    );
}

#[test]
fn a_package_cannot_loosen_another_package_access_rule() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| {
            if name == "integration.linear/authorization.yaml" {
                yaml.replace(
                "entities:\n",
                "entities:\n  User: {scope: global, permission: read, properties: {open: [id, username, email]}}\n",
            )
            .replace(
                "relationships:\n",
                "relationships:\n  AUTHORED: {scope: namespace, permission: read, enforce: {}}\n",
            )
            } else {
                yaml
            }
        },
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: User.id has two access classes",
            "integration.linear: User.username has two access classes",
            "integration.linear: User.email has two access classes",
            "integration.linear: User has two authorization entries",
            "integration.linear: AUTHORED has two authorization entries",
            "integration.linear: User is in Global scope global, which a Integration package may not use",
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
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| match name {
            "integration.linear/package.yaml" => yaml.replace("requires: [gitlab]", "requires: []"),
            _ => yaml,
        },
    ));
    assert!(
        errors.contains(
            &"integration.linear: integration.linear names namespace from package gitlab, which it does not require"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn a_package_directory_holds_its_manifest() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| match name {
            "integration.linear/package.yaml" => {
                yaml.replace("name: integration.linear", "name: linear")
            }
            _ => yaml,
        },
    ));
    assert!(
        errors.contains(
            &"integration.linear/package.yaml: package.yaml names package linear, but sits in directory integration.linear"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn an_integration_package_declares_no_vocabulary() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| match name {
            "integration.linear/authorization.yaml" => yaml.replace(
                "package: integration.linear\n",
                "package: integration.linear\npermissions: [read_linear]\n",
            ),
            _ => yaml,
        },
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: integration.linear is a Integration package and declares permissions or scopes, which only a platform package may"
        ]
    );
}

#[test]
fn packages_cannot_redeclare_another_package_entity() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| {
            if name == "integration.linear/graph.yaml" {
                yaml.replace("  LinearIssue:\n", "  MergeRequest:\n")
            } else {
                yaml
            }
        },
    ));
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
            "gitlab: User.username is neither stored nor excluded by any store",
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
    let model = load(&["gitlab", "integration.linear"]).unwrap();
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
        "unknown entity Linear; candidates: LinearIssue"
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
    let has_note = graph.relationship_id("HAS_NOTE").unwrap();
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
    let thread = graph.attribute_id(has_note, "thread").unwrap();
    assert_eq!(
        graph.attribute(thread).description.as_deref(),
        Some("Discussion the note belongs to")
    );
    assert_eq!(graph.attribute_id(has_note, "missing"), None);
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
    let graph = model.graph();
    let has_note = graph.relationship_id("HAS_NOTE").unwrap();
    let access = |name| {
        model
            .access()
            .attribute(graph.attribute_id(has_note, name).unwrap())
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
    attributes: { source_id: int64, mood: feeling, state: enum }\n",
        ),
    ));
    assert_eq!(
        errors,
        [
            "gitlab: attribute HAS_NOTE.source_id is a reserved edge column",
            "gitlab: attribute HAS_NOTE.mood has unsupported type `feeling`",
            "gitlab: attribute HAS_NOTE.state has unsupported type `enum`",
        ]
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
    move |name, yaml| {
        match name {
        "gitlab/graph.yaml" => yaml.replace(HAS_NOTE_VARIANT, HAS_NOTE_ATTRIBUTES),
        "gitlab/authorization.yaml" => attribute_access(yaml, "{open: [position, thread]}"),
        "gitlab/storage/clickhouse.yaml" => yaml.replace(
            "sort_key: [traversal_path, source_id, relationship_kind, target_id] }",
            &format!(
                "sort_key: [traversal_path, source_id, relationship_kind, target_id], columns: {{ {edge_columns} }} }}"
            ),
        ),
        _ => yaml,
    }
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
    let catalog = model.physical::<ClickHouse>("main").unwrap();
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
    let model = load(&["gitlab", "integration.linear"]).unwrap();
    let issue = model.entity("LinearIssue").unwrap();
    let state = model.property(issue, "state").unwrap();
    assert_eq!(state.enum_type, query_data_model::EnumType::String);
    assert_eq!(state.enum_values.as_ref().unwrap()[&3], "done");
}

#[test]
fn model_identity_changes_with_the_spec() {
    let first = load(&["gitlab"]).unwrap();
    let again = load(&["gitlab"]).unwrap();
    let extended = load(&["gitlab", "integration.linear"]).unwrap();
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
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        |name, yaml| match name {
            "gitlab/graph.yaml" => {
                yaml.replace("domain: core\n", "domain: core\ndomain_description: Core\n")
            }
            "integration.linear/graph.yaml" => yaml.replace(
                "domain: planning\n",
                "domain: core\ndomain_description: Planning\n",
            ),
            _ => yaml,
        },
    ));
    assert_eq!(
        errors,
        ["integration.linear: domain core already has another description"]
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
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        edit_one(
            "integration.linear/storage/clickhouse.lake.yaml",
            "  connection: main\n",
            "  connection: main\n  trusted: true\n",
        ),
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: integration.linear uses trusted store lake, which only a platform package may",
            "integration.linear: store lake and store graph share connection main but only one is trusted",
        ]
    );
}

#[test]
fn one_connection_has_one_backend() {
    let errors = spec_errors(load_with(
        &["gitlab", "integration.linear"],
        edit_one(
            "integration.linear/storage/clickhouse.lake.yaml",
            "  kind: clickhouse",
            "  kind: duckdb",
        ),
    ));
    assert_eq!(
        errors,
        [
            "integration.linear: store lake is duckdb but connection main is clickhouse",
            "integration.linear: LinearIssue is stored in lake but enforces no duckdb policy",
            "integration.linear: RELATES_TO is stored in lake but enforces no duckdb policy",
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

    for packages in [Packages::embedded(), testkit::with_demo()] {
        let model = packages.load().unwrap();
        for store in model.stores() {
            for source in store.sources() {
                let entity = model.graph().entity(source.entity);
                let declared = model.access().entity(source.entity);
                let guard = store.guard(source.entity).unwrap();
                assert_eq!(
                    (guard.scope, guard.permission),
                    (declared.scope, declared.permission),
                    "{} {}",
                    store.name,
                    entity.name
                );
                assert_eq!(store.trusted, guard.rule == Rule::Trusted);
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
        ["gitlab: Project.full_path is neither stored nor excluded by any store"]
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
    assert_eq!(errors, ["gitlab: store graph binds Note without a table"]);
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
        "  File:\n    columns:",
        "  File:\n    table: gl_file\n    columns:",
    )]);
    let errors = spec_errors(packages.load());
    assert_eq!(
        errors,
        [
            "gitlab.source_code: store code is orbit_code and holds no table, but binds File to gl_file"
        ]
    );
}

#[test]
fn variants_that_share_a_key_column_name_distinct_types() {
    let packages = query_data_model::testkit::patched([(
        "gitlab/storage/clickhouse.yaml",
        "type_value: [Epic, Issue]",
        "type_value: [Epic, MergeRequest]",
    )]);
    let errors = spec_errors(packages.load());
    assert_eq!(
        errors,
        [
            "gitlab: store graph keys HAS_NOTE(MergeRequest -> Note) and HAS_NOTE(WorkItem -> Note) on noteable_id without distinct type values"
        ]
    );
}
