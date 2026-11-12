mod support;

use query_data_model::catalog::Rule;
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::{DataModelError, DataType, Direction, Endpoint};
use support::demo;

const ENTITIES: [&str; 5] = [
    "JiraProject",
    "JiraIssue",
    "JiraUser",
    "JiraComment",
    "GitLabRef",
];

fn spec_errors<T: std::fmt::Debug>(result: Result<T, DataModelError>) -> Vec<String> {
    match result {
        Err(DataModelError::Spec(errors)) => errors.iter().map(ToString::to_string).collect(),
        other => panic!("expected spec errors, got {other:?}"),
    }
}

fn edit(
    file: &'static str,
    find: &'static str,
    replace: &'static str,
) -> impl Fn(&str, String) -> String {
    move |name, yaml| match name == file {
        true => {
            assert_eq!(
                yaml.matches(find).count(),
                1,
                "{file} must contain {find:?} once"
            );
            yaml.replacen(find, replace, 1)
        }
        false => yaml,
    }
}

#[test]
fn every_jira_entity_has_one_complete_store_on_the_lake_connection() {
    let model = demo(|_, yaml| yaml).unwrap();
    for name in ENTITIES {
        let entity = model.entity(name).unwrap();
        let holders: Vec<_> = model
            .stores()
            .iter()
            .filter(|store| store.source(entity).is_some())
            .map(|store| {
                (
                    store.name.as_str(),
                    store.kind.as_str(),
                    store.connection.as_str(),
                )
            })
            .collect();
        assert_eq!(holders, [("jira", "duckdb", "jira_lake")], "{name}");
        let store = model
            .stores()
            .iter()
            .find(|store| store.name == "jira")
            .unwrap();
        assert!(store.complete(entity), "{name}");
        assert!(!store.trusted);
    }
    let catalog = model.physical::<DuckDb>("jira_lake").unwrap();
    let tables: Vec<_> = catalog.tables().map(|table| table.name.as_str()).collect();
    assert_eq!(
        tables,
        [
            "jira_gitlab_ref",
            "jira_comment",
            "jira_issue",
            "jira_project",
            "jira_user"
        ]
    );
    let issue = catalog
        .tables()
        .find(|table| table.name == "jira_issue")
        .unwrap();
    let typed: Vec<_> = issue
        .columns
        .iter()
        .map(|column| {
            (
                column.name.as_str(),
                column.storage_type.as_str(),
                column.nullable,
            )
        })
        .collect();
    assert_eq!(
        typed,
        [
            ("id", "VARCHAR", false),
            ("key", "VARCHAR", false),
            ("summary", "VARCHAR", false),
            ("status", "VARCHAR", false),
            ("status_category", "VARCHAR", false),
            ("jira_project_id", "VARCHAR", false),
            ("assignee_id", "VARCHAR", true),
            ("merge_request_id", "BIGINT", true),
            ("created_at", "TIMESTAMP", false),
            ("traversal_path", "VARCHAR", false),
        ]
    );
}

#[test]
fn jira_keys_are_strings_and_the_site_is_a_uuid() {
    let model = demo(|_, yaml| yaml).unwrap();
    let identity = |entity: &str, property: &str| {
        let entity = model.entity(entity).unwrap();
        model.property(entity, property).unwrap().data_type
    };
    assert_eq!(identity("JiraProject", "id"), DataType::String);
    assert_eq!(identity("JiraIssue", "id"), DataType::String);
    assert_eq!(identity("JiraUser", "account_id"), DataType::String);
    assert_eq!(identity("JiraComment", "id"), DataType::String);
    assert_eq!(identity("JiraProject", "cloud_id"), DataType::Uuid);
}

#[test]
fn jira_foreign_keys_reference_gitlab_entities_on_another_connection() {
    let model = demo(|_, yaml| yaml).unwrap();
    let graph = model.graph();
    for (relationship, source, target, column) in [
        (
            "DEVELOPED_IN",
            "JiraIssue",
            "MergeRequest",
            "merge_request_id",
        ),
        ("MAPPED_TO", "JiraProject", "Project", "project_id"),
        (
            "IN_JIRA_PROJECT",
            "JiraIssue",
            "JiraProject",
            "jira_project_id",
        ),
        ("COMMENTS_ON", "JiraComment", "JiraIssue", "issue_id"),
    ] {
        let variant = graph.variant_named(relationship, source, target).unwrap();
        let key = model.foreign_key(variant.id).unwrap();
        assert_eq!(key.holder, Endpoint::Source, "{relationship}");
        assert_eq!(graph.property(key.property).name, column, "{relationship}");
    }
    let cites = graph
        .variant_named("CITES", "JiraIssue", "GitLabRef")
        .unwrap();
    let key = model.foreign_key(cites.id).unwrap();
    assert_eq!(key.holder, Endpoint::Target);
    assert_eq!(graph.property(key.property).name, "issue_id");
    let target = |entity: &str| {
        let entity = model.entity(entity).unwrap();
        model
            .stores()
            .iter()
            .find(|store| store.complete(entity))
            .map(|store| store.connection.as_str())
            .unwrap()
    };
    assert_eq!(target("MergeRequest"), "main");
    assert_eq!(target("Project"), "main");
    assert_eq!(target("JiraIssue"), "jira_lake");

    let issue = model.entity("JiraIssue").unwrap();
    let request = model.entity("MergeRequest").unwrap();
    let hops = model
        .connect(request, &[], Some(issue), Direction::Both)
        .unwrap();
    let names: Vec<_> = hops
        .iter()
        .map(|hop| graph.relationship(hop.relationship).name.as_str())
        .collect();
    assert_eq!(names, ["DEVELOPED_IN"]);
}

#[test]
fn jira_reads_are_scoped_by_the_path_the_parquet_rows_carry() {
    let model = demo(|_, yaml| yaml).unwrap();
    let access = model.access();
    let catalog = model.physical::<DuckDb>("jira_lake").unwrap();
    let store = catalog.store("jira").unwrap();
    for name in ENTITIES {
        let guard = store.guard(model.entity(name).unwrap()).unwrap();
        assert_eq!(guard.rule, Rule::Column("traversal_path".into()), "{name}");
        assert_eq!(access.permission(guard.permission).name, "read");
        assert_eq!(access.scope(guard.scope).name, "namespace");
    }
}

#[test]
fn a_jira_foreign_key_of_another_type_than_its_target_identity_fails_the_load() {
    let errors = spec_errors(demo(edit(
        "integration.jira/graph.yaml",
        "merge_request_id: {type: int64?",
        "merge_request_id: {type: string?",
    )));
    assert!(
        errors
            .iter()
            .any(|error| error.starts_with("integration.jira: ")
                && error.contains("DEVELOPED_IN(JiraIssue -> MergeRequest)")
                && error.ends_with("keys are never cast")),
        "{errors:?}"
    );
}

#[test]
fn a_jira_foreign_key_references_an_entity_some_store_holds() {
    let errors = spec_errors(demo(edit(
        "gitlab.code_review/storage/clickhouse.yaml",
        "  MergeRequest:\n",
        "  Unbound:\n",
    )));
    assert!(
        errors.iter().any(|error| error.ends_with(
            "binds DEVELOPED_IN(JiraIssue -> MergeRequest), but no store holds MergeRequest"
        )),
        "{errors:?}"
    );
}

#[test]
fn a_jira_entity_without_a_complete_store_fails_the_load() {
    let errors = spec_errors(demo(edit(
        "integration.jira/storage/duckdb.yaml",
        "  JiraUser:\n    complete: true\n",
        "  JiraUser:\n",
    )));
    assert_eq!(
        errors,
        ["integration.jira: JiraUser is stored in jira but no store marks it complete: true"]
    );
}

#[test]
fn a_jira_entity_without_a_duckdb_policy_fails_the_load() {
    let errors = spec_errors(demo(edit(
        "integration.jira/authorization.yaml",
        "  JiraComment:\n    scope: namespace\n    permission: read\n    enforce: {duckdb: {scope_key: traversal_path}}\n",
        "  JiraComment:\n    scope: namespace\n    permission: read\n    enforce: {}\n",
    )));
    assert_eq!(
        errors,
        ["integration.jira: JiraComment is stored in jira but enforces no duckdb policy"]
    );
}

#[test]
fn the_jira_package_may_not_trust_its_store() {
    let errors = spec_errors(demo(edit(
        "integration.jira/storage/duckdb.yaml",
        "  connection: jira_lake\n",
        "  connection: jira_lake\n  trusted: true\n",
    )));
    assert!(
        errors.contains(
            &"integration.jira: integration.jira uses trusted store jira, which only a platform package may"
                .to_string()
        ),
        "{errors:?}"
    );
}

#[test]
fn a_jira_column_without_a_storage_type_fails_the_load() {
    let errors = spec_errors(demo(edit(
        "integration.jira/storage/duckdb.yaml",
        "      summary: {backend: {type: VARCHAR}}\n",
        "      summary: {}\n",
    )));
    assert_eq!(
        errors,
        ["integration.jira: store jira column JiraIssue.summary declares no storage type"]
    );
}

#[test]
fn the_des_source_declares_the_lifecycle_columns_its_parquet_rows_carry() {
    let model = demo(|_, yaml| yaml).unwrap();
    let sources = model.sources();
    let des = sources.source_named("des").unwrap();
    assert_eq!(
        (
            des.connection.as_str(),
            des.version.as_str(),
            des.watermark.as_str(),
            des.deleted.as_str()
        ),
        (
            "jira_lake",
            "_siphon_replicated_at",
            "_siphon_watermark",
            "_siphon_deleted"
        )
    );
    let store = model
        .stores()
        .iter()
        .find(|store| store.name == "jira")
        .unwrap();
    for name in ENTITIES {
        let entity = model.entity(name).unwrap();
        let table = store.source(entity).unwrap().table.clone().unwrap();
        let pipelines: Vec<_> = sources
            .entity_pipelines(entity)
            .map(|pipeline| {
                (
                    sources.data_source(pipeline).name.as_str(),
                    pipeline.tables.clone(),
                )
            })
            .collect();
        assert_eq!(pipelines, [("des", vec![table])], "{name}");
    }
}
