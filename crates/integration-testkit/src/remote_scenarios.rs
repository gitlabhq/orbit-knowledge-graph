//! The ADR 024 code and live queries end to end: GQL text, logical plan, planned and emitted
//! physical plans, rendered request, and rows from the seeded services.

use std::sync::Arc;

use compiler::QueryError;
use compiler::orbit_authorization::{Grants, Key};
use compiler::remote::{self, RemotePlan, RemoteRequest};
use query_data_model::catalog::Row;
use query_data_model::implementations::live::{DataExtraction, ExtractionRequest, RequestFilter};
use query_data_model::implementations::orbit_code::{
    CodeSearch, RequestField, StructuralSearchRequest,
};
use query_data_model::plan::code::CodeOperation;
use query_data_model::plan::live::{Condition, LiveOperation};
use query_data_model::plan::logical::{LogicalOperation, LogicalPlan, Projection};
use query_data_model::{Model, testkit};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use serde_json::json;

use crate::seeded_remote::{SeededCodeSearch, SeededDataExtraction, seeded_namespaces};

const CODE: &str = "MATCH (f:File) WHERE f.project_id = 42 AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.path, match.line, match.ARGS LIMIT 50";
const SLACK: &str = "MATCH (m:SlackMessage) WHERE m.channel = 'orbit-dev' AND m.text CONTAINS 'deploy' RETURN m.author, m.text LIMIT 20";

fn model() -> Arc<Model> {
    testkit::with_demo().load().unwrap()
}

fn caller(path: &str) -> Grants {
    let session = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new(path, 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, &model(), session).unwrap()
}

fn logical(plan: &LogicalPlan) -> Vec<String> {
    let name = |output| plan.name(output).unwrap_or("_").to_string();
    plan.nodes()
        .map(|node| match plan.operation(node) {
            LogicalOperation::Entities(_) => {
                format!("Entities {}", name(plan.outputs(node).last().unwrap()))
            }
            LogicalOperation::Filter(predicate) => format!("Filter {}", predicate.leaves()),
            LogicalOperation::Structural(structural) => {
                let language: &str = structural.language.into();
                format!("Structural {} {language}", structural.pattern.text())
            }
            LogicalOperation::Select(items) => {
                let items: Vec<&str> = items
                    .iter()
                    .map(|item| match item {
                        Projection::Graph { .. } => "graph",
                        Projection::Value(_) => "value",
                    })
                    .collect();
                format!("Select {}", items.join(","))
            }
            LogicalOperation::Limit { rows, .. } => format!("Limit {rows}"),
            other => format!("{other:?}"),
        })
        .collect()
}

fn physical(plan: &RemotePlan) -> Vec<String> {
    match plan {
        RemotePlan::Code(plan) => plan
            .nodes()
            .map(|node| match plan.operation(node) {
                CodeOperation::Repositories { projects, .. } => {
                    format!("Repositories {projects:?}")
                }
                CodeOperation::Restrict(projects) => format!("Restrict {projects:?}"),
                CodeOperation::Languages(languages) => format!("Languages {languages:?}"),
                CodeOperation::Prefilter(atoms) => format!("Prefilter {atoms:?}"),
                CodeOperation::Structural(pattern) => format!("Structural {}", pattern.text()),
                CodeOperation::Top(count) => format!("Top {count}"),
                CodeOperation::Select(fields) => format!(
                    "Select {}",
                    fields
                        .iter()
                        .map(|field| plan.name(*field).unwrap_or("_"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            })
            .collect(),
        RemotePlan::Live(plan) => plan
            .nodes()
            .map(|node| match plan.operation(node) {
                LiveOperation::Call(_) => "Call".to_string(),
                LiveOperation::Filter(conditions) => format!(
                    "Filter {}",
                    conditions
                        .iter()
                        .map(|condition| match condition {
                            Condition::Equals { field, .. } => {
                                format!("{} =", plan.name(*field).unwrap_or("_"))
                            }
                            Condition::Contains { field, .. } => {
                                format!("{} CONTAINS", plan.name(*field).unwrap_or("_"))
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                LiveOperation::Window { .. } => "Window".to_string(),
                LiveOperation::Workspaces(workspaces) => format!("Workspaces {workspaces:?}"),
                LiveOperation::Limit(rows) => format!("Limit {rows}"),
                LiveOperation::Select(fields) => format!(
                    "Select {}",
                    fields
                        .iter()
                        .map(|field| plan.name(*field).unwrap_or("_"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            })
            .collect(),
    }
}

/// The stages of one compile, each checked plan kept for its assertions.
struct Compiled {
    logical: Vec<String>,
    planned: Vec<String>,
    emitted: Vec<String>,
    request: RemoteRequest,
}

fn stages(query: &str, ctx: &Grants) -> Result<Compiled, QueryError> {
    let model = model();
    let plan = remote::parse(query, &model, ctx, query_data_model::testkit::limits())?;
    let (target, planned) = remote::plan(&plan, &model)?;
    let planned_shape = physical(&planned);
    assert!(matches!(
        remote::check(&planned),
        Err(QueryError::Security(_))
    ));
    let emitted = remote::secure(&target, planned, ctx, &seeded_namespaces())?;
    remote::check(&emitted)?;
    let request = remote::render(&target, &emitted)?;
    assert_eq!(
        remote::compile(
            query,
            &model,
            ctx,
            &seeded_namespaces(),
            query_data_model::testkit::limits()
        )?,
        request
    );
    Ok(Compiled {
        logical: logical(&plan),
        planned: planned_shape,
        emitted: physical(&emitted),
        request,
    })
}

fn rows(request: &RemoteRequest, user: &str) -> Vec<Row> {
    match request {
        RemoteRequest::Code(request) => SeededCodeSearch::from_seed_file().search(request),
        RemoteRequest::Live(request) => {
            SeededDataExtraction::from_seed_file().extract(user, request)
        }
    }
    .unwrap()
}

fn row(value: serde_json::Value) -> Row {
    value.as_object().cloned().unwrap()
}

#[test]
fn the_code_query_plans_into_a_structural_search_and_returns_matches() {
    let compiled = stages(CODE, &caller("1/9970/")).unwrap();
    assert_eq!(
        compiled.logical,
        [
            "Entities f",
            "Filter 1",
            "Structural compile($$$ARGS) rust",
            "Select graph,value,value",
            "Limit 50",
        ]
    );
    assert_eq!(
        compiled.planned,
        [
            "Repositories [42]",
            "Languages [Rust]",
            "Prefilter [\"compile\"]",
            "Structural compile($$$ARGS)",
            "Top 50",
            "Select path,line,ARGS,project_id",
        ]
    );
    assert_eq!(
        compiled.emitted[..2],
        ["Repositories [42]", "Restrict [42]"]
    );
    assert_eq!(compiled.emitted[2..], compiled.planned[1..]);
    assert_eq!(
        compiled.request,
        RemoteRequest::Code(StructuralSearchRequest {
            repositories: vec![42],
            languages: vec!["rust".into()],
            trigrams: vec!["compile".into()],
            pattern: "compile($$$ARGS)".into(),
            limit: Some(50),
            fields: vec![
                RequestField::Path,
                RequestField::Line,
                RequestField::Capture("ARGS".into()),
                RequestField::Repository("project_id".into()),
            ],
        })
    );
    assert_eq!(
        serde_json::to_value(&compiled.request).unwrap(),
        json!({
            "backend": "code",
            "repositories": [42],
            "languages": ["rust"],
            "trigrams": ["compile"],
            "pattern": "compile($$$ARGS)",
            "limit": 50,
            "fields": ["path", "line", {"capture": "ARGS"}, {"repository": "project_id"}],
        })
    );
    assert_eq!(
        rows(&compiled.request, "alice"),
        [
            row(
                json!({"path": "crates/query-engine/compiler/src/lib.rs", "line": 120, "ARGS": "raw, fe, &data_model, &ctx", "project_id": 42})
            ),
            row(
                json!({"path": "crates/orbit-server/src/query.rs", "line": 48, "ARGS": "&text, Frontend::Gql, &catalog, &security", "project_id": 42})
            ),
        ]
    );
}

#[test]
fn the_slack_query_plans_into_a_live_call_and_returns_visible_messages() {
    let compiled = stages(SLACK, &caller("1/9970/")).unwrap();
    assert_eq!(
        compiled.logical,
        ["Entities m", "Filter 2", "Select graph", "Limit 20"]
    );
    assert_eq!(
        compiled.planned,
        [
            "Call",
            "Filter channel =, text CONTAINS",
            "Limit 20",
            "Select author,text",
        ]
    );
    assert_eq!(
        compiled.emitted,
        [
            "Call",
            "Workspaces [\"T01\"]",
            "Filter channel =, text CONTAINS",
            "Limit 20",
            "Select author,text",
        ]
    );
    assert_eq!(
        compiled.request,
        RemoteRequest::Live(ExtractionRequest {
            operation: "search.messages".into(),
            workspaces: vec!["T01".into()],
            filters: vec![
                RequestFilter {
                    field: "channel".into(),
                    op: "eq",
                    value: "orbit-dev".into(),
                },
                RequestFilter {
                    field: "text".into(),
                    op: "contains",
                    value: "deploy".into(),
                },
            ],
            window: None,
            limit: Some(20),
            fields: vec!["author".into(), "text".into()],
        })
    );
    assert_eq!(
        rows(&compiled.request, "alice"),
        [row(
            json!({"author": "ann", "text": "deploy to staging at 5pm"})
        )]
    );
    assert_eq!(
        rows(&compiled.request, "bob"),
        [
            row(json!({"author": "ann", "text": "deploy to staging at 5pm"})),
            row(
                json!({"author": "ben", "text": "rolled back the deploy, see the private incident channel"})
            ),
        ]
    );
}

#[test]
fn the_repository_restriction_drops_projects_the_caller_cannot_read() {
    let query = CODE.replace("f.project_id = 42", "f.project_id IN [42, 77]");
    let compiled = stages(&query, &caller("1/9970/")).unwrap();
    assert_eq!(compiled.planned[0], "Repositories [42, 77]");
    assert_eq!(compiled.emitted[1], "Restrict [42]");
    let RemoteRequest::Code(request) = &compiled.request else {
        panic!("expected a code request");
    };
    assert_eq!(request.repositories, [42]);
    let paths: Vec<_> = rows(&compiled.request, "alice")
        .into_iter()
        .map(|row| row["path"].clone())
        .collect();
    assert!(!paths.contains(&json!("src/vault.rs")), "{paths:?}");

    let both = stages(&query, &caller("1/")).unwrap();
    let paths: Vec<_> = rows(&both.request, "alice")
        .into_iter()
        .map(|row| row["path"].clone())
        .collect();
    assert!(paths.contains(&json!("src/vault.rs")), "{paths:?}");
}

#[test]
fn the_workspace_precondition_keeps_unreadable_namespaces_out_of_the_call() {
    let everywhere = stages(SLACK, &caller("1/")).unwrap();
    let RemoteRequest::Live(request) = &everywhere.request else {
        panic!("expected a live request");
    };
    assert_eq!(request.workspaces, ["T01", "T02"]);
    assert_eq!(rows(&everywhere.request, "alice").len(), 2);

    let narrow = stages(SLACK, &caller("1/9970/")).unwrap();
    let texts: Vec<_> = rows(&narrow.request, "alice")
        .into_iter()
        .map(|row| row["text"].clone())
        .collect();
    assert!(!texts.contains(&json!("deploy uses the production keys")));
}

#[test]
fn a_caller_who_can_read_nothing_gets_no_request() {
    let nobody = caller("1/424242/");
    for (query, message) in [
        (
            CODE,
            "authorization error: the caller can read none of the requested repositories",
        ),
        (
            SLACK,
            "authorization error: the caller can read none of the workspaces",
        ),
    ] {
        let error = stages(query, &nobody).err().expect("the plan is rejected");
        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn construction_rejects_a_filter_without_push_down_and_an_unknown_capture() {
    let ctx = caller("1/9970/");
    let pushed = stages(
        "MATCH (m:SlackMessage) WHERE m.id = 'm1' RETURN m.text LIMIT 5",
        &ctx,
    );
    assert_eq!(
        pushed.err().unwrap().to_string(),
        "schema violation: search.messages cannot push down equality on SlackMessage.id; it filters on channel, author, sent_at, text (contains)"
    );
    let capture = stages(&CODE.replace("match.ARGS", "match.BODY"), &ctx);
    assert_eq!(
        capture.err().unwrap().to_string(),
        "reference error: unknown column match.BODY; available: match.line, match.ARGS"
    );
    let relational = compiler::compile(
        CODE,
        &Arc::new(query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap()),
        &ctx,
        query_data_model::testkit::limits(),
    );
    assert_eq!(
        relational.err().unwrap().to_string(),
        "schema violation: structural matches run on an orbit_code connection, not in SQL"
    );
}

/// The rows the graph connection would return for each lookup, from the seeded namespaces.
fn lookup_rows(lookup: &remote::ScopeLookup) -> Vec<Row> {
    let seeded = seeded_namespaces();
    let scope =
        |path: &orbit_utils::traversal_path::TraversalPath| serde_json::Value::from(path.as_str());
    match lookup.resolves {
        remote::Resolves::Repositories => seeded
            .repositories
            .iter()
            .map(|(id, path)| row(json!({"id": id, lookup.scope.clone(): scope(path)})))
            .collect(),
        remote::Resolves::Workspaces => seeded
            .workspaces
            .iter()
            .map(|(id, path)| row(json!({"id": id, lookup.scope.clone(): scope(path)})))
            .collect(),
    }
}

#[test]
fn via_chains_resolve_their_scope_keys_on_the_graph_connection() {
    let model = model();
    let graph =
        query_data_model::Catalog::<query_data_model::ClickHouse>::from_model(&model).unwrap();
    let grants = caller("1/9970/");
    let sql = |query: &str| {
        let logical =
            remote::parse(query, &model, &grants, query_data_model::testkit::limits()).unwrap();
        let (target, planned) = remote::plan(&logical, &model).unwrap();
        let lookups = remote::lookups(&target, &planned, &graph, &grants).unwrap();
        let results: Vec<_> = lookups
            .iter()
            .map(|lookup| (lookup, lookup_rows(lookup)))
            .collect();
        let namespaces = remote::namespaces(&results).unwrap();
        let emitted = remote::secure(&target, planned, &grants, &namespaces).unwrap();
        remote::check(&emitted).unwrap();
        (
            lookups
                .iter()
                .map(|lookup| lookup.query.sql.clone())
                .collect::<Vec<_>>(),
            physical(&emitted),
        )
    };
    let (code, emitted) = sql(CODE);
    assert_eq!(
        code,
        [
            "SELECT k0.id, k0.traversal_path FROM gl_project AS k0 FINAL WHERE (startsWith(k0.traversal_path, {p0:String}) AND k0.id IN {p1:Array(Int64)})"
        ]
    );
    assert_eq!(emitted[..2], ["Repositories [42]", "Restrict [42]"]);
    let (live, emitted) = sql(SLACK);
    assert_eq!(
        live,
        [
            "SELECT k0.id, k1.traversal_path FROM gl_slack_workspace AS k0 FINAL INNER JOIN gl_group AS k1 FINAL ON (k0.group_id = k1.id) WHERE startsWith(k1.traversal_path, {p0:String})"
        ]
    );
    assert_eq!(emitted[1], "Workspaces [\"T01\"]");
}

#[test]
fn code_rows_go_through_the_read_code_check() {
    let model = model();
    let logical = remote::parse(
        CODE,
        &model,
        &caller("1/9970/"),
        query_data_model::testkit::limits(),
    )
    .unwrap();
    let checks = remote::row_checks(&logical, &model);
    assert_eq!(
        checks,
        [remote::RowCheck {
            provider: "rails".into(),
            permission: "read_code".into(),
            resource: "project".into(),
            field: "project_id".into(),
            kind: compiler::KeyKind::Int,
        }]
    );
    let compiled = stages(CODE, &caller("1/9970/")).unwrap();
    let found = rows(&compiled.request, "alice");
    assert_eq!(
        remote::apply_check(found.clone(), &checks[0], &[Key::Int(42)].into()).len(),
        2
    );
    assert!(remote::apply_check(found, &checks[0], &Default::default()).is_empty());
}
