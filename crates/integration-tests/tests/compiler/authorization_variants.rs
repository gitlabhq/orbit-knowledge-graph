//! One authorization contract serves GitLab and other systems: the same packages and compiler
//! run against a Zanzibar-style, an OIDC-groups and a Slack sign-in deployment of seeded fakes.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use compiler::CompiledQueryContext;
use compiler::orbit_authorization::{Caller, CheckRequest, Grants, Key, Request, ScopeSet};
use integration_testkit::authorization_variants::fakes::OidcToken;
use integration_testkit::authorization_variants::{deployment, platform, registry};
use orbit_server::authorization::{BindError, Deployment};
use orbit_server::pipeline::{AuthorizationStage, RedactionStage};
use query_data_model::{Catalog, ClickHouse, Packages, Tier};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext};
use query_engine::shared::ExtractionOutput;
use query_engine::types::QueryResult;

fn bind(variant: &str, packages: &[&str]) -> (Arc<Catalog<ClickHouse>>, Deployment) {
    let model = platform(packages).load().unwrap();
    let deployment = Deployment::bind(&deployment(variant), &model, &registry()).unwrap();
    (
        Arc::new(Catalog::<ClickHouse>::from_model(&model).unwrap()),
        deployment,
    )
}

async fn signed_in(deployment: &Deployment, subject: &str, groups: &[&str]) -> (Caller, Grants) {
    let token = OidcToken {
        subject: subject.into(),
        groups: groups.iter().map(|group| group.to_string()).collect(),
    };
    let caller = deployment.identify(Request::new(token)).await.unwrap();
    let grants = deployment.grants(&caller).await.unwrap();
    (caller, grants)
}

fn keys(keys: &[&str]) -> ScopeSet {
    ScopeSet::Keys(keys.iter().map(|key| Key::Text(key.to_string())).collect())
}

/// The compiled SQL and the values of its array parameters.
fn compiled(
    catalog: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    query: &str,
) -> (String, Vec<serde_json::Value>) {
    let compiled = compiler::compile(query, catalog, grants).unwrap();
    let arrays = compiled
        .base
        .params
        .values()
        .filter(|param| param.value.is_array())
        .map(|param| param.value.clone())
        .collect();
    (compiled.base.sql, arrays)
}

/// The rows that survive the server's authorization and redaction stages, which ask the
/// deployment's providers about every row check.
async fn through_the_server(
    deployment: Arc<Deployment>,
    caller: Caller,
    compiled: &CompiledQueryContext,
    batch: RecordBatch,
) -> QueryResult {
    let mut ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: None,
        security_context: None,
        grants: None,
        server_extensions: Default::default(),
        phases: Default::default(),
    };
    ctx.server_extensions.insert(deployment);
    ctx.server_extensions.insert(caller);
    ctx.phases.insert(ExtractionOutput {
        query_result: QueryResult::from_batches(&[batch], &compiled.base.result_context).unwrap(),
    });
    let authorized = AuthorizationStage
        .execute(&mut ctx, &mut NoOpObserver)
        .await
        .unwrap();
    ctx.phases.insert(authorized);
    RedactionStage
        .execute(&mut ctx, &mut NoOpObserver)
        .await
        .unwrap()
        .query_result
}

/// The ids of `(document, binder)` rows that both checks allow: the approver checks the
/// document's id, the policy its binder's.
async fn redacted(
    deployment: Arc<Deployment>,
    caller: Caller,
    catalog: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    rows: &[(i64, i64)],
) -> Vec<i64> {
    let compiled = compiler::compile(
        "MATCH (d:Document) WHERE d.id IN [1, 2, 3, 4] RETURN d.title LIMIT 10",
        catalog,
        grants,
    )
    .unwrap();
    let sql = &compiled.base.sql;
    assert!(sql.contains("d.binder_id AS _orbit_d_id"), "{sql}");
    assert!(sql.contains("AS _orbit_d_pk"), "{sql}");
    let schema = Arc::new(Schema::new(vec![
        Field::new("_orbit_d_pk", DataType::Int64, false),
        Field::new("_orbit_d_id", DataType::Int64, false),
        Field::new("_orbit_d_type", DataType::Utf8, false),
    ]));
    let (documents, binders): (Vec<i64>, Vec<i64>) = rows.iter().copied().unzip();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(documents)),
            Arc::new(Int64Array::from(binders)),
            Arc::new(StringArray::from(vec!["Document"; rows.len()])),
        ],
    )
    .unwrap();
    let result = through_the_server(deployment, caller, &compiled, batch).await;
    let mut ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|row| row.get("_orbit_d_pk")?.as_int64().copied())
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn a_zanzibar_deployment_scopes_with_keys_and_checks_every_row_check() {
    let (catalog, deployment) = bind("zanzibar", &["acme", "acme.checked"]);
    let deployment = Arc::new(deployment);
    let (caller, grants) = signed_in(&deployment, "alice", &[]).await;
    assert_eq!(
        grants.scope_set("team", "read").unwrap(),
        &keys(&["team-a", "team-b"])
    );
    assert_eq!(
        grants.scope_set("everyone", "read").unwrap(),
        &ScopeSet::All
    );

    let (sql, arrays) = compiled(
        &catalog,
        &grants,
        "MATCH (f:Folder) WHERE f.id IN [1, 2] RETURN f.name LIMIT 5",
    );
    assert!(sql.contains("f.team_key IN {p"), "{sql}");
    assert!(
        arrays.contains(&serde_json::json!(["team-a", "team-b"])),
        "{arrays:?}"
    );

    assert_eq!(
        redacted(
            deployment.clone(),
            caller,
            &catalog,
            &grants,
            &[(1, 10), (2, 20), (3, 10), (4, 10)]
        )
        .await,
        [1, 3]
    );
    let (bob, bob_grants) = signed_in(&deployment, "bob", &[]).await;
    assert_eq!(
        redacted(
            deployment,
            bob,
            &catalog,
            &bob_grants,
            &[(1, 10), (2, 20), (4, 10)]
        )
        .await,
        [4]
    );
}

#[tokio::test]
async fn an_oidc_deployment_scopes_with_the_exact_groups_claim_and_no_checks() {
    let (catalog, deployment) = bind("oidc", &["acme"]);
    let (_, grants) = signed_in(&deployment, "carol", &["platform", "infra"]).await;
    assert_eq!(
        grants.scope_set("team", "read").unwrap(),
        &keys(&["platform", "infra"])
    );

    let (sql, arrays) = compiled(
        &catalog,
        &grants,
        "MATCH (f:Folder) WHERE f.id IN [1, 2] RETURN f.name LIMIT 5",
    );
    assert!(sql.contains("f.team_key IN {p"), "{sql}");
    assert!(
        arrays.contains(&serde_json::json!(["platform", "infra"])),
        "{arrays:?}"
    );
    let error = compiler::compile(
        "MATCH (f:Folder) WHERE f.id = 1 RETURN f.team_key",
        &catalog,
        &grants,
    );
    assert!(error.is_err());
}

#[tokio::test]
async fn a_gitlab_deployment_with_slack_answers_the_delegated_check_with_a_sign_in() {
    let model = query_data_model::testkit::with_demo().load().unwrap();
    let deployment = Deployment::bind(&deployment("slack"), &model, &registry()).unwrap();
    let caller = Caller::new("user:1", false, ());
    let credential = deployment
        .credentials(&caller, "slack", "slack")
        .await
        .unwrap();
    assert_eq!(credential.handle, "slack-sign-in-1");
    let stranger = Caller::new("user:2", false, ());
    assert!(
        deployment
            .credentials(&stranger, "slack", "slack")
            .await
            .is_err()
    );
}

#[test]
fn a_package_naming_an_unbound_provider_fails_the_bind() {
    let model = platform(&["acme", "acme.checked"]).load().unwrap();
    let Err(BindError(errors)) = Deployment::bind(&deployment("oidc"), &model, &registry()) else {
        panic!("the OIDC deployment binds no checks");
    };
    assert_eq!(
        errors,
        [
            "packages name provider approver, which the deployment does not bind",
            "packages name provider policy, which the deployment does not bind",
        ]
    );
}

#[test]
fn a_module_that_does_not_answer_the_needed_query_fails_the_bind() {
    let model = platform(&["acme", "acme.checked"]).load().unwrap();
    let mut config = deployment("zanzibar");
    let oidc = config.providers["oidc"].clone();
    config.providers.insert("policy".into(), oidc);
    let Err(BindError(errors)) = Deployment::bind(&config, &model, &registry()) else {
        panic!("fake.oidc answers no Check");
    };
    assert_eq!(
        errors,
        ["provider policy is bound to module fake.oidc, which does not answer Check"]
    );
    let model = query_data_model::testkit::with_demo().load().unwrap();
    let mut config = deployment("slack");
    config.providers.remove("slack");
    let Err(BindError(errors)) = Deployment::bind(&config, &model, &registry()) else {
        panic!("the delegated Slack check needs a provider");
    };
    assert_eq!(
        errors,
        ["packages name provider slack, which the deployment does not bind"]
    );
}

#[tokio::test]
async fn a_check_answers_string_keyed_rows_with_text_keys() {
    let (_, deployment) = bind("messages", &["acme", "acme.messages"]);
    let (alice, _) = signed_in(&deployment, "alice", &["team-a"]).await;
    let text =
        |ids: &[&str]| -> Vec<Key> { ids.iter().map(|id| Key::Text(id.to_string())).collect() };
    let request = CheckRequest {
        permission: "read".into(),
        resource: "message".into(),
        ids: text(&["m1", "m2", "m3"]),
    };
    let allowed = deployment
        .check(&alice, "slack", vec![request.clone()])
        .await
        .unwrap();
    assert_eq!(allowed, [BTreeSet::from_iter(text(&["m1", "m3"]))]);
    let (bob, _) = signed_in(&deployment, "bob", &["team-c"]).await;
    let allowed = deployment
        .check(&bob, "slack", vec![request])
        .await
        .unwrap();
    assert_eq!(allowed, [BTreeSet::new()]);
}

#[tokio::test]
async fn the_server_redacts_string_keyed_rows_a_check_denies() {
    let (catalog, deployment) = bind("messages", &["acme", "acme.messages"]);
    let (alice, grants) = signed_in(&deployment, "alice", &["team-a"]).await;
    let compiled = compiler::compile(
        "MATCH (m:Message) WHERE m.id IN ['m1', 'm2', 'm3'] RETURN m.text LIMIT 10",
        &catalog,
        &grants,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("_orbit_m_id", DataType::Utf8, false),
        Field::new("_orbit_m_type", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["m1", "m2", "m3"])),
            Arc::new(StringArray::from(vec!["Message"; 3])),
        ],
    )
    .unwrap();
    let result = through_the_server(Arc::new(deployment), alice, &compiled, batch).await;
    let ids: Vec<&str> = result
        .authorized_rows()
        .filter_map(|row| row.get("_orbit_m_id")?.as_string().map(String::as_str))
        .collect();
    assert_eq!(ids, ["m1", "m3"]);
    assert_eq!(result.len() - result.authorized_count(), 1);
}

#[test]
fn a_check_that_keys_rows_by_a_boolean_fails_the_load() {
    let mut sources = platform(&["acme", "acme.messages"]).sources().clone();
    let mut edit = |path: &str, from: &str, to: &str| {
        let text = sources.get_mut(path).unwrap();
        assert!(text.contains(from), "{path} holds no {from}");
        *text = text.replace(from, to);
    };
    edit(
        "acme.messages/graph.yaml",
        "      team_key: string\n",
        "      team_key: string\n      pinned: boolean\n",
    );
    edit(
        "acme.messages/storage/clickhouse.yaml",
        "      team_key: {backend: {type: String}}\n",
        "      team_key: {backend: {type: String}}\n      pinned: {backend: {type: Bool}}\n",
    );
    edit(
        "acme.messages/authorization.yaml",
        "open: [id, text]",
        "open: [id, text, pinned]",
    );
    edit(
        "acme.messages/authorization.yaml",
        "checks: [{provider: slack, permission: read, resource: message}]",
        "checks: [{provider: slack, permission: read, resource: message}, \
         {provider: slack, permission: read, resource: message, object: Message, id: pinned}]",
    );
    let error = Packages::new(Tier::Platform, sources)
        .load()
        .unwrap_err()
        .to_string();
    let expected = "Message check keys rows by pinned, a Bool; a key is an integer, string or uuid";
    assert!(error.contains(expected), "{error}");
}
