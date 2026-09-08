use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use integration_testkit::{TestContext, load_ontology, load_seed, run_subtests_shared, t};
use jsonwebtoken::{EncodingKey, Header, encode};
use orbit_server::auth::{Claims, JwtValidator};
use orbit_server::cluster_health::ClusterHealthChecker;
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::orbit_service_server::OrbitServiceServer;
use orbit_server::proto::{
    ExecuteQueryMessage, ExecuteQueryRequest, ExecuteQueryResult, QueryType, RedactionExchange,
    RedactionResponse, ResourceAuthorization, ResponseFormat, execute_query_message,
    execute_query_result, redaction_exchange,
};
use orbit_server_config::{AnalyticsConfig, GrpcConfig};
use prost::Message;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::Request;
use tonic::transport::{Channel, Server};

use crate::common::{DummyClaims, GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL};

const TEST_SECRET: &str = "response-budget-test-secret-at-least-32-bytes";

struct QueryServer {
    client: OrbitServiceClient<Channel>,
    task: JoinHandle<()>,
    response_budget: usize,
}

impl QueryServer {
    async fn start(context: &TestContext, response_budget: usize) -> Self {
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(TEST_SECRET, 0).unwrap()),
            Arc::new(load_ontology()),
            &context.config,
            ClusterHealthChecker::default().into_arc(),
            GrpcConfig::default().stream_timeout_secs,
            Arc::new(AnalyticsConfig::default()),
        )
        .with_max_query_response_bytes(response_budget);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(OrbitServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = OrbitServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap()
            .max_decoding_message_size(GrpcConfig::default().max_query_response_bytes);
        Self {
            client,
            task,
            response_budget,
        }
    }

    async fn query(&self, query: &Value, format: ResponseFormat, denied: &[i64]) -> QueryPage {
        let token = encode(
            &Header::default(),
            &Claims::dummy(),
            &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(4);
        sender
            .send(ExecuteQueryMessage {
                content: Some(execute_query_message::Content::Request(
                    ExecuteQueryRequest {
                        query: query.to_string(),
                        format: format.into(),
                        query_type: QueryType::Json.into(),
                    },
                )),
            })
            .await
            .unwrap();
        let mut request = Request::new(ReceiverStream::new(receiver));
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let mut stream = self
            .client
            .clone()
            .execute_query(request)
            .await
            .unwrap()
            .into_inner();

        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), stream.message())
                .await
                .unwrap()
                .unwrap()
                .expect("query must return a result or in-band error");
            match message.content.as_ref().unwrap() {
                execute_query_message::Content::Redaction(exchange) => {
                    let Some(redaction_exchange::Content::Required(required)) = &exchange.content
                    else {
                        panic!("expected authorization request");
                    };
                    let authorizations = required
                        .resources
                        .iter()
                        .map(|resource| ResourceAuthorization {
                            resource_type: resource.resource_type.clone(),
                            authorized: resource
                                .resource_ids
                                .iter()
                                .map(|id| (*id, !denied.contains(id)))
                                .collect(),
                        })
                        .collect();
                    sender
                        .send(ExecuteQueryMessage {
                            content: Some(execute_query_message::Content::Redaction(
                                RedactionExchange {
                                    content: Some(redaction_exchange::Content::Response(
                                        RedactionResponse {
                                            result_id: required.result_id.clone(),
                                            authorizations,
                                        },
                                    )),
                                },
                            )),
                        })
                        .await
                        .unwrap();
                }
                execute_query_message::Content::Result(_) => {
                    assert!(message.encoded_len() <= self.response_budget);
                    return QueryPage(message);
                }
                execute_query_message::Content::Error(_) => return QueryPage(message),
                other => panic!("unexpected message: {other:?}"),
            }
        }
    }
}

impl Drop for QueryServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug)]
struct QueryPage(ExecuteQueryMessage);

impl QueryPage {
    fn result(&self) -> &ExecuteQueryResult {
        match self.0.content.as_ref().unwrap() {
            execute_query_message::Content::Result(result) => result,
            other => panic!("expected success: {other:?}"),
        }
    }

    fn row_count(&self) -> i32 {
        self.result().metadata.as_ref().unwrap().row_count
    }

    fn pagination(&self) -> Value {
        match self.result().content.as_ref().unwrap() {
            execute_query_result::Content::ResultJson(text) => {
                serde_json::from_str::<Value>(text).unwrap()["pagination"].clone()
            }
            execute_query_result::Content::FormattedText(text) => {
                let header = text.split("\n\n").next().unwrap();
                let cursor = header
                    .lines()
                    .find_map(|line| line.strip_prefix("next_cursor:"));
                json!({
                    "has_more": header.lines().any(|line| line == "has_more:true"),
                    "truncated": header.lines().any(|line| line == "truncated:true"),
                    "next_cursor": cursor
                })
            }
        }
    }

    fn facts(&self) -> BTreeSet<String> {
        let execute_query_result::Content::ResultJson(text) =
            self.result().content.as_ref().unwrap()
        else {
            return BTreeSet::new();
        };
        let value: Value = serde_json::from_str(text).unwrap();
        let mut facts = BTreeSet::new();
        for collection in ["nodes", "edges", "rows"] {
            for mut entry in value[collection].as_array().into_iter().flatten().cloned() {
                entry.as_object_mut().unwrap().remove("path_id");
                entry.sort_all_objects();
                facts.insert(format!("{collection}:{entry}"));
            }
        }
        facts
    }

    fn assert_matches(&self, expected: &Self) {
        assert_eq!(self.result().metadata, expected.result().metadata);
        assert_eq!(self.pagination(), expected.pagination());
        match self.result().content.as_ref().unwrap() {
            execute_query_result::Content::ResultJson(_) => {
                assert_eq!(self.facts(), expected.facts())
            }
            execute_query_result::Content::FormattedText(_) => {
                assert_eq!(self.result().content, expected.result().content)
            }
        }
    }

    fn assert_too_large(&self) {
        let Some(execute_query_message::Content::Error(error)) = &self.0.content else {
            panic!("expected size error: {self:?}");
        };
        assert_eq!(error.code, "result_too_large");
        assert!(error.message.contains("fewer columns"));
    }
}

fn users_query() -> Value {
    json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "user", "entity": "User", "id_range": {"start": 1, "end": 7},
            "columns": ["username", "name"]
        }],
        "order_by": "user.id",
        "cursor": {"page_size": 20}
    })
}

async fn assert_lossless_pages(
    context: &TestContext,
    query: Value,
    first_page_rows: usize,
    denied: &[i64],
) {
    let reference =
        QueryServer::start(context, GrpcConfig::default().max_query_response_bytes).await;
    for format in [ResponseFormat::Raw, ResponseFormat::Llm] {
        let full = reference.query(&query, format, denied).await;
        let mut prefix_query = query.clone();
        prefix_query["cursor"]["page_size"] = first_page_rows.into();
        let prefix = reference.query(&prefix_query, format, denied).await;
        let budget = prefix.0.encoded_len();
        assert!(
            full.0.encoded_len() > budget,
            "fixture must exceed byte limit: format={format:?}, full={}, budget={budget}, query={query}",
            full.0.encoded_len(),
        );
        let limited = QueryServer::start(context, budget).await;
        let mut current = query.clone();
        let mut seen_rows = 0;
        let mut seen_facts = BTreeSet::new();
        let mut cursors = BTreeSet::new();

        loop {
            let page = limited.query(&current, format, denied).await;
            let retry = limited.query(&current, format, denied).await;
            retry.assert_matches(&page);
            let mut expected = reference.query(&current, format, denied).await;
            if expected.0.encoded_len() > budget {
                for rows in (1..current["cursor"]["page_size"].as_u64().unwrap()).rev() {
                    let mut candidate = current.clone();
                    candidate["cursor"]["page_size"] = rows.into();
                    expected = reference.query(&candidate, format, denied).await;
                    if expected.0.encoded_len() <= budget {
                        break;
                    }
                }
            }
            page.assert_matches(&expected);
            seen_rows += page.row_count();
            seen_facts.extend(page.facts());
            let pagination = page.pagination();
            let cursor = pagination["next_cursor"].as_str();
            assert_eq!(pagination["has_more"], cursor.is_some());
            assert_eq!(pagination["truncated"], cursor.is_some());
            let Some(cursor) = cursor else {
                break;
            };
            assert!(
                cursors.insert(cursor.to_string()),
                "cursor must make progress"
            );
            current["cursor"]["after"] = cursor.into();
        }
        assert_eq!(seen_rows, full.row_count());
        assert_eq!(seen_facts, full.facts());
    }
}

async fn fullest_page_and_retryable_terminal_suffix(context: &TestContext) {
    assert_lossless_pages(context, users_query(), 4, &[]).await;
}

async fn composite_cursor_keeps_every_note(context: &TestContext) {
    let query = json!({
        "query_type": "traversal",
        "nodes": [
            {"id": "merge_request", "entity": "MergeRequest", "node_ids": [2000, 2001], "columns": ["title"]},
            {"id": "note", "entity": "Note", "columns": ["note"]}
        ],
        "relationships": [{"type": "HAS_NOTE", "from": "merge_request", "to": "note"}],
        "order_by": "merge_request.id",
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query, 2, &[]).await;
}

async fn nullable_descending_keys(context: &TestContext) {
    let query = json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "item", "entity": "WorkItem", "id_range": {"start": 4000, "end": 4010},
            "columns": ["title", "weight"]
        }],
        "order_by": "-item.weight",
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query, 2, &[]).await;
}

async fn aggregation_groups_are_complete(context: &TestContext) {
    let query = json!({
        "query_type": "aggregation",
        "nodes": [
            {"id": "user", "entity": "User", "id_range": {"start": 1, "end": 7}, "columns": ["username", "name"]},
            {"id": "merge_request", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "user", "to": "merge_request"}],
        "group_by": ["user"],
        "aggregations": [{"count": "merge_request", "as": "count"}],
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query, 1, &[]).await;
}

async fn neighbors_keep_relationships(context: &TestContext) {
    let query = json!({
        "query_type": "neighbors",
        "nodes": [{"id": "group", "entity": "Group", "node_ids": [100]}],
        "neighbors": {"direction": "both"},
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query, 4, &[]).await;
}

async fn paths_remain_complete(context: &TestContext) {
    let query = json!({
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004, 1010]}
        ],
        "path": {
            "type": "shortest", "from": "start", "to": "end", "max_depth": 3,
            "rel_types": ["MEMBER_OF", "CONTAINS"]
        },
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query, 2, &[]).await;
}

async fn denied_rows_do_not_lose_authorized_suffix(context: &TestContext) {
    assert_lossless_pages(context, users_query(), 4, &[2, 4]).await;

    let reference =
        QueryServer::start(context, GrpcConfig::default().max_query_response_bytes).await;
    let mut query = users_query();
    query["cursor"]["page_size"] = 2.into();
    let denied = [1, 2, 3, 4, 5, 6, 7];
    let first = reference.query(&query, ResponseFormat::Raw, &denied).await;
    let limited = QueryServer::start(context, first.0.encoded_len()).await;
    let mut cursors = BTreeSet::new();
    loop {
        let page = limited.query(&query, ResponseFormat::Raw, &denied).await;
        assert_eq!(page.row_count(), 0);
        assert!(page.facts().is_empty());
        let pagination = page.pagination();
        let Some(cursor) = pagination["next_cursor"].as_str() else {
            break;
        };
        assert!(cursors.insert(cursor.to_string()));
        query["cursor"]["after"] = cursor.into();
    }
    assert_eq!(cursors.len(), 3);
}

async fn exact_limit_and_oversized_results(context: &TestContext) {
    let reference =
        QueryServer::start(context, GrpcConfig::default().max_query_response_bytes).await;
    for format in [ResponseFormat::Raw, ResponseFormat::Llm] {
        let mut query = users_query();
        query["cursor"]["page_size"] = 4.into();
        let expected = reference.query(&query, format, &[]).await;
        let exact = QueryServer::start(context, expected.0.encoded_len()).await;
        exact
            .query(&query, format, &[])
            .await
            .assert_matches(&expected);
        let smaller = QueryServer::start(context, expected.0.encoded_len() - 1).await;
        assert!(smaller.query(&query, format, &[]).await.row_count() < expected.row_count());

        query["nodes"][0]["node_ids"] = json!([1]);
        query["nodes"][0]
            .as_object_mut()
            .unwrap()
            .remove("id_range");
        let single = reference.query(&query, format, &[]).await;
        let too_small = QueryServer::start(context, single.0.encoded_len() - 1).await;
        too_small
            .query(&query, format, &[])
            .await
            .assert_too_large();
        too_small
            .query(&query, format, &[])
            .await
            .assert_too_large();

        query = users_query();
        query.as_object_mut().unwrap().remove("cursor");
        smaller.query(&query, format, &[]).await.assert_too_large();
    }
}

async fn variable_length_cursors_still_choose_fullest_page(context: &TestContext) {
    let query = json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "user", "entity": "User", "node_ids": [800, 801, 802, 803, 804, 805],
            "columns": ["name"]
        }],
        "order_by": "user.username",
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query, 4, &[]).await;
}

async fn uneven_rows_still_choose_fullest_page(context: &TestContext) {
    let mut query = json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "user", "entity": "User", "id_range": {"start": 900, "end": 915},
            "columns": ["username", "name"]
        }],
        "order_by": "user.id",
        "cursor": {"page_size": 20}
    });
    assert_lossless_pages(context, query.clone(), 8, &[]).await;

    query["order_by"] = "-user.id".into();
    assert_lossless_pages(context, query, 8, &[]).await;
}

async fn equal_cursor_keys_are_not_split(context: &TestContext) {
    let reference =
        QueryServer::start(context, GrpcConfig::default().max_query_response_bytes).await;
    let query = json!({
        "query_type": "traversal",
        "nodes": [
            {"id": "user", "entity": "User", "node_ids": [1], "columns": ["username"]},
            {"id": "merge_request", "entity": "MergeRequest", "columns": ["title"]}
        ],
        "relationships": [{"type": "*", "from": "user", "to": "merge_request"}],
        "cursor": {"page_size": 20}
    });

    for format in [ResponseFormat::Raw, ResponseFormat::Llm] {
        let mut prefix = query.clone();
        prefix["cursor"]["page_size"] = 1.into();
        let first = reference.query(&prefix, format, &[]).await;
        prefix["cursor"]["page_size"] = 2.into();
        let group = reference.query(&prefix, format, &[]).await;
        assert_eq!(
            first.pagination()["next_cursor"],
            group.pagination()["next_cursor"]
        );
        assert!(first.0.encoded_len() < group.0.encoded_len());

        let too_small = QueryServer::start(context, first.0.encoded_len()).await;
        too_small
            .query(&query, format, &[])
            .await
            .assert_too_large();
        too_small
            .query(&query, format, &[])
            .await
            .assert_too_large();
        let exact_group = QueryServer::start(context, group.0.encoded_len()).await;
        exact_group
            .query(&query, format, &[])
            .await
            .assert_matches(&group);
    }
    assert_lossless_pages(context, query, 2, &[]).await;
}

async fn graph_select_count(context: &TestContext) -> u64 {
    context.execute("SYSTEM FLUSH LOGS").await;
    let batches = context.query(&format!(
        "SELECT count() AS selects FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Select' AND arrayExists(table -> startsWith(table, '{}.'), tables)",
        context.config.database,
    )).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap()
        .value(0)
}

async fn sizing_does_not_execute_more_sql(context: &TestContext) {
    let reference =
        QueryServer::start(context, GrpcConfig::default().max_query_response_bytes).await;
    let query = users_query();
    let mut prefix = query.clone();
    prefix["cursor"]["page_size"] = 4.into();
    let budget = reference
        .query(&prefix, ResponseFormat::Raw, &[])
        .await
        .0
        .encoded_len();
    let limited = QueryServer::start(context, budget).await;
    let before = graph_select_count(context).await;
    reference.query(&query, ResponseFormat::Raw, &[]).await;
    let after_full = graph_select_count(context).await;
    limited.query(&query, ResponseFormat::Raw, &[]).await;
    let after_sized = graph_select_count(context).await;
    assert!(after_full > before);
    assert_eq!(after_sized - after_full, after_full - before);
}

#[tokio::test]
async fn grpc_query_response_byte_budget() {
    let context = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    load_seed(&context, "data_correctness").await;
    context.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) SELECT 800 + number, concat(char(97 + number), if(number = 2, repeat('x', 4000), '')), repeat('Name', 50), 'active', 'human' FROM numbers(6)",
        t("gl_user"),
    )).await;
    context.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) SELECT 900 + number, repeat('x', multiIf(number = 0, 1500, number = 15, 5000, 500)), 'Uneven row', 'active', 'human' FROM numbers(16)",
        t("gl_user"),
    )).await;
    context.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES ('1/100/1000/', 1, 'User', 'APPROVED', 2000, 'MergeRequest')",
        t("gl_edge"),
    )).await;
    run_subtests_shared!(
        &context,
        fullest_page_and_retryable_terminal_suffix,
        composite_cursor_keeps_every_note,
        nullable_descending_keys,
        aggregation_groups_are_complete,
        neighbors_keep_relationships,
        paths_remain_complete,
        denied_rows_do_not_lose_authorized_suffix,
        exact_limit_and_oversized_results,
        variable_length_cursors_still_choose_fullest_page,
        uneven_rows_still_choose_fullest_page,
        equal_cursor_keys_are_not_split,
    );
    sizing_does_not_execute_more_sql(&context).await;
}
