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
    RedactionResponse, ResourceAuthorization, ResponseFormat,
    execute_query_message::Content as MessageContent,
    execute_query_result::Content as ResultContent,
    redaction_exchange::Content as RedactionContent,
};
use orbit_server::redaction::RedactionMessage;
use orbit_server_config::{AnalyticsConfig, GrpcConfig};
use prost::Message;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use crate::common::{DummyClaims, GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL};

const TEST_SECRET: &str = "response-budget-test-secret-at-least-32-bytes";

struct QueryServer {
    client: OrbitServiceClient<OrbitServiceServer<OrbitServiceImpl>>,
    response_budget: usize,
}

impl QueryServer {
    fn new(context: &TestContext, response_budget: usize) -> Self {
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(TEST_SECRET, 0).unwrap()),
            Arc::new(load_ontology()),
            &context.config,
            ClusterHealthChecker::default().into_arc(),
            GrpcConfig::default().stream_timeout_secs,
            Arc::new(AnalyticsConfig::default()),
        )
        .with_max_query_response_bytes(response_budget);
        let client = OrbitServiceClient::new(OrbitServiceServer::new(service))
            .max_decoding_message_size(GrpcConfig::default().max_query_response_bytes);
        Self {
            client,
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
        let query_request = ExecuteQueryRequest {
            query: query.to_string(),
            format: format.into(),
            query_type: QueryType::Json.into(),
        };
        sender
            .send(ExecuteQueryMessage {
                content: Some(MessageContent::Request(query_request)),
            })
            .await
            .unwrap();
        let mut request = Request::new(ReceiverStream::new(receiver));
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let mut client = self.client.clone();
        let mut stream = client.execute_query(request).await.unwrap().into_inner();

        loop {
            let mut message = tokio::time::timeout(Duration::from_secs(30), stream.message())
                .await
                .unwrap()
                .unwrap()
                .expect("query must return a result or in-band error");
            let encoded_bytes = message.encoded_len();
            match message.content.as_mut().unwrap() {
                MessageContent::Redaction(RedactionExchange {
                    content: Some(RedactionContent::Required(required)),
                }) => {
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
                    let response = RedactionExchange {
                        content: Some(RedactionContent::Response(RedactionResponse {
                            result_id: required.result_id.clone(),
                            authorizations,
                        })),
                    };
                    sender
                        .send(ExecuteQueryMessage::wrap_redaction(response))
                        .await
                        .unwrap();
                    continue;
                }
                MessageContent::Result(result) => {
                    assert!(encoded_bytes <= self.response_budget);
                    if let Some(ResultContent::ResultJson(text)) = &mut result.content {
                        let mut content: Value = serde_json::from_str(text).unwrap();
                        content.sort_all_objects();
                        for collection in ["nodes", "edges"] {
                            let records = content[collection].as_array_mut().unwrap();
                            records.sort_by_key(Value::to_string);
                        }
                        *text = content.to_string();
                    }
                }
                MessageContent::Error(_) => {}
                other => panic!("unexpected message: {other:?}"),
            }
            return QueryPage {
                message,
                encoded_bytes,
            };
        }
    }
}

#[derive(Debug, PartialEq)]
struct QueryPage {
    message: ExecuteQueryMessage,
    encoded_bytes: usize,
}

impl QueryPage {
    fn result(&self) -> &ExecuteQueryResult {
        match self.message.content.as_ref().unwrap() {
            MessageContent::Result(result) => result,
            other => panic!("expected success: {other:?}"),
        }
    }

    fn row_count(&self) -> i32 {
        self.result().metadata.as_ref().unwrap().row_count
    }

    fn next_cursor(&self) -> Option<String> {
        match self.result().content.as_ref().unwrap() {
            ResultContent::ResultJson(text) => {
                serde_json::from_str::<Value>(text).unwrap()["pagination"]["next_cursor"]
                    .as_str()
                    .map(str::to_owned)
            }
            ResultContent::FormattedText(text) => text
                .lines()
                .take_while(|line| !line.is_empty())
                .find_map(|line| line.strip_prefix("next_cursor:"))
                .map(str::to_owned),
        }
    }

    fn assert_too_large(&self) {
        let Some(MessageContent::Error(error)) = &self.message.content else {
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
    let reference = QueryServer::new(context, GrpcConfig::default().max_query_response_bytes);
    for format in [ResponseFormat::Raw, ResponseFormat::Llm] {
        let full = reference.query(&query, format, denied).await;
        let mut prefix_query = query.clone();
        prefix_query["cursor"]["page_size"] = first_page_rows.into();
        let prefix = reference.query(&prefix_query, format, denied).await;
        let budget = prefix.encoded_bytes;
        assert!(
            full.encoded_bytes > budget,
            "fixture must exceed byte limit: format={format:?}, full={}, budget={budget}, query={query}",
            full.encoded_bytes,
        );
        let limited = QueryServer::new(context, budget);
        let mut current = query.clone();
        let mut seen_rows = 0;
        let mut cursors = BTreeSet::new();

        loop {
            let page = limited.query(&current, format, denied).await;
            let retry = limited.query(&current, format, denied).await;
            assert_eq!(retry, page);
            let mut expected = reference.query(&current, format, denied).await;
            if expected.encoded_bytes > budget {
                for rows in (1..current["cursor"]["page_size"].as_u64().unwrap()).rev() {
                    let mut candidate = current.clone();
                    candidate["cursor"]["page_size"] = rows.into();
                    expected = reference.query(&candidate, format, denied).await;
                    if expected.encoded_bytes <= budget {
                        break;
                    }
                }
            }
            assert_eq!(page, expected);
            seen_rows += page.row_count();
            let Some(cursor) = page.next_cursor() else {
                break;
            };
            assert!(cursors.insert(cursor.clone()), "cursor must make progress");
            current["cursor"]["after"] = cursor.into();
        }
        assert_eq!(seen_rows, full.row_count());
    }
}

async fn byte_limited_pages_remain_full_and_retryable(context: &TestContext) {
    for (start, end, order, first_page_rows) in [
        (1, 7, "user.id", 4),
        (800, 805, "user.username", 4),
        (900, 915, "user.id", 8),
        (900, 915, "-user.id", 8),
    ] {
        let mut query = users_query();
        query["nodes"][0]["id_range"] = json!({"start": start, "end": end});
        query["order_by"] = order.into();
        assert_lossless_pages(context, query, first_page_rows, &[]).await;
    }
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

async fn denied_rows_do_not_lose_authorized_suffix(context: &TestContext) {
    assert_lossless_pages(context, users_query(), 4, &[2, 4]).await;

    let reference = QueryServer::new(context, GrpcConfig::default().max_query_response_bytes);
    let mut query = users_query();
    query["cursor"]["page_size"] = 2.into();
    let denied = [1, 2, 3, 4, 5, 6, 7];
    let first = reference.query(&query, ResponseFormat::Raw, &denied).await;
    let limited = QueryServer::new(context, first.encoded_bytes);
    let mut cursors = BTreeSet::new();
    loop {
        let page = limited.query(&query, ResponseFormat::Raw, &denied).await;
        assert_eq!(page.row_count(), 0);
        assert_eq!(
            page,
            reference.query(&query, ResponseFormat::Raw, &denied).await
        );
        let Some(cursor) = page.next_cursor() else {
            break;
        };
        assert!(cursors.insert(cursor.clone()));
        query["cursor"]["after"] = cursor.into();
    }
    assert_eq!(cursors.len(), 3);
}

async fn exact_limit_and_oversized_results(context: &TestContext) {
    let reference = QueryServer::new(context, GrpcConfig::default().max_query_response_bytes);
    for format in [ResponseFormat::Raw, ResponseFormat::Llm] {
        let mut query = users_query();
        query["cursor"]["page_size"] = 4.into();
        let expected = reference.query(&query, format, &[]).await;
        let exact = QueryServer::new(context, expected.encoded_bytes);
        assert_eq!(exact.query(&query, format, &[]).await, expected);
        let smaller = QueryServer::new(context, expected.encoded_bytes - 1);
        assert!(smaller.query(&query, format, &[]).await.row_count() < expected.row_count());

        query["nodes"][0]["id_range"] = json!({"start": 1, "end": 1});
        let single = reference.query(&query, format, &[]).await;
        let too_small = QueryServer::new(context, single.encoded_bytes - 1);
        for _ in 0..2 {
            too_small
                .query(&query, format, &[])
                .await
                .assert_too_large();
        }

        query = users_query();
        query.as_object_mut().unwrap().remove("cursor");
        smaller.query(&query, format, &[]).await.assert_too_large();
    }
}

async fn equal_cursor_keys_are_not_split(context: &TestContext) {
    let reference = QueryServer::new(context, GrpcConfig::default().max_query_response_bytes);
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
        assert_eq!(first.next_cursor(), group.next_cursor());
        assert!(first.encoded_bytes < group.encoded_bytes);

        let too_small = QueryServer::new(context, first.encoded_bytes);
        for _ in 0..2 {
            too_small
                .query(&query, format, &[])
                .await
                .assert_too_large();
        }
        let exact_group = QueryServer::new(context, group.encoded_bytes);
        assert_eq!(exact_group.query(&query, format, &[]).await, group);
    }
    assert_lossless_pages(context, query, 2, &[]).await;
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
        byte_limited_pages_remain_full_and_retryable,
        nullable_descending_keys,
        denied_rows_do_not_lose_authorized_suffix,
        exact_limit_and_oversized_results,
        equal_cursor_keys_are_not_split,
    );
}
