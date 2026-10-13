use std::time::Duration;

use axum::Router;
use orbit_analytics::InMemoryAnalyticsTracker;
use orbit_billing::InMemoryBillingTracker;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::*;
use crate::auth::claims::TraversalPathClaim;
use crate::proto::{ExecuteQueryRequest, execute_query_message};

#[derive(Default)]
struct StubClickHouse {
    received: Notify,
    answer: Notify,
}

async fn stub_clickhouse() -> (String, Arc<StubClickHouse>) {
    let stub = Arc::new(StubClickHouse::default());
    let state = stub.clone();
    let app = Router::new().fallback(move || {
        let state = state.clone();
        async move {
            state.received.notify_one();
            state.answer.notified().await;
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), stub)
}

struct Harness {
    client: OrbitServiceClient<tonic::transport::Channel>,
    billing: Arc<InMemoryBillingTracker>,
    analytics: Arc<InMemoryAnalyticsTracker>,
    clickhouse: Arc<StubClickHouse>,
}

impl Harness {
    async fn start() -> Self {
        let (url, clickhouse) = stub_clickhouse().await;
        let billing = Arc::new(InMemoryBillingTracker::default());
        let analytics = Arc::new(InMemoryAnalyticsTracker::new());
        let service = test_service_on(&ClickHouseConfiguration {
            url,
            ..test_config()
        })
        .with_billing(billing.clone())
        .with_analytics(analytics.clone());
        Self {
            client: serve(service).await,
            billing,
            analytics,
            clickhouse,
        }
    }

    async fn analytics_event(&self) {
        while self.analytics.count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn billable_project_query() -> Request<impl tokio_stream::Stream<Item = ExecuteQueryMessage>> {
    let message = ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Request(
            ExecuteQueryRequest {
                query: "MATCH (p:Project {id: 1}) RETURN p LIMIT 10".into(),
                language: QueryLanguage::Gql as i32,
                ..Default::default()
            },
        )),
    };
    let claims = Claims {
        realm: Some("SaaS".into()),
        root_namespace_id: Some(9970),
        group_traversal_ids: vec![TraversalPathClaim {
            path: TraversalPath::new_unchecked("1/"),
            access_levels: vec![20],
        }],
        ..test_claims()
    };
    signed_request(tokio_stream::iter([message]), claims)
}

#[tokio::test]
async fn bills_a_query_after_the_client_gets_its_result() {
    let mut harness = Harness::start().await;
    harness.clickhouse.answer.notify_one();

    let mut responses = harness
        .client
        .execute_query(billable_project_query())
        .await
        .unwrap()
        .into_inner();
    let message = responses.message().await.unwrap().unwrap();

    assert!(
        matches!(
            message.content,
            Some(execute_query_message::Content::Result(_))
        ),
        "{message:?}"
    );
    assert!(responses.message().await.unwrap().is_none());
    assert_eq!(harness.billing.count(), 1);
}

#[tokio::test]
async fn does_not_bill_a_query_the_client_abandoned() {
    let mut harness = Harness::start().await;

    let responses = harness
        .client
        .execute_query(billable_project_query())
        .await
        .unwrap();
    harness.clickhouse.received.notified().await;
    drop(responses);
    let _ = tokio::time::timeout(Duration::from_secs(5), harness.analytics_event()).await;
    harness.clickhouse.answer.notify_one();

    tokio::time::timeout(Duration::from_secs(10), harness.analytics_event())
        .await
        .unwrap();
    assert_eq!(harness.billing.count(), 0, "billed an abandoned query");
    let event = &harness.analytics.drain()[0];
    assert_eq!(event.contexts()[1].data["status"], "streaming_error");
}

fn self_managed_query(
    license_checksum: Option<&str>,
) -> Request<impl tokio_stream::Stream<Item = ExecuteQueryMessage>> {
    let query = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
        "limit": 10
    });
    let message = ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Request(
            ExecuteQueryRequest {
                query: query.to_string(),
                ..Default::default()
            },
        )),
    };
    let claims = Claims {
        realm: Some("self-managed".into()),
        root_namespace_id: None,
        group_traversal_ids: vec![TraversalPathClaim {
            path: TraversalPath::new_unchecked("1/"),
            access_levels: vec![20],
        }],
        ..test_claims()
    };
    // `Claims` never serializes `license_checksum`, so sign the payload as Rails would.
    let now = chrono::Utc::now().timestamp();
    let mut payload = serde_json::to_value(Claims {
        iat: now,
        exp: now + 3600,
        ..claims
    })
    .unwrap();
    if let Some(checksum) = license_checksum {
        payload["license_checksum"] = checksum.into();
    }
    let token = encode(
        &Header::new(Algorithm::HS256),
        &payload,
        &EncodingKey::from_secret(b"test-secret-that-is-at-least-32-bytes-long"),
    )
    .unwrap();
    let mut request = Request::new(tokio_stream::iter([message]));
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    request
}

async fn run_to_completion(
    harness: &mut Harness,
    request: Request<impl tokio_stream::Stream<Item = ExecuteQueryMessage> + Send + 'static>,
) {
    harness.clickhouse.answer.notify_one();
    let mut responses = harness
        .client
        .execute_query(request)
        .await
        .unwrap()
        .into_inner();
    let message = responses.message().await.unwrap().unwrap();
    assert!(
        matches!(
            message.content,
            Some(execute_query_message::Content::Result(_))
        ),
        "{message:?}"
    );
    assert!(responses.message().await.unwrap().is_none());
}

#[tokio::test]
async fn does_not_bill_a_self_managed_query_without_license_checksum() {
    let mut harness = Harness::start().await;
    run_to_completion(&mut harness, self_managed_query(None)).await;
    assert_eq!(harness.billing.count(), 0);
}

#[tokio::test]
async fn bills_a_self_managed_query_with_license_checksum() {
    let mut harness = Harness::start().await;
    run_to_completion(&mut harness, self_managed_query(Some("abc123"))).await;
    assert_eq!(harness.billing.count(), 1);
}
