use std::time::Duration;

use axum::Router;
use orbit_analytics::InMemoryAnalyticsTracker;
use orbit_billing::InMemoryBillingTracker;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_stream::wrappers::ReceiverStream;

use super::*;
use crate::auth::claims::TraversalPathClaim;
use crate::proto::{ExecuteQueryRequest, execute_query_message};

async fn clickhouse_with_no_rows(answers: bool) -> (String, Arc<Notify>) {
    let received = Arc::new(Notify::new());
    let signal = received.clone();
    let app = Router::new().fallback(move || {
        let signal = signal.clone();
        async move {
            signal.notify_one();
            if !answers {
                std::future::pending::<()>().await;
            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), received)
}

struct Trackers {
    billing: Arc<InMemoryBillingTracker>,
    analytics: Arc<InMemoryAnalyticsTracker>,
}

async fn start(
    answers: bool,
) -> (
    OrbitServiceClient<tonic::transport::Channel>,
    Trackers,
    Arc<Notify>,
) {
    let (url, received) = clickhouse_with_no_rows(answers).await;
    let trackers = Trackers {
        billing: Arc::new(InMemoryBillingTracker::default()),
        analytics: Arc::new(InMemoryAnalyticsTracker::new()),
    };
    let service = OrbitServiceImpl::new(
        Arc::new(mock_validator()),
        ActiveSchema::pinned(test_ontology()),
        &ClickHouseConfiguration {
            url,
            ..test_config()
        },
        ClusterHealthChecker::default().into_arc(),
        60,
        Arc::new(orbit_server_config::AppConfig::embedded_defaults().analytics),
    )
    .with_billing(trackers.billing.clone())
    .with_analytics(trackers.analytics.clone());
    (serve(service).await, trackers, received)
}

fn billable_project_query() -> (
    mpsc::Sender<ExecuteQueryMessage>,
    Request<ReceiverStream<ExecuteQueryMessage>>,
) {
    let (requests, stream) = mpsc::channel(1);
    let query = serde_json::json!({
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
        "limit": 10
    });
    requests
        .try_send(ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Request(
                ExecuteQueryRequest {
                    query: query.to_string(),
                    ..Default::default()
                },
            )),
        })
        .unwrap();
    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        iat: now,
        exp: now + 3600,
        realm: Some("SaaS".into()),
        group_traversal_ids: vec![TraversalPathClaim {
            path: TraversalPath::new_unchecked("1/"),
            access_levels: vec![20],
        }],
        ..test_claims()
    };
    (
        requests,
        signed_request(ReceiverStream::new(stream), claims),
    )
}

async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition not met within 10 s");
}

#[tokio::test]
async fn bills_a_query_after_the_client_gets_its_result() {
    let (mut client, trackers, _) = start(true).await;
    let (_requests, request) = billable_project_query();

    let mut responses = client.execute_query(request).await.unwrap().into_inner();
    let message = responses.message().await.unwrap().unwrap();

    assert!(
        matches!(
            message.content,
            Some(execute_query_message::Content::Result(_))
        ),
        "{message:?}"
    );
    wait_until(|| trackers.billing.count() == 1).await;
}

#[tokio::test]
async fn does_not_bill_a_query_the_client_abandoned() {
    let (mut client, trackers, clickhouse_received) = start(false).await;
    let (requests, request) = billable_project_query();

    let responses = client.execute_query(request).await.unwrap();
    clickhouse_received.notified().await;
    drop((requests, responses));

    wait_until(|| trackers.analytics.count() == 1).await;
    assert_eq!(trackers.billing.count(), 0);
    let event = &trackers.analytics.drain()[0];
    assert_eq!(event.contexts()[1].data["status"], "streaming_error");
}
