use std::io;
use std::sync::Mutex;

use labkit::verbose_logs::ExpandedLogsLayer;
use orbit_server_config::{AppConfig, ExpandedLogsConfig};
use tracing_subscriber::layer::SubscriberExt;

use super::*;
use crate::grpc::GrpcServer;

fn gql_list_tools() -> ListToolsRequest {
    ListToolsRequest {
        language: QueryLanguage::Gql as i32,
    }
}

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn serve_with_layers() -> OrbitServiceClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let defaults = AppConfig::embedded_defaults();
    let server = GrpcServer::new(
        Arc::new(mock_validator()),
        ActiveSchema::pinned(test_ontology()),
        test_client(),
        ClusterHealthChecker::default().into_arc(),
        None,
        defaults.grpc,
        Arc::new(defaults.analytics),
    );
    tokio::spawn(server.run(listener));
    OrbitServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

async fn handler_log_line(feature_flags: Option<&'static str>) -> String {
    let mut request = authed_request(gql_list_tools());
    if let Some(flags) = feature_flags {
        let value = MetadataValue::from_static(flags);
        request
            .metadata_mut()
            .insert("x-gitlab-enabled-feature-flags", value);
    }
    log_line_of(request, Vec::new()).await
}

async fn log_line_of(request: Request<ListToolsRequest>, root_namespace_ids: Vec<u64>) -> String {
    let logs = Buffer::default();
    let writer = logs.clone();
    let config = ExpandedLogsConfig {
        global_override: false,
        root_namespace_ids,
    };
    let policy = crate::expanded_logs::policy(&config);
    let layer = ExpandedLogsLayer::new(policy).with_writer(move || writer.clone());
    let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    serve_with_layers().await.list_tools(request).await.unwrap();

    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    logs.lines()
        .find(|line| line.contains("Listing tools for user"))
        .unwrap_or_else(|| panic!("no handler line in {logs}"))
        .to_owned()
}

fn request_with_paths(admin: bool, paths: &[&str]) -> Request<ListToolsRequest> {
    let group_traversal_ids = paths
        .iter()
        .map(|path| crate::auth::claims::TraversalPathClaim {
            path: TraversalPath::new_unchecked(path.to_string()),
            access_levels: vec![30],
        })
        .collect();
    let claims = Claims {
        admin,
        group_traversal_ids,
        ..test_claims()
    };
    signed_request(gql_list_tools(), claims)
}

#[tokio::test]
async fn verbose_logs_flag_expands_the_request() {
    let line = handler_log_line(Some("verbose_gkg_logs")).await;

    assert!(line.contains(r#""expanded_logs_requested":true"#), "{line}");
    assert!(line.contains(r#""log_expanded":true"#), "{line}");
    assert!(line.contains(r#""gl_user_id":1"#), "{line}");
}

#[tokio::test]
async fn request_without_the_flag_is_not_expanded() {
    let line = handler_log_line(None).await;

    assert!(
        line.contains(r#""expanded_logs_requested":false"#),
        "{line}"
    );
    assert!(line.contains(r#""log_expanded":false"#), "{line}");
}

#[tokio::test]
async fn token_under_an_internal_root_namespace_expands_the_request() {
    let request = request_with_paths(false, &["1/9970/", "1/9970/123/"]);

    let line = log_line_of(request, vec![9970]).await;

    assert!(line.contains(r#""log_expanded":true"#), "{line}");
}

#[tokio::test]
async fn token_under_two_root_namespaces_is_not_expanded() {
    let request = request_with_paths(false, &["1/9970/", "1/42/"]);

    let line = log_line_of(request, vec![9970]).await;

    assert!(line.contains(r#""log_expanded":false"#), "{line}");
}

#[tokio::test]
async fn admin_token_is_not_expanded() {
    let request = request_with_paths(true, &["1/9970/"]);

    let line = log_line_of(request, vec![9970]).await;

    assert!(line.contains(r#""log_expanded":false"#), "{line}");
}
