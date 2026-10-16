use std::io;
use std::sync::Mutex;

use labkit::verbose_logs::ExpandedLogsLayer;
use orbit_server_config::{AppConfig, ExpandedLogsConfig};
use tracing_subscriber::layer::SubscriberExt;

use super::*;
use crate::grpc::GrpcServer;

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
    let logs = Buffer::default();
    let writer = logs.clone();
    let policy = crate::expanded_logs::policy(&ExpandedLogsConfig::default());
    let layer = ExpandedLogsLayer::new(policy).with_writer(move || writer.clone());
    let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let mut request = authed_request(ListToolsRequest::default());
    if let Some(flags) = feature_flags {
        let value = MetadataValue::from_static(flags);
        request
            .metadata_mut()
            .insert("x-gitlab-enabled-feature-flags", value);
    }
    serve_with_layers().await.list_tools(request).await.unwrap();

    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    logs.lines()
        .find(|line| line.contains("Listing tools for user"))
        .unwrap_or_else(|| panic!("no handler line in {logs}"))
        .to_owned()
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
    let line = handler_log_line(Some("other_flag")).await;

    assert!(
        line.contains(r#""expanded_logs_requested":false"#),
        "{line}"
    );
    assert!(line.contains(r#""log_expanded":false"#), "{line}");
}
