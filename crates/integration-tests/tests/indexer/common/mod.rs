#![allow(dead_code, unused_imports)]

use std::time::Duration;

pub mod handlers;
pub mod siphon;

pub use handlers::{
    default_test_watermark, global_envelope, global_handler, handler_context, namespace_envelope,
    namespace_handler,
};
pub use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, assert_edge_count,
    assert_edge_count_for_traversal_path, assert_edges_have_traversal_path, assert_node_count,
};
pub use siphon::{
    create_member, create_namespace, create_namespace_with_path, create_project,
    create_project_with_path, create_route, create_user,
};

/// Poll until a NATS connection succeeds, or panic after `timeout`.
pub async fn wait_for_nats(url: &str, timeout: Duration) {
    let nats_url = format!("nats://{url}");
    let poll_interval = Duration::from_millis(100);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if async_nats::connect(&nats_url).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("NATS not reachable at {url} after {timeout:?}");
        }
        tokio::time::sleep(poll_interval).await;
    }
}
