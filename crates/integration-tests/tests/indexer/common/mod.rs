#![allow(dead_code, unused_imports)]

use std::time::Duration;

use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

pub mod dispatch;
pub mod handlers;
pub mod scenarios;
pub mod siphon;

pub async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
    let container = Nats::default()
        .with_cmd(&NatsServerCmd::default().with_jetstream())
        .with_tag("2.11-alpine")
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .start()
        .await
        .expect("failed to start NATS container");

    let host = container.get_host().await.expect("failed to get NATS host");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to get NATS port");
    let url = format!("{host}:{port}");
    wait_for_nats_port(&url).await;

    (container, url)
}

pub(super) async fn wait_for_nats_port(url: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while tokio::net::TcpStream::connect(url).await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("NATS port did not become ready");
}

pub use handlers::{
    default_test_watermark, entity_handler_with_batch_size, entity_handler_with_partitions,
    global_envelope, global_handler, handler_context, namespace_envelope, namespace_handler,
};
pub use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, assert_edge_count,
    assert_edge_count_for_traversal_path, assert_edge_tags, assert_edge_tags_by_source,
    assert_edge_tags_by_target, assert_edges_have_traversal_path, assert_node_count,
};
pub use siphon::{create_namespace, create_namespace_with_path, create_project, create_user};
