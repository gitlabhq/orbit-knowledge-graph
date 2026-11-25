use futures::TryStreamExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const NATS_IMAGE_TAG: &str = "2.15.0-alpine";

pub async fn connect_to_empty_nats() -> (async_nats::Client, Option<ContainerAsync<Nats>>) {
    let (url, container) = match std::env::var("ENGINE_TEST_NATS_URL") {
        Ok(url) => (url, None),
        Err(_) => {
            let container = Nats::default()
                .with_cmd(&NatsServerCmd::default().with_jetstream())
                .with_tag(NATS_IMAGE_TAG)
                .with_mapped_port(0, ContainerPort::Tcp(4222))
                .start()
                .await
                .expect("the NATS container starts");
            let host = container
                .get_host()
                .await
                .expect("the container has a host");
            let port = container
                .get_host_port_ipv4(4222)
                .await
                .expect("the container maps the NATS port");
            (format!("nats://{host}:{port}"), Some(container))
        }
    };
    let client = async_nats::connect(url)
        .await
        .expect("the client connects to NATS");
    delete_every_stream(&client).await;
    (client, container)
}

pub async fn delete_every_stream(client: &async_nats::Client) {
    let jetstream = async_nats::jetstream::new(client.clone());
    let streams: Vec<String> = jetstream
        .stream_names()
        .try_collect()
        .await
        .expect("NATS lists its streams");
    for stream in streams {
        jetstream
            .delete_stream(stream)
            .await
            .expect("NATS deletes the stream");
    }
}
