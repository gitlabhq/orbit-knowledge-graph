use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Kubernetes error: {0}")]
    Kube(#[from] kube::Error),

    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("NATS error: {0}")]
    Nats(#[from] nats_client::NatsError),

    #[error("Configuration error: {0}")]
    Config(String),
}
