mod health_client;
mod router;

use std::net::SocketAddr;
use std::sync::Arc;

use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::nats::{NatsBroker, NatsConfiguration, NatsServicesImpl};
use mailbox::http::{MailboxState, create_mailbox_router};
use mailbox::storage::{MigrationStore, PluginStore};
use ontology::Ontology;
use tokio::net::TcpListener;
use tracing::info;

use crate::auth::JwtValidator;

pub use health_client::InfrastructureHealthClient;
pub use router::create_router;

pub struct Server {
    listener: TcpListener,
    router: axum::Router,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("NATS connection failed: {0}")]
    Nats(#[from] etl_engine::nats::NatsError),
    #[error("Failed to load ontology: {0}")]
    Ontology(#[from] ontology::OntologyError),
}

impl Server {
    pub async fn bind(
        addr: SocketAddr,
        validator: JwtValidator,
        health_check_url: Option<String>,
        nats_config: &NatsConfiguration,
        graph_config: &ClickHouseConfiguration,
    ) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr).await?;

        info!(url = %nats_config.url, "connecting to NATS for mailbox");
        let broker = Arc::new(NatsBroker::connect(nats_config).await?);
        let nats_services = Arc::new(NatsServicesImpl::new(broker));

        let clickhouse_client = Arc::new(graph_config.build_client());
        let plugin_store = Arc::new(PluginStore::new(clickhouse_client.clone()));
        let migration_store = Arc::new(MigrationStore::new(clickhouse_client));

        let ontology = Arc::new(Ontology::load_embedded()?);

        let mailbox_state = MailboxState {
            plugin_store,
            migration_store,
            nats: nats_services,
            ontology,
        };

        let mailbox_router = create_mailbox_router(mailbox_state);
        let router = create_router(validator, health_check_url, mailbox_router);

        Ok(Self { listener, router })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> std::io::Result<()> {
        info!("listening on {}", self.listener.local_addr()?);
        axum::serve(self.listener, self.router).await
    }
}
