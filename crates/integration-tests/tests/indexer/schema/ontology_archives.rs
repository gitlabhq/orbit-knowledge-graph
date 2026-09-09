use std::sync::Arc;
use std::time::Duration;

use indexer::config::{DispatcherConfig, DispatcherError};
use nats_client::NatsClient;
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_migrations::catalog::{CatalogError, OntologyCatalog};
use orbit_migrations::version::SCHEMA_VERSION;
use orbit_server_config::NatsConfiguration;
use testcontainers::ContainerAsync;
use testcontainers_modules::nats::Nats;
use tokio_util::sync::CancellationToken;

use super::super::common::dispatch::start_nats;

#[tokio::test]
async fn published_archives_survive_a_nats_restart() {
    let mut context = TestContext::new().await;
    let archive = embedded_archive(1);
    let catalog = context.catalog().await;
    catalog.publish(&archive).await.unwrap();
    drop(catalog);

    context.restart_nats().await;
    let catalog = context.catalog().await;

    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());
}

#[tokio::test]
async fn concurrent_identical_publications_return_the_archived_ontology() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let archive = archive_with_distinct_ontology(1);
    let expected = archive.load_ontology().unwrap();

    let (first, retry) = tokio::join!(catalog.publish(&archive), catalog.publish(&archive));

    assert_eq!(first.unwrap(), expected);
    assert_eq!(retry.unwrap(), expected);
    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());
}

#[tokio::test]
async fn published_versions_cannot_be_overwritten() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let archive = embedded_archive(1);
    let conflicting = archive_with_extra_newline(1);
    catalog.publish(&archive).await.unwrap();

    assert!(matches!(
        catalog.publish(&conflicting).await,
        Err(CatalogError::Conflict(1))
    ));
    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());
}

#[tokio::test]
async fn new_versions_do_not_replace_previous_archives() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let archive = embedded_archive(1);
    let next_version = archive_with_extra_newline(2);
    catalog.publish(&archive).await.unwrap();

    catalog.publish(&next_version).await.unwrap();

    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());
    assert_eq!(catalog.load(2).await.unwrap().bytes(), next_version.bytes());
}

#[tokio::test]
async fn dispatcher_rejects_conflicting_archives_before_migration() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let archive = embedded_archive(*SCHEMA_VERSION);
    let conflicting = archive_with_extra_newline(*SCHEMA_VERSION);
    catalog.publish(&archive).await.unwrap();

    let result =
        indexer::run_dispatcher(&context.config, &conflicting, CancellationToken::new()).await;

    assert!(matches!(
        result,
        Err(DispatcherError::Archive(CatalogError::Conflict(_)))
    ));
    assert_eq!(
        catalog.load(*SCHEMA_VERSION).await.unwrap().bytes(),
        archive.bytes()
    );
}

#[tokio::test]
async fn dispatcher_rejects_invalid_archives_before_migration() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let archive = embedded_archive(*SCHEMA_VERSION);
    let invalid = archive_with_invalid_schema(*SCHEMA_VERSION);
    catalog.publish(&archive).await.unwrap();

    let result = indexer::run_dispatcher(&context.config, &invalid, CancellationToken::new()).await;

    assert!(matches!(
        result,
        Err(DispatcherError::Archive(CatalogError::Archive(_)))
    ));
    assert_eq!(
        catalog.load(*SCHEMA_VERSION).await.unwrap().bytes(),
        archive.bytes()
    );
}

struct TestContext {
    server: ContainerAsync<Nats>,
    config: DispatcherConfig,
}

impl TestContext {
    async fn new() -> Self {
        let (server, url) = start_nats().await;
        let defaults = orbit_server_config::AppConfig::embedded_defaults();
        let config = DispatcherConfig {
            nats: NatsConfiguration {
                url,
                ..defaults.nats
            },
            graph: defaults.graph,
            datalake: defaults.datalake,
            schedule: defaults.schedule,
            schema: defaults.schema,
            health_bind_address: "127.0.0.1:0".parse().unwrap(),
        };
        Self { server, config }
    }

    async fn catalog(&self) -> OntologyCatalog {
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
        const RETRY_INTERVAL: Duration = Duration::from_millis(100);

        let client = tokio::time::timeout(CONNECT_TIMEOUT, async {
            loop {
                if let Ok(client) = NatsClient::connect(&self.config.nats).await {
                    break Arc::new(client);
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        })
        .await
        .expect("NATS did not become ready");

        OntologyCatalog::open(client).await.unwrap()
    }

    async fn restart_nats(&mut self) {
        const NATS_CLIENT_PORT: u16 = 4222;

        self.server.stop_with_timeout(None).await.unwrap();
        self.server.start().await.unwrap();
        let host = self.server.get_host().await.unwrap();
        let port = self
            .server
            .get_host_port_ipv4(NATS_CLIENT_PORT)
            .await
            .unwrap();
        self.config.nats.url = format!("{host}:{port}");
    }
}

fn embedded_archive(version: u32) -> OntologyArchive {
    OntologyArchive::from_sources(version, &embedded_sources()).unwrap()
}

fn archive_with_extra_newline(version: u32) -> OntologyArchive {
    let mut sources = embedded_sources();
    sources.get_mut("schema.yaml").unwrap().push('\n');
    OntologyArchive::from_sources(version, &sources).unwrap()
}

fn archive_with_invalid_schema(version: u32) -> OntologyArchive {
    let mut sources = embedded_sources();
    sources.insert("schema.yaml".into(), "[".into());
    OntologyArchive::from_sources(version, &sources).unwrap()
}

fn archive_with_distinct_ontology(version: u32) -> OntologyArchive {
    let mut sources = embedded_sources();
    let schema = sources.get_mut("schema.yaml").unwrap();
    let mut document: serde_json::Value = orbit_utils::yaml::from_str(schema).unwrap();
    document["schema_version"] = serde_json::json!("archived-schema");
    *schema = orbit_utils::yaml::to_string(&document).unwrap();
    OntologyArchive::from_sources(version, &sources).unwrap()
}
