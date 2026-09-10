use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexer::config::{DispatcherConfig, DispatcherError};
use nats_client::{KvBucketConfig, KvPutOptions, NatsClient};
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_migrations::catalog::{CatalogError, ONTOLOGY_ARCHIVES_BUCKET, OntologyCatalog};
use orbit_migrations::version::SCHEMA_VERSION;
use orbit_server_config::NatsConfiguration;
use testcontainers::ContainerAsync;
use testcontainers_modules::nats::Nats;
use tokio_util::sync::CancellationToken;

use super::super::common::dispatch::start_nats;

const LEGACY_SCHEMA_VERSION: u32 = 93;

#[tokio::test]
async fn missing_legacy_archive_is_bootstrapped_and_survives_restart() {
    let mut context = TestContext::new().await;
    let catalog = context.catalog().await;
    let expected = OntologyArchive::bundled(LEGACY_SCHEMA_VERSION)
        .unwrap()
        .unwrap();
    assert!(matches!(
        catalog.load(LEGACY_SCHEMA_VERSION).await,
        Err(CatalogError::Missing(_))
    ));

    catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await.unwrap();
    context.restart_nats().await;
    let catalog = context.catalog().await;
    catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await.unwrap();

    assert_eq!(
        catalog.load(LEGACY_SCHEMA_VERSION).await.unwrap().bytes(),
        expected.bytes()
    );
    assert_eq!(
        context
            .client()
            .await
            .kv_get(ONTOLOGY_ARCHIVES_BUCKET, &LEGACY_SCHEMA_VERSION.to_string())
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
}

#[tokio::test]
async fn concurrent_bootstraps_publish_the_exact_bundle_only_once() {
    let context = TestContext::new().await;
    let first_catalog = context.catalog().await;
    let second_catalog = context.catalog().await;

    let (first, second) = tokio::join!(
        first_catalog.ensure_archive(LEGACY_SCHEMA_VERSION),
        second_catalog.ensure_archive(LEGACY_SCHEMA_VERSION),
    );
    first.unwrap();
    second.unwrap();

    let expected = OntologyArchive::bundled(LEGACY_SCHEMA_VERSION)
        .unwrap()
        .unwrap();
    assert_eq!(
        first_catalog
            .load(LEGACY_SCHEMA_VERSION)
            .await
            .unwrap()
            .bytes(),
        expected.bytes()
    );
    assert_eq!(
        context
            .client()
            .await
            .kv_get(ONTOLOGY_ARCHIVES_BUCKET, &LEGACY_SCHEMA_VERSION.to_string())
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
}

#[tokio::test]
async fn an_existing_valid_archive_is_reused_even_when_the_bundle_differs() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let existing = archive_with_distinct_ontology(LEGACY_SCHEMA_VERSION);
    catalog.publish(&existing).await.unwrap();

    catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await.unwrap();

    assert_eq!(
        catalog.load(LEGACY_SCHEMA_VERSION).await.unwrap().bytes(),
        existing.bytes()
    );
}

#[tokio::test]
async fn an_unbundled_version_requires_an_existing_valid_archive() {
    let context = TestContext::new().await;
    let catalog = context.catalog().await;
    let unbundled_version = u32::MAX;

    assert!(
        matches!(catalog.ensure_archive(unbundled_version).await, Err(CatalogError::Missing(version)) if version == unbundled_version)
    );
    assert!(matches!(
        catalog.load(unbundled_version).await,
        Err(CatalogError::Missing(_))
    ));

    let archive = embedded_archive(unbundled_version);
    catalog.publish(&archive).await.unwrap();
    catalog.ensure_archive(unbundled_version).await.unwrap();
    assert_eq!(
        catalog.load(unbundled_version).await.unwrap().bytes(),
        archive.bytes()
    );
}

#[tokio::test]
async fn bootstrap_never_replaces_corrupt_or_mismatched_entries() {
    for bytes in [
        b"not an archive".to_vec(),
        embedded_archive(LEGACY_SCHEMA_VERSION + 1).bytes().to_vec(),
        archive_with_invalid_schema(LEGACY_SCHEMA_VERSION)
            .bytes()
            .to_vec(),
    ] {
        let context = TestContext::new().await;
        let catalog = context.catalog().await;
        let client = context.client().await;
        let key = LEGACY_SCHEMA_VERSION.to_string();
        client
            .kv_put(
                ONTOLOGY_ARCHIVES_BUCKET,
                &key,
                Bytes::copy_from_slice(&bytes),
                KvPutOptions::create_only(),
            )
            .await
            .unwrap();

        assert!(matches!(
            catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await,
            Err(CatalogError::Archive(_))
        ));
        let stored = client
            .kv_get(ONTOLOGY_ARCHIVES_BUCKET, &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.value.as_ref(), bytes.as_slice());
        assert_eq!(stored.revision, 1);
    }
}

#[tokio::test]
async fn failed_bootstrap_can_be_retried_after_reconnecting() {
    let context = TestContext::new().await;
    let client = context.client().await;
    let catalog = OntologyCatalog::open(client.clone()).await.unwrap();
    client.nats_client().drain().await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        catalog.ensure_archive(LEGACY_SCHEMA_VERSION),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(CatalogError::Nats(_))));

    let catalog = context.catalog().await;
    assert!(matches!(
        catalog.load(LEGACY_SCHEMA_VERSION).await,
        Err(CatalogError::Missing(_))
    ));
    catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await.unwrap();
    catalog.verify_archive(LEGACY_SCHEMA_VERSION).await.unwrap();
}

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
        OntologyCatalog::open(self.client().await).await.unwrap()
    }

    async fn client(&self) -> Arc<NatsClient> {
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
        client
            .ensure_kv_bucket_exists(ONTOLOGY_ARCHIVES_BUCKET, KvBucketConfig::default())
            .await
            .unwrap();
        client
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
