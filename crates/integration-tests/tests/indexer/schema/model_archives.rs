use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexer::config::{DispatcherConfig, DispatcherError};
use nats_client::{KvPutOptions, NatsClient};
use orbit_migrations::archive::ModelArchive;
use orbit_migrations::catalog::{CatalogError, MODEL_ARCHIVES_BUCKET, ModelCatalog};
use orbit_migrations::version::SCHEMA_VERSION;
use orbit_server_config::NatsConfiguration;
use testcontainers::ContainerAsync;
use testcontainers_modules::nats::Nats;
use tokio_util::sync::CancellationToken;

use super::super::common::dispatch::serving_flag;
use super::super::common::start_nats;

const LEGACY_SCHEMA_VERSION: u32 = 93;

#[tokio::test]
async fn missing_legacy_archive_is_bootstrapped_and_survives_restart() {
    let mut context = TestContext::new().await;
    let (_, catalog) = context.connect_catalog().await;
    let expected = ModelArchive::bundled(LEGACY_SCHEMA_VERSION)
        .unwrap()
        .unwrap();
    assert!(matches!(
        catalog.load(LEGACY_SCHEMA_VERSION).await,
        Err(CatalogError::Missing(_))
    ));

    catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await.unwrap();
    context.restart_nats().await;
    let (client, catalog) = context.connect_catalog().await;
    catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await.unwrap();

    assert_eq!(
        catalog.load(LEGACY_SCHEMA_VERSION).await.unwrap().bytes(),
        expected.bytes()
    );
    let stored = client
        .kv_get(MODEL_ARCHIVES_BUCKET, &LEGACY_SCHEMA_VERSION.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.revision, 1);
}

#[tokio::test]
async fn concurrent_bootstraps_publish_the_exact_bundle_only_once() {
    let context = TestContext::new().await;
    let (client, first_catalog) = context.connect_catalog().await;
    let (_, second_catalog) = context.connect_catalog().await;

    let (first, second) = tokio::join!(
        first_catalog.ensure_archive(LEGACY_SCHEMA_VERSION),
        second_catalog.ensure_archive(LEGACY_SCHEMA_VERSION),
    );
    first.unwrap();
    second.unwrap();

    let expected = ModelArchive::bundled(LEGACY_SCHEMA_VERSION)
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
    let stored = client
        .kv_get(MODEL_ARCHIVES_BUCKET, &LEGACY_SCHEMA_VERSION.to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.revision, 1);
}

#[tokio::test]
async fn an_existing_valid_archive_is_reused_even_when_the_bundle_differs() {
    let context = TestContext::new().await;
    let (_, catalog) = context.connect_catalog().await;
    let existing = archive_with_distinct_model(LEGACY_SCHEMA_VERSION);
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
    let (_, catalog) = context.connect_catalog().await;
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
        let (client, catalog) = context.connect_catalog().await;
        let key = LEGACY_SCHEMA_VERSION.to_string();
        client
            .kv_put(
                MODEL_ARCHIVES_BUCKET,
                &key,
                Bytes::copy_from_slice(&bytes),
                KvPutOptions::create_only(),
            )
            .await
            .unwrap();

        assert!(matches!(
            catalog.ensure_archive(LEGACY_SCHEMA_VERSION).await,
            Err(CatalogError::Archive(_) | CatalogError::DataModel(_))
        ));
        let stored = client
            .kv_get(MODEL_ARCHIVES_BUCKET, &key)
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
    let (client, catalog) = context.connect_catalog().await;
    client.nats_client().drain().await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        catalog.ensure_archive(LEGACY_SCHEMA_VERSION),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(CatalogError::Nats(_))));

    let (_, catalog) = context.connect_catalog().await;
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
    let (_, catalog) = context.connect_catalog().await;
    catalog.publish(&archive).await.unwrap();
    drop(catalog);

    context.restart_nats().await;
    let (_, catalog) = context.connect_catalog().await;

    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());
}

#[tokio::test]
async fn concurrent_identical_publications_return_the_archived_model() {
    let context = TestContext::new().await;
    let (_, catalog) = context.connect_catalog().await;
    let archive = archive_with_distinct_model(1);
    let expected = archive.packages().deployment("server").unwrap().id();

    let (first, retry) = tokio::join!(catalog.publish(&archive), catalog.publish(&archive));

    assert_eq!(first.unwrap().id(), expected);
    assert_eq!(retry.unwrap().id(), expected);
    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());
}

#[tokio::test]
async fn published_versions_cannot_be_overwritten() {
    let context = TestContext::new().await;
    let (_, catalog) = context.connect_catalog().await;
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
    let (_, catalog) = context.connect_catalog().await;
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
    let (_, catalog) = context.connect_catalog().await;
    let archive = embedded_archive(*SCHEMA_VERSION);
    let conflicting = archive_with_extra_newline(*SCHEMA_VERSION);
    catalog.publish(&archive).await.unwrap();

    let result = indexer::run_dispatcher(
        &context.config,
        &conflicting,
        serving_flag(),
        CancellationToken::new(),
    )
    .await;

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
    let (_, catalog) = context.connect_catalog().await;
    let archive = embedded_archive(*SCHEMA_VERSION);
    let invalid = archive_with_invalid_schema(*SCHEMA_VERSION);
    catalog.publish(&archive).await.unwrap();

    let result = indexer::run_dispatcher(
        &context.config,
        &invalid,
        serving_flag(),
        CancellationToken::new(),
    )
    .await;

    assert!(matches!(result, Err(DispatcherError::Layout(_))));
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
        };
        Self { server, config }
    }

    async fn connect_catalog(&self) -> (Arc<NatsClient>, ModelCatalog) {
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
        let catalog = ModelCatalog::open(client.clone(), "server".into())
            .await
            .unwrap();
        (client, catalog)
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

fn embedded_archive(version: u32) -> ModelArchive {
    integration_testkit::embedded_archive(version)
}

fn archive_with_sources(
    version: u32,
    edit: impl FnOnce(&mut std::collections::BTreeMap<String, String>),
) -> ModelArchive {
    let mut sources = query_data_model::Packages::embedded().sources().clone();
    edit(&mut sources);
    integration_testkit::model_archive(
        version,
        &query_data_model::Packages::new(query_data_model::Tier::Platform, sources),
    )
}

fn archive_with_extra_newline(version: u32) -> ModelArchive {
    archive_with_sources(version, |sources| {
        sources.get_mut("gitlab/graph.yaml").unwrap().push('\n')
    })
}

fn archive_with_invalid_schema(version: u32) -> ModelArchive {
    archive_with_sources(version, |sources| {
        sources.insert("gitlab/graph.yaml".into(), "[".into());
    })
}

fn archive_with_distinct_model(version: u32) -> ModelArchive {
    archive_with_sources(version, |sources| {
        let graph = sources.get_mut("gitlab.core/graph.yaml").unwrap();
        assert!(graph.contains("domain_description: Entities"));
        *graph = graph.replacen(
            "domain_description: Entities",
            "domain_description: Archived entities",
            1,
        );
    })
}
