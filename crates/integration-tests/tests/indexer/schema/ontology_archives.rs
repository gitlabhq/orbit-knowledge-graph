use std::sync::Arc;
use std::time::Duration;

use indexer::config::DispatcherError;
use nats_client::NatsClient;
use ontology::Ontology;
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_migrations::catalog::{CatalogError, OntologyCatalog};
use orbit_migrations::version::SCHEMA_VERSION;
use orbit_server_config::NatsConfiguration;
use tokio_util::sync::CancellationToken;

use super::super::common::dispatch::start_nats;

#[tokio::test]
async fn published_archives_survive_a_nats_restart() {
    const RESTART_TIMEOUT: Duration = Duration::from_secs(10);
    const RECONNECT_INTERVAL: Duration = Duration::from_millis(100);
    const NATS_CLIENT_PORT: u16 = 4222;

    let (server, url) = start_nats().await;
    let mut config = NatsConfiguration {
        url,
        ..Default::default()
    };
    let client = Arc::new(NatsClient::connect(&config).await.unwrap());
    let catalog = OntologyCatalog::open(client, "archive_persistence")
        .await
        .unwrap();
    let archive = OntologyArchive::from_sources(1, &embedded_sources()).unwrap();
    catalog.publish(&archive).await.unwrap();
    drop(catalog);

    server.stop_with_timeout(None).await.unwrap();
    server.start().await.unwrap();

    let host = server.get_host().await.unwrap();
    let port = server.get_host_port_ipv4(NATS_CLIENT_PORT).await.unwrap();
    config.url = format!("{host}:{port}");

    let reconnected = tokio::time::timeout(RESTART_TIMEOUT, async {
        loop {
            if let Ok(client) = NatsClient::connect(&config).await {
                break Arc::new(client);
            }
            tokio::time::sleep(RECONNECT_INTERVAL).await;
        }
    })
    .await
    .expect("NATS did not restart");

    let catalog = OntologyCatalog::open(reconnected, "archive_persistence")
        .await
        .unwrap();
    let restored = catalog.load(1).await.unwrap();
    assert_eq!(restored.bytes(), archive.bytes());
    assert_eq!(
        restored.load_ontology().unwrap(),
        archive.load_ontology().unwrap()
    );
}

#[tokio::test]
async fn publication_is_idempotent_immutable_and_scoped_to_the_graph_database() {
    let (_server, url) = start_nats().await;
    let config = NatsConfiguration {
        url,
        ..Default::default()
    };
    let client = Arc::new(NatsClient::connect(&config).await.unwrap());
    let catalog = OntologyCatalog::open(client.clone(), "first_graph")
        .await
        .unwrap();
    let archive = OntologyArchive::from_sources(1, &embedded_sources()).unwrap();

    let (first, retry) = tokio::join!(catalog.publish(&archive), catalog.publish(&archive));
    first.unwrap();
    retry.unwrap();

    let mut changed_sources = embedded_sources();
    changed_sources.get_mut("schema.yaml").unwrap().push('\n');
    let conflicting = OntologyArchive::from_sources(1, &changed_sources).unwrap();
    assert!(matches!(
        catalog.publish(&conflicting).await,
        Err(CatalogError::Conflict(1))
    ));
    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());

    let next_version = OntologyArchive::from_sources(2, &changed_sources).unwrap();
    catalog.publish(&next_version).await.unwrap();
    assert_eq!(catalog.load(2).await.unwrap().bytes(), next_version.bytes());
    assert_eq!(catalog.load(1).await.unwrap().bytes(), archive.bytes());

    let other_graph = OntologyCatalog::open(client, "second_graph").await.unwrap();
    assert!(matches!(
        other_graph.load(1).await,
        Err(CatalogError::Missing(1))
    ));
    other_graph.publish(&conflicting).await.unwrap();
    assert_eq!(
        other_graph.load(1).await.unwrap().bytes(),
        conflicting.bytes()
    );
}

#[tokio::test]
async fn dispatcher_stops_before_migration_if_the_archive_conflicts() {
    let (_server, url) = start_nats().await;
    let config = indexer::DispatcherConfig {
        nats: NatsConfiguration {
            url,
            ..Default::default()
        },
        graph: Default::default(),
        datalake: Default::default(),
        schedule: Default::default(),
        schema: Default::default(),
        health_bind_address: "127.0.0.1:0".parse().unwrap(),
    };
    let client = Arc::new(NatsClient::connect(&config.nats).await.unwrap());
    let catalog = OntologyCatalog::open(client, &config.graph.database)
        .await
        .unwrap();
    let archive = OntologyArchive::from_sources(*SCHEMA_VERSION, &embedded_sources()).unwrap();
    catalog.publish(&archive).await.unwrap();

    let mut changed_sources = embedded_sources();
    changed_sources.get_mut("schema.yaml").unwrap().push('\n');
    let conflicting = OntologyArchive::from_sources(*SCHEMA_VERSION, &changed_sources).unwrap();
    let result = indexer::run_dispatcher(
        &config,
        &Ontology::load_embedded().unwrap(),
        &conflicting,
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
