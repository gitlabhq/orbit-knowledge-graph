use std::sync::Arc;

use bytes::Bytes;
use clickhouse_client::ArrowClickHouseClient;
use futures::StreamExt;
use futures::stream::BoxStream;
use nats_client::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult, NatsClient};
use ontology::Ontology;
use ontology::archive::{ArchiveError, OntologyArchive};

use crate::version::{SchemaVersionError, read_active_version};

pub const ONTOLOGY_ARCHIVES_BUCKET: &str = "orbit_ontology_archives";
pub const ACTIVE_VERSION_KEY: &str = "active_version";

pub type ActiveVersionChanges = BoxStream<'static, Result<Option<u32>, CatalogError>>;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Nats(#[from] nats_client::NatsError),
    #[error(transparent)]
    Archive(#[from] ArchiveError),
    #[error(transparent)]
    SchemaVersion(#[from] SchemaVersionError),
    #[error(
        "ontology archive v{0} is missing; seed the catalog from the exact deployed release before upgrading"
    )]
    Missing(u32),
    #[error("ontology archive v{0} conflicts with the published archive; use a new schema version")]
    Conflict(u32),
    #[error(
        "ontology archive contains {size} bytes, exceeding NATS payload limit {limit}; archive publication aborted"
    )]
    TooLarge { size: usize, limit: usize },
    #[error("unexpected revision mismatch publishing ontology archive")]
    RevisionMismatch,
    #[error("active version key holds {0:?}, expected a schema version number")]
    CorruptActiveVersion(Bytes),
}

#[derive(Clone)]
pub struct OntologyCatalog {
    client: Arc<NatsClient>,
}

impl OntologyCatalog {
    pub async fn open(client: Arc<NatsClient>) -> Result<Self, CatalogError> {
        client
            .ensure_kv_bucket_exists(ONTOLOGY_ARCHIVES_BUCKET, KvBucketConfig::default())
            .await?;
        Ok(Self { client })
    }

    pub async fn publish(&self, archive: &OntologyArchive) -> Result<Ontology, CatalogError> {
        let ontology = archive.load_ontology()?;
        let limit = self.client.nats_client().max_payload();
        if archive.bytes().len() > limit {
            return Err(CatalogError::TooLarge {
                size: archive.bytes().len(),
                limit,
            });
        }
        let version = archive.schema_version();
        let result = self
            .client
            .kv_put(
                ONTOLOGY_ARCHIVES_BUCKET,
                &version.to_string(),
                Bytes::copy_from_slice(archive.bytes()),
                KvPutOptions::create_only(),
            )
            .await?;
        match result {
            KvPutResult::Success(_) => Ok(ontology),
            KvPutResult::AlreadyExists => {
                let stored = self.load(version).await?;
                if stored.bytes() != archive.bytes() {
                    return Err(CatalogError::Conflict(version));
                }
                Ok(ontology)
            }
            KvPutResult::RevisionMismatch => Err(CatalogError::RevisionMismatch),
        }
    }

    pub async fn load(&self, version: u32) -> Result<OntologyArchive, CatalogError> {
        let entry = self
            .client
            .kv_get(ONTOLOGY_ARCHIVES_BUCKET, &version.to_string())
            .await?
            .ok_or(CatalogError::Missing(version))?;
        Ok(OntologyArchive::from_bytes(version, &entry.value)?)
    }

    pub async fn verify_archive(&self, version: u32) -> Result<(), CatalogError> {
        self.load(version).await?.load_ontology()?;
        Ok(())
    }

    pub async fn ensure_archive(&self, version: u32) -> Result<(), CatalogError> {
        match self.verify_archive(version).await {
            Err(CatalogError::Missing(_)) => {
                let archive =
                    OntologyArchive::bundled(version)?.ok_or(CatalogError::Missing(version))?;
                self.publish(&archive).await?;
                tracing::info!(
                    version,
                    "bootstrapped missing ontology archive from release bundle"
                );
                Ok(())
            }
            result => result,
        }
    }

    pub async fn sync_active_version(
        &self,
        graph: &ArrowClickHouseClient,
    ) -> Result<(), CatalogError> {
        let active = read_active_version(graph).await?;
        if self.active_version().await? != active {
            self.write_active_version(active).await?;
        }
        Ok(())
    }

    pub async fn active_version(&self) -> Result<Option<u32>, CatalogError> {
        self.client
            .kv_get(ONTOLOGY_ARCHIVES_BUCKET, ACTIVE_VERSION_KEY)
            .await?
            .map(parse_active_version)
            .transpose()
    }

    pub async fn active_version_changes(&self) -> Result<ActiveVersionChanges, CatalogError> {
        let changes = self
            .client
            .kv_watch(ONTOLOGY_ARCHIVES_BUCKET, ACTIVE_VERSION_KEY)
            .await?;
        Ok(changes
            .map(|entry| entry?.map(parse_active_version).transpose())
            .boxed())
    }

    async fn write_active_version(&self, version: Option<u32>) -> Result<(), CatalogError> {
        match version {
            Some(version) => {
                self.client
                    .kv_put(
                        ONTOLOGY_ARCHIVES_BUCKET,
                        ACTIVE_VERSION_KEY,
                        Bytes::from(version.to_string()),
                        KvPutOptions::default(),
                    )
                    .await?;
            }
            None => {
                self.client
                    .kv_delete(ONTOLOGY_ARCHIVES_BUCKET, ACTIVE_VERSION_KEY)
                    .await?;
            }
        }
        Ok(())
    }
}

fn parse_active_version(entry: KvEntry) -> Result<u32, CatalogError> {
    std::str::from_utf8(&entry.value)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(CatalogError::CorruptActiveVersion(entry.value))
}
