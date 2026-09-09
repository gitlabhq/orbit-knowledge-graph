use std::sync::Arc;

use bytes::Bytes;
use nats_client::{KvBucketConfig, KvPutOptions, KvPutResult, NatsClient};
use ontology::Ontology;
use ontology::archive::{ArchiveError, OntologyArchive};

pub const ONTOLOGY_ARCHIVES_BUCKET: &str = "orbit_ontology_archives";

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Nats(#[from] nats_client::NatsError),
    #[error(transparent)]
    Archive(#[from] ArchiveError),
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
}
