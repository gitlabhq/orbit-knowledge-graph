use std::sync::Arc;

use bytes::Bytes;
use clickhouse_client::ArrowClickHouseClient;
use futures::StreamExt;
use futures::stream::BoxStream;
use nats_client::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult, NatsClient};
use ontology::archive::{ArchiveError, OntologyArchive};
use orbit_utils::source_archive::SourceArchiveError;
use query_data_model::DataModelError;

use crate::archive::{LoadedRelease, ModelArchive, Release};
use crate::version::{SchemaVersionError, read_active_version};

pub const ONTOLOGY_ARCHIVES_BUCKET: &str = "orbit_ontology_archives";
pub const MODEL_ARCHIVES_BUCKET: &str = "orbit_model_archives";
pub const ACTIVE_VERSION_KEY: &str = "active_version";

pub type ActiveVersionChanges = BoxStream<'static, Result<Option<u32>, CatalogError>>;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogError {
    #[error(transparent)]
    Nats(#[from] nats_client::NatsError),
    #[error(transparent)]
    Archive(#[from] ArchiveError),
    #[error("model {0}")]
    ModelArchive(#[from] SourceArchiveError),
    #[error(transparent)]
    DataModel(#[from] DataModelError),
    #[error("ontology archive v{ontology} and model archive v{model} are not one release")]
    SplitRelease { ontology: u32, model: u32 },
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
        for bucket in [ONTOLOGY_ARCHIVES_BUCKET, MODEL_ARCHIVES_BUCKET] {
            client
                .ensure_kv_bucket_exists(bucket, KvBucketConfig::default())
                .await?;
        }
        Ok(Self { client })
    }

    pub async fn publish(&self, release: &Release) -> Result<LoadedRelease, CatalogError> {
        let loaded = release.load()?;
        let version = release.schema_version();
        self.put(
            ONTOLOGY_ARCHIVES_BUCKET,
            version,
            release.ontology_archive().bytes(),
        )
        .await?;
        self.put(
            MODEL_ARCHIVES_BUCKET,
            version,
            release.model_archive().bytes(),
        )
        .await?;
        Ok(loaded)
    }

    async fn put(&self, bucket: &str, version: u32, bytes: &[u8]) -> Result<(), CatalogError> {
        let limit = self.client.nats_client().max_payload();
        if bytes.len() > limit {
            return Err(CatalogError::TooLarge {
                size: bytes.len(),
                limit,
            });
        }
        let result = self
            .client
            .kv_put(
                bucket,
                &version.to_string(),
                Bytes::copy_from_slice(bytes),
                KvPutOptions::create_only(),
            )
            .await?;
        match result {
            KvPutResult::Success(_) => Ok(()),
            KvPutResult::AlreadyExists => {
                let stored = self.get(bucket, version).await?;
                if stored != bytes {
                    return Err(CatalogError::Conflict(version));
                }
                Ok(())
            }
            KvPutResult::RevisionMismatch => Err(CatalogError::RevisionMismatch),
        }
    }

    async fn get(&self, bucket: &str, version: u32) -> Result<Bytes, CatalogError> {
        Ok(self
            .client
            .kv_get(bucket, &version.to_string())
            .await?
            .ok_or(CatalogError::Missing(version))?
            .value)
    }

    pub async fn load(&self, version: u32) -> Result<Release, CatalogError> {
        let ontology = self.load_ontology_archive(version).await?;
        let model = self.load_model_archive(version).await?;
        Release::new(ontology, model)
    }

    pub async fn verify_archive(&self, version: u32) -> Result<(), CatalogError> {
        self.load_ontology_archive(version).await?.load_ontology()?;
        self.load_model_archive(version).await?.packages().load()?;
        Ok(())
    }

    async fn load_ontology_archive(&self, version: u32) -> Result<OntologyArchive, CatalogError> {
        let bytes = self.get(ONTOLOGY_ARCHIVES_BUCKET, version).await?;
        Ok(OntologyArchive::from_bytes(version, &bytes)?)
    }

    async fn load_model_archive(&self, version: u32) -> Result<ModelArchive, CatalogError> {
        let bytes = self.get(MODEL_ARCHIVES_BUCKET, version).await?;
        Ok(ModelArchive::from_bytes(version, &bytes)?)
    }

    pub async fn ensure_archive(&self, version: u32) -> Result<(), CatalogError> {
        match self.verify_archive(version).await {
            Err(CatalogError::Missing(_)) => {
                let release = Release::bundled(version)?.ok_or(CatalogError::Missing(version))?;
                self.publish(&release).await?;
                orbit_log_fields::info!(
                    safe(SCHEMA_VERSION) = version,
                    "bootstrapped missing schema archives from release bundle"
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
