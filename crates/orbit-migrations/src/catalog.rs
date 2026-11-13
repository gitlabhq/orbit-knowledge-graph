use std::sync::Arc;

use bytes::Bytes;
use clickhouse_client::ArrowClickHouseClient;
use futures::StreamExt;
use futures::stream::BoxStream;
use nats_client::{KvBucketConfig, KvEntry, KvPutOptions, KvPutResult, NatsClient};
use orbit_utils::source_archive::SourceArchiveError;
use query_data_model::{DataModelError, Model};

use crate::archive::ModelArchive;
use crate::version::{SchemaVersionError, read_active_version};

pub const MODEL_ARCHIVES_BUCKET: &str = "orbit_model_archives";
/// Running servers watch the active version in the bucket that held the ontology archives.
pub const ACTIVE_VERSION_BUCKET: &str = "orbit_ontology_archives";
pub const ACTIVE_VERSION_KEY: &str = "active_version";

pub type ActiveVersionChanges = BoxStream<'static, Result<Option<u32>, CatalogError>>;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogError {
    #[error(transparent)]
    Nats(#[from] nats_client::NatsError),
    #[error("model {0}")]
    Archive(#[from] SourceArchiveError),
    #[error(transparent)]
    DataModel(#[from] DataModelError),
    #[error(transparent)]
    SchemaVersion(#[from] SchemaVersionError),
    #[error(
        "model archive v{0} is missing; seed the catalog from the exact deployed release before upgrading"
    )]
    Missing(u32),
    #[error("model archive v{0} conflicts with the published archive; use a new schema version")]
    Conflict(u32),
    #[error(
        "model archive contains {size} bytes, exceeding NATS payload limit {limit}; archive publication aborted"
    )]
    TooLarge { size: usize, limit: usize },
    #[error("unexpected revision mismatch publishing model archive")]
    RevisionMismatch,
    #[error("active version key holds {0:?}, expected a schema version number")]
    CorruptActiveVersion(Bytes),
}

#[derive(Clone)]
pub struct ModelCatalog {
    client: Arc<NatsClient>,
    /// The deployment shape whose connections each archive's model loads.
    deployment: String,
}

impl ModelCatalog {
    pub async fn open(client: Arc<NatsClient>, deployment: String) -> Result<Self, CatalogError> {
        for bucket in [ACTIVE_VERSION_BUCKET, MODEL_ARCHIVES_BUCKET] {
            client
                .ensure_kv_bucket_exists(bucket, KvBucketConfig::default())
                .await?;
        }
        Ok(Self { client, deployment })
    }

    pub async fn publish(&self, archive: &ModelArchive) -> Result<Arc<Model>, CatalogError> {
        let model = archive.packages().deployment(&self.deployment)?;
        let version = archive.schema_version();
        let bytes = archive.bytes();
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
                MODEL_ARCHIVES_BUCKET,
                &version.to_string(),
                Bytes::copy_from_slice(bytes),
                KvPutOptions::create_only(),
            )
            .await?;
        match result {
            KvPutResult::Success(_) => Ok(model),
            KvPutResult::AlreadyExists => {
                if self.get(version).await? != bytes {
                    return Err(CatalogError::Conflict(version));
                }
                Ok(model)
            }
            KvPutResult::RevisionMismatch => Err(CatalogError::RevisionMismatch),
        }
    }

    async fn get(&self, version: u32) -> Result<Bytes, CatalogError> {
        Ok(self
            .client
            .kv_get(MODEL_ARCHIVES_BUCKET, &version.to_string())
            .await?
            .ok_or(CatalogError::Missing(version))?
            .value)
    }

    pub async fn load(&self, version: u32) -> Result<ModelArchive, CatalogError> {
        let bytes = self.get(version).await?;
        Ok(ModelArchive::from_bytes(version, &bytes)?)
    }

    /// The model of the archive `version`, as this deployment loads it.
    pub async fn model(&self, version: u32) -> Result<Arc<Model>, CatalogError> {
        Ok(self
            .load(version)
            .await?
            .packages()
            .deployment(&self.deployment)?)
    }

    pub async fn verify_archive(&self, version: u32) -> Result<(), CatalogError> {
        self.model(version).await?;
        Ok(())
    }

    pub async fn ensure_archive(&self, version: u32) -> Result<(), CatalogError> {
        match self.verify_archive(version).await {
            Err(CatalogError::Missing(_)) => {
                let archive =
                    ModelArchive::bundled(version)?.ok_or(CatalogError::Missing(version))?;
                self.publish(&archive).await?;
                orbit_log_fields::info!(
                    safe(SCHEMA_VERSION) = version,
                    "bootstrapped missing schema archive from release bundle"
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
            .kv_get(ACTIVE_VERSION_BUCKET, ACTIVE_VERSION_KEY)
            .await?
            .map(parse_active_version)
            .transpose()
    }

    pub async fn active_version_changes(&self) -> Result<ActiveVersionChanges, CatalogError> {
        let changes = self
            .client
            .kv_watch(ACTIVE_VERSION_BUCKET, ACTIVE_VERSION_KEY)
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
                        ACTIVE_VERSION_BUCKET,
                        ACTIVE_VERSION_KEY,
                        Bytes::from(version.to_string()),
                        KvPutOptions::default(),
                    )
                    .await?;
            }
            None => {
                self.client
                    .kv_delete(ACTIVE_VERSION_BUCKET, ACTIVE_VERSION_KEY)
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
