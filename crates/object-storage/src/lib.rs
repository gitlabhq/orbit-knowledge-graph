use bytes::Bytes;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{Certificate, ClientOptions, ObjectStore, ObjectStoreExt, PutPayload};
use orbit_server_config::{ObjectStorageAuth, ObjectStorageConfig, ObjectStorageProvider};

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageError {
    #[error("object_storage.ca_cert_path: {0}")]
    CaCert(#[from] std::io::Error),
    #[error(transparent)]
    Store(#[from] object_store::Error),
}

pub struct ObjectStorage {
    store: PrefixStore<Box<dyn ObjectStore>>,
}

impl ObjectStorage {
    pub fn new(config: &ObjectStorageConfig) -> Result<Self, ObjectStorageError> {
        let store: Box<dyn ObjectStore> = match config.provider {
            ObjectStorageProvider::S3 => Box::new(s3(config)?),
            ObjectStorageProvider::Gcs => Box::new(gcs(config)?),
        };
        let store = PrefixStore::new(store, Path::from(config.prefix.as_str()));
        Ok(Self { store })
    }

    pub async fn write(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStorageError> {
        self.store
            .put(&Path::from(key), PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    pub async fn read(&self, key: &str) -> Result<Bytes, ObjectStorageError> {
        Ok(self.store.get(&Path::from(key)).await?.bytes().await?)
    }

    pub async fn delete(&self, key: &str) -> Result<(), ObjectStorageError> {
        Ok(self.store.delete(&Path::from(key)).await?)
    }
}

fn client_options(config: &ObjectStorageConfig) -> Result<ClientOptions, ObjectStorageError> {
    let mut options = ClientOptions::new().with_allow_http(config.allow_http);
    if let Some(path) = &config.ca_cert_path {
        for certificate in Certificate::from_pem_bundle(&std::fs::read(path)?)? {
            options = options.with_root_certificate(certificate);
        }
    }
    Ok(options)
}

fn s3(config: &ObjectStorageConfig) -> Result<AmazonS3, ObjectStorageError> {
    let mut builder = match config.auth {
        ObjectStorageAuth::Identity => AmazonS3Builder::from_env(),
        ObjectStorageAuth::Static => AmazonS3Builder::new()
            .with_access_key_id(config.access_key_id.clone().unwrap_or_default())
            .with_secret_access_key(config.secret_access_key.clone().unwrap_or_default()),
    };
    builder = builder
        .with_bucket_name(&config.bucket)
        .with_client_options(client_options(config)?)
        .with_virtual_hosted_style_request(!config.path_style);
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(token) = &config.session_token {
        builder = builder.with_token(token);
    }
    Ok(builder.build()?)
}

fn gcs(config: &ObjectStorageConfig) -> Result<GoogleCloudStorage, ObjectStorageError> {
    let mut builder = match config.auth {
        ObjectStorageAuth::Identity => GoogleCloudStorageBuilder::from_env(),
        ObjectStorageAuth::Static => GoogleCloudStorageBuilder::new()
            .with_service_account_key(config.service_account_key.clone().unwrap_or_default()),
    };
    builder = builder
        .with_bucket_name(&config.bucket)
        .with_client_options(client_options(config)?);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_base_url(endpoint);
    }
    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_compatible_static_store_builds_offline() {
        let mut config = orbit_server_config::AppConfig::embedded_defaults().object_storage;
        config.bucket = "orbit".into();
        config.auth = ObjectStorageAuth::Static;
        config.endpoint = Some("http://localhost:9010".into());
        config.path_style = true;
        config.allow_http = true;
        config.access_key_id = Some("k".into());
        config.secret_access_key = Some("s".into());
        ObjectStorage::new(&config).unwrap();
    }
}
