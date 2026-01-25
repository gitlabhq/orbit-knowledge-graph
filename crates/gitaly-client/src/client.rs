use crate::auth::authorization_metadata;
use crate::proto::repository_service_client::RepositoryServiceClient;
use crate::{GitalyConfig, GitalyError};
use hyper_util::rt::TokioIo;
use regex::Regex;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;
use tokio::net::UnixStream;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

static ADDRESS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:unix:(?P<unix>.+)|tcp://(?P<tcp>.+)|(?P<other>.+))$")
        .expect("invalid address regex")
});

/// Gitaly gRPC client for repository operations.
///
/// The client uses tonic's `Channel` which provides built-in HTTP/2 connection
/// multiplexing. A single `Channel` can handle multiple concurrent requests
/// over the same connection, and tonic manages reconnection automatically.
/// Clone this client freely - clones share the underlying connection pool.
#[derive(Clone)]
pub struct GitalyClient {
    channel: Channel,
    config: GitalyConfig,
}

impl GitalyClient {
    pub async fn connect(config: GitalyConfig) -> Result<Self, GitalyError> {
        let caps = ADDRESS_PATTERN
            .captures(&config.address)
            .ok_or_else(|| GitalyError::Connection("invalid address format".to_string()))?;

        let channel = if let Some(unix_path) = caps.name("unix") {
            let path = unix_path.as_str().to_string();
            Endpoint::from_static("http://[::]:50051")
                .connect_with_connector(service_fn(move |_| {
                    let p = path.clone();
                    async move {
                        let stream = UnixStream::connect(p).await?;
                        Ok::<_, std::io::Error>(TokioIo::new(stream))
                    }
                }))
                .await?
        } else if let Some(tcp_addr) = caps.name("tcp") {
            let address = format!("http://{}", tcp_addr.as_str());
            Endpoint::from_shared(address)
                .map_err(|e| GitalyError::Connection(e.to_string()))?
                .connect()
                .await?
        } else if let Some(other) = caps.name("other") {
            Endpoint::from_shared(other.as_str().to_string())
                .map_err(|e| GitalyError::Connection(e.to_string()))?
                .connect()
                .await?
        } else {
            return Err(GitalyError::Connection(
                "invalid address format".to_string(),
            ));
        };

        Ok(Self { channel, config })
    }

    pub fn config(&self) -> &GitalyConfig {
        &self.config
    }

    pub async fn repository_exists(&self) -> Result<bool, GitalyError> {
        let mut client = RepositoryServiceClient::new(self.channel.clone());
        let repo = crate::proto::Repository {
            storage_name: self.config.storage.clone(),
            relative_path: self.config.relative_path.clone(),
            ..Default::default()
        };
        let mut req = Request::new(crate::proto::RepositoryExistsRequest {
            repository: Some(repo),
        });
        if let Some(t) = &self.config.token {
            let auth = authorization_metadata(t)?;
            req.metadata_mut().insert("authorization", auth);
        }
        let resp = client.repository_exists(req).await?;
        Ok(resp.into_inner().exists)
    }

    pub async fn pull_and_extract_repository(
        &self,
        target_dir: &Path,
        commit_id: Option<&str>,
    ) -> Result<(), GitalyError> {
        let mut client = RepositoryServiceClient::new(self.channel.clone());

        let mut req = Request::new(crate::proto::GetArchiveRequest {
            repository: Some(crate::proto::Repository {
                storage_name: self.config.storage.clone(),
                relative_path: self.config.relative_path.clone(),
                ..Default::default()
            }),
            commit_id: commit_id.unwrap_or("HEAD").to_string(),
            format: crate::proto::get_archive_request::Format::Tar as i32,
            path: vec![],
            ..Default::default()
        });
        if let Some(t) = &self.config.token {
            let auth = authorization_metadata(t)?;
            req.metadata_mut().insert("authorization", auth);
        }

        let mut stream = client.get_archive(req).await?.into_inner();

        std::fs::create_dir_all(target_dir).map_err(|e| GitalyError::Io(e.to_string()))?;
        let tar_path = target_dir.join("repo.tar");
        let mut tar_file = File::create(&tar_path).map_err(|e| GitalyError::Io(e.to_string()))?;
        while let Some(chunk) = stream.message().await? {
            tar_file
                .write_all(&chunk.data)
                .map_err(|e| GitalyError::Io(e.to_string()))?;
        }

        let file = File::open(&tar_path).map_err(|e| GitalyError::Io(e.to_string()))?;
        let mut archive = tar::Archive::new(file);

        let target_canonical = target_dir
            .canonicalize()
            .map_err(|e| GitalyError::Io(e.to_string()))?;

        for entry in archive
            .entries()
            .map_err(|e| GitalyError::Archive(e.to_string()))?
        {
            let mut entry = entry.map_err(|e| GitalyError::Archive(e.to_string()))?;
            let entry_path = entry
                .path()
                .map_err(|e| GitalyError::Archive(e.to_string()))?;

            let dest = target_canonical.join(&entry_path);

            let dest_canonical = if dest.exists() {
                dest.canonicalize()
                    .map_err(|e| GitalyError::Io(e.to_string()))?
            } else if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(|e| GitalyError::Io(e.to_string()))?;
                parent
                    .canonicalize()
                    .map_err(|e| GitalyError::Io(e.to_string()))?
                    .join(dest.file_name().unwrap_or_default())
            } else {
                dest.clone()
            };

            if !dest_canonical.starts_with(&target_canonical) {
                return Err(GitalyError::Archive(format!(
                    "path traversal detected: {}",
                    entry_path.display()
                )));
            }

            entry
                .unpack(&dest_canonical)
                .map_err(|e| GitalyError::Archive(e.to_string()))?;
        }

        std::fs::remove_file(&tar_path).map_err(|e| GitalyError::Io(e.to_string()))?;
        Ok(())
    }
}
