use crate::auth::authorization_metadata;
use crate::proto::ref_service_client::RefServiceClient;
use crate::proto::repository_service_client::RepositoryServiceClient;
use crate::{GitalyError, GitalyRepositoryConfig};
use hyper_util::rt::TokioIo;
use regex::Regex;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use tokio::net::UnixStream;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

static ADDRESS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:unix:(?P<unix>.+)|tcp://(?P<tcp>.+)|(?P<other>.+))$")
        .expect("invalid address regex")
});

fn normalize_path(path: &Path) -> std::path::PathBuf {
    use std::path::Component;
    path.components()
        .fold(std::path::PathBuf::new(), |mut acc, c| {
            match c {
                Component::ParentDir => {
                    acc.pop();
                }
                Component::CurDir => {}
                _ => acc.push(c),
            }
            acc
        })
}

fn extract_archive<R: std::io::Read>(
    archive: &mut tar::Archive<R>,
    target_dir: &Path,
) -> Result<(), GitalyError> {
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

        let entry_path_str = entry_path.to_string_lossy();
        if entry_path_str == "/" || entry_path_str == "." || entry_path_str.is_empty() {
            continue;
        }

        let relative_path = entry_path.strip_prefix("/").unwrap_or(&entry_path);
        let dest = target_canonical.join(relative_path);

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
                relative_path.display()
            )));
        }

        let entry_type = entry.header().entry_type();
        let is_link = entry_type == tar::EntryType::Symlink || entry_type == tar::EntryType::Link;
        if let (true, Ok(Some(link_name))) = (is_link, entry.link_name()) {
            let link_target = if link_name.is_absolute() {
                link_name.to_path_buf()
            } else {
                dest_canonical
                    .parent()
                    .unwrap_or(&target_canonical)
                    .join(&link_name)
            };

            let normalized = normalize_path(&link_target);
            if !normalized.starts_with(&target_canonical) {
                return Err(GitalyError::Archive(format!(
                    "symlink target escapes target directory: {} -> {}",
                    relative_path.display(),
                    link_name.display()
                )));
            }
        }

        entry
            .unpack(&dest_canonical)
            .map_err(|e| GitalyError::Archive(e.to_string()))?;
    }

    Ok(())
}

/// Gitaly gRPC client for repository operations.
///
/// The client uses tonic's `Channel` which provides built-in HTTP/2 connection
/// multiplexing. A single `Channel` can handle multiple concurrent requests
/// over the same connection, and tonic manages reconnection automatically.
/// Clone this client freely - clones share the underlying connection pool.
#[derive(Clone)]
pub struct GitalyClient {
    channel: Channel,
    config: GitalyRepositoryConfig,
}

impl GitalyClient {
    pub async fn connect(config: GitalyRepositoryConfig) -> Result<Self, GitalyError> {
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

    pub fn config(&self) -> &GitalyRepositoryConfig {
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

    pub async fn fetch_archive(
        &self,
        target_dir: &Path,
        commit_id: Option<&str>,
    ) -> Result<PathBuf, GitalyError> {
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

        Ok(tar_path)
    }

    pub async fn pull_and_extract_repository(
        &self,
        target_dir: &Path,
        commit_id: Option<&str>,
    ) -> Result<(), GitalyError> {
        let archive_path = self.fetch_archive(target_dir, commit_id).await?;
        let file = File::open(&archive_path).map_err(|e| GitalyError::Io(e.to_string()))?;
        let mut archive = tar::Archive::new(file);
        extract_archive(&mut archive, target_dir)?;
        std::fs::remove_file(&archive_path).map_err(|e| GitalyError::Io(e.to_string()))?;
        Ok(())
    }

    pub async fn find_default_branch_name(&self) -> Result<Option<String>, GitalyError> {
        let mut client = RefServiceClient::new(self.channel.clone());
        let mut req = Request::new(crate::proto::FindDefaultBranchNameRequest {
            repository: Some(crate::proto::Repository {
                storage_name: self.config.storage.clone(),
                relative_path: self.config.relative_path.clone(),
                ..Default::default()
            }),
            head_only: false,
        });
        if let Some(t) = &self.config.token {
            let auth = authorization_metadata(t)?;
            req.metadata_mut().insert("authorization", auth);
        }
        let resp = client.find_default_branch_name(req).await?;
        let name = resp.into_inner().name;
        if name.is_empty() {
            return Ok(None);
        }
        Ok(Some(String::from_utf8_lossy(&name).to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tar::{Builder, Header};
    use tempfile::TempDir;

    fn extract_tar_to_dir(tar_data: &[u8], target_dir: &Path) -> Result<(), GitalyError> {
        std::fs::create_dir_all(target_dir).map_err(|e| GitalyError::Io(e.to_string()))?;
        let cursor = Cursor::new(tar_data);
        let mut archive = tar::Archive::new(cursor);
        extract_archive(&mut archive, target_dir)
    }

    fn create_tar_with_file(path: &str, content: &[u8]) -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_path(path).unwrap();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, content).unwrap();
        builder.into_inner().unwrap()
    }

    fn create_tar_with_symlink(link_path: &str, target: &str) -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_path(link_path).unwrap();
        header.set_link_name(target).unwrap();
        header.set_size(0);
        header.set_mode(0o777);
        header.set_cksum();
        builder.append(&header, std::io::empty()).unwrap();
        builder.into_inner().unwrap()
    }

    fn create_tar_with_symlink_and_file(
        link_path: &str,
        link_target: &str,
        file_path: &str,
        file_content: &[u8],
    ) -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());

        let mut header = Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_path(link_path).unwrap();
        header.set_link_name(link_target).unwrap();
        header.set_size(0);
        header.set_mode(0o777);
        header.set_cksum();
        builder.append(&header, std::io::empty()).unwrap();

        let mut header = Header::new_gnu();
        header.set_path(file_path).unwrap();
        header.set_size(file_content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, file_content).unwrap();

        builder.into_inner().unwrap()
    }

    #[test]
    fn test_normal_file_extraction() {
        let temp_dir = TempDir::new().unwrap();
        let tar_data = create_tar_with_file("test.txt", b"hello world");

        let result = extract_tar_to_dir(&tar_data, temp_dir.path());
        assert!(result.is_ok(), "normal file extraction should succeed");

        let content = std::fs::read_to_string(temp_dir.path().join("test.txt")).unwrap();
        assert_eq!(content, "hello world");
    }

    #[test]
    fn test_symlink_pointing_outside_target_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let tar_data = create_tar_with_symlink("evil_link", "/etc");

        let result = extract_tar_to_dir(&tar_data, temp_dir.path());

        assert!(
            result.is_err(),
            "symlink pointing outside target should be rejected"
        );

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("symlink") || err.to_string().contains("path traversal"),
            "error should mention symlink or path traversal: {}",
            err
        );

        let link_path = temp_dir.path().join("evil_link");
        assert!(
            !link_path.exists() && !link_path.is_symlink(),
            "symlink should NOT be created"
        );
    }

    #[test]
    fn test_symlink_with_relative_escape_rejected() {
        let temp_dir = TempDir::new().unwrap();

        std::fs::create_dir_all(temp_dir.path().join("subdir")).unwrap();
        let tar_data = create_tar_with_symlink("subdir/evil", "../../etc/passwd");

        let result = extract_tar_to_dir(&tar_data, temp_dir.path());

        assert!(
            result.is_err(),
            "symlink with relative path escaping target should be rejected"
        );
    }

    #[test]
    fn test_file_through_symlink_to_existing_path_rejected() {
        let temp_dir = TempDir::new().unwrap();

        // Create symlink pointing to /tmp (which exists)
        let tar_data =
            create_tar_with_symlink_and_file("evil_link", "/tmp", "evil_link/pwned.txt", b"pwned");

        let result = extract_tar_to_dir(&tar_data, temp_dir.path());

        // This should be rejected because writing through symlink would escape target_dir
        assert!(
            result.is_err(),
            "writing file through symlink to existing path should be rejected"
        );

        // Verify pwned.txt was NOT written to /tmp
        assert!(
            !Path::new("/tmp/pwned.txt").exists(),
            "/tmp/pwned.txt should not exist"
        );
    }

    #[test]
    fn test_symlink_inside_target_allowed() {
        let temp_dir = TempDir::new().unwrap();

        // Create a real file first
        std::fs::write(temp_dir.path().join("real_file.txt"), "content").unwrap();

        // Create a tar with symlink pointing to relative path inside target
        let tar_data = create_tar_with_symlink("link_to_real", "real_file.txt");

        let result = extract_tar_to_dir(&tar_data, temp_dir.path());
        assert!(
            result.is_ok(),
            "symlink to relative path inside target should be allowed"
        );
    }

    /// Create a tar entry with arbitrary path (including absolute paths).
    fn append_entry(builder: &mut Builder<Vec<u8>>, path: &str, content: Option<&[u8]>) {
        let mut header = Header::new_gnu();
        let path_bytes = path.as_bytes();
        header.as_mut_bytes()[..path_bytes.len().min(100)]
            .copy_from_slice(&path_bytes[..path_bytes.len().min(100)]);

        match content {
            Some(data) => {
                header.set_size(data.len() as u64);
                // Set file mode to regular file permissions (rw-r--r--)
                header.set_mode(0o644);
                header.set_entry_type(tar::EntryType::Regular);
                header.set_cksum();
                builder.append(&header, data).unwrap();
            }
            None => {
                header.set_size(0);
                // Set directory mode to rwxr-xr-x
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                builder.append(&header, std::io::empty()).unwrap();
            }
        }
    }

    #[test]
    fn test_root_slash_directory_skipped() {
        let temp_dir = TempDir::new().unwrap();
        let mut builder = Builder::new(Vec::new());
        append_entry(&mut builder, "/", None);
        append_entry(&mut builder, "/test.txt", Some(b"content"));
        let tar_data = builder.into_inner().unwrap();

        extract_tar_to_dir(&tar_data, temp_dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join("test.txt")).unwrap(),
            "content"
        );
    }

    #[test]
    fn test_dot_directory_skipped() {
        let temp_dir = TempDir::new().unwrap();
        let mut builder = Builder::new(Vec::new());
        append_entry(&mut builder, ".", None);
        append_entry(&mut builder, "test.txt", Some(b"hello"));
        let tar_data = builder.into_inner().unwrap();

        extract_tar_to_dir(&tar_data, temp_dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join("test.txt")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_absolute_path_leading_slash_stripped() {
        let temp_dir = TempDir::new().unwrap();
        let mut builder = Builder::new(Vec::new());
        append_entry(&mut builder, "/subdir/file.txt", Some(b"absolute"));
        let tar_data = builder.into_inner().unwrap();

        extract_tar_to_dir(&tar_data, temp_dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join("subdir/file.txt")).unwrap(),
            "absolute"
        );
    }

    #[test]
    fn test_nested_absolute_path() {
        let temp_dir = TempDir::new().unwrap();
        let mut builder = Builder::new(Vec::new());
        append_entry(&mut builder, "/a/b/c/deep.txt", Some(b"deep"));
        let tar_data = builder.into_inner().unwrap();

        extract_tar_to_dir(&tar_data, temp_dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join("a/b/c/deep.txt")).unwrap(),
            "deep"
        );
    }
}
