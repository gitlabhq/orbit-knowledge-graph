use crate::{GitalyClient, GitalyError};
use async_trait::async_trait;
use std::path::Path;

#[async_trait]
pub trait RepositorySource: Send + Sync {
    async fn extract_to(
        &self,
        target_dir: &Path,
        commit_id: Option<&str>,
    ) -> Result<(), GitalyError>;
    async fn exists(&self) -> Result<bool, GitalyError>;
}

#[async_trait]
impl RepositorySource for GitalyClient {
    async fn extract_to(
        &self,
        target_dir: &Path,
        commit_id: Option<&str>,
    ) -> Result<(), GitalyError> {
        self.pull_and_extract_repository(target_dir, commit_id)
            .await
    }

    async fn exists(&self) -> Result<bool, GitalyError> {
        self.repository_exists().await
    }
}
