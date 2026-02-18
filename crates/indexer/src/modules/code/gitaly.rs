//! Gitaly utilities for repository operations.

use std::env;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use gitaly_client::{GitalyClient, GitalyError, GitalyRepositoryConfig, RepositorySource};
use sha2::{Digest, Sha256};

#[async_trait]
pub trait RepositoryService: Send + Sync {
    async fn find_default_branch(&self, project_id: i64) -> Result<Option<String>, GitalyError>;
    async fn extract_repository(
        &self,
        project_id: i64,
        target_dir: &Path,
        commit_id: &str,
    ) -> Result<(), GitalyError>;
}

pub struct GitalyRepositoryService {
    config: GitalyConfiguration,
}

impl GitalyRepositoryService {
    pub fn create(config: GitalyConfiguration) -> Arc<dyn RepositoryService> {
        Arc::new(Self { config })
    }
}

#[async_trait]
impl RepositoryService for GitalyRepositoryService {
    async fn find_default_branch(&self, project_id: i64) -> Result<Option<String>, GitalyError> {
        find_default_branch_name(&self.config, project_id).await
    }

    async fn extract_repository(
        &self,
        project_id: i64,
        target_dir: &Path,
        commit_id: &str,
    ) -> Result<(), GitalyError> {
        extract_repository(&self.config, project_id, target_dir, commit_id).await
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GitalyConfiguration {
    pub address: String,
    pub storage: String,
    pub token: Option<String>,
}

impl Default for GitalyConfiguration {
    fn default() -> Self {
        Self {
            address: "localhost:9999".to_string(),
            storage: "default".to_string(),
            token: None,
        }
    }
}

impl GitalyConfiguration {
    pub fn from_env() -> Result<Self, GitalyError> {
        let address = env::var("GITALY_ADDRESS")
            .map_err(|_| GitalyError::Config("GITALY_ADDRESS".to_string()))?;
        let storage = env::var("GITALY_STORAGE").unwrap_or_else(|_| "default".to_string());
        let token = env::var("GITALY_TOKEN").ok();

        Ok(Self {
            address,
            storage,
            token,
        })
    }
}

pub fn compute_hashed_path(project_id: i64) -> String {
    let hash = format!("{:x}", Sha256::digest(project_id.to_string()));
    format!("@hashed/{}/{}/{}.git", &hash[0..2], &hash[2..4], hash)
}

pub async fn extract_repository(
    config: &GitalyConfiguration,
    project_id: i64,
    target_dir: &Path,
    commit_id: &str,
) -> Result<(), GitalyError> {
    // TODO: This should be fetched from Rails instead before starting the communication
    let gitaly_config = GitalyRepositoryConfig {
        address: config.address.clone(),
        storage: config.storage.clone(),
        relative_path: compute_hashed_path(project_id),
        token: config.token.clone(),
    };

    let client = GitalyClient::connect(gitaly_config).await?;
    RepositorySource::extract_to(&client, target_dir, Some(commit_id)).await
}

pub async fn find_default_branch_name(
    config: &GitalyConfiguration,
    project_id: i64,
) -> Result<Option<String>, GitalyError> {
    let gitaly_config = GitalyRepositoryConfig {
        address: config.address.clone(),
        storage: config.storage.clone(),
        relative_path: compute_hashed_path(project_id),
        token: config.token.clone(),
    };

    let client = GitalyClient::connect(gitaly_config).await?;
    client.find_default_branch_name().await
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    pub struct MockRepositoryService {
        default_branches: Mutex<HashMap<i64, String>>,
    }

    impl MockRepositoryService {
        pub fn with_default_branch(project_id: i64, branch: &str) -> Arc<Self> {
            let service = Self {
                default_branches: Mutex::new(HashMap::new()),
            };
            service
                .default_branches
                .lock()
                .insert(project_id, branch.to_string());
            Arc::new(service)
        }
    }

    #[async_trait]
    impl RepositoryService for MockRepositoryService {
        async fn find_default_branch(
            &self,
            project_id: i64,
        ) -> Result<Option<String>, GitalyError> {
            Ok(self.default_branches.lock().get(&project_id).cloned())
        }

        async fn extract_repository(
            &self,
            _project_id: i64,
            _target_dir: &Path,
            _commit_id: &str,
        ) -> Result<(), GitalyError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_hashed_path_format() {
        let path = compute_hashed_path(1);
        assert!(path.starts_with("@hashed/"));
        assert!(path.ends_with(".git"));

        let parts: Vec<&str> = path.split('/').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "@hashed");
        assert_eq!(parts[1].len(), 2);
        assert_eq!(parts[2].len(), 2);
    }

    #[test]
    fn compute_hashed_path_deterministic() {
        let path1 = compute_hashed_path(123);
        let path2 = compute_hashed_path(123);
        assert_eq!(path1, path2);
    }

    #[test]
    fn compute_hashed_path_different_for_different_ids() {
        let path1 = compute_hashed_path(1);
        let path2 = compute_hashed_path(2);
        assert_ne!(path1, path2);
    }
}
