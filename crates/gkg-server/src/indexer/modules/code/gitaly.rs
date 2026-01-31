//! Gitaly utilities for repository operations.

use std::env;
use std::path::Path;

use gitaly_client::{GitalyClient, GitalyConfig, GitalyError, RepositorySource};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
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
    let gitaly_config = GitalyConfig {
        address: config.address.clone(),
        storage: config.storage.clone(),
        relative_path: compute_hashed_path(project_id),
        token: config.token.clone(),
    };

    let client = GitalyClient::connect(gitaly_config).await?;
    RepositorySource::extract_to(&client, target_dir, Some(commit_id)).await
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
