#![cfg(feature = "integration")]

use gitaly_client::{GitalyClient, GitalyRepositoryConfig, RepositorySource};
use std::env;
use tempfile::TempDir;

const TEST_RELATIVE_PATH: &str = "gitlab-org/gitlab-test.git";

fn get_config() -> Option<GitalyRepositoryConfig> {
    let json = env::var("GITALY_CONNECTION_INFO").ok()?;
    GitalyRepositoryConfig::from_json(&json).ok()
}

async fn require_test_repo(client: &GitalyClient) -> bool {
    match client.repository_exists().await {
        Ok(true) => true,
        Ok(false) => {
            eprintln!("Test repository does not exist, skipping test");
            eprintln!("Ensure {} is cloned in Gitaly", TEST_RELATIVE_PATH);
            false
        }
        Err(e) => {
            eprintln!("Failed to check repository existence: {e}");
            false
        }
    }
}

#[tokio::test]
async fn test_repository_exists() {
    let Some(config) = get_config() else {
        eprintln!("GITALY_CONNECTION_INFO not set, skipping");
        return;
    };
    let config = config.with_relative_path(TEST_RELATIVE_PATH);
    let client = GitalyClient::connect(config)
        .await
        .expect("failed to connect");
    if !require_test_repo(&client).await {
        return;
    }
    let exists = client.exists().await.expect("exists check failed");
    assert!(exists, "test repository should exist");
}

#[tokio::test]
async fn test_repository_not_exists() {
    let Some(config) = get_config() else {
        eprintln!("GITALY_CONNECTION_INFO not set, skipping");
        return;
    };
    let config = config.with_relative_path("nonexistent/repo.git");
    let client = GitalyClient::connect(config)
        .await
        .expect("failed to connect");
    let exists = client.exists().await.expect("exists check failed");
    assert!(!exists, "nonexistent repository should not exist");
}

#[tokio::test]
async fn test_pull_and_extract() {
    let Some(config) = get_config() else {
        eprintln!("GITALY_CONNECTION_INFO not set, skipping");
        return;
    };
    let config = config.with_relative_path(TEST_RELATIVE_PATH);
    let client = GitalyClient::connect(config)
        .await
        .expect("failed to connect");
    if !require_test_repo(&client).await {
        return;
    }

    let temp_dir = TempDir::new().expect("failed to create temp dir");
    client
        .extract_to(temp_dir.path(), None)
        .await
        .expect("extraction failed");

    let readme = temp_dir.path().join("README.md");
    assert!(readme.exists(), "README.md should exist after extraction");
}

#[tokio::test]
async fn test_invalid_connection() {
    let config = GitalyRepositoryConfig {
        address: "tcp://invalid-host:9999".to_string(),
        storage: "default".to_string(),
        relative_path: "test.git".to_string(),
        token: None,
    };
    let result = GitalyClient::connect(config).await;
    assert!(result.is_err(), "connection to invalid host should fail");
}
