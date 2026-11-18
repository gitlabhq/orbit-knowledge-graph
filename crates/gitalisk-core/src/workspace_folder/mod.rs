pub mod gitalisk_workspace;

// Re‑export the public types so that callers don't have to reference the
// `gitalisk_workspace` sub‑module directly.
pub use gitalisk_workspace::{CoreGitaliskWorkspaceFolder, WorkspaceFolderStatistics};

#[cfg(test)]
mod tests {
    use super::gitalisk_workspace::{CoreGitaliskWorkspaceFolder, WorkspaceFolderStatistics};
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    fn create_mock_workspace_with_repos(
        workspace_path: &Path,
        repo_names: &[&str],
    ) -> std::io::Result<()> {
        for repo_name in repo_names {
            let repo_path = workspace_path.join(repo_name);
            fs::create_dir_all(&repo_path)?;

            // Create .git directory with config
            let git_dir = repo_path.join(".git");
            fs::create_dir_all(&git_dir)?;

            let config_content = r#"[core]
	repositoryformatversion = 0
	filemode = true
	bare = false
	logallrefupdates = true
"#;
            fs::write(git_dir.join("config"), config_content)?;

            // Create some sample files
            fs::write(repo_path.join("README.md"), format!("# {repo_name}"))?;
            fs::create_dir_all(repo_path.join("src"))?;
            fs::write(repo_path.join("src").join("main.rs"), "fn main() {}")?;
        }

        // Create some non-repo files in workspace
        fs::write(workspace_path.join("workspace.md"), "Workspace info")?;

        Ok(())
    }

    #[test]
    fn test_new_workspace() {
        let workspace = CoreGitaliskWorkspaceFolder::new("/test/workspace".to_string());

        // Test that we can create a new workspace
        assert!(workspace.get_repositories().is_empty());
    }

    #[test]
    fn test_index_empty_workspace() {
        let temp_dir = tempdir().unwrap();
        let workspace_path = temp_dir.path().to_str().unwrap();

        let workspace = CoreGitaliskWorkspaceFolder::new(workspace_path.to_string());
        let stats = workspace.index_repositories().unwrap();

        assert_eq!(stats.to_owned().file_count, 0);
        assert_eq!(stats.to_owned().repo_count, 0);
    }

    #[test]
    fn test_index_workspace_with_single_repo() {
        let temp_dir = tempdir().unwrap();
        let workspace_path = temp_dir.path();

        // Create a single repository
        create_mock_workspace_with_repos(workspace_path, &["test-repo"]).unwrap();

        let workspace =
            CoreGitaliskWorkspaceFolder::new(workspace_path.to_str().unwrap().to_string());
        let stats = workspace.index_repositories().unwrap();

        assert_eq!(stats.to_owned().repo_count, 1);
    }

    #[test]
    fn test_index_workspace_with_multiple_repos() {
        let temp_dir = tempdir().unwrap();
        let workspace_path = temp_dir.path();

        // Create multiple repositories
        create_mock_workspace_with_repos(workspace_path, &["repo-1", "repo-2", "repo-3"]).unwrap();

        let workspace =
            CoreGitaliskWorkspaceFolder::new(workspace_path.to_str().unwrap().to_string());
        let stats = workspace.index_repositories().unwrap();

        assert_eq!(stats.to_owned().repo_count, 3);

        // Check that all repos are found
        let repo_names: Vec<String> = workspace
            .get_repositories()
            .iter()
            .map(|r| {
                Path::new(&r.path)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect();

        assert!(repo_names.contains(&"repo-1".to_string()));
        assert!(repo_names.contains(&"repo-2".to_string()));
        assert!(repo_names.contains(&"repo-3".to_string()));

        assert_eq!(stats.to_owned().repo_count, 3);
    }

    #[test]
    fn test_get_repositories() {
        let temp_dir = tempdir().unwrap();
        let workspace_path = temp_dir.path();

        create_mock_workspace_with_repos(workspace_path, &["test-repo"]).unwrap();

        let workspace =
            CoreGitaliskWorkspaceFolder::new(workspace_path.to_str().unwrap().to_string());

        // Before indexing, should be empty
        let repos_before = workspace.get_repositories();
        assert_eq!(repos_before.len(), 0);

        // After indexing, should have repos
        let _indexed_repos = workspace.index_repositories().unwrap();
        let repos_after = workspace.get_repositories();
        assert_eq!(repos_after.len(), 1);
    }

    #[test]
    fn test_workspace_statistics_for_nonexistent_workspace() {
        let workspace = CoreGitaliskWorkspaceFolder::new("/nonexistent/path".to_string());
        let stats = workspace.index_repositories().unwrap();

        assert_eq!(stats.to_owned().file_count, 0);
        assert_eq!(stats.to_owned().repo_count, 0);
    }

    #[test]
    fn test_cleanup() {
        let temp_dir = tempdir().unwrap();
        let workspace_path = temp_dir.path();

        create_mock_workspace_with_repos(workspace_path, &["test-repo"]).unwrap();

        let workspace =
            CoreGitaliskWorkspaceFolder::new(workspace_path.to_str().unwrap().to_string());
        let _repos = workspace.index_repositories().unwrap();

        // Should have repos before cleanup
        let repos_before = workspace.get_repositories();
        assert_eq!(repos_before.len(), 1);

        // After cleanup, should be empty
        workspace.cleanup();
        let repos_after = workspace.get_repositories();
        assert_eq!(repos_after.len(), 0);

        let _stats = workspace.index_repositories().unwrap();
        // After cleanup, statistics are not cleared in current implementation
        // This is expected behavior as statistics represent last indexing results
    }

    #[test]
    fn test_workspace_statistics_clone() {
        let stats = WorkspaceFolderStatistics {
            file_count: 10,
            repo_count: 2,
        };

        let cloned_stats = stats.clone();
        assert_eq!(cloned_stats.file_count, 10);
        assert_eq!(cloned_stats.repo_count, 2);
    }

    #[test]
    fn test_index_workspace_with_invalid_git_dirs() {
        let temp_dir = tempdir().unwrap();
        let workspace_path = temp_dir.path();

        // Create a directory that looks like a git repo but isn't
        let fake_repo = workspace_path.join("fake-repo");
        fs::create_dir_all(&fake_repo).unwrap();
        fs::create_dir_all(fake_repo.join(".git")).unwrap();
        // Note: No config file, so it shouldn't be counted as a valid repo

        // Create a valid repo
        create_mock_workspace_with_repos(workspace_path, &["valid-repo"]).unwrap();

        let workspace =
            CoreGitaliskWorkspaceFolder::new(workspace_path.to_str().unwrap().to_string());
        let stats = workspace.index_repositories().unwrap();

        // Should only find the valid repo
        assert_eq!(stats.to_owned().repo_count, 1);
        assert_eq!(
            workspace.get_repositories()[0].path,
            workspace_path.join("valid-repo").to_str().unwrap()
        );

        let stats2 = workspace.index_repositories().unwrap();
        println!("stats2: {stats2:?}");
        assert_eq!(stats2.to_owned().repo_count, 1);

        // get repository names
        let repo_names: Vec<String> = workspace
            .get_repositories()
            .iter()
            .map(|r| {
                Path::new(&r.path)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect();

        assert!(repo_names.contains(&"valid-repo".to_string()));
    }

    #[test]
    fn test_multiple_workspaces() {
        let temp_dir1 = tempdir().unwrap();
        let temp_dir2 = tempdir().unwrap();
        let workspace_path1 = temp_dir1.path();
        let workspace_path2 = temp_dir2.path();

        create_mock_workspace_with_repos(workspace_path1, &["repo-1a", "repo-1b"]).unwrap();
        create_mock_workspace_with_repos(workspace_path2, &["repo-2a"]).unwrap();

        let workspace1 =
            CoreGitaliskWorkspaceFolder::new(workspace_path1.to_str().unwrap().to_string());
        let workspace2 =
            CoreGitaliskWorkspaceFolder::new(workspace_path2.to_str().unwrap().to_string());

        let stats1 = workspace1.index_repositories().unwrap();
        let stats2 = workspace2.index_repositories().unwrap();

        assert_eq!(stats1.to_owned().repo_count, 2);
        assert_eq!(stats2.to_owned().repo_count, 1);

        // Check that both workspaces are tracked separately
        let repos1_cached = workspace1.get_repositories();
        let repos2_cached = workspace2.get_repositories();

        assert_eq!(repos1_cached.len(), 2);
        assert_eq!(repos2_cached.len(), 1);

        assert_eq!(stats1.to_owned().repo_count, 2);
        assert_eq!(stats2.to_owned().repo_count, 1);
    }
}
