use crate::repository::gitalisk_repository::CoreGitaliskRepository;
use ignore::WalkBuilder;
use orbit_log_fields::info;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct WorkspaceFolderStatistics {
    pub file_count: u32,
    pub repo_count: u32,
}

pub struct CoreGitaliskWorkspaceFolder {
    workspace_path: String,
    repositories: Arc<Mutex<HashMap<String, CoreGitaliskRepository>>>,
    statistics: Arc<Mutex<WorkspaceFolderStatistics>>,
}

impl CoreGitaliskWorkspaceFolder {
    pub fn new(workspace_path: String) -> Self {
        Self {
            workspace_path,
            repositories: Arc::new(Mutex::new(HashMap::new())),
            statistics: Arc::new(Mutex::new(WorkspaceFolderStatistics {
                file_count: 0,
                repo_count: 0,
            })),
        }
    }

    pub fn index_repositories(&self) -> Result<WorkspaceFolderStatistics, String> {
        self.cleanup();

        info!(
            expanded(LOCAL_PATH) = %self.workspace_path,
            "Indexing repositories in workspace"
        );
        let repos = Arc::new(Mutex::new(Vec::new()));
        let repositories = Arc::clone(&self.repositories);
        let workspace_path = self.workspace_path.clone();
        let statistics = Arc::clone(&self.statistics);

        WalkBuilder::new(&workspace_path)
            .hidden(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .ignore(false)
            .parents(false)
            .build_parallel()
            .run(|| {
                let repos = Arc::clone(&repos);
                let repositories = Arc::clone(&repositories);
                let workspace_path = workspace_path.clone();
                let statistics = statistics.clone();

                Box::new(move |result| {
                    if let Ok(entry) = result {
                        if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                            statistics.lock().unwrap().file_count += 1;
                        }

                        if entry.file_name() == ".git"
                            && let Some(parent) = entry.path().parent()
                            && let Some(repo_path) = parent.to_str()
                        {
                            let mut is_valid_repo = false;

                            if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                                let git_config_path = entry.path().join("config");
                                is_valid_repo = git_config_path.is_file();
                            } else if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                                && let Ok(file) = std::fs::File::open(entry.path())
                            {
                                let mut reader = BufReader::new(file);
                                let mut first_line = String::new();
                                if reader.read_line(&mut first_line).is_ok() {
                                    is_valid_repo = first_line
                                        .trim()
                                        .strip_prefix("gitdir:")
                                        .map(|s| s.trim())
                                        .and_then(|s| fs::metadata(s).ok())
                                        .map(|md| md.is_dir())
                                        .unwrap_or(false);
                                }
                            }

                            if is_valid_repo {
                                let repo_obj = CoreGitaliskRepository::new(
                                    repo_path.to_string(),
                                    workspace_path.clone(),
                                );

                                {
                                    let mut stats = statistics.lock().unwrap();
                                    stats.repo_count += 1;
                                }

                                {
                                    let mut repos_map = repositories.lock().unwrap();
                                    repos_map.insert(repo_path.to_string(), repo_obj.clone());
                                }

                                {
                                    repos.lock().unwrap().push(repo_obj);
                                }
                            }
                        }
                    }
                    ignore::WalkState::Continue
                })
            });

        let statistics = self.statistics.lock().unwrap().clone();
        info!(
            expanded(LOCAL_PATH) = %self.workspace_path,
            safe(FILE_COUNT) = statistics.file_count,
            safe(PROJECT_COUNT) = statistics.repo_count,
            "Finished indexing repositories in workspace"
        );

        Ok(self.statistics.lock().unwrap().clone())
    }

    pub fn get_repositories(&self) -> Vec<CoreGitaliskRepository> {
        let repos = self.repositories.lock().unwrap();
        repos.values().cloned().collect()
    }

    pub fn get_workspace_path(&self) -> &str {
        &self.workspace_path
    }

    pub fn cleanup(&self) {
        info!(expanded(LOCAL_PATH) = %self.workspace_path, "Cleaning up workspace");
        self.repositories.lock().unwrap().clear();
        let mut stats = self.statistics.lock().unwrap();
        stats.file_count = 0;
        stats.repo_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_real_worktree_integration() {
        use std::process::Command;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let repos_dir = base_path.join("repos");
        let repo_dir = repos_dir.join("test-repo");
        let worktrees_dir = repos_dir.join("test-repo-worktrees");

        fs::create_dir_all(&repo_dir).unwrap();
        fs::create_dir_all(&worktrees_dir).unwrap();

        Command::new("git")
            .args(["init"])
            .current_dir(&repo_dir)
            .output()
            .unwrap();

        Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(&repo_dir)
            .output()
            .unwrap();

        fs::write(repo_dir.join("README.md"), "# Test Repository").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&repo_dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "Initial commit"])
            .current_dir(&repo_dir)
            .output()
            .unwrap();

        let worktree1 = worktrees_dir.join("feature1");
        let worktree2 = worktrees_dir.join("feature2");

        Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                "feature1",
                &worktree1.to_string_lossy(),
            ])
            .current_dir(&repo_dir)
            .output()
            .unwrap();

        Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                "feature2",
                &worktree2.to_string_lossy(),
            ])
            .current_dir(&repo_dir)
            .output()
            .unwrap();

        let workspace =
            CoreGitaliskWorkspaceFolder::new(worktrees_dir.to_string_lossy().to_string());
        let stats = workspace.index_repositories().unwrap();

        assert_eq!(
            stats.repo_count, 2,
            "Expected 2 worktree repositories to be detected"
        );

        let repositories = workspace.get_repositories();
        assert_eq!(repositories.len(), 2);

        let repo_paths: Vec<String> = repositories.iter().map(|r| r.path.clone()).collect();
        assert!(repo_paths.iter().any(|p| p.contains("feature1")));
        assert!(repo_paths.iter().any(|p| p.contains("feature2")));

        let full_workspace =
            CoreGitaliskWorkspaceFolder::new(repos_dir.to_string_lossy().to_string());
        let full_stats = full_workspace.index_repositories().unwrap();

        assert_eq!(
            full_stats.repo_count, 3,
            "Expected 1 main repo + 2 worktrees = 3 repositories"
        );
    }

    #[test]
    fn test_bad_worktree_ignored() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let bad_worktree_dir = base_path.join("bad-worktree");
        fs::create_dir_all(&bad_worktree_dir).unwrap();

        let git_file = bad_worktree_dir.join(".git");
        fs::write(&git_file, "this is not a valid gitdir line").unwrap();

        let bad_worktree_dir2 = base_path.join("bad-worktree2");
        fs::create_dir_all(&bad_worktree_dir2).unwrap();
        let git_file2 = bad_worktree_dir2.join(".git");
        fs::write(&git_file2, "gitdir: /this/path/does/not/exist").unwrap();

        let bad_worktree_dir3 = base_path.join("bad-worktree3");
        fs::create_dir_all(&bad_worktree_dir3).unwrap();
        let git_file3 = bad_worktree_dir3.join(".git");
        fs::write(&git_file3, "hello world").unwrap();

        let workspace = CoreGitaliskWorkspaceFolder::new(base_path.to_string_lossy().to_string());
        let stats = workspace.index_repositories().unwrap();

        assert_eq!(
            stats.repo_count, 0,
            "Expected 0 repositories since all worktrees are invalid"
        );

        let repositories = workspace.get_repositories();
        assert_eq!(repositories.len(), 0);
    }
}
