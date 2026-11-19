pub mod git;
pub mod gitalisk_repository;
pub mod testing;

#[cfg(test)]
mod tests {
    use super::gitalisk_repository::CoreGitaliskRepository;
    use crate::repository::gitalisk_repository::IterFileOptions;
    use crate::repository::gitalisk_repository::{FileStatusInfo, GetStatusOptions, StatusCode};
    use crate::repository::testing::local::LocalGitRepository;
    use crate::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;
    use std::path::Path;
    use std::process::Command;

    fn init_test_workspace_with_repo() -> LocalGitRepository {
        let mut repo = LocalGitRepository::new(None);

        println!("repo.user_config(): {}", repo.user_config());

        repo.fs.create_directory("src");
        repo.fs.create_file("README.md", Some("test"));
        repo.fs.create_file("Cargo.toml", Some("test"));
        repo.fs.create_file(
            "src/main.rs",
            Some("fn main() { println!(\"Hello, world!\"); }"),
        );

        repo
    }

    #[test]
    fn test_get_status_invalid_repo_path() {
        let local_repo = init_test_workspace_with_repo();

        let workspace = CoreGitaliskWorkspaceFolder::new(
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );
        let _ = workspace.index_repositories();
        let repos = workspace.get_repositories();
        let repo = repos.first().unwrap();

        let repo_path_str = repo.path.clone();
        let real_path_but_malicious_cmd: String = format!("{repo_path_str}; ls -la");

        let malicious_path_struct = Path::new(&real_path_but_malicious_cmd);
        assert!(!malicious_path_struct.try_exists().unwrap_or(false));
        println!("NOT PATH: malicious_path_struct: {malicious_path_struct:?}");

        let fake_malicious_path = Path::new("/test/path; ls -la");
        let repo_path = Path::new(fake_malicious_path);
        println!("FAKE PATH: repo_path: {repo_path:?}");
        assert!(!repo_path.try_exists().unwrap_or(false));
    }

    #[test]
    fn test_get_status_invalid_repo_path_sanitization() {
        let local_repo = init_test_workspace_with_repo();

        let workspace = CoreGitaliskWorkspaceFolder::new(
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );
        let _ = workspace.index_repositories();
        let repos = workspace.get_repositories();
        let repo = repos.first().unwrap();

        let repo_path_str = repo.path.clone();
        let real_path_but_malicious_cmd: String = format!("{repo_path_str}; ls -la");

        let malicious_path_struct = Path::new(&real_path_but_malicious_cmd);
        assert!(!malicious_path_struct.try_exists().unwrap_or(false));
        println!("NOT PATH: malicious_path_struct: {malicious_path_struct:?}");

        let fake_malicious_path = Path::new("/test/path; ls -la");
        let repo_path = Path::new(fake_malicious_path);
        println!("FAKE PATH: repo_path: {repo_path:?}");
        assert!(!repo_path.try_exists().unwrap_or(false));
    }

    #[test]
    fn test_get_status_basic() {
        let mut local_repo = init_test_workspace_with_repo();

        local_repo.add_all();

        let workspace = CoreGitaliskWorkspaceFolder::new(
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );
        let _ = workspace.index_repositories();
        let repos = workspace.get_repositories();
        println!("Found {} repositories", repos.len());
        for (i, repo) in repos.iter().enumerate() {
            println!("Repo {}: path = {}", i, repo.path);
        }
        let repo = repos.first().unwrap();

        let statuses = repo.get_status().unwrap();

        let unique_statuses: Vec<StatusCode> = statuses.iter().map(|s| s.status.index).collect();
        for status in statuses.clone() {
            println!("status: {status:?}");
        }
        assert!(unique_statuses.contains(&StatusCode::Added));
        assert_eq!(statuses.len(), 3);

        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );
    }

    #[test]
    fn test_get_status_commit_and_modify() {
        let mut local_repo = init_test_workspace_with_repo();

        let workspace = CoreGitaliskWorkspaceFolder::new(
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );
        let _ = workspace.index_repositories();
        let repos = workspace.get_repositories();
        let repo = repos.first().unwrap();

        local_repo.add_all();

        let statuses = repo.get_status().unwrap();
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        let unique_statuses: Vec<StatusCode> = statuses.iter().map(|s| s.status.index).collect();
        for status in statuses.clone() {
            println!("status: {status:?}");
        }
        assert!(unique_statuses.contains(&StatusCode::Added));
        assert_eq!(statuses.len(), 3);

        local_repo.commit("test commit 1");
        println!("\nPOST COMMIT");
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        local_repo
            .fs
            .append_file_content("README.md", "test commit 2");
        println!("\nPOST MODIFY");
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        let statuses_2 = repo.get_status().unwrap();
        let unique_statuses_2: Vec<StatusCode> =
            statuses_2.iter().map(|s| s.status.worktree).collect();
        for status_2 in statuses_2.clone() {
            println!("status_2: {status_2:?}");
        }
        assert_eq!(statuses_2.len(), 1);
        assert!(unique_statuses_2.contains(&StatusCode::Modified));
    }

    #[test]
    fn test_get_status_commit_and_rename() {
        let mut local_repo = init_test_workspace_with_repo();

        let workspace = CoreGitaliskWorkspaceFolder::new(
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );
        let _ = workspace.index_repositories();
        let repos = workspace.get_repositories();
        let repo = repos.first().unwrap();

        local_repo.add_all();

        let statuses = repo.get_status().unwrap();
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        let unique_statuses: Vec<StatusCode> = statuses.iter().map(|s| s.status.index).collect();
        for status in statuses.clone() {
            println!("status: {status:?}");
        }
        assert!(unique_statuses.contains(&StatusCode::Added));
        assert_eq!(statuses.len(), 3);

        local_repo.commit("test commit 1");
        println!("\nPOST COMMIT");
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        local_repo.fs.rename_to("README.md", "README.md.new");
        println!("\nPOST MODIFY");
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );
        let statuses_2 = repo.get_status().unwrap();
        assert_eq!(statuses_2.len(), 1);
        let first_status_2 = statuses_2.first().unwrap();
        println!("first_status_2: {first_status_2:?}");
        assert!(!first_status_2.path.is_empty() && first_status_2.original_path.is_some());
    }

    #[test]
    fn test_get_status_commit_and_move_file() {
        let mut local_repo = init_test_workspace_with_repo();

        let workspace = CoreGitaliskWorkspaceFolder::new(
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );
        let _ = workspace.index_repositories();
        let repos = workspace.get_repositories();
        let repo = repos.first().unwrap();

        local_repo.add_all();

        let statuses = repo.get_status().unwrap();
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        let unique_statuses: Vec<StatusCode> = statuses.iter().map(|s| s.status.index).collect();
        for status in statuses.clone() {
            println!("status: {status:?}");
        }
        assert!(unique_statuses.contains(&StatusCode::Added));
        assert_eq!(statuses.len(), 3);

        local_repo.commit("test commit 1");
        println!("\nPOST COMMIT");
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        local_repo.fs.move_to("README.md", "src/README.md");
        println!("\nPOST MOVE");
        println!("local_repo.status(): {}", local_repo.status());
        println!(
            "local_repo.status_porcelain(): {}",
            local_repo.status_porcelain()
        );

        let statuses_2 = repo.get_status().unwrap();
        assert_eq!(statuses_2.len(), 1);

        let first_status_2 = statuses_2.first().unwrap();
        println!("first_status_2: {first_status_2:?}");
        assert!(!first_status_2.path.is_empty() && first_status_2.original_path.is_some());
        assert_eq!(first_status_2.status.index, StatusCode::Unmodified);
        assert_eq!(first_status_2.status.worktree, StatusCode::Renamed);
    }

    #[test]
    fn test_new_repository() {
        let repo =
            CoreGitaliskRepository::new("/test/path".to_string(), "/test/workspace".to_string());

        assert_eq!(repo.path, "/test/path");
        assert_eq!(repo.workspace_path, "/test/workspace");
    }

    #[test]
    fn test_list_branches() {
        let repo =
            CoreGitaliskRepository::new("/test/path".to_string(), "/test/workspace".to_string());

        let branches = repo.list_branches().unwrap();
        assert_eq!(branches, vec!["main"]);
    }

    #[test]
    fn test_find_repo_files() {
        let local_repo = init_test_workspace_with_repo();

        let repo = CoreGitaliskRepository::new(
            local_repo.path.to_str().unwrap().to_string(),
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );

        let files = repo
            .get_repo_files(IterFileOptions {
                include_ignored: true,
                include_hidden: true,
                exclude_patterns: vec![],
            })
            .unwrap();
        assert!(files.len() >= 3);

        let file_names: Vec<String> = files
            .iter()
            .filter_map(|f| Path::new(f.path()).file_name()?.to_str())
            .map(String::from)
            .collect();

        assert!(file_names.contains(&"README.md".to_string()));
        assert!(file_names.contains(&"Cargo.toml".to_string()));
        assert!(file_names.contains(&"main.rs".to_string()));
    }

    #[test]
    fn test_find_repo_files_empty_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_path = temp_dir.path();

        let repo = CoreGitaliskRepository::new(
            repo_path.to_str().unwrap().to_string(),
            "/test/workspace".to_string(),
        );

        let files = repo
            .get_repo_files(IterFileOptions {
                include_ignored: true,
                include_hidden: false,
                exclude_patterns: vec![],
            })
            .unwrap();
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn test_find_repo_files_with_gitignore() {
        let mut local_repo = init_test_workspace_with_repo();

        local_repo
            .fs
            .create_file(".gitignore", Some("target/\n*.log\n"))
            .create_directory("target")
            .create_file("target/debug", Some("binary"))
            .create_file("test.log", Some("log content"))
            .create_file("src.rs", Some("code"));

        let repo = CoreGitaliskRepository::new(
            local_repo.path.to_str().unwrap().to_string(),
            local_repo.workspace_path.to_str().unwrap().to_string(),
        );

        let files = repo
            .get_repo_files(IterFileOptions {
                include_ignored: false,
                include_hidden: true,
                exclude_patterns: vec![],
            })
            .unwrap();

        let file_names: Vec<String> = files
            .iter()
            .filter_map(|f| Path::new(f.path()).file_name()?.to_str())
            .map(String::from)
            .collect();
        println!("file_names: {file_names:?}");

        assert!(file_names.contains(&".gitignore".to_string()));
        assert!(file_names.contains(&"src.rs".to_string()));
        assert!(!file_names.contains(&"target/debug".to_string()));
        assert!(!file_names.contains(&"test.log".to_string()));
    }

    fn relative(
        repo: &LocalGitRepository,
        info: &FileStatusInfo,
    ) -> (String, Option<String>, StatusCode) {
        let strip = |p: &Path| {
            p.strip_prefix(&repo.path)
                .unwrap()
                .to_string_lossy()
                .to_string()
        };
        (
            strip(&info.path),
            info.original_path.as_deref().map(strip),
            info.status.index,
        )
    }

    #[test]
    fn get_status_reports_deleted_files_and_nested_untracked_ones() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo
            .fs
            .create_file("src/edit.rs", Some("fn edit() {}"))
            .create_file("src/gone.rs", Some("fn gone() {}"));
        local_repo.add_all().commit("base");
        local_repo
            .fs
            .append_file_content("src/edit.rs", "\nfn more() {}")
            .delete_file("src/gone.rs")
            .create_file("lib/deep/untracked.rs", Some("fn u() {}"));

        let repo = CoreGitaliskRepository::new(
            local_repo.path.to_string_lossy().to_string(),
            local_repo.workspace_path.to_string_lossy().to_string(),
        );
        let mut statuses: Vec<_> = repo
            .get_status()
            .unwrap()
            .iter()
            .map(|s| relative(&local_repo, s))
            .collect();
        statuses.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            statuses,
            [
                (
                    "lib/deep/untracked.rs".to_string(),
                    None,
                    StatusCode::Untracked
                ),
                ("src/edit.rs".to_string(), None, StatusCode::Unmodified),
                ("src/gone.rs".to_string(), None, StatusCode::Unmodified),
            ]
        );
        let worktree: Vec<StatusCode> = repo
            .get_status()
            .unwrap()
            .iter()
            .map(|s| s.status.worktree)
            .collect();
        assert!(
            worktree.contains(&StatusCode::Deleted) && worktree.contains(&StatusCode::Modified)
        );
    }

    #[test]
    fn changes_since_a_base_commit() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo
            .fs
            .create_file("src/keep.rs", Some("fn keep() {}"))
            .create_file("src/edit.rs", Some("fn edit() {}"))
            .create_file("src/old.rs", Some("fn old() {}"))
            .create_file("README.md", Some("readme"));
        local_repo.add_all().commit("base");
        let repo = CoreGitaliskRepository::new(
            local_repo.path.to_string_lossy().to_string(),
            local_repo.workspace_path.to_string_lossy().to_string(),
        );
        let base = repo.get_current_commit_hash().unwrap();

        local_repo
            .fs
            .append_file_content("src/edit.rs", "\nfn more() {}")
            .rename_to("src/old.rs", "src/new.rs")
            .delete_file("README.md")
            .create_file("src/fresh.rs", Some("fn fresh() {}"));
        local_repo.add_all().commit("changes");

        let mut changes: Vec<_> = repo
            .changes_since(&base)
            .unwrap()
            .iter()
            .map(|s| relative(&local_repo, s))
            .collect();
        changes.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            changes,
            [
                ("README.md".to_string(), None, StatusCode::Deleted),
                ("src/edit.rs".to_string(), None, StatusCode::Modified),
                ("src/fresh.rs".to_string(), None, StatusCode::Added),
                (
                    "src/new.rs".to_string(),
                    Some("src/old.rs".to_string()),
                    StatusCode::Renamed
                ),
            ]
        );
        assert!(repo.changes_since("HEAD").unwrap().is_empty());
    }

    #[test]
    fn changes_since_rejects_option_like_base_without_side_effects() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("a.rs", Some("fn a() {}"));
        local_repo.add_all().commit("base");
        let repo = repo_handle(&local_repo);
        let target = local_repo.path.join("injected");

        let error = repo
            .changes_since(&format!("--output={}", target.display()))
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(!target.exists());
    }

    fn repo_handle(local_repo: &LocalGitRepository) -> CoreGitaliskRepository {
        CoreGitaliskRepository::new(
            local_repo.path.to_string_lossy().to_string(),
            local_repo.workspace_path.to_string_lossy().to_string(),
        )
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_names_come_back_as_paths_that_exist() {
        use std::os::unix::ffi::OsStrExt;
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("README.md", Some("readme"));
        let repo = repo_handle(&local_repo);
        local_repo.add_all().commit("base");
        let base = repo.get_current_commit_hash().unwrap();

        let odd = std::ffi::OsStr::from_bytes(b"caf\xe9.rs");
        std::fs::write(local_repo.path.join(odd), "fn odd() {}").unwrap();
        let untracked = repo.get_status().unwrap();
        assert_eq!(untracked.len(), 1);
        assert!(untracked[0].path.exists());
        assert_eq!(untracked[0].path.file_name().unwrap(), odd);

        local_repo.add_all().commit("odd");
        std::fs::write(local_repo.path.join(odd), "fn odd() { 1 }").unwrap();
        let modified = repo.get_status().unwrap();
        assert!(modified[0].path.exists());

        let changes = repo.changes_since(&base).unwrap();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].path.exists());

        let tracked = repo.tracked_files(&[] as &[&str]).unwrap();
        assert!(tracked.iter().any(|p| p.file_name() == Some(odd)));
        assert!(tracked.iter().all(|p| p.exists()));

        let listed = repo.get_repo_files(IterFileOptions::default()).unwrap();
        assert!(listed.iter().any(|f| f.path().file_name() == Some(odd)));
    }

    #[test]
    fn status_can_be_limited_to_pathspecs_and_skip_untracked_files() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo
            .fs
            .create_file("src/a.rs", Some("fn a() {}"))
            .create_file("src/b.rs", Some("fn b() {}"));
        local_repo.add_all().commit("base");
        local_repo
            .fs
            .append_file_content("src/a.rs", "\nfn more() {}")
            .append_file_content("src/b.rs", "\nfn more() {}")
            .create_file("src/new.rs", Some("fn new() {}"));
        let repo = repo_handle(&local_repo);

        let options = GetStatusOptions {
            pathspecs: vec!["src/a.rs".into(), "src/new.rs".into()],
            ..Default::default()
        };
        let statuses = repo.get_status_with(&options).unwrap();
        let paths: Vec<_> = statuses
            .iter()
            .map(|s| relative(&local_repo, s).0)
            .collect();
        assert_eq!(paths, ["src/a.rs"]);

        let with_untracked = GetStatusOptions {
            include_untracked: true,
            ..options
        };
        assert_eq!(repo.get_status_with(&with_untracked).unwrap().len(), 2);
    }

    #[test]
    fn tracked_files_and_tracked_ignored_files() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo
            .fs
            .create_file("src/a.rs", Some("fn a() {}"))
            .create_file("src/b.rs", Some("fn b() {}"))
            .create_file("out.log", Some("log"));
        local_repo.add_all().commit("base");
        local_repo.fs.create_file(".gitignore", Some("*.log\n"));
        local_repo.add_all().commit("ignore");
        local_repo.fs.create_file("untracked.rs", Some("fn u() {}"));
        let repo = repo_handle(&local_repo);

        let tracked = repo.tracked_files(&["src/a.rs", "untracked.rs"]).unwrap();
        assert_eq!(tracked, [local_repo.path.join("src/a.rs")]);
        assert_eq!(
            repo.tracked_ignored_files().unwrap(),
            [local_repo.path.join("out.log")]
        );
    }

    #[test]
    fn index_files_lists_tracked_and_unignored_untracked_files() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo
            .fs
            .create_file("src/a.rs", Some("fn a() {}"))
            .create_file("build/out.rs", Some("fn out() {}"))
            .create_file("hidden.rs", Some("fn h() {}"));
        local_repo.add_all().commit("base");
        local_repo
            .fs
            .create_file(".gitignore", Some("build/\n*.log\n"));
        local_repo.fs.create_file("untracked.rs", Some("fn u() {}"));
        local_repo.fs.create_file("noise.log", Some("log"));
        let repo = repo_handle(&local_repo);
        Command::new("git")
            .args(["update-index", "--skip-worktree", "hidden.rs"])
            .current_dir(&local_repo.path)
            .output()
            .unwrap();

        let listed: Vec<_> = repo
            .index_files()
            .unwrap()
            .into_iter()
            .map(|file| {
                (
                    file.path.to_string_lossy().into_owned(),
                    file.tracked,
                    file.skip_worktree,
                )
            })
            .collect();
        assert_eq!(
            listed,
            [
                (".gitignore".to_string(), false, false),
                ("build/out.rs".to_string(), true, false),
                ("hidden.rs".to_string(), true, true),
                ("src/a.rs".to_string(), true, false),
                ("untracked.rs".to_string(), false, false),
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn index_files_keeps_names_that_are_not_utf8() {
        use std::os::unix::ffi::OsStrExt;
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("a.rs", Some("fn a() {}"));
        local_repo.add_all().commit("base");
        let name = std::ffi::OsStr::from_bytes(b"bad-\xff.rs");
        std::fs::write(local_repo.path.join(name), "fn b() {}").unwrap();
        let repo = repo_handle(&local_repo);

        let listed = repo.index_files().unwrap();
        assert!(listed.iter().any(|file| file.path.as_os_str() == name));
    }

    #[test]
    fn index_files_error_carries_gits_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let repo = CoreGitaliskRepository::new(path.clone(), path);
        let error = repo.index_files().unwrap_err().to_string();
        assert!(error.contains("not a git repository"), "{error}");
    }

    #[test]
    fn remote_url_is_none_for_a_missing_remote() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("README.md", Some("readme"));
        local_repo.add_all().commit("base");
        let repo = repo_handle(&local_repo);
        assert_eq!(repo.remote_url("origin").unwrap(), None);

        let url = "git@gitlab.com:group/project.git";
        Command::new("git")
            .args(["remote", "add", "origin", url])
            .current_dir(&local_repo.path)
            .output()
            .unwrap();
        assert_eq!(repo.remote_url("origin").unwrap().as_deref(), Some(url));
        assert_eq!(repo.remote_url("upstream").unwrap(), None);
    }

    #[test]
    fn remote_url_errors_outside_a_repository() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = CoreGitaliskRepository::new(
            dir.path().to_string_lossy().to_string(),
            dir.path().to_string_lossy().to_string(),
        );
        assert!(repo.remote_url("origin").is_err());
    }

    #[test]
    fn current_branch_name_is_none_on_a_detached_head() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("README.md", Some("readme"));
        local_repo.add_all().commit("base");
        let repo = repo_handle(&local_repo);
        let branch = repo.current_branch_name().unwrap().unwrap();
        assert_eq!(repo.get_current_branch().unwrap(), branch);

        Command::new("git")
            .args(["checkout", "--detach", "-q"])
            .current_dir(&local_repo.path)
            .output()
            .unwrap();
        assert_eq!(repo.current_branch_name().unwrap(), None);
        assert!(
            repo.get_current_branch()
                .unwrap()
                .starts_with("HEAD detached")
        );
    }

    #[test]
    fn get_status_pairs_a_worktree_side_rename() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("README.md", Some("readme"));
        local_repo.add_all().commit("base");
        std::fs::rename(
            local_repo.path.join("README.md"),
            local_repo.path.join("README2.md"),
        )
        .unwrap();
        Command::new("git")
            .args(["add", "-N", "README2.md"])
            .current_dir(&local_repo.path)
            .output()
            .unwrap();

        let statuses = repo_handle(&local_repo).get_status().unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(
            relative(&local_repo, &statuses[0]),
            (
                "README2.md".to_string(),
                Some("README.md".to_string()),
                StatusCode::Unmodified
            )
        );
        assert_eq!(statuses[0].status.worktree, StatusCode::Renamed);
    }

    #[test]
    fn ignore_submodules_hides_a_dirty_submodule() {
        let upstream = tempfile::TempDir::new().unwrap();
        let git = |dir: &Path, args: &[&str]| {
            let output = Command::new("git")
                .args(["-c", "protocol.file.allow=always"])
                .args(args)
                .current_dir(dir)
                .output()
                .unwrap();
            assert!(output.status.success(), "{args:?}: {output:?}");
        };
        git(upstream.path(), &["init", "-q"]);
        std::fs::write(upstream.path().join("lib.rs"), "fn lib() {}").unwrap();
        git(upstream.path(), &["add", "."]);
        git(
            upstream.path(),
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "commit",
                "-q",
                "-m",
                "lib",
            ],
        );

        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("README.md", Some("readme"));
        git(
            &local_repo.path,
            &[
                "submodule",
                "add",
                "-q",
                upstream.path().to_str().unwrap(),
                "sub",
            ],
        );
        local_repo.add_all().commit("with submodule");
        std::fs::write(local_repo.path.join("sub/lib.rs"), "fn changed() {}").unwrap();
        let repo = repo_handle(&local_repo);

        let shown = GetStatusOptions::default();
        assert_eq!(repo.get_status_with(&shown).unwrap().len(), 1);
        let hidden = GetStatusOptions {
            ignore_submodules: true,
            ..Default::default()
        };
        assert!(repo.get_status_with(&hidden).unwrap().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pathspecs_are_literal_not_globs() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo
            .fs
            .create_file("a.rs", Some("fn a() {}"))
            .create_file("*.rs", Some("fn star() {}"));
        local_repo.add_all().commit("base");
        let repo = repo_handle(&local_repo);

        assert_eq!(
            repo.tracked_files(&["*.rs"]).unwrap(),
            [local_repo.path.join("*.rs")]
        );
    }

    #[test]
    fn get_status_error_includes_git_stderr() {
        let mut local_repo = LocalGitRepository::new(None);
        local_repo.fs.create_file("README.md", Some("readme"));
        local_repo.add_all().commit("base");
        std::fs::write(local_repo.path.join(".git/index"), "corrupt").unwrap();

        let error = repo_handle(&local_repo).get_status().unwrap_err();
        let message = error.to_string();
        assert!(message.starts_with("git status failed: "), "{message}");
        assert!(message.len() > "git status failed: ".len(), "{message}");
    }
}
