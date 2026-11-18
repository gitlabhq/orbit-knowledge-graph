use std::path::{Path, PathBuf};
use std::process::Command;

/// Returns `true` if the repository at `repo_path` is a git worktree
/// (`.git` is a file pointing to the main repo) rather than a regular
/// repository (`.git` is a directory).
pub fn is_worktree(repo_path: &Path) -> bool {
    repo_path.join(".git").is_file()
}

/// Resolve the root path of the repository this checkout belongs to.
///
/// For worktrees, follows `git rev-parse --git-common-dir` back to the
/// shared `.git` directory and returns its parent (the origin repo root).
/// For regular repos, returns `repo_path` itself. The result is
/// canonicalized.
/// The root of the working tree that contains `path`: this checkout's own
/// root, which for a worktree differs from `parent_repo_path`.
pub fn toplevel(path: &Path) -> Result<PathBuf, std::io::Error> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(std::io::Error::other(format!(
            "git rev-parse --show-toplevel failed: {}",
            stderr.trim()
        )));
    }
    let top = std::str::from_utf8(&output.stdout)
        .map_err(|e| std::io::Error::other(format!("git output is not valid UTF-8: {e}")))?
        .trim();
    dunce::canonicalize(top)
}

/// Whether the repository's ignore rules exclude `path`.
pub fn is_ignored(repo_path: &Path, path: &Path) -> bool {
    Command::new("git")
        .args(["check-ignore", "--quiet"])
        .arg(path)
        .current_dir(repo_path)
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub fn parent_repo_path(repo_path: &Path) -> Result<PathBuf, std::io::Error> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
        .current_dir(repo_path)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(std::io::Error::other(format!(
            "git rev-parse --git-common-dir failed: {stderr}"
        )));
    }

    let common_dir = std::str::from_utf8(&output.stdout)
        .map_err(|e| std::io::Error::other(format!("git output is not valid UTF-8: {e}")))?
        .trim()
        .to_string();

    // Regular repos: git returns ".git" (relative) on all platforms.
    // Worktrees: git returns an absolute path (Unix: "/home/user/repo/.git",
    // Windows: "C:/Users/user/repo/.git"). Path::is_absolute() handles
    // both formats. dunce::canonicalize at the end normalizes the result.
    let git_dir = if Path::new(&common_dir).is_absolute() {
        PathBuf::from(&common_dir)
    } else {
        repo_path.join(&common_dir)
    };

    // The repo root is the parent of the .git directory.
    let root = git_dir
        .parent()
        .ok_or_else(|| std::io::Error::other("git common dir has no parent"))?;

    dunce::canonicalize(root)
}

#[cfg(test)]
mod tests {
    #[test]
    fn toplevel_is_the_checkout_root_from_any_directory_inside() {
        let mut repo = crate::repository::testing::local::LocalGitRepository::new(None);
        repo.fs.create_file("src/deep/file.rs", Some("fn f() {}"));
        repo.add_all().commit("base");
        let expected = dunce::canonicalize(&repo.path).unwrap();
        assert_eq!(
            super::toplevel(&repo.path.join("src/deep")).unwrap(),
            expected
        );
    }

    #[test]
    fn is_ignored_follows_gitignore() {
        let mut repo = crate::repository::testing::local::LocalGitRepository::new(None);
        repo.fs
            .create_file(".gitignore", Some("vendor/\n"))
            .create_file("src/kept.rs", Some("fn k() {}"));
        repo.add_all().commit("base");
        assert!(super::is_ignored(&repo.path, &repo.path.join("vendor/lib")));
        assert!(!super::is_ignored(
            &repo.path,
            &repo.path.join("src/kept.rs")
        ));
    }

    use super::*;
    use crate::repository::testing::local::LocalGitRepository;

    fn repo_with_commit() -> LocalGitRepository {
        let mut repo = LocalGitRepository::new(None);
        repo.fs.create_file("test.txt", Some("content"));
        repo.add_all().commit("initial");
        repo
    }

    fn add_worktree(repo: &LocalGitRepository, branch: &str) -> PathBuf {
        let wt_path = repo.workspace_path.join(format!("wt-{branch}"));
        let output = Command::new("git")
            .args(["worktree", "add", "-b", branch, wt_path.to_str().unwrap()])
            .current_dir(&repo.path)
            .output()
            .unwrap();
        assert!(output.status.success(), "git worktree add failed");
        wt_path
    }

    // ── is_worktree ─────────────────────────────────────────────

    #[test]
    fn regular_repo_is_not_worktree() {
        let repo = repo_with_commit();
        assert!(!is_worktree(&repo.path));
    }

    #[test]
    fn worktree_is_detected() {
        let repo = repo_with_commit();
        let wt = add_worktree(&repo, "feature");
        assert!(is_worktree(&wt));
    }

    #[test]
    fn non_git_dir_is_not_worktree() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(!is_worktree(tmp.path()));
    }

    // ── parent_repo_path ──────────────────────────────────────────

    #[test]
    fn regular_repo_returns_itself() {
        let repo = repo_with_commit();
        let result = parent_repo_path(&repo.path).unwrap();
        assert_eq!(result, dunce::canonicalize(&repo.path).unwrap());
    }

    #[test]
    fn worktree_returns_main_repo() {
        let repo = repo_with_commit();
        let wt = add_worktree(&repo, "wt-branch");

        let result = parent_repo_path(&wt).unwrap();
        assert_eq!(result, dunce::canonicalize(&repo.path).unwrap());
    }

    #[test]
    fn multiple_worktrees_all_resolve_to_same_main() {
        let repo = repo_with_commit();
        let wt1 = add_worktree(&repo, "branch-a");
        let wt2 = add_worktree(&repo, "branch-b");
        let wt3 = add_worktree(&repo, "branch-c");

        let main = dunce::canonicalize(&repo.path).unwrap();
        assert_eq!(parent_repo_path(&wt1).unwrap(), main);
        assert_eq!(parent_repo_path(&wt2).unwrap(), main);
        assert_eq!(parent_repo_path(&wt3).unwrap(), main);

        // And the main repo also returns itself.
        assert_eq!(parent_repo_path(&repo.path).unwrap(), main);
    }

    #[test]
    fn not_git_errors() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(parent_repo_path(tmp.path()).is_err());
    }

    // ── consistency with branch/commit ──────────────────────────

    #[test]
    fn worktree_has_different_branch_than_main() {
        let repo = repo_with_commit();
        let wt = add_worktree(&repo, "other-branch");

        let main_branch =
            crate::repository::git::current_branch::get_current_branch(repo.path.to_str().unwrap())
                .unwrap();
        let wt_branch =
            crate::repository::git::current_branch::get_current_branch(wt.to_str().unwrap())
                .unwrap();

        assert_ne!(main_branch, wt_branch);
        assert_eq!(wt_branch, "other-branch");
    }

    #[test]
    fn worktree_shares_commit_history_with_main() {
        let repo = repo_with_commit();
        let wt = add_worktree(&repo, "shared-history");

        let main_sha = crate::repository::git::commit::get_current_commit_hash(&repo.path).unwrap();
        let wt_sha = crate::repository::git::commit::get_current_commit_hash(&wt).unwrap();

        // Worktree starts at the same commit as main.
        assert_eq!(main_sha, wt_sha);

        // After a commit in the worktree, they diverge.
        std::fs::write(wt.join("new.txt"), "worktree file").unwrap();
        let add_out = Command::new("git")
            .args(["add", "."])
            .current_dir(&wt)
            .output()
            .unwrap();
        assert!(add_out.status.success(), "git add failed");
        let commit_out = Command::new("git")
            .args(["commit", "-m", "wt commit"])
            .current_dir(&wt)
            .output()
            .unwrap();
        assert!(commit_out.status.success(), "git commit failed");

        let wt_sha_after = crate::repository::git::commit::get_current_commit_hash(&wt).unwrap();
        assert_ne!(main_sha, wt_sha_after);

        // But parent_repo_path still resolves to the same root.
        assert_eq!(
            parent_repo_path(&wt).unwrap(),
            dunce::canonicalize(&repo.path).unwrap()
        );
    }

    #[test]
    fn worktree_with_nested_path() {
        let repo = repo_with_commit();
        let nested = repo.workspace_path.join("deep/nested/wt");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();

        let output = Command::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                "nested-branch",
                nested.to_str().unwrap(),
            ])
            .current_dir(&repo.path)
            .output()
            .unwrap();
        assert!(output.status.success());

        assert!(is_worktree(&nested));
        assert_eq!(
            parent_repo_path(&nested).unwrap(),
            dunce::canonicalize(&repo.path).unwrap()
        );
    }
}
