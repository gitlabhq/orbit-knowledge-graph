use std::process::Command;

pub fn get_current_branch(repo_path: &str) -> Result<String, std::io::Error> {
    if let Some(branch_name) = get_current_branch_name(repo_path)? {
        return Ok(branch_name);
    }

    let rev_parse_output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(repo_path)
        .output()?;

    if rev_parse_output.status.success() {
        let commit_hash = String::from_utf8_lossy(&rev_parse_output.stdout)
            .trim()
            .to_string();

        if commit_hash.is_empty() {
            return Err(std::io::Error::other(
                "Unable to determine current branch or commit",
            ));
        }

        return Ok(format!("HEAD detached at {commit_hash}"));
    }

    Err(std::io::Error::other("Failed to get current branch"))
}

pub fn get_current_branch_name(repo_path: &str) -> Result<Option<String>, std::io::Error> {
    let output = Command::new("git")
        .args(["symbolic-ref", "--short", "-q", "HEAD"])
        .current_dir(repo_path)
        .output()?;

    match output.status.code() {
        Some(0) => String::from_utf8(output.stdout)
            .map(|name| Some(name.trim().to_string()))
            .map_err(|e| std::io::Error::other(format!("branch name is not valid UTF-8: {e}"))),
        Some(1) => Ok(None),
        _ => Err(std::io::Error::other(format!(
            "git symbolic-ref failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::testing::local::LocalGitRepository;
    use std::process::Command;
    use tempfile::TempDir;

    fn init_test_repo() -> LocalGitRepository {
        LocalGitRepository::new(None)
    }

    fn create_initial_commit(repo: &mut LocalGitRepository) -> std::io::Result<()> {
        repo.fs.create_file("test.txt", Some("initial content"));
        repo.add_all().commit("Initial commit");
        Ok(())
    }

    #[test]
    fn test_get_current_branch_not_git_repo() {
        let temp_dir = TempDir::new().unwrap();
        let error = get_current_branch(temp_dir.path().to_str().unwrap()).unwrap_err();
        assert!(error.to_string().contains("git symbolic-ref failed"));
    }

    #[test]
    fn test_get_current_branch_main_branch() {
        let mut repo = init_test_repo();
        create_initial_commit(&mut repo).unwrap();

        let result = get_current_branch(repo.path.to_str().unwrap());
        assert!(result.is_ok());
        let branch = result.unwrap();
        assert!(branch == "main" || branch == "master");
    }

    #[test]
    fn test_get_current_branch_custom_branch() {
        let mut repo = init_test_repo();
        create_initial_commit(&mut repo).unwrap();

        Command::new("git")
            .args(["checkout", "-b", "feature/test-branch"])
            .current_dir(repo.path.as_path())
            .output()
            .unwrap();

        let result = get_current_branch(repo.path.to_str().unwrap());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "feature/test-branch");
    }

    #[test]
    fn test_get_current_branch_detached_head() {
        let mut repo = init_test_repo();
        create_initial_commit(&mut repo).unwrap();

        let commit_output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo.path.as_path())
            .output()
            .unwrap();
        let commit_hash = String::from_utf8_lossy(&commit_output.stdout)
            .trim()
            .to_string();

        Command::new("git")
            .args(["checkout", &commit_hash])
            .current_dir(repo.path.as_path())
            .output()
            .unwrap();

        let result = get_current_branch(repo.path.to_str().unwrap());
        assert!(result.is_ok());
        let branch_info = result.unwrap();
        assert!(branch_info.starts_with("HEAD detached at "));
    }

    #[test]
    fn test_get_current_branch_new_repo_no_commits() {
        let repo = init_test_repo();

        let result = get_current_branch(repo.path.to_str().unwrap());
        assert!(result.is_ok());
        let branch = result.unwrap();
        assert!(branch == "main" || branch == "master");
    }

    #[test]
    fn test_get_current_branch_branch_with_special_characters() {
        let mut repo = init_test_repo();
        create_initial_commit(&mut repo).unwrap();

        let branch_name = "feature/fix-123_test-branch";
        Command::new("git")
            .args(["checkout", "-b", branch_name])
            .current_dir(repo.path.as_path())
            .output()
            .unwrap();

        let result = get_current_branch(repo.path.to_str().unwrap());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), branch_name);
    }
}
