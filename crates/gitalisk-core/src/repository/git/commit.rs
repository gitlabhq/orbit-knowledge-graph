use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone)]
pub struct CommitInfo {
    pub hash: String,
    pub message: String,
    pub author_name: Option<String>,
    pub author_email: Option<String>,
}

pub fn get_current_commit_hash(repo_path: &Path) -> Result<String, std::io::Error> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()?;

    if output.status.success() {
        let commit_hash = String::from_utf8(output.stdout)
            .map_err(|e| {
                std::io::Error::other(format!("Invalid UTF-8 in git rev-parse output: {e}"))
            })?
            .trim()
            .to_string();

        if commit_hash.is_empty() {
            return Err(std::io::Error::other("Empty commit hash returned"));
        }

        Ok(commit_hash)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(std::io::Error::other(format!(
            "Git command failed: {stderr}"
        )))
    }
}

/// Get commit info for multiple commits in a single git command for better performance
pub fn get_commits_info(
    repo_path: &Path,
    commit_hashes: &[&str],
) -> Result<Vec<CommitInfo>, std::io::Error> {
    if commit_hashes.is_empty() {
        return Ok(Vec::new());
    }

    // Use ASCII Unit Separator (0x1f) as delimiter - safer than pipe for commit messages
    const DELIMITER: &str = "\x1f";
    let format = format!("%H{DELIMITER}%s{DELIMITER}%an{DELIMITER}%ae");

    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(repo_path)
        .arg("show")
        .arg("-s")
        .arg(format!("--format={format}"));

    for hash in commit_hashes {
        cmd.arg(hash);
    }

    let output = cmd.output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(std::io::Error::other(format!(
            "Git command failed: {stderr}"
        )));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|e| std::io::Error::other(format!("Invalid UTF-8 in git show output: {e}")))?;

    let mut results = Vec::with_capacity(commit_hashes.len());

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split(DELIMITER).collect();

        if parts.len() != 4 {
            continue;
        }

        let hash = parts[0].trim();
        let message = parts[1].trim();
        let author_name_part = parts[2].trim();
        let author_email_part = parts[3].trim();

        let author_name = if author_name_part.is_empty() {
            None
        } else {
            Some(author_name_part.to_string())
        };

        let author_email = if author_email_part.is_empty() {
            None
        } else {
            Some(author_email_part.to_string())
        };

        results.push(CommitInfo {
            hash: hash.to_string(),
            message: message.to_string(),
            author_name,
            author_email,
        });
    }

    Ok(results)
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

    fn create_commit(
        repo: &mut LocalGitRepository,
        filename: &str,
        content: &str,
        message: &str,
    ) -> std::io::Result<String> {
        repo.fs.create_file(filename, Some(content));
        repo.add_all();
        repo.commit(message);

        let output = Command::new("git")
            .arg("-C")
            .arg(repo.path.as_path())
            .arg("rev-parse")
            .arg("HEAD")
            .output()?;

        Ok(String::from_utf8(output.stdout).unwrap().trim().to_string())
    }

    #[test]
    fn test_get_current_commit_hash() {
        let mut repo = init_test_repo();
        create_commit(&mut repo, "test.txt", "initial content", "Initial commit").unwrap();

        let result = get_current_commit_hash(repo.path.as_path());
        assert!(result.is_ok());
        let commit_hash = result.unwrap();
        assert_eq!(commit_hash.len(), 40); // Full SHA-1 hash
        assert!(commit_hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_get_current_commit_hash_no_repo() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        let result = get_current_commit_hash(repo_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_commits_info_single() {
        let mut repo = init_test_repo();

        let commit_hash =
            create_commit(&mut repo, "test.txt", "initial content", "Initial commit").unwrap();

        let result = get_commits_info(repo.path.as_path(), &[&commit_hash]);
        assert!(result.is_ok());
        let commits = result.unwrap();
        assert_eq!(commits.len(), 1);

        let commit = &commits[0];
        assert_eq!(commit.hash, commit_hash);
        assert_eq!(commit.message, "Initial commit");
        assert_eq!(commit.author_name, Some("test-gl-user".to_string()));
        assert_eq!(
            commit.author_email,
            Some("test-gl-user@gitlab.com".to_string())
        );
    }

    #[test]
    fn test_get_commits_info_multiple() {
        let mut repo = init_test_repo();

        let hash1 = create_commit(&mut repo, "test1.txt", "content 1", "First commit").unwrap();
        let hash2 = create_commit(&mut repo, "test2.txt", "content 2", "Second commit").unwrap();
        let hash3 = create_commit(&mut repo, "test3.txt", "content 3", "Third commit").unwrap();

        let result = get_commits_info(repo.path.as_path(), &[&hash1, &hash2, &hash3]);
        assert!(result.is_ok());
        let commits = result.unwrap();
        assert_eq!(commits.len(), 3);

        assert_eq!(commits[0].hash, hash1);
        assert_eq!(commits[0].message, "First commit");

        assert_eq!(commits[1].hash, hash2);
        assert_eq!(commits[1].message, "Second commit");

        assert_eq!(commits[2].hash, hash3);
        assert_eq!(commits[2].message, "Third commit");
    }

    #[test]
    fn test_get_commits_info_empty() {
        let mut repo = init_test_repo();
        create_commit(&mut repo, "test.txt", "content", "Initial commit").unwrap();

        let result = get_commits_info(repo.path.as_path(), &[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_get_commits_info_invalid_hash() {
        let mut repo = init_test_repo();
        create_commit(&mut repo, "test.txt", "content", "Initial commit").unwrap();

        let result = get_commits_info(repo.path.as_path(), &["invalid_hash"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_commits_with_special_characters() {
        let mut repo = init_test_repo();

        let special_message = "feat: add special chars éñ español 🚀";
        let hash = create_commit(&mut repo, "test.txt", "test content", special_message).unwrap();

        let result = get_commits_info(repo.path.as_path(), &[&hash]);
        assert!(result.is_ok());
        let commits = result.unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].message, special_message);
    }

    #[test]
    fn test_commits_with_pipe_in_message() {
        let mut repo = init_test_repo();
        // Test message with pipe character - should be preserved with new delimiter
        let message_with_pipe = "fix: update parser | add tests";
        let hash = create_commit(&mut repo, "test.txt", "test content", message_with_pipe).unwrap();

        let result = get_commits_info(repo.path.as_path(), &[&hash]);
        assert!(result.is_ok());
        let commits = result.unwrap();
        assert_eq!(commits.len(), 1);
        // The message should be preserved completely with our new delimiter
        assert_eq!(commits[0].message, message_with_pipe);
    }

    #[test]
    fn multiple_commits() {
        let mut repo = init_test_repo();

        let mut hashes = Vec::new();
        for i in 1..=10 {
            let hash = create_commit(
                &mut repo,
                &format!("file{i}.txt"),
                &format!("content {i}"),
                &format!("Commit {i}"),
            )
            .unwrap();
            hashes.push(hash);
        }

        let hash_refs: Vec<&str> = hashes.iter().map(|h| h.as_str()).collect();

        let result = get_commits_info(repo.path.as_path(), &hash_refs);

        assert!(result.is_ok());
        let commits = result.unwrap();
        assert_eq!(commits.len(), 10);

        for (i, commit) in commits.iter().enumerate() {
            assert_eq!(commit.hash, hashes[i]);
            assert_eq!(commit.message, format!("Commit {}", i + 1));
            assert_eq!(commit.author_name, Some("test-gl-user".to_string()));
            assert_eq!(
                commit.author_email,
                Some("test-gl-user@gitlab.com".to_string())
            );
        }
    }
}
