use std::path::Path;
use std::process::Command;

const NO_SUCH_REMOTE: i32 = 2;

pub fn remote_url(repo_path: &Path, name: &str) -> Result<Option<String>, std::io::Error> {
    let output = Command::new("git")
        .args(["remote", "get-url", "--", name])
        .current_dir(repo_path)
        .output()?;

    match output.status.code() {
        Some(0) => String::from_utf8(output.stdout)
            .map(|url| Some(url.trim().to_string()))
            .map_err(|e| std::io::Error::other(format!("remote URL is not valid UTF-8: {e}"))),
        Some(NO_SUCH_REMOTE) => Ok(None),
        _ => Err(std::io::Error::other(format!(
            "git remote get-url failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))),
    }
}
