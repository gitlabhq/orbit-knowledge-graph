//! Method A: git archive --format=tar | gzip -c -n
//! Mimics what Gitaly does today for GetArchive with TAR_GZ format.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

use flate2::read::GzDecoder;
use crate::{git, BenchError, BenchResult, Method, MethodOutput};

pub struct ArchiveMethod;

impl Method for ArchiveMethod {
    fn key(&self) -> char { 'a' }
    fn label(&self) -> &'static str { "A: archive | gzip" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

/// Run the archive method: execute git archive | gzip, then extract and hash all files.
pub fn run(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<BenchResult, crate::BenchError> {
    // Phase 1: Run git commands (simulates Gitaly server-side work)
    let cmd_start = Instant::now();

    let archive_proc = Command::new("git")
        .args(["archive", "--format=tar", commit, "--", "."])
        .current_dir(repo_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Git(format!("git archive spawn: {e}")))?;

    let gzip_proc = Command::new("gzip")
        .args(["-c", "-n"])
        .stdin(archive_proc.stdout.unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Git(format!("gzip spawn: {e}")))?;

    let gzip_output = gzip_proc
        .wait_with_output()
        .map_err(|e| crate::BenchError::Git(format!("gzip wait: {e}")))?;

    if !gzip_output.status.success() {
        return Err(crate::BenchError::Git(format!(
            "git archive | gzip failed: {}",
            String::from_utf8_lossy(&gzip_output.stderr)
        )));
    }

    let cmd_duration = cmd_start.elapsed();
    let output_bytes = gzip_output.stdout.len() as u64;

    // Phase 2: Extract (simulates GKG client-side work)
    let extract_start = Instant::now();
    let file_hashes = extract_tar_gz(&gzip_output.stdout, output_dir)?;
    let extract_duration = extract_start.elapsed();

    Ok(BenchResult {
        method: "git archive | gzip".to_string(),
        git_cmd_time: cmd_duration,
        transfer_bytes: output_bytes,
        extract_time: extract_duration,
        total_time: cmd_duration + extract_duration,
        file_count: file_hashes.len(),
        file_hashes,
    })
}

/// Extract a tar.gz stream, write files to output_dir, return sorted map of path -> sha256.
fn extract_tar_gz(data: &[u8], output_dir: &Path) -> Result<BTreeMap<String, String>, crate::BenchError> {
    let gz = GzDecoder::new(data);
    let mut archive = tar::Archive::new(gz);
    let mut file_hashes = BTreeMap::new();

    for entry in archive.entries().map_err(|e| crate::BenchError::Extract(e.to_string()))? {
        let mut entry = entry.map_err(|e| crate::BenchError::Extract(e.to_string()))?;
        let entry_type = entry.header().entry_type();

        // Skip directories, symlinks, pax headers -- only process regular files
        if !entry_type.is_file() {
            continue;
        }

        let path = entry
            .path()
            .map_err(|e| crate::BenchError::Extract(e.to_string()))?
            .to_path_buf();

        // No --prefix used, so paths are already repo-relative.
        // (When Gitaly uses --prefix=<slug>/, the first component would need stripping.)
        let stripped = path;

        // Read content and hash
        let mut content = Vec::new();
        entry
            .read_to_end(&mut content)
            .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

        let hash = git::hex_sha256(&content);

        // Write to disk
        let dest = output_dir.join(&stripped);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| crate::BenchError::Extract(e.to_string()))?;
        }
        std::fs::write(&dest, &content)
            .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

        file_hashes.insert(stripped.to_string_lossy().to_string(), hash);
    }

    Ok(file_hashes)
}


