//! Method B: git rev-list --objects | git pack-objects --stdout
//! Then extract via git ls-tree -r + git cat-file --batch (single process each).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

use crate::{git, BenchError, BenchResult, Method, MethodOutput};

pub struct PackCatfileMethod;

impl Method for PackCatfileMethod {
    fn key(&self) -> char { 'b' }
    fn label(&self) -> &'static str { "B: pack + cat-file" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

pub fn run(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<BenchResult, BenchError> {
    let (_pack_data, cmd_duration, root_tree_oid) =
        git::generate_packfile_stdout(repo_path, commit, &[])?;
    let output_bytes = _pack_data.len() as u64;

    let extract_start = Instant::now();
    let file_hashes = extract_tree_fast(repo_path, &root_tree_oid, output_dir)?;
    let extract_duration = extract_start.elapsed();

    Ok(BenchResult {
        method: "rev-list | pack-objects".to_string(),
        git_cmd_time: cmd_duration,
        transfer_bytes: output_bytes,
        extract_time: extract_duration,
        total_time: cmd_duration + extract_duration,
        file_count: file_hashes.len(),
        file_hashes,
    })
}

/// Single `git ls-tree -r` + single `git cat-file --batch` to extract all blobs.
fn extract_tree_fast(
    repo_path: &Path, root_tree_oid: &str, output_dir: &Path,
) -> Result<BTreeMap<String, String>, BenchError> {
    let ls_output = Command::new("git")
        .args(["ls-tree", "-r", root_tree_oid])
        .current_dir(repo_path)
        .output()
        .map_err(|e| BenchError::Extract(format!("ls-tree: {e}")))?;

    if !ls_output.status.success() {
        return Err(BenchError::Extract("ls-tree -r failed".into()));
    }

    let listing = String::from_utf8_lossy(&ls_output.stdout);
    let mut entries: Vec<(String, String)> = Vec::new();

    for line in listing.lines() {
        let Some((meta, path)) = line.split_once('\t') else { continue };
        let parts: Vec<&str> = meta.split_whitespace().collect();
        if parts.len() < 3 || parts[1] != "blob" { continue }
        entries.push((parts[2].to_string(), path.to_string()));
    }

    let mut cat_file = Command::new("git")
        .args(["cat-file", "--batch"])
        .current_dir(repo_path)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn()
        .map_err(|e| BenchError::Extract(format!("cat-file spawn: {e}")))?;

    let mut cat_stdin = cat_file.stdin.take().unwrap();
    let cat_stdout = cat_file.stdout.take().unwrap();

    let oids: Vec<String> = entries.iter().map(|(oid, _)| oid.clone()).collect();
    let writer_thread = std::thread::spawn(move || {
        for oid in &oids {
            if writeln!(cat_stdin, "{}", oid).is_err() { break }
        }
        drop(cat_stdin);
    });

    let mut reader = BufReader::new(cat_stdout);
    let mut file_hashes = BTreeMap::new();

    for (_oid, path) in &entries {
        let mut header = String::new();
        reader.read_line(&mut header)
            .map_err(|e| BenchError::Extract(format!("cat-file header: {e}")))?;

        let header = header.trim();
        if header.ends_with("missing") { continue }

        let size: usize = header.rsplit_once(' ')
            .and_then(|(_, s)| s.parse().ok())
            .ok_or_else(|| BenchError::Extract(format!("bad header: {header}")))?;

        let mut content = vec![0u8; size];
        reader.read_exact(&mut content)
            .map_err(|e| BenchError::Extract(format!("cat-file read: {e}")))?;

        let mut nl = [0u8; 1];
        let _ = reader.read_exact(&mut nl);

        let dest = output_dir.join(path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| BenchError::Extract(e.to_string()))?;
        }
        std::fs::write(&dest, &content).map_err(|e| BenchError::Extract(e.to_string()))?;

        file_hashes.insert(path.clone(), git::hex_sha256(&content));
    }

    writer_thread.join().expect("writer thread");
    let _ = cat_file.wait();
    Ok(file_hashes)
}
