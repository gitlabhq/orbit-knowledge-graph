//! Method D: rev-list | pack-objects, then ls-tree + cat-file --batch
//! with GKG's actual filter applied -- only write blobs that pass.
//! Skipped blobs still get an inventory entry (path + size) but no disk I/O.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::{format_bytes, git, BenchError, BenchResult, Method, MethodOutput};

pub struct FilteredMethod;

impl Method for FilteredMethod {
    fn key(&self) -> char { 'd' }
    fn label(&self) -> &'static str { "D: pack + filtered" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run(repo, commit, out).map(|fr| MethodOutput {
            detail: Some(format!(
                "    filter: wrote={} skipped={} ({} skipped)  ls-tree={:.2?} cat-file={:.2?}",
                fr.files_written, fr.files_skipped,
                format_bytes(fr.bytes_skipped),
                fr.ls_tree_time, fr.cat_file_time,
            )),
            result: fr.bench,
        })
    }
}

use crate::filter::{self, MAX_FILE_SIZE};

pub struct FilteredResult {
    pub bench: BenchResult,
    pub files_written: usize,
    pub files_skipped: usize,
    pub bytes_skipped: u64,
    pub ls_tree_time: Duration,
    pub cat_file_time: Duration,
}

pub fn run(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<FilteredResult, crate::BenchError> {
    let root_tree_oid = git::resolve_tree_oid(repo_path, commit)?;

    // Phase 1: Generate packfile (Gitaly side)
    let (_pack_data, cmd_duration, _root) = git::generate_packfile_stdout(repo_path, commit, &[])?;
    let output_bytes = _pack_data.len() as u64;

    // Phase 2: Filtered extraction
    let extract_start = Instant::now();

    // Step 1: ls-tree -rl (recursive, long format -- includes size)
    let t = Instant::now();
    let ls_output = Command::new("git")
        .args(["ls-tree", "-rl", &root_tree_oid])
        .current_dir(repo_path)
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("ls-tree: {e}")))?;

    if !ls_output.status.success() {
        return Err(crate::BenchError::Extract("ls-tree failed".into()));
    }

    // Parse entries: "<mode> blob <oid>    <size>\t<path>"
    let listing = String::from_utf8_lossy(&ls_output.stdout);
    let mut all_entries: Vec<(String, String, u64)> = Vec::new(); // (oid, path, size)
    let mut write_entries: Vec<(String, String)> = Vec::new(); // (oid, path) -- only those passing filter

    let mut files_skipped = 0usize;
    let mut bytes_skipped = 0u64;

    for line in listing.lines() {
        let Some((meta, path)) = line.split_once('\t') else { continue };
        let parts: Vec<&str> = meta.split_whitespace().collect();
        if parts.len() < 4 || parts[1] != "blob" { continue }

        let oid = parts[2].to_string();
        let size: u64 = parts[3].parse().unwrap_or(0);

        all_entries.push((oid.clone(), path.to_string(), size));

        if size > MAX_FILE_SIZE || filter::is_excluded(path) {
            files_skipped += 1;
            bytes_skipped += size;
        } else {
            write_entries.push((oid, path.to_string()));
        }
    }

    let ls_tree_time = t.elapsed();

    // Step 2: Only cat-file the blobs we need
    let t = Instant::now();

    let mut cat_file = Command::new("git")
        .args(["cat-file", "--batch"])
        .current_dir(repo_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Extract(format!("cat-file spawn: {e}")))?;

    let mut cat_stdin = cat_file.stdin.take().unwrap();
    let cat_stdout = cat_file.stdout.take().unwrap();

    let oids: Vec<String> = write_entries.iter().map(|(oid, _)| oid.clone()).collect();
    let writer_thread = std::thread::spawn(move || {
        for oid in &oids {
            if writeln!(cat_stdin, "{}", oid).is_err() { break }
        }
        drop(cat_stdin);
    });

    let mut reader = BufReader::with_capacity(256 * 1024, cat_stdout);
    let mut file_hashes = BTreeMap::new();

    for (_oid, path) in &write_entries {
        let mut header = String::new();
        reader.read_line(&mut header)
            .map_err(|e| crate::BenchError::Extract(format!("cat-file header: {e}")))?;

        let header = header.trim();
        if header.ends_with("missing") { continue }

        let size: usize = header
            .rsplit_once(' ')
            .and_then(|(_, s)| s.parse().ok())
            .ok_or_else(|| crate::BenchError::Extract(format!("bad header: {header}")))?;

        let mut content = vec![0u8; size];
        reader.read_exact(&mut content)
            .map_err(|e| crate::BenchError::Extract(format!("cat-file read: {e}")))?;

        // trailing newline
        let mut nl = [0u8; 1];
        let _ = reader.read_exact(&mut nl);

        let hash = git::hex_sha256(&content);

        let dest = output_dir.join(path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| crate::BenchError::Extract(e.to_string()))?;
        }
        std::fs::write(&dest, &content)
            .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

        file_hashes.insert(path.clone(), hash);
    }

    writer_thread.join().expect("writer thread");
    let _ = cat_file.wait();

    let cat_file_time = t.elapsed();
    let extract_duration = extract_start.elapsed();

    let files_written = file_hashes.len();

    Ok(FilteredResult {
        bench: BenchResult {
            method: "pack + filtered".to_string(),
            git_cmd_time: cmd_duration,
            transfer_bytes: output_bytes,
            extract_time: extract_duration,
            total_time: cmd_duration + extract_duration,
            file_count: all_entries.len(),
            file_hashes,
        },
        files_written,
        files_skipped,
        bytes_skipped,
        ls_tree_time,
        cat_file_time,
    })
}


