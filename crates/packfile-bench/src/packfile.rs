//! Method B: git rev-list --objects | git pack-objects --stdout
//! Then parse the packfile with git cat-file --batch (single process).
//! This is what we'd do with the proposed GetTreePackfile RPC.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::{BenchError, BenchResult, Method, MethodOutput};

pub struct PackCatfileMethod;

impl Method for PackCatfileMethod {
    fn key(&self) -> char { 'b' }
    fn label(&self) -> &'static str { "B: pack + cat-file" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

/// Run the packfile method: execute rev-list | pack-objects, then parse and extract.
pub fn run(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<BenchResult, crate::BenchError> {
    // First resolve the root tree OID (Gitaly would do this too)
    let root_tree_oid = resolve_tree_oid(repo_path, commit)?;

    // Phase 1: Run git commands (simulates Gitaly server-side work)
    let cmd_start = Instant::now();

    let mut rev_list = Command::new("git")
        .args(["rev-list", "--objects", "--stdin"])
        .current_dir(repo_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Git(format!("rev-list spawn: {e}")))?;

    // Write the tree ref to rev-list's stdin, then close it
    {
        let stdin = rev_list.stdin.take().unwrap();
        let mut stdin = stdin;
        writeln!(stdin, "{}^{{tree}}", commit)
            .map_err(|e| crate::BenchError::Git(format!("rev-list stdin write: {e}")))?;
        drop(stdin); // close stdin so rev-list can finish
    }

    let pack_objects = Command::new("git")
        .args(["pack-objects", "--stdout", "-q", "--delta-base-offset"])
        .current_dir(repo_path)
        .stdin(rev_list.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Git(format!("pack-objects spawn: {e}")))?;

    let pack_output = pack_objects
        .wait_with_output()
        .map_err(|e| crate::BenchError::Git(format!("pack-objects wait: {e}")))?;

    // Also wait for rev-list to avoid zombie
    let _ = rev_list.wait();

    if !pack_output.status.success() {
        return Err(crate::BenchError::Git(format!(
            "pack-objects failed: {}",
            String::from_utf8_lossy(&pack_output.stderr)
        )));
    }

    let cmd_duration = cmd_start.elapsed();
    let output_bytes = pack_output.stdout.len() as u64;

    // Phase 2: Extract files from the tree (simulates GKG client-side work)
    // We use `git ls-tree -r` (single call) + `git cat-file --batch` (single process)
    // to avoid per-file subprocess overhead.
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

fn resolve_tree_oid(repo_path: &Path, commit: &str) -> Result<String, crate::BenchError> {
    let output = Command::new("git")
        .args(["rev-parse", &format!("{commit}^{{tree}}")])
        .current_dir(repo_path)
        .output()
        .map_err(|e| crate::BenchError::Git(format!("rev-parse: {e}")))?;

    if !output.status.success() {
        return Err(crate::BenchError::Git(format!(
            "rev-parse failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Fast extraction: single `git ls-tree -r` to get all blob paths+OIDs,
/// then single `git cat-file --batch` to read all blob contents.
fn extract_tree_fast(
    repo_path: &Path,
    root_tree_oid: &str,
    output_dir: &Path,
) -> Result<BTreeMap<String, String>, crate::BenchError> {
    // Step 1: Get full recursive tree listing in one call
    let ls_output = Command::new("git")
        .args(["ls-tree", "-r", root_tree_oid])
        .current_dir(repo_path)
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("ls-tree: {e}")))?;

    if !ls_output.status.success() {
        return Err(crate::BenchError::Extract(format!(
            "ls-tree -r failed: {}",
            String::from_utf8_lossy(&ls_output.stderr)
        )));
    }

    // Parse entries: "<mode> blob <oid>\t<path>"
    let listing = String::from_utf8_lossy(&ls_output.stdout);
    let mut entries: Vec<(String, String)> = Vec::new(); // (oid, path)

    for line in listing.lines() {
        let Some((meta, path)) = line.split_once('\t') else {
            continue;
        };
        let parts: Vec<&str> = meta.split_whitespace().collect();
        if parts.len() < 3 || parts[1] != "blob" {
            continue;
        }
        entries.push((parts[2].to_string(), path.to_string()));
    }

    // Step 2: Read all blobs via a single `git cat-file --batch` process
    let mut cat_file = Command::new("git")
        .args(["cat-file", "--batch"])
        .current_dir(repo_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Extract(format!("cat-file --batch spawn: {e}")))?;

    let mut cat_stdin = cat_file.stdin.take().unwrap();
    let cat_stdout = cat_file.stdout.take().unwrap();

    // Feed all OIDs to cat-file in a background thread, read results on main thread
    let oids: Vec<String> = entries.iter().map(|(oid, _)| oid.clone()).collect();
    let writer_thread = std::thread::spawn(move || {
        for oid in &oids {
            if writeln!(cat_stdin, "{}", oid).is_err() {
                break;
            }
        }
        drop(cat_stdin); // close stdin when done
    });

    let mut reader = BufReader::new(cat_stdout);
    let mut file_hashes = BTreeMap::new();

    for (_oid, path) in &entries {
        // Read header line: "<oid> <type> <size>\n"
        let mut header = String::new();
        reader
            .read_line(&mut header)
            .map_err(|e| crate::BenchError::Extract(format!("cat-file header read: {e}")))?;

        let header = header.trim();
        if header.ends_with("missing") {
            continue;
        }

        let size: usize = header
            .rsplit_once(' ')
            .and_then(|(_, s)| s.parse().ok())
            .ok_or_else(|| {
                crate::BenchError::Extract(format!("bad cat-file header: {header}"))
            })?;

        // Read exactly `size` bytes of content
        let mut content = vec![0u8; size];
        reader
            .read_exact(&mut content)
            .map_err(|e| crate::BenchError::Extract(format!("cat-file content read: {e}")))?;

        // Read trailing newline
        let mut newline = [0u8; 1];
        let _ = reader.read_exact(&mut newline);

        let hash = hex_sha256(&content);

        // Write to disk
        let dest = output_dir.join(path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| crate::BenchError::Extract(e.to_string()))?;
        }
        std::fs::write(&dest, &content)
            .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

        file_hashes.insert(path.clone(), hash);
    }

    writer_thread.join().expect("writer thread panicked");
    let _ = cat_file.wait();

    Ok(file_hashes)
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}
