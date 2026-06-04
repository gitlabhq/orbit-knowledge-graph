//! Method C: git rev-list --objects | git pack-objects --stdout
//! Then: git init + index-pack + read-tree + checkout-index
//! Each step is timed individually to find the bottleneck.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::{BenchError, BenchResult, Method, MethodOutput};

pub struct PackCheckoutMethod;

impl Method for PackCheckoutMethod {
    fn key(&self) -> char { 'c' }
    fn label(&self) -> &'static str { "C: pack + checkout" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

struct ExtractTimings {
    init: Duration,
    index_pack: Duration,
    read_tree: Duration,
    checkout: Duration,
    hash: Duration,
}

pub fn run(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<BenchResult, crate::BenchError> {
    let root_tree_oid = resolve_tree_oid(repo_path, commit)?;

    // Phase 1: Generate packfile (Gitaly side)
    let cmd_start = Instant::now();

    let mut rev_list = Command::new("git")
        .args(["rev-list", "--objects", "--stdin"])
        .current_dir(repo_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| crate::BenchError::Git(format!("rev-list spawn: {e}")))?;

    {
        let mut stdin = rev_list.stdin.take().unwrap();
        writeln!(stdin, "{}^{{tree}}", commit)
            .map_err(|e| crate::BenchError::Git(format!("rev-list stdin: {e}")))?;
        drop(stdin);
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

    let _ = rev_list.wait();

    if !pack_output.status.success() {
        return Err(crate::BenchError::Git(format!(
            "pack-objects failed: {}",
            String::from_utf8_lossy(&pack_output.stderr)
        )));
    }

    let cmd_duration = cmd_start.elapsed();
    let output_bytes = pack_output.stdout.len() as u64;

    // Phase 2: Extract with per-step timing
    let extract_start = Instant::now();
    let (file_hashes, timings) = extract_via_checkout(&pack_output.stdout, &root_tree_oid, output_dir)?;
    let extract_duration = extract_start.elapsed();

    println!(
        "    breakdown: init={:.2?} index-pack={:.2?} read-tree={:.2?} checkout={:.2?} hash={:.2?}",
        timings.init, timings.index_pack, timings.read_tree, timings.checkout, timings.hash
    );

    Ok(BenchResult {
        method: "pack + checkout".to_string(),
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
        return Err(crate::BenchError::Git("rev-parse failed".into()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn extract_via_checkout(
    pack_data: &[u8],
    root_tree_oid: &str,
    output_dir: &Path,
) -> Result<(BTreeMap<String, String>, ExtractTimings), crate::BenchError> {
    let git_dir = output_dir.join(".git-tmp");

    // Step 1: git init --bare
    let t = Instant::now();
    let init = Command::new("git")
        .args(["init", "--bare", "-q", git_dir.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("git init: {e}")))?;
    if !init.status.success() {
        return Err(crate::BenchError::Extract("git init failed".into()));
    }
    let init_dur = t.elapsed();

    // Step 2: Write pack + index-pack
    let t = Instant::now();
    let pack_dir = git_dir.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir)
        .map_err(|e| crate::BenchError::Extract(format!("mkdir: {e}")))?;
    let pack_path = pack_dir.join("recv.pack");
    std::fs::write(&pack_path, pack_data)
        .map_err(|e| crate::BenchError::Extract(format!("write pack: {e}")))?;

    let idx = Command::new("git")
        .args(["index-pack", pack_path.to_str().unwrap()])
        .env("GIT_DIR", &git_dir)
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("index-pack: {e}")))?;
    if !idx.status.success() {
        return Err(crate::BenchError::Extract(format!(
            "index-pack failed: {}",
            String::from_utf8_lossy(&idx.stderr)
        )));
    }
    let index_pack_dur = t.elapsed();

    // Step 3: read-tree
    let t = Instant::now();
    let rt = Command::new("git")
        .args(["read-tree", root_tree_oid])
        .env("GIT_DIR", &git_dir)
        .env("GIT_INDEX_FILE", git_dir.join("index"))
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("read-tree: {e}")))?;
    if !rt.status.success() {
        return Err(crate::BenchError::Extract(format!(
            "read-tree failed: {}",
            String::from_utf8_lossy(&rt.stderr)
        )));
    }
    let read_tree_dur = t.elapsed();

    // Step 4: checkout-index
    let t = Instant::now();
    let co = Command::new("git")
        .args(["checkout-index", "--all", "--force"])
        .env("GIT_DIR", &git_dir)
        .env("GIT_WORK_TREE", output_dir)
        .env("GIT_INDEX_FILE", git_dir.join("index"))
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("checkout-index: {e}")))?;
    if !co.status.success() {
        return Err(crate::BenchError::Extract(format!(
            "checkout-index failed: {}",
            String::from_utf8_lossy(&co.stderr)
        )));
    }
    let checkout_dur = t.elapsed();

    // Step 5: Hash files for correctness
    let t = Instant::now();
    std::fs::remove_dir_all(&git_dir).ok();
    let file_hashes = hash_directory(output_dir)?;
    let hash_dur = t.elapsed();

    Ok((
        file_hashes,
        ExtractTimings {
            init: init_dur,
            index_pack: index_pack_dur,
            read_tree: read_tree_dur,
            checkout: checkout_dur,
            hash: hash_dur,
        },
    ))
}

fn hash_directory(dir: &Path) -> Result<BTreeMap<String, String>, crate::BenchError> {
    let mut file_hashes = BTreeMap::new();
    walk_dir(dir, dir, &mut file_hashes)?;
    Ok(file_hashes)
}

fn walk_dir(base: &Path, dir: &Path, hashes: &mut BTreeMap<String, String>) -> Result<(), crate::BenchError> {
    let entries = std::fs::read_dir(dir).map_err(|e| crate::BenchError::Extract(e.to_string()))?;
    for entry in entries {
        let entry = entry.map_err(|e| crate::BenchError::Extract(e.to_string()))?;
        let path = entry.path();
        let ft = entry.file_type().map_err(|e| crate::BenchError::Extract(e.to_string()))?;
        if ft.is_dir() {
            walk_dir(base, &path, hashes)?;
        } else if ft.is_file() {
            let content = std::fs::read(&path).map_err(|e| crate::BenchError::Extract(e.to_string()))?;
            let rel = path.strip_prefix(base).unwrap().to_string_lossy().to_string();
            hashes.insert(rel, hex_sha256(&content));
        }
    }
    Ok(())
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}
