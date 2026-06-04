//! Method C: pack-objects --stdout + git init + index-pack + read-tree + checkout-index

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::{git, BenchError, BenchResult, Method, MethodOutput};

pub struct PackCheckoutMethod;

impl Method for PackCheckoutMethod {
    fn key(&self) -> char { 'c' }
    fn label(&self) -> &'static str { "C: pack + checkout" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

pub fn run(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<BenchResult, BenchError> {
    let (pack_data, cmd_duration, root_tree_oid) =
        git::generate_packfile_stdout(repo_path, commit, &[])?;
    let output_bytes = pack_data.len() as u64;

    let extract_start = Instant::now();
    let (file_hashes, timings) = extract_via_checkout(&pack_data, &root_tree_oid, output_dir)?;
    let extract_duration = extract_start.elapsed();

    println!(
        "    breakdown: init={:.2?} index-pack={:.2?} read-tree={:.2?} checkout={:.2?} hash={:.2?}",
        timings.init, timings.index_pack, timings.read_tree, timings.checkout, timings.hash
    );

    Ok(BenchResult {
        method: "pack + checkout".to_string(),
        git_cmd_time: cmd_duration, transfer_bytes: output_bytes,
        extract_time: extract_duration, total_time: cmd_duration + extract_duration,
        file_count: file_hashes.len(), file_hashes,
    })
}

struct ExtractTimings { init: Duration, index_pack: Duration, read_tree: Duration, checkout: Duration, hash: Duration }

fn extract_via_checkout(
    pack_data: &[u8], root_tree_oid: &str, output_dir: &Path,
) -> Result<(BTreeMap<String, String>, ExtractTimings), BenchError> {
    let git_dir = output_dir.join(".git-tmp");

    let t = Instant::now();
    let init = Command::new("git")
        .args(["init", "--bare", "-q", git_dir.to_str().unwrap()])
        .stdout(Stdio::null()).stderr(Stdio::null())
        .output().map_err(|e| BenchError::Extract(format!("git init: {e}")))?;
    if !init.status.success() { return Err(BenchError::Extract("git init failed".into())); }
    let init_dur = t.elapsed();

    let t = Instant::now();
    let pack_dir = git_dir.join("objects/pack");
    std::fs::create_dir_all(&pack_dir).map_err(|e| BenchError::Extract(e.to_string()))?;
    let pack_path = pack_dir.join("recv.pack");
    std::fs::write(&pack_path, pack_data).map_err(|e| BenchError::Extract(e.to_string()))?;
    let idx = Command::new("git").args(["index-pack", pack_path.to_str().unwrap()])
        .env("GIT_DIR", &git_dir).output().map_err(|e| BenchError::Extract(format!("index-pack: {e}")))?;
    if !idx.status.success() {
        return Err(BenchError::Extract(format!("index-pack: {}", String::from_utf8_lossy(&idx.stderr))));
    }
    let index_pack_dur = t.elapsed();

    let t = Instant::now();
    let rt = Command::new("git").args(["read-tree", root_tree_oid])
        .env("GIT_DIR", &git_dir).env("GIT_INDEX_FILE", git_dir.join("index"))
        .output().map_err(|e| BenchError::Extract(format!("read-tree: {e}")))?;
    if !rt.status.success() {
        return Err(BenchError::Extract(format!("read-tree: {}", String::from_utf8_lossy(&rt.stderr))));
    }
    let read_tree_dur = t.elapsed();

    let t = Instant::now();
    let co = Command::new("git").args(["checkout-index", "--all", "--force"])
        .env("GIT_DIR", &git_dir).env("GIT_WORK_TREE", output_dir)
        .env("GIT_INDEX_FILE", git_dir.join("index"))
        .output().map_err(|e| BenchError::Extract(format!("checkout-index: {e}")))?;
    if !co.status.success() {
        return Err(BenchError::Extract(format!("checkout-index: {}", String::from_utf8_lossy(&co.stderr))));
    }
    let checkout_dur = t.elapsed();

    let t = Instant::now();
    std::fs::remove_dir_all(&git_dir).ok();
    let file_hashes = hash_directory(output_dir)?;
    let hash_dur = t.elapsed();

    Ok((file_hashes, ExtractTimings { init: init_dur, index_pack: index_pack_dur, read_tree: read_tree_dur, checkout: checkout_dur, hash: hash_dur }))
}

fn hash_directory(dir: &Path) -> Result<BTreeMap<String, String>, BenchError> {
    let mut hashes = BTreeMap::new();
    walk_dir(dir, dir, &mut hashes)?;
    Ok(hashes)
}

fn walk_dir(base: &Path, dir: &Path, hashes: &mut BTreeMap<String, String>) -> Result<(), BenchError> {
    for entry in std::fs::read_dir(dir).map_err(|e| BenchError::Extract(e.to_string()))? {
        let entry = entry.map_err(|e| BenchError::Extract(e.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            walk_dir(base, &path, hashes)?;
        } else if path.is_file() {
            let content = std::fs::read(&path).map_err(|e| BenchError::Extract(e.to_string()))?;
            hashes.insert(path.strip_prefix(base).unwrap().to_string_lossy().to_string(), git::hex_sha256(&content));
        }
    }
    Ok(())
}
