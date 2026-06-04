//! Gix-based extraction methods (E-H).
//!
//! All variants share: generate packfile -> open bundle -> walk tree -> resolve blobs.
//! They differ in: pack-objects flags, index source, parallelism, and profiling detail.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use gix_features::zlib;
use sha2::{Digest, Sha256};

use crate::filter::{self, MAX_FILE_SIZE};
use crate::{format_bytes, BenchError, BenchResult, Method, MethodOutput};

// ─── Shared result type ─────────────────────────────────────────────────────

pub struct GixResult {
    pub bench: BenchResult,
    pub files_written: usize,
    pub files_skipped: usize,
    pub bytes_skipped: u64,
    pub index_pack_time: Duration,
    pub walk_extract_time: Duration,
}

fn gix_detail(gr: &GixResult) -> String {
    format!(
        "    wrote={} skipped={} ({} skipped)  idx-pack={:.2?} walk+extract={:.2?}",
        gr.files_written, gr.files_skipped, format_bytes(gr.bytes_skipped),
        gr.index_pack_time, gr.walk_extract_time,
    )
}

fn gix_output(gr: GixResult) -> MethodOutput {
    MethodOutput { detail: Some(gix_detail(&gr)), result: gr.bench }
}

// ─── Method impls ───────────────────────────────────────────────────────────

pub struct GixMethod;
impl Method for GixMethod {
    fn key(&self) -> char { 'e' }
    fn label(&self) -> &'static str { "E: pack + gix" }
    fn run(&self, r: &Path, c: &str, o: &Path) -> Result<MethodOutput, BenchError> {
        run_single_threaded(r, c, o, &[], "E: pack+gix").map(gix_output)
    }
}

pub struct NodeltaMethod;
impl Method for NodeltaMethod {
    fn key(&self) -> char { 'f' }
    fn label(&self) -> &'static str { "F: nodelta + gix" }
    fn run(&self, r: &Path, c: &str, o: &Path) -> Result<MethodOutput, BenchError> {
        run_single_threaded(r, c, o, &["--depth=0", "--no-reuse-delta"], "F: nodelta+gix").map(gix_output)
    }
}

pub struct RayonMethod;
impl Method for RayonMethod {
    fn key(&self) -> char { 'g' }
    fn label(&self) -> &'static str { "G: pack + rayon" }
    fn run(&self, r: &Path, c: &str, o: &Path) -> Result<MethodOutput, BenchError> {
        run_rayon(r, c, o, PackSource::Stdout, "G: pack+rayon").map(gix_output)
    }
}

pub struct BundledIdxMethod;
impl Method for BundledIdxMethod {
    fn key(&self) -> char { 'h' }
    fn label(&self) -> &'static str { "H: bundled idx" }
    fn run(&self, r: &Path, c: &str, o: &Path) -> Result<MethodOutput, BenchError> {
        run_rayon(r, c, o, PackSource::DiskWithIdx, "H: bundled idx").map(gix_output)
    }
}

// ─── Pack source variants ───────────────────────────────────────────────────

enum PackSource { Stdout, DiskWithIdx }

/// Generate packfile via --stdout, return (bytes, duration, root_tree_oid).
fn generate_packfile_stdout(
    repo: &Path, commit: &str, extra_flags: &[&str],
) -> Result<(Vec<u8>, Duration, String), BenchError> {
    let root = resolve_tree_oid(repo, commit)?;
    let t = Instant::now();

    let mut rev_list = Command::new("git")
        .args(["rev-list", "--objects", "--stdin"])
        .current_dir(repo).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().map_err(|e| BenchError::Git(format!("rev-list: {e}")))?;

    { let mut s = rev_list.stdin.take().unwrap(); writeln!(s, "{commit}^{{tree}}").ok(); }

    let mut args = vec!["pack-objects", "--stdout", "-q", "--delta-base-offset"];
    args.extend_from_slice(extra_flags);

    let out = Command::new("git").args(&args)
        .current_dir(repo).stdin(rev_list.stdout.take().unwrap())
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .output().map_err(|e| BenchError::Git(format!("pack-objects: {e}")))?;
    let _ = rev_list.wait();

    if !out.status.success() {
        return Err(BenchError::Git(format!("pack-objects: {}", String::from_utf8_lossy(&out.stderr))));
    }
    Ok((out.stdout, t.elapsed(), root))
}

/// Generate packfile to disk (produces .pack + .idx). Returns (pack_path, duration, root_tree_oid, transfer_bytes).
fn generate_packfile_disk(
    repo: &Path, commit: &str, tmp: &Path,
) -> Result<(PathBuf, Duration, String, u64), BenchError> {
    let root = resolve_tree_oid(repo, commit)?;
    let t = Instant::now();
    let prefix = tmp.join("out");

    let mut rev_list = Command::new("git")
        .args(["rev-list", "--objects", "--stdin"])
        .current_dir(repo).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().map_err(|e| BenchError::Git(format!("rev-list: {e}")))?;

    { let mut s = rev_list.stdin.take().unwrap(); writeln!(s, "{commit}^{{tree}}").ok(); }

    let out = Command::new("git")
        .args(["pack-objects", "-q", "--delta-base-offset", prefix.to_str().unwrap()])
        .current_dir(repo).stdin(rev_list.stdout.take().unwrap())
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .output().map_err(|e| BenchError::Git(format!("pack-objects: {e}")))?;
    let _ = rev_list.wait();

    if !out.status.success() {
        return Err(BenchError::Git(format!("pack-objects: {}", String::from_utf8_lossy(&out.stderr))));
    }

    let hash = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let pack_path = tmp.join(format!("out-{hash}.pack"));
    let pack_bytes = std::fs::metadata(&pack_path).map(|m| m.len()).unwrap_or(0);
    let idx_bytes = std::fs::metadata(pack_path.with_extension("idx")).map(|m| m.len()).unwrap_or(0);

    Ok((pack_path, t.elapsed(), root, pack_bytes + idx_bytes))
}

/// Write pack bytes to disk + run git index-pack -> Bundle.
fn open_bundle_via_index_pack(data: &[u8]) -> Result<(gix_pack::Bundle, tempfile::TempDir, Duration), BenchError> {
    let t = Instant::now();
    let tmp = tempfile::tempdir().map_err(|e| BenchError::Extract(format!("tempdir: {e}")))?;
    let path = tmp.path().join("objects.pack");
    std::fs::write(&path, data).map_err(|e| BenchError::Extract(format!("write: {e}")))?;

    let r = Command::new("git").args(["index-pack", path.to_str().unwrap()])
        .output().map_err(|e| BenchError::Extract(format!("index-pack: {e}")))?;
    if !r.status.success() {
        return Err(BenchError::Extract(format!("index-pack: {}", String::from_utf8_lossy(&r.stderr))));
    }

    let bundle = gix_pack::Bundle::at(&path, gix_hash::Kind::Sha1)
        .map_err(|e| BenchError::Extract(format!("Bundle::at: {e}")))?;
    Ok((bundle, tmp, t.elapsed()))
}

// ─── E/F: single-threaded walk + extract ────────────────────────────────────

fn run_single_threaded(
    repo: &Path, commit: &str, output_dir: &Path, extra_flags: &[&str], label: &str,
) -> Result<GixResult, BenchError> {
    let (pack_data, cmd_dur, root_str) = generate_packfile_stdout(repo, commit, extra_flags)?;
    let bytes = pack_data.len() as u64;
    let ext_start = Instant::now();
    let (bundle, _tmp, idx_time) = open_bundle_via_index_pack(&pack_data)?;

    let t = Instant::now();
    let root = parse_oid(&root_str)?;
    let mut inflate = zlib::Inflate::default();
    let mut buf = Vec::with_capacity(256 * 1024);
    let mut cache = gix_pack::cache::lru::MemoryCappedHashmap::new(64 * 1024 * 1024);
    let mut hashes = BTreeMap::new();
    let mut written = 0usize;
    let mut skipped = 0usize;
    let mut skip_bytes = 0u64;

    walk_and_extract(
        &bundle, &root, &PathBuf::new(), output_dir,
        &mut inflate, &mut buf, &mut cache,
        &mut hashes, &mut written, &mut skipped, &mut skip_bytes,
    )?;

    Ok(GixResult {
        bench: BenchResult {
            method: label.to_string(), git_cmd_time: cmd_dur, transfer_bytes: bytes,
            extract_time: ext_start.elapsed(), total_time: cmd_dur + ext_start.elapsed(),
            file_count: written + skipped, file_hashes: hashes,
        },
        files_written: written, files_skipped: skipped, bytes_skipped: skip_bytes,
        index_pack_time: idx_time, walk_extract_time: t.elapsed(),
    })
}

// ─── G/H: rayon parallel extraction ─────────────────────────────────────────

fn run_rayon(
    repo: &Path, commit: &str, output_dir: &Path, source: PackSource, label: &str,
) -> Result<GixResult, BenchError> {
    // Phase 1: Generate pack + open bundle
    let (bundle, cmd_dur, bytes, root_str, idx_time, _tmp_guard) = match source {
        PackSource::Stdout => {
            let (data, dur, root) = generate_packfile_stdout(repo, commit, &[])?;
            let b = data.len() as u64;
            let (bundle, tmp, idx_t) = open_bundle_via_index_pack(&data)?;
            (bundle, dur, b, root, idx_t, Some(tmp))
        }
        PackSource::DiskWithIdx => {
            let tmp = tempfile::tempdir().map_err(|e| BenchError::Git(format!("tmpdir: {e}")))?;
            let (pack_path, dur, root, b) = generate_packfile_disk(repo, commit, tmp.path())?;
            let t = Instant::now();
            let bundle = gix_pack::Bundle::at(&pack_path, gix_hash::Kind::Sha1)
                .map_err(|e| BenchError::Extract(format!("Bundle::at: {e}")))?;
            let idx_t = t.elapsed();
            (bundle, dur, b, root, idx_t, Some(tmp))
        }
    };

    let ext_start = Instant::now();
    let t = Instant::now();
    let root = parse_oid(&root_str)?;

    // Walk tree, collect blob entries
    let mut inflate = zlib::Inflate::default();
    let mut buf = Vec::with_capacity(256 * 1024);
    let mut cache = gix_pack::cache::lru::MemoryCappedHashmap::new(64 * 1024 * 1024);
    let mut blob_entries: Vec<(String, gix_hash::ObjectId)> = Vec::new();
    let mut skipped = 0usize;

    collect_blobs(&bundle, &root, &PathBuf::new(), &mut inflate, &mut buf, &mut cache, &mut blob_entries, &mut skipped)?;

    // Pre-create directories
    {
        let mut dirs = std::collections::BTreeSet::new();
        for (path, _) in &blob_entries {
            let mut p = Path::new(path);
            while let Some(parent) = p.parent() {
                if parent.as_os_str().is_empty() || !dirs.insert(parent.to_path_buf()) { break; }
                p = parent;
            }
        }
        for dir in &dirs {
            let _ = std::fs::create_dir(output_dir.join(dir));
        }
    }

    // Parallel resolve + write
    use rayon::prelude::*;
    let results: Vec<Result<(String, u64, bool), BenchError>> = blob_entries.par_iter()
        .map(|(path, oid)| {
            let mut inf = zlib::Inflate::default();
            let mut b = Vec::with_capacity(64 * 1024);
            let mut c = gix_pack::cache::lru::MemoryCappedHashmap::new(8 * 1024 * 1024);

            let (blob, _) = bundle.find(oid, &mut b, &mut inf, &mut c)
                .map_err(|e| BenchError::Extract(format!("find blob: {e}")))?
                .ok_or_else(|| BenchError::Extract(format!("blob {oid} not found")))?;

            let size = blob.data.len() as u64;
            if size > MAX_FILE_SIZE { return Ok((path.clone(), size, false)); }

            std::fs::write(output_dir.join(path), blob.data)
                .map_err(|e| BenchError::Extract(e.to_string()))?;
            Ok((path.clone(), size, true))
        })
        .collect();

    let mut hashes = BTreeMap::new();
    let mut written = 0usize;
    let mut skip_bytes = 0u64;
    for r in results {
        let (path, size, ok) = r?;
        if ok { hashes.insert(path, String::new()); written += 1; }
        else { skipped += 1; skip_bytes += size; }
    }

    let walk_time = t.elapsed();

    Ok(GixResult {
        bench: BenchResult {
            method: label.to_string(), git_cmd_time: cmd_dur, transfer_bytes: bytes,
            extract_time: ext_start.elapsed(), total_time: cmd_dur + ext_start.elapsed(),
            file_count: written + skipped, file_hashes: hashes,
        },
        files_written: written, files_skipped: skipped, bytes_skipped: skip_bytes,
        index_pack_time: idx_time, walk_extract_time: walk_time,
    })
}

// ─── Tree walking ───────────────────────────────────────────────────────────

/// Walk tree and extract blobs directly (single-threaded, used by E/F).
fn walk_and_extract(
    bundle: &gix_pack::Bundle, tree_oid: &gix_hash::oid, prefix: &Path, out_dir: &Path,
    inflate: &mut zlib::Inflate, buf: &mut Vec<u8>, cache: &mut dyn gix_pack::cache::DecodeEntry,
    hashes: &mut BTreeMap<String, String>, written: &mut usize, skipped: &mut usize, skip_bytes: &mut u64,
) -> Result<(), BenchError> {
    for (mode, name, oid) in tree_entries(bundle, tree_oid, inflate, buf, cache)? {
        let path = prefix.join(&name);
        match mode {
            EntryKind::Tree => {
                walk_and_extract(bundle, &oid, &path, out_dir, inflate, buf, cache, hashes, written, skipped, skip_bytes)?;
            }
            EntryKind::Blob => {
                let path_str = path.to_string_lossy().to_string();
                if filter::is_excluded(&path_str) { *skipped += 1; continue; }

                buf.clear();
                let (blob, _) = bundle.find(&oid, buf, inflate, cache)
                    .map_err(|e| BenchError::Extract(format!("find blob: {e}")))?
                    .ok_or_else(|| BenchError::Extract(format!("blob {oid} not found")))?;

                let size = blob.data.len() as u64;
                if size > MAX_FILE_SIZE { *skipped += 1; *skip_bytes += size; continue; }

                let dest = out_dir.join(&path);
                if let Some(p) = dest.parent() { let _ = std::fs::create_dir_all(p); }
                std::fs::write(&dest, blob.data).map_err(|e| BenchError::Extract(e.to_string()))?;
                hashes.insert(path_str, hex_sha256(blob.data));
                *written += 1;
            }
            EntryKind::Skip => { *skipped += 1; }
        }
    }
    Ok(())
}

/// Walk tree, collect (path, oid) pairs without reading blob content (used by G/H).
fn collect_blobs(
    bundle: &gix_pack::Bundle, tree_oid: &gix_hash::oid, prefix: &Path,
    inflate: &mut zlib::Inflate, buf: &mut Vec<u8>, cache: &mut dyn gix_pack::cache::DecodeEntry,
    entries: &mut Vec<(String, gix_hash::ObjectId)>, skipped: &mut usize,
) -> Result<(), BenchError> {
    for (mode, name, oid) in tree_entries(bundle, tree_oid, inflate, buf, cache)? {
        let path = prefix.join(&name);
        match mode {
            EntryKind::Tree => collect_blobs(bundle, &oid, &path, inflate, buf, cache, entries, skipped)?,
            EntryKind::Blob => {
                let s = path.to_string_lossy().to_string();
                if filter::is_excluded(&s) { *skipped += 1; } else { entries.push((s, oid)); }
            }
            EntryKind::Skip => { *skipped += 1; }
        }
    }
    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

enum EntryKind { Tree, Blob, Skip }

/// Parse a tree object, return (kind, name, oid) triples.
fn tree_entries(
    bundle: &gix_pack::Bundle, tree_oid: &gix_hash::oid,
    inflate: &mut zlib::Inflate, buf: &mut Vec<u8>, cache: &mut dyn gix_pack::cache::DecodeEntry,
) -> Result<Vec<(EntryKind, String, gix_hash::ObjectId)>, BenchError> {
    buf.clear();
    let (data, _) = bundle.find(tree_oid, buf, inflate, cache)
        .map_err(|e| BenchError::Extract(format!("find tree: {e}")))?
        .ok_or_else(|| BenchError::Extract("tree not found".into()))?;

    let tree_data = data.data.to_vec();
    gix_object::TreeRefIter::from_bytes(&tree_data, gix_hash::Kind::Sha1)
        .map(|e| {
            let e = e.map_err(|e| BenchError::Extract(format!("tree parse: {e}")))?;
            let kind = match e.mode.kind() {
                gix_object::tree::EntryKind::Tree => EntryKind::Tree,
                gix_object::tree::EntryKind::Blob | gix_object::tree::EntryKind::BlobExecutable => EntryKind::Blob,
                _ => EntryKind::Skip,
            };
            Ok((kind, e.filename.to_string(), e.oid.to_owned()))
        })
        .collect()
}

fn resolve_tree_oid(repo: &Path, commit: &str) -> Result<String, BenchError> {
    let o = Command::new("git").args(["rev-parse", &format!("{commit}^{{tree}}")])
        .current_dir(repo).output().map_err(|e| BenchError::Git(format!("rev-parse: {e}")))?;
    if !o.status.success() { return Err(BenchError::Git("rev-parse failed".into())); }
    Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn parse_oid(hex: &str) -> Result<gix_hash::ObjectId, BenchError> {
    gix_hash::ObjectId::from_hex(hex.as_bytes()).map_err(|e| BenchError::Extract(format!("parse oid: {e}")))
}

fn hex_sha256(data: &[u8]) -> String {
    let mut h = Sha256::new(); h.update(data); format!("{:x}", h.finalize())
}
