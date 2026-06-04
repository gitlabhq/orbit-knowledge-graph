//! Method E: pack-objects + gix in-process parse + filtered extract (single-threaded)
//! Method F: same but with --depth=0 --no-reuse-delta (no deltas, just zlib)
//! Method G: like E but with parallel blob resolution + file writes via rayon

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use gix_features::zlib;
use sha2::{Digest, Sha256};

use crate::{format_bytes, BenchError, BenchResult, Method, MethodOutput};

fn gix_detail(gr: &GixResult) -> String {
    format!(
        "    wrote={} skipped={} ({} skipped)  idx-pack={:.2?} walk+extract={:.2?}",
        gr.files_written, gr.files_skipped,
        format_bytes(gr.bytes_skipped),
        gr.index_pack_time, gr.walk_extract_time,
    )
}

pub struct GixMethod;
impl Method for GixMethod {
    fn key(&self) -> char { 'e' }
    fn label(&self) -> &'static str { "E: pack + gix" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run_e(repo, commit, out).map(|gr| MethodOutput { detail: Some(gix_detail(&gr)), result: gr.bench })
    }
}

pub struct NodeltaMethod;
impl Method for NodeltaMethod {
    fn key(&self) -> char { 'f' }
    fn label(&self) -> &'static str { "F: nodelta + gix" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run_f(repo, commit, out).map(|gr| MethodOutput { detail: Some(gix_detail(&gr)), result: gr.bench })
    }
}

pub struct RayonMethod;
impl Method for RayonMethod {
    fn key(&self) -> char { 'g' }
    fn label(&self) -> &'static str { "G: pack + rayon" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run_g(repo, commit, out).map(|gr| MethodOutput { detail: Some(gix_detail(&gr)), result: gr.bench })
    }
}

pub struct BundledIdxMethod;
impl Method for BundledIdxMethod {
    fn key(&self) -> char { 'h' }
    fn label(&self) -> &'static str { "H: bundled idx" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        run_h(repo, commit, out).map(|gr| MethodOutput { detail: Some(gix_detail(&gr)), result: gr.bench })
    }
}

const MAX_FILE_SIZE: u64 = 5_000_000;

const EXCLUDED_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "bmp", "ico", "webp", "avif", "tiff", "tif", "svg",
    "ttf", "otf", "woff", "woff2", "eot",
    "mp3", "mp4", "mov", "webm", "ogg", "wav", "flac", "m4a", "m4v", "avi", "mkv", "opus",
    "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar", "lz4", "zst",
    "exe", "dll", "so", "dylib", "class", "jar", "war", "pyc", "pyo", "o", "a", "lib",
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp",
    "db", "sqlite", "sqlite3", "iso", "dmg", "bin", "dat",
];

fn is_excluded(path: &str) -> bool {
    let lower = path.to_lowercase();
    if let Some(ext) = lower.rsplit('.').next() {
        EXCLUDED_EXTENSIONS.contains(&ext)
    } else {
        false
    }
}

pub struct GixResult {
    pub bench: BenchResult,
    pub files_written: usize,
    pub files_skipped: usize,
    pub bytes_skipped: u64,
    pub index_pack_time: Duration,
    pub walk_extract_time: Duration,
}

/// Shared: generate packfile with given extra flags
fn generate_packfile(
    repo_path: &Path,
    commit: &str,
    extra_pack_flags: &[&str],
) -> Result<(Vec<u8>, Duration, String), crate::BenchError> {
    let root_tree_oid = resolve_tree_oid(repo_path, commit)?;

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

    let mut pack_args = vec!["pack-objects", "--stdout", "-q", "--delta-base-offset"];
    pack_args.extend_from_slice(extra_pack_flags);

    let pack_objects = Command::new("git")
        .args(&pack_args)
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

    Ok((pack_output.stdout, cmd_start.elapsed(), root_tree_oid))
}

/// Shared: write pack to disk + index-pack -> Bundle
fn open_bundle(
    pack_data: &[u8],
) -> Result<(gix_pack::Bundle, tempfile::TempDir, Duration), crate::BenchError> {
    let t = Instant::now();
    let tmp = tempfile::tempdir()
        .map_err(|e| crate::BenchError::Extract(format!("tempdir: {e}")))?;
    let pack_path = tmp.path().join("objects.pack");
    std::fs::write(&pack_path, pack_data)
        .map_err(|e| crate::BenchError::Extract(format!("write pack: {e}")))?;

    let idx_result = Command::new("git")
        .args(["index-pack", pack_path.to_str().unwrap()])
        .output()
        .map_err(|e| crate::BenchError::Extract(format!("index-pack: {e}")))?;
    if !idx_result.status.success() {
        return Err(crate::BenchError::Extract(format!(
            "index-pack failed: {}",
            String::from_utf8_lossy(&idx_result.stderr)
        )));
    }

    let bundle = gix_pack::Bundle::at(&pack_path, gix_hash::Kind::Sha1)
        .map_err(|e| crate::BenchError::Extract(format!("Bundle::at: {e}")))?;

    Ok((bundle, tmp, t.elapsed()))
}

// ─── Method E: single-threaded gix, with deltas ─────────────────────────────

pub fn run_e(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<GixResult, crate::BenchError> {
    let (pack_data, cmd_duration, root_tree_oid_str) = generate_packfile(repo_path, commit, &[])?;
    let output_bytes = pack_data.len() as u64;

    let extract_start = Instant::now();
    let (bundle, _tmp, index_pack_time) = open_bundle(&pack_data)?;

    let t = Instant::now();
    let root_oid = parse_oid(&root_tree_oid_str)?;
    let mut inflate = zlib::Inflate::default();
    let mut buf = Vec::with_capacity(256 * 1024);
    let mut cache = gix_pack::cache::lru::MemoryCappedHashmap::new(64 * 1024 * 1024);

    let mut file_hashes = BTreeMap::new();
    let mut files_written = 0usize;
    let mut files_skipped = 0usize;
    let mut bytes_skipped = 0u64;

    walk_tree(
        &bundle, &root_oid, &PathBuf::new(), output_dir,
        &mut inflate, &mut buf, &mut cache,
        &mut file_hashes, &mut files_written, &mut files_skipped, &mut bytes_skipped,
    )?;

    let walk_extract_time = t.elapsed();
    let extract_duration = extract_start.elapsed();

    Ok(GixResult {
        bench: BenchResult {
            method: "E: pack+gix".to_string(),
            git_cmd_time: cmd_duration,
            transfer_bytes: output_bytes,
            extract_time: extract_duration,
            total_time: cmd_duration + extract_duration,
            file_count: files_written + files_skipped,
            file_hashes,
        },
        files_written, files_skipped, bytes_skipped, index_pack_time, walk_extract_time,
    })
}

// ─── Method F: no-delta packfile + gix ──────────────────────────────────────

pub fn run_f(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<GixResult, crate::BenchError> {
    let (pack_data, cmd_duration, root_tree_oid_str) =
        generate_packfile(repo_path, commit, &["--depth=0", "--no-reuse-delta"])?;
    let output_bytes = pack_data.len() as u64;

    let extract_start = Instant::now();
    let (bundle, _tmp, index_pack_time) = open_bundle(&pack_data)?;

    let t = Instant::now();
    let root_oid = parse_oid(&root_tree_oid_str)?;
    let mut inflate = zlib::Inflate::default();
    let mut buf = Vec::with_capacity(256 * 1024);
    let mut cache = gix_pack::cache::lru::MemoryCappedHashmap::new(64 * 1024 * 1024);

    let mut file_hashes = BTreeMap::new();
    let mut files_written = 0usize;
    let mut files_skipped = 0usize;
    let mut bytes_skipped = 0u64;

    walk_tree(
        &bundle, &root_oid, &PathBuf::new(), output_dir,
        &mut inflate, &mut buf, &mut cache,
        &mut file_hashes, &mut files_written, &mut files_skipped, &mut bytes_skipped,
    )?;

    let walk_extract_time = t.elapsed();
    let extract_duration = extract_start.elapsed();

    Ok(GixResult {
        bench: BenchResult {
            method: "F: nodelta+gix".to_string(),
            git_cmd_time: cmd_duration,
            transfer_bytes: output_bytes,
            extract_time: extract_duration,
            total_time: cmd_duration + extract_duration,
            file_count: files_written + files_skipped,
            file_hashes,
        },
        files_written, files_skipped, bytes_skipped, index_pack_time, walk_extract_time,
    })
}

// ─── Method G: deltas + parallel rayon extraction ───────────────────────────

pub fn run_g(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<GixResult, crate::BenchError> {
    let (pack_data, cmd_duration, root_tree_oid_str) = generate_packfile(repo_path, commit, &[])?;
    let output_bytes = pack_data.len() as u64;

    let extract_start = Instant::now();
    let (bundle, _tmp, index_pack_time) = open_bundle(&pack_data)?;

    let t = Instant::now();
    let root_oid = parse_oid(&root_tree_oid_str)?;

    // Phase 1: Walk tree single-threaded, collect (path, blob_oid) pairs
    let mut inflate = zlib::Inflate::default();
    let mut buf = Vec::with_capacity(256 * 1024);
    let mut cache = gix_pack::cache::lru::MemoryCappedHashmap::new(64 * 1024 * 1024);

    let mut blob_entries: Vec<(String, gix_hash::ObjectId)> = Vec::new();
    let mut files_skipped = 0usize;

    collect_blobs(
        &bundle, &root_oid, &PathBuf::new(),
        &mut inflate, &mut buf, &mut cache,
        &mut blob_entries, &mut files_skipped,
    )?;

    // Phase 2: Resolve blobs + write files in parallel with rayon
    use rayon::prelude::*;

    let results: Vec<Result<(String, String, u64), crate::BenchError>> = blob_entries
        .par_iter()
        .map(|(path, oid)| {
            let mut local_inflate = zlib::Inflate::default();
            let mut local_buf = Vec::with_capacity(64 * 1024);
            let mut local_cache = gix_pack::cache::lru::MemoryCappedHashmap::new(8 * 1024 * 1024);

            local_buf.clear();
            let (blob_data, _) = bundle
                .find(oid, &mut local_buf, &mut local_inflate, &mut local_cache)
                .map_err(|e| crate::BenchError::Extract(format!("find blob: {e}")))?
                .ok_or_else(|| crate::BenchError::Extract(format!("blob {oid} not found")))?;

            let size = blob_data.data.len() as u64;
            if size > MAX_FILE_SIZE {
                return Ok((path.clone(), String::new(), size)); // marker for skipped
            }

            let hash = hex_sha256(blob_data.data);

            let dest = output_dir.join(path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| crate::BenchError::Extract(e.to_string()))?;
            }
            std::fs::write(&dest, blob_data.data)
                .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

            Ok((path.clone(), hash, size))
        })
        .collect();

    let mut file_hashes = BTreeMap::new();
    let mut files_written = 0usize;
    let mut bytes_skipped = 0u64;

    for r in results {
        let (path, hash, size) = r?;
        if hash.is_empty() {
            files_skipped += 1;
            bytes_skipped += size;
        } else {
            file_hashes.insert(path, hash);
            files_written += 1;
        }
    }

    let walk_extract_time = t.elapsed();
    let extract_duration = extract_start.elapsed();

    Ok(GixResult {
        bench: BenchResult {
            method: "G: pack+rayon".to_string(),
            git_cmd_time: cmd_duration,
            transfer_bytes: output_bytes,
            extract_time: extract_duration,
            total_time: cmd_duration + extract_duration,
            file_count: files_written + files_skipped,
            file_hashes,
        },
        files_written, files_skipped, bytes_skipped, index_pack_time, walk_extract_time,
    })
}

// ─── Method H: server generates .pack+.idx, client skips indexing entirely ──

pub fn run_h(repo_path: &Path, commit: &str, output_dir: &Path) -> Result<GixResult, crate::BenchError> {
    let root_tree_oid_str = resolve_tree_oid(repo_path, commit)?;

    // Phase 1: Server generates .pack + .idx together (no --stdout)
    // This is what Gitaly would do: pack-objects writes to a directory,
    // producing .pack, .idx, and .rev as a single operation.
    let cmd_start = Instant::now();

    let tmp_server = tempfile::tempdir()
        .map_err(|e| crate::BenchError::Git(format!("tempdir: {e}")))?;
    let pack_prefix = tmp_server.path().join("out");

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

    // pack-objects with a path prefix instead of --stdout:
    // writes .pack + .idx + .rev in one shot
    let pack_obj = Command::new("git")
        .args(["pack-objects", "-q", "--delta-base-offset", pack_prefix.to_str().unwrap()])
        .current_dir(repo_path)
        .stdin(rev_list.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| crate::BenchError::Git(format!("pack-objects: {e}")))?;

    let _ = rev_list.wait();

    if !pack_obj.status.success() {
        return Err(crate::BenchError::Git(format!(
            "pack-objects failed: {}",
            String::from_utf8_lossy(&pack_obj.stderr)
        )));
    }

    let cmd_duration = cmd_start.elapsed();

    // Find the generated files
    let hash_hex = String::from_utf8_lossy(&pack_obj.stdout).trim().to_string();
    let pack_path = tmp_server.path().join(format!("out-{hash_hex}.pack"));
    let output_bytes = std::fs::metadata(&pack_path)
        .map(|m| m.len())
        .unwrap_or(0);

    // Also count .idx size as transfer overhead
    let idx_path = pack_path.with_extension("idx");
    let idx_bytes = std::fs::metadata(&idx_path)
        .map(|m| m.len())
        .unwrap_or(0);

    // Phase 2: Client receives .pack + .idx -- just open the bundle, zero indexing
    let extract_start = Instant::now();

    let t = Instant::now();
    let bundle = gix_pack::Bundle::at(&pack_path, gix_hash::Kind::Sha1)
        .map_err(|e| crate::BenchError::Extract(format!("Bundle::at: {e}")))?;
    let index_pack_time = t.elapsed(); // should be ~0 (just mmap)

    // Phase 2a: Walk tree to collect blob entries (single-threaded, tree objects only)
    let t = Instant::now();
    let root_oid = parse_oid(&root_tree_oid_str)?;

    let mut inflate = zlib::Inflate::default();
    let mut buf = Vec::with_capacity(256 * 1024);
    let mut cache = gix_pack::cache::lru::MemoryCappedHashmap::new(64 * 1024 * 1024);

    let mut blob_entries: Vec<(String, gix_hash::ObjectId)> = Vec::new();
    let mut files_skipped = 0usize;

    collect_blobs(
        &bundle, &root_oid, &PathBuf::new(),
        &mut inflate, &mut buf, &mut cache,
        &mut blob_entries, &mut files_skipped,
    )?;
    let tree_walk_time = t.elapsed();

    // Phase 2b: Pre-create all directories.
    // We already know the full tree structure from the walk. Collect every
    // unique directory path, sort them (so parents sort before children),
    // then create each with a single mkdir syscall.
    let t_dirs = Instant::now();
    {
        // Collect all ancestor dirs for every blob path
        let mut dirs = std::collections::BTreeSet::new();
        for (path, _) in &blob_entries {
            let mut p = Path::new(path);
            while let Some(parent) = p.parent() {
                if parent.as_os_str().is_empty() {
                    break;
                }
                if !dirs.insert(parent.to_path_buf()) {
                    break; // already seen this and all its ancestors
                }
                p = parent;
            }
        }
        // BTreeSet is sorted, so parents come before children.
        // Single mkdir per dir, ignore AlreadyExists.
        for dir in &dirs {
            match std::fs::create_dir(output_dir.join(dir)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => {} // temp dir, shouldn't fail
            }
        }
    }
    let dir_create_time = t_dirs.elapsed();

    // Phase 2c: Parallel blob resolve + write
    let t_resolve = Instant::now();
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    let resolved_bytes = AtomicU64::new(0);

    // Resolve only (no disk write) to isolate CPU cost
    blob_entries
        .par_iter()
        .for_each(|(_path, oid)| {
            let mut local_inflate = zlib::Inflate::default();
            let mut local_buf = Vec::with_capacity(64 * 1024);
            let mut local_cache = gix_pack::cache::lru::MemoryCappedHashmap::new(8 * 1024 * 1024);

            local_buf.clear();
            if let Ok(Some((blob_data, _))) = bundle.find(oid, &mut local_buf, &mut local_inflate, &mut local_cache) {
                resolved_bytes.fetch_add(blob_data.data.len() as u64, Ordering::Relaxed);
            }
        });

    let resolve_only_time = t_resolve.elapsed();

    // Now actually write
    let t_write = Instant::now();

    let results: Vec<Result<(String, u64, bool), crate::BenchError>> = blob_entries
        .par_iter()
        .map(|(path, oid)| {
            let mut local_inflate = zlib::Inflate::default();
            let mut local_buf = Vec::with_capacity(64 * 1024);
            let mut local_cache = gix_pack::cache::lru::MemoryCappedHashmap::new(8 * 1024 * 1024);

            local_buf.clear();
            let (blob_data, _) = bundle
                .find(oid, &mut local_buf, &mut local_inflate, &mut local_cache)
                .map_err(|e| crate::BenchError::Extract(format!("find blob: {e}")))?
                .ok_or_else(|| crate::BenchError::Extract(format!("blob {oid} not found")))?;

            let size = blob_data.data.len() as u64;
            if size > MAX_FILE_SIZE {
                return Ok((path.clone(), size, false));
            }

            let dest = output_dir.join(path);
            std::fs::write(&dest, blob_data.data)
                .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

            Ok((path.clone(), size, true))
        })
        .collect();

    let resolve_and_write_time = t_write.elapsed();

    let mut file_hashes = BTreeMap::new();
    let mut files_written = 0usize;
    let mut bytes_skipped = 0u64;

    for r in results {
        let (path, size, written) = r?;
        if !written {
            files_skipped += 1;
            bytes_skipped += size;
        } else {
            file_hashes.insert(path, String::new()); // no hash in prod
            files_written += 1;
        }
    }

    let walk_extract_time = tree_walk_time + dir_create_time + resolve_and_write_time;
    let extract_duration = extract_start.elapsed();

    println!(
        "    phases: tree_walk={:.2?} mkdir={:.2?} resolve_only={:.2?} resolve+write={:.2?} (write_cost~={:.2?})",
        tree_walk_time, dir_create_time, resolve_only_time, resolve_and_write_time,
        resolve_and_write_time.saturating_sub(resolve_only_time),
    );

    Ok(GixResult {
        bench: BenchResult {
            method: "H: bundled idx".to_string(),
            git_cmd_time: cmd_duration,
            transfer_bytes: output_bytes + idx_bytes,
            extract_time: extract_duration,
            total_time: cmd_duration + extract_duration,
            file_count: files_written + files_skipped,
            file_hashes,
        },
        files_written, files_skipped, bytes_skipped, index_pack_time, walk_extract_time,
    })
}

/// Walk tree, but only collect blob (path, oid) pairs -- don't read blob content.
fn collect_blobs(
    bundle: &gix_pack::Bundle,
    tree_oid: &gix_hash::oid,
    prefix: &Path,
    inflate: &mut zlib::Inflate,
    buf: &mut Vec<u8>,
    cache: &mut dyn gix_pack::cache::DecodeEntry,
    entries: &mut Vec<(String, gix_hash::ObjectId)>,
    files_skipped: &mut usize,
) -> Result<(), crate::BenchError> {
    buf.clear();
    let (data, _) = bundle
        .find(tree_oid, buf, inflate, cache)
        .map_err(|e| crate::BenchError::Extract(format!("find tree: {e}")))?
        .ok_or_else(|| crate::BenchError::Extract(format!("tree not found")))?;

    let tree_data = data.data.to_vec();
    let tree = gix_object::TreeRefIter::from_bytes(&tree_data, gix_hash::Kind::Sha1);

    let parsed: Vec<_> = tree
        .map(|e| e.map_err(|e| crate::BenchError::Extract(format!("tree parse: {e}"))))
        .collect::<Result<Vec<_>, _>>()?;

    for entry in parsed {
        let name = entry.filename.to_string();
        let entry_path = prefix.join(&name);

        match entry.mode.kind() {
            gix_object::tree::EntryKind::Tree => {
                collect_blobs(bundle, entry.oid, &entry_path, inflate, buf, cache, entries, files_skipped)?;
            }
            gix_object::tree::EntryKind::Blob | gix_object::tree::EntryKind::BlobExecutable => {
                let path_str = entry_path.to_string_lossy().to_string();
                if is_excluded(&path_str) {
                    *files_skipped += 1;
                    continue;
                }
                entries.push((path_str, entry.oid.to_owned()));
            }
            gix_object::tree::EntryKind::Link => {
                *files_skipped += 1;
            }
            _ => {}
        }
    }
    Ok(())
}

// ─── Shared helpers ─────────────────────────────────────────────────────────

fn walk_tree(
    bundle: &gix_pack::Bundle,
    tree_oid: &gix_hash::oid,
    prefix: &Path,
    output_dir: &Path,
    inflate: &mut zlib::Inflate,
    buf: &mut Vec<u8>,
    cache: &mut dyn gix_pack::cache::DecodeEntry,
    file_hashes: &mut BTreeMap<String, String>,
    files_written: &mut usize,
    files_skipped: &mut usize,
    bytes_skipped: &mut u64,
) -> Result<(), crate::BenchError> {
    buf.clear();
    let (data, _loc) = bundle
        .find(tree_oid, buf, inflate, cache)
        .map_err(|e| crate::BenchError::Extract(format!("find tree {tree_oid}: {e}")))?
        .ok_or_else(|| crate::BenchError::Extract(format!("tree {tree_oid} not found")))?;

    let tree_data = data.data.to_vec();
    let tree = gix_object::TreeRefIter::from_bytes(&tree_data, gix_hash::Kind::Sha1);

    let entries: Vec<_> = tree
        .map(|e| e.map_err(|e| crate::BenchError::Extract(format!("tree parse: {e}"))))
        .collect::<Result<Vec<_>, _>>()?;

    for entry in entries {
        let name = entry.filename.to_string();
        let entry_path = prefix.join(&name);

        match entry.mode.kind() {
            gix_object::tree::EntryKind::Tree => {
                walk_tree(
                    bundle, entry.oid, &entry_path, output_dir,
                    inflate, buf, cache,
                    file_hashes, files_written, files_skipped, bytes_skipped,
                )?;
            }
            gix_object::tree::EntryKind::Blob | gix_object::tree::EntryKind::BlobExecutable => {
                let path_str = entry_path.to_string_lossy().to_string();
                if is_excluded(&path_str) {
                    *files_skipped += 1;
                    continue;
                }

                buf.clear();
                let (blob_data, _) = bundle
                    .find(entry.oid, buf, inflate, cache)
                    .map_err(|e| crate::BenchError::Extract(format!("find blob: {e}")))?
                    .ok_or_else(|| crate::BenchError::Extract(format!("blob {} not found", entry.oid)))?;

                let size = blob_data.data.len() as u64;
                if size > MAX_FILE_SIZE {
                    *files_skipped += 1;
                    *bytes_skipped += size;
                    continue;
                }

                let hash = hex_sha256(blob_data.data);
                let dest = output_dir.join(&entry_path);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| crate::BenchError::Extract(e.to_string()))?;
                }
                std::fs::write(&dest, blob_data.data)
                    .map_err(|e| crate::BenchError::Extract(e.to_string()))?;

                file_hashes.insert(path_str, hash);
                *files_written += 1;
            }
            gix_object::tree::EntryKind::Link => {
                *files_skipped += 1;
            }
            _ => {}
        }
    }
    Ok(())
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

fn parse_oid(hex: &str) -> Result<gix_hash::ObjectId, crate::BenchError> {
    gix_hash::ObjectId::from_hex(hex.as_bytes())
        .map_err(|e| crate::BenchError::Extract(format!("parse oid: {e}")))
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}
