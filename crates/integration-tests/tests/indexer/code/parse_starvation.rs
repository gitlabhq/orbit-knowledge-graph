//! Measures whether a wide parse starves concurrent archive extraction, which is the
//! mechanism behind download-phase job timeouts in the code pool. Set `PARSE_THREADS` to
//! pick the rayon width under test; `0` reproduces production's "use every core".

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use code_graph::v2::config::{CodeFilter, FilterSkip, detect_language_from_path};
use code_graph::v2::linker::CodeGraph;
use code_graph::v2::{
    Decision, FileInventoryEntry, GraphConverter, Pipeline, PipelineConfig, SinkError,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use gkg_utils::archive::extract_tar_gz;
use gkg_utils::walk::walk_dir;
use rustc_hash::FxHashMap;

const ARCHIVE_FILES: usize = 600;
const EXTRACT_REPS: usize = 5;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct NullConverter;

impl GraphConverter for NullConverter {
    fn convert(
        &self,
        _graph: CodeGraph,
    ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, SinkError> {
        Ok(Vec::new())
    }
}

fn java_source(i: usize) -> String {
    let mut s = format!("package p{};\n\npublic class C{} {{\n", i % 50, i);
    for m in 0..12 {
        s.push_str(&format!(
            "  public int m{m}(int a, int b) {{ int c = a + b; for (int i = 0; i < b; i++) {{ c += i * a; }} return c; }}\n"
        ));
    }
    s.push_str("}\n");
    s
}

fn write_parse_tree(root: &Path, count: usize) -> Vec<FileInventoryEntry> {
    let mut inventory = Vec::with_capacity(count);
    for i in 0..count {
        let rel = format!("src/p{}/C{}.java", i % 50, i);
        let path = root.join(&rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let body = java_source(i);
        std::fs::write(&path, &body).unwrap();
        inventory.push(FileInventoryEntry {
            path: rel,
            size: body.len() as u64,
            decision: Decision::Parse,
        });
    }
    inventory
}

fn build_archive(count: usize) -> Vec<u8> {
    let mut tb = tar::Builder::new(Vec::new());
    for i in 0..count {
        let body = java_source(i).into_bytes();
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tb.append_data(
            &mut h,
            format!("repo/src/q{}/D{}.java", i % 50, i),
            &body[..],
        )
        .unwrap();
    }
    let tar_bytes = tb.into_inner().unwrap();
    let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(&tar_bytes).unwrap();
    enc.finish().unwrap()
}

fn extract_once(archive: &[u8]) -> Duration {
    let dir = tempfile::TempDir::new().unwrap();
    let mut filter = CodeFilter::new(5_000_000, 2_000_000_000, detect_language_from_path);
    let started = Instant::now();
    extract_tar_gz(std::io::Cursor::new(archive), dir.path(), &mut filter).unwrap();
    started.elapsed()
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn sample_peak_rss_kib(stop: Arc<AtomicBool>, peak: Arc<AtomicU64>) {
    let pid = std::process::id();
    while !stop.load(Ordering::Relaxed) {
        if let Ok(out) = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            && let Ok(text) = String::from_utf8(out.stdout)
            && let Ok(kib) = text.trim().parse::<u64>()
        {
            peak.fetch_max(kib, Ordering::Relaxed);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn parse_width_versus_extraction_latency() {
    let threads: usize = std::env::var("PARSE_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let tree = tempfile::TempDir::new().unwrap();
    let real_root = std::env::var("REAL_ROOT").ok();
    let (root_path, inventory) = match &real_root {
        Some(dir) => {
            let mut filter = CodeFilter::new(5_000_000, 0, detect_language_from_path);
            let inv = walk_dir(Path::new(dir), &mut filter).unwrap();
            (std::path::PathBuf::from(dir), inv)
        }
        None => {
            let n = env_usize("PARSE_FILES", 4000);
            let inv = write_parse_tree(tree.path(), n);
            (tree.path().to_path_buf(), inv)
        }
    };
    let parse_files = inventory
        .iter()
        .filter(|e| e.decision == Decision::Parse)
        .count();
    let archive = build_archive(ARCHIVE_FILES);

    let idle: Vec<Duration> = (0..EXTRACT_REPS).map(|_| extract_once(&archive)).collect();
    let idle_median = median(idle);

    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(0));
    let sampler = {
        let (stop, peak) = (stop.clone(), peak.clone());
        std::thread::spawn(move || sample_peak_rss_kib(stop, peak))
    };

    let parse_done = Arc::new(AtomicBool::new(false));
    let parse_elapsed = Arc::new(Mutex::new(Duration::ZERO));
    let parser = {
        let root = root_path.clone();
        let inventory = inventory.clone();
        let parse_done = parse_done.clone();
        let parse_elapsed = parse_elapsed.clone();
        std::thread::spawn(move || {
            let reasons: FxHashMap<String, FilterSkip> = FxHashMap::default();
            let on_batch: Arc<code_graph::v2::OnBatch> =
                Arc::new(|_: &str, _: arrow::record_batch::RecordBatch| Ok(()));
            let config = PipelineConfig {
                worker_threads: threads,
                max_files: env_usize("MAX_FILES", 0),
                ..PipelineConfig::default()
            };
            let started = Instant::now();
            let _ = Pipeline::run(
                &root,
                Arc::from(inventory),
                config,
                &reasons,
                Arc::new(NullConverter) as Arc<dyn GraphConverter>,
                on_batch,
            );
            *parse_elapsed.lock().unwrap() = started.elapsed();
            parse_done.store(true, Ordering::Release);
        })
    };

    let mut loaded = Vec::new();
    while !parse_done.load(Ordering::Acquire) {
        loaded.push(extract_once(&archive));
    }
    parser.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();

    let loaded_median = median(loaded.clone());
    let parse_secs = parse_elapsed.lock().unwrap().as_secs_f64();

    println!("---- parse width vs extraction latency ----");
    println!("cores available           : {cores}");
    println!(
        "PARSE_THREADS             : {threads}{}",
        if threads == 0 {
            " (0 = every core, production default)"
        } else {
            ""
        }
    );
    println!(
        "source                    : {}",
        real_root.as_deref().unwrap_or("synthetic java")
    );
    println!("inventory entries         : {}", inventory.len());
    println!("parseable files           : {parse_files}");
    println!(
        "max_files cap             : {}",
        match env_usize("MAX_FILES", 0) {
            0 => "none".to_string(),
            n => n.to_string(),
        }
    );
    println!("parse wall time           : {parse_secs:.2}s");
    let peak_kib = peak.load(Ordering::Relaxed);
    println!("peak process RSS          : {} MiB", peak_kib / 1024);
    if parse_files > 0 {
        println!(
            "peak RSS per parsed file  : {:.1} KiB",
            peak_kib as f64 / parse_files as f64
        );
    }
    println!(
        "extract median, idle      : {:.3}s",
        idle_median.as_secs_f64()
    );
    println!(
        "extract median, under load: {:.3}s",
        loaded_median.as_secs_f64()
    );
    println!(
        "EXTRACTION SLOWDOWN       : {:.1}x  (over {} samples)",
        loaded_median.as_secs_f64() / idle_median.as_secs_f64().max(1e-9),
        loaded.len()
    );
}
