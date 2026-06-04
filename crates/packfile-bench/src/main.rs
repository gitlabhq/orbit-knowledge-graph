//! Benchmark harness for comparing git object transfer methods.
//!
//! Each method implements the `Method` trait and is registered with a single-char key.
//! Run any combination with `--methods <chars>`.
//!
//! Usage:
//!   packfile-bench --repo /path/to/repo [--commit HEAD] [--iterations 3] [--methods agh]
//!   packfile-bench --repo repo1,repo2,repo3

mod archive;
mod filtered;
mod gix_extract;
mod packfile;
mod packfile_checkout;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use clap::Parser;

// ─── Core types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct BenchResult {
    pub method: String,
    pub git_cmd_time: Duration,
    pub transfer_bytes: u64,
    pub extract_time: Duration,
    pub total_time: Duration,
    pub file_count: usize,
    pub file_hashes: BTreeMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("git error: {0}")]
    Git(String),
    #[error("extraction error: {0}")]
    Extract(String),
}

/// Extra detail lines printed after the main timing line.
pub struct MethodOutput {
    pub result: BenchResult,
    pub detail: Option<String>,
}

// ─── Method trait ───────────────────────────────────────────────────────────

pub trait Method {
    /// Single-char key used in `--methods`.
    fn key(&self) -> char;

    /// Short label for display (max ~20 chars).
    fn label(&self) -> &'static str;

    /// Run the method once. `output_dir` is a fresh temp directory.
    fn run(
        &self,
        repo_path: &Path,
        commit: &str,
        output_dir: &Path,
    ) -> Result<MethodOutput, BenchError>;
}

// ─── Method implementations ─────────────────────────────────────────────────

struct ArchiveMethod;
impl Method for ArchiveMethod {
    fn key(&self) -> char { 'a' }
    fn label(&self) -> &'static str { "A: archive | gzip" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        archive::run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

struct PackCatfileMethod;
impl Method for PackCatfileMethod {
    fn key(&self) -> char { 'b' }
    fn label(&self) -> &'static str { "B: pack + cat-file" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        packfile::run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

struct PackCheckoutMethod;
impl Method for PackCheckoutMethod {
    fn key(&self) -> char { 'c' }
    fn label(&self) -> &'static str { "C: pack + checkout" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        packfile_checkout::run(repo, commit, out).map(|r| MethodOutput { result: r, detail: None })
    }
}

struct FilteredMethod;
impl Method for FilteredMethod {
    fn key(&self) -> char { 'd' }
    fn label(&self) -> &'static str { "D: pack + filtered" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        filtered::run(repo, commit, out).map(|fr| MethodOutput {
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

fn gix_detail(gr: &gix_extract::GixResult) -> String {
    format!(
        "    wrote={} skipped={} ({} skipped)  idx-pack={:.2?} walk+extract={:.2?}",
        gr.files_written, gr.files_skipped,
        format_bytes(gr.bytes_skipped),
        gr.index_pack_time, gr.walk_extract_time,
    )
}

struct GixMethod;
impl Method for GixMethod {
    fn key(&self) -> char { 'e' }
    fn label(&self) -> &'static str { "E: pack + gix" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        gix_extract::run_e(repo, commit, out).map(|gr| MethodOutput {
            detail: Some(gix_detail(&gr)),
            result: gr.bench,
        })
    }
}

struct NodeltaMethod;
impl Method for NodeltaMethod {
    fn key(&self) -> char { 'f' }
    fn label(&self) -> &'static str { "F: nodelta + gix" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        gix_extract::run_f(repo, commit, out).map(|gr| MethodOutput {
            detail: Some(gix_detail(&gr)),
            result: gr.bench,
        })
    }
}

struct RayonMethod;
impl Method for RayonMethod {
    fn key(&self) -> char { 'g' }
    fn label(&self) -> &'static str { "G: pack + rayon" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        gix_extract::run_g(repo, commit, out).map(|gr| MethodOutput {
            detail: Some(gix_detail(&gr)),
            result: gr.bench,
        })
    }
}

struct BundledIdxMethod;
impl Method for BundledIdxMethod {
    fn key(&self) -> char { 'h' }
    fn label(&self) -> &'static str { "H: bundled idx" }
    fn run(&self, repo: &Path, commit: &str, out: &Path) -> Result<MethodOutput, BenchError> {
        gix_extract::run_h(repo, commit, out).map(|gr| MethodOutput {
            detail: Some(gix_detail(&gr)),
            result: gr.bench,
        })
    }
}

/// All available methods in order.
fn all_methods() -> Vec<Box<dyn Method>> {
    vec![
        Box::new(ArchiveMethod),
        Box::new(PackCatfileMethod),
        Box::new(PackCheckoutMethod),
        Box::new(FilteredMethod),
        Box::new(GixMethod),
        Box::new(NodeltaMethod),
        Box::new(RayonMethod),
        Box::new(BundledIdxMethod),
    ]
}

// ─── CLI ────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "packfile-bench", about = "Benchmark: git archive vs packfile transfer methods")]
struct Cli {
    /// Path to a git repository (comma-separated for batch mode)
    #[arg(short, long, required_unless_present = "list")]
    repo: Option<String>,

    /// Git commit/ref to test against
    #[arg(short, long, default_value = "HEAD")]
    commit: String,

    /// Number of iterations per method
    #[arg(short, long, default_value = "3")]
    iterations: usize,

    /// Which methods to run (combine chars). Use --list to see available methods.
    #[arg(short, long, default_value = "agh")]
    methods: String,

    /// List available methods and exit
    #[arg(long)]
    list: bool,
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();
    let methods = all_methods();

    if cli.list {
        println!("Available methods:");
        for m in &methods {
            println!("  {} = {}", m.key(), m.label());
        }
        return;
    }

    let active: Vec<&dyn Method> = methods
        .iter()
        .filter(|m| cli.methods.contains(m.key()))
        .map(|m| m.as_ref())
        .collect();

    if active.is_empty() {
        eprintln!("No methods selected. Use --list to see available methods.");
        return;
    }

    let repo_str = cli.repo.expect("--repo is required when not using --list");
    let repos: Vec<&str> = repo_str.split(',').collect();

    for repo_str in &repos {
        let repo_path = PathBuf::from(repo_str.trim());
        if !repo_path.join(".git").exists() && !repo_path.join("HEAD").exists() {
            eprintln!("ERROR: {} is not a git repository", repo_path.display());
            continue;
        }

        let commit = resolve_commit(&repo_path, &cli.commit);
        let short_sha = &commit[..8.min(commit.len())];

        println!();
        println!("================================================================");
        println!("Repository: {}", repo_path.display());
        println!("Commit:     {} ({})", short_sha, &cli.commit);
        println!("Iterations: {}", cli.iterations);
        println!("Methods:    {} ({})",
            cli.methods,
            active.iter().map(|m| m.label()).collect::<Vec<_>>().join(", "),
        );
        println!("================================================================");

        print_repo_stats(&repo_path, &commit);

        // Collect results per method key
        let mut results: BTreeMap<char, Vec<BenchResult>> = BTreeMap::new();
        for m in &active {
            results.insert(m.key(), Vec::new());
        }

        for i in 0..cli.iterations {
            println!("\n--- Iteration {}/{} ---", i + 1, cli.iterations);

            for m in &active {
                let tmp = tempfile::tempdir().expect("create tempdir");
                match m.run(&repo_path, &commit, tmp.path()) {
                    Ok(output) => {
                        print_iteration_line(&output.result);
                        if let Some(detail) = &output.detail {
                            println!("{detail}");
                        }
                        results.get_mut(&m.key()).unwrap().push(output.result);
                    }
                    Err(e) => eprintln!("  {:<20} FAILED: {e}", m.label()),
                }
            }
        }

        // Validate correctness: compare every pair that both have file hashes
        let keys_with_results: Vec<char> = active
            .iter()
            .filter(|m| {
                results.get(&m.key())
                    .map(|r| r.first().map(|r| !r.file_hashes.is_empty()).unwrap_or(false))
                    .unwrap_or(false)
            })
            .map(|m| m.key())
            .collect();

        if keys_with_results.len() >= 2 {
            let first_key = keys_with_results[0];
            let first = results[&first_key].first().unwrap();
            for &other_key in &keys_with_results[1..] {
                let other = results[&other_key].first().unwrap();
                validate_correctness(first, other);
            }
        }

        // Print summary
        print_summary(&active, &results);
    }
}

// ─── Output helpers ─────────────────────────────────────────────────────────

fn print_iteration_line(r: &BenchResult) {
    println!(
        "  {:<20} cmd={:>8.2?}  extract={:>8.2?}  total={:>8.2?}  bytes={:>10}  files={}",
        r.method, r.git_cmd_time, r.extract_time, r.total_time,
        format_bytes(r.transfer_bytes), r.file_count
    );
}

fn resolve_commit(repo_path: &Path, refspec: &str) -> String {
    let output = Command::new("git")
        .args(["rev-parse", refspec])
        .current_dir(repo_path)
        .output()
        .expect("git rev-parse");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn print_repo_stats(repo_path: &Path, commit: &str) {
    let output = Command::new("git")
        .args(["rev-list", "--objects", "--count", &format!("{commit}^{{tree}}")])
        .current_dir(repo_path)
        .output();

    if let Ok(out) = output {
        let count = String::from_utf8_lossy(&out.stdout).trim().to_string();
        println!("Objects in tree: {count}");
    }

    let output = Command::new("git")
        .args(["count-objects", "-v"])
        .current_dir(repo_path)
        .output();

    if let Ok(out) = output {
        let stats = String::from_utf8_lossy(&out.stdout);
        for line in stats.lines() {
            if line.starts_with("size-pack:") || line.starts_with("in-pack:") || line.starts_with("packs:") {
                println!("{}", line.trim());
            }
        }
    }
}

fn validate_correctness(a: &BenchResult, b: &BenchResult) {
    println!("\n--- Correctness: {} vs {} ---", a.method, b.method);

    if a.file_count != b.file_count {
        println!(
            "  MISMATCH: {} has {} files, {} has {} files",
            a.method, a.file_count, b.method, b.file_count
        );

        let a_keys: std::collections::BTreeSet<_> = a.file_hashes.keys().collect();
        let b_keys: std::collections::BTreeSet<_> = b.file_hashes.keys().collect();

        let only_in_a: Vec<_> = a_keys.difference(&b_keys).take(10).collect();
        let only_in_b: Vec<_> = b_keys.difference(&a_keys).take(10).collect();

        if !only_in_a.is_empty() {
            println!("  Only in {}: {:?}", a.method, only_in_a);
        }
        if !only_in_b.is_empty() {
            println!("  Only in {}: {:?}", b.method, only_in_b);
        }
    } else {
        println!("  File count: OK ({} files)", a.file_count);
    }

    let mut mismatches = 0;
    for (path, a_hash) in &a.file_hashes {
        if a_hash.is_empty() {
            continue; // skip methods that don't hash
        }
        if let Some(b_hash) = b.file_hashes.get(path) {
            if !b_hash.is_empty() && a_hash != b_hash {
                mismatches += 1;
                if mismatches <= 5 {
                    println!("  CONTENT MISMATCH: {path}");
                }
            }
        }
    }

    if mismatches == 0 {
        println!("  Content hashes: OK");
    } else {
        println!("  CONTENT MISMATCHES: {mismatches} files differ");
    }
}

fn print_summary(active: &[&dyn Method], results: &BTreeMap<char, Vec<BenchResult>>) {
    println!("\n================================================================");
    println!("SUMMARY (averages)");
    println!("================================================================");

    let avg_dur = |rs: &[BenchResult], f: fn(&BenchResult) -> Duration| -> Duration {
        if rs.is_empty() { return Duration::ZERO; }
        rs.iter().map(f).sum::<Duration>() / rs.len() as u32
    };

    let avg_bytes = |rs: &[BenchResult]| -> u64 {
        if rs.is_empty() { return 0; }
        rs.iter().map(|r| r.transfer_bytes).sum::<u64>() / rs.len() as u64
    };

    struct Row {
        label: String,
        cmd: Duration,
        ext: Duration,
        tot: Duration,
        bytes: u64,
    }

    let rows: Vec<Row> = active
        .iter()
        .filter_map(|m| {
            let rs = results.get(&m.key())?;
            if rs.is_empty() { return None; }
            Some(Row {
                label: m.label().to_string(),
                cmd: avg_dur(rs, |r| r.git_cmd_time),
                ext: avg_dur(rs, |r| r.extract_time),
                tot: avg_dur(rs, |r| r.total_time),
                bytes: avg_bytes(rs),
            })
        })
        .collect();

    if rows.is_empty() { return; }

    println!();
    println!("  {:<24} {:>12} {:>12} {:>12} {:>12}",
        "", "Git Cmd", "Extract", "Total", "Bytes");
    println!("  {:<24} {:>12} {:>12} {:>12} {:>12}",
        "-".repeat(24), "-".repeat(12), "-".repeat(12), "-".repeat(12), "-".repeat(12));

    for row in &rows {
        println!(
            "  {:<24} {:>12.2?} {:>12.2?} {:>12.2?} {:>12}",
            row.label, row.cmd, row.ext, row.tot, format_bytes(row.bytes)
        );
    }

    // Compare each method against the first
    if rows.len() >= 2 {
        let baseline = &rows[0];
        for row in &rows[1..] {
            if baseline.tot.as_secs_f64() == 0.0 { continue; }

            let cmd_ratio = row.cmd.as_secs_f64() / baseline.cmd.as_secs_f64();
            let ext_ratio = row.ext.as_secs_f64() / baseline.ext.as_secs_f64();
            let tot_ratio = row.tot.as_secs_f64() / baseline.tot.as_secs_f64();
            let bytes_ratio = if baseline.bytes > 0 { row.bytes as f64 / baseline.bytes as f64 } else { 0.0 };

            println!();
            println!("  {} vs {}:", row.label, baseline.label);
            println!("    Git command time:  {:.2}x ({})", cmd_ratio, if cmd_ratio < 1.0 { "faster" } else { "slower" });
            println!("    Extract time:      {:.2}x ({})", ext_ratio, if ext_ratio < 1.0 { "faster" } else { "slower" });
            println!("    Total time:        {:.2}x ({})", tot_ratio, if tot_ratio < 1.0 { "faster" } else { "slower" });
            println!("    Transfer size:     {:.2}x ({})", bytes_ratio, if bytes_ratio < 1.0 { "smaller" } else { "larger" });

            if tot_ratio < 1.0 {
                println!("    --> {:.0}% faster end-to-end", (1.0 - tot_ratio) * 100.0);
            }
        }
    }
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
