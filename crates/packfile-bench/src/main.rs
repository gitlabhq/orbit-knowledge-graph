//! A/B/C benchmark comparing:
//!   A) git archive | gzip  (current GKG path via Gitaly GetArchive)
//!   B) rev-list | pack-objects + cat-file --batch  (per-blob IPC)
//!   C) rev-list | pack-objects + unpack-objects + checkout-index  (batch extraction)
//!
//! Usage:
//!   packfile-bench --repo /path/to/repo [--commit HEAD] [--iterations 3]
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

#[derive(Parser)]
#[command(name = "packfile-bench", about = "A/B/C benchmark: git archive vs packfile transfer")]
struct Cli {
    /// Path to a git repository (or comma-separated list for batch mode)
    #[arg(short, long)]
    repo: String,

    /// Git commit to test against (default: HEAD)
    #[arg(short, long, default_value = "HEAD")]
    commit: String,

    /// Number of iterations per method
    #[arg(short, long, default_value = "3")]
    iterations: usize,

    /// Which methods to run:
    ///   a=archive, b=pack+cat-file, c=pack+checkout, d=pack+filtered,
    ///   e=pack+gix, f=nodelta+gix, g=pack+rayon
    /// Combine letters, e.g. --methods efg
    #[arg(short, long, default_value = "efg")]
    methods: String,
}

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

fn main() {
    let cli = Cli::parse();

    let repos: Vec<&str> = cli.repo.split(',').collect();

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
        println!("================================================================");

        print_repo_stats(&repo_path, &commit);

        let run_a = cli.methods.contains('a');
        let run_b = cli.methods.contains('b');
        let run_c = cli.methods.contains('c');
        let run_d = cli.methods.contains('d');
        let run_e = cli.methods.contains('e');
        let run_f = cli.methods.contains('f');
        let run_g = cli.methods.contains('g');
        let run_h = cli.methods.contains('h');

        println!("Methods:    {}", cli.methods);

        let mut archive_results: Vec<BenchResult> = Vec::new();
        let mut packfile_results: Vec<BenchResult> = Vec::new();
        let mut checkout_results: Vec<BenchResult> = Vec::new();
        let mut filtered_results: Vec<BenchResult> = Vec::new();
        let mut gix_results: Vec<BenchResult> = Vec::new();
        let mut nodelta_results: Vec<BenchResult> = Vec::new();
        let mut rayon_results: Vec<BenchResult> = Vec::new();
        let mut full_gix_results: Vec<BenchResult> = Vec::new();

        for i in 0..cli.iterations {
            println!("\n--- Iteration {}/{} ---", i + 1, cli.iterations);

            if run_a {
                let tmp_a = tempfile::tempdir().expect("create tempdir");
                match archive::run(&repo_path, &commit, tmp_a.path()) {
                    Ok(result) => {
                        print_iteration_line(&result);
                        archive_results.push(result);
                    }
                    Err(e) => eprintln!("  archive          FAILED: {e}"),
                }
            }

            if run_b {
                let tmp_b = tempfile::tempdir().expect("create tempdir");
                match packfile::run(&repo_path, &commit, tmp_b.path()) {
                    Ok(result) => {
                        print_iteration_line(&result);
                        packfile_results.push(result);
                    }
                    Err(e) => eprintln!("  pack + cat-file  FAILED: {e}"),
                }
            }

            if run_c {
                let tmp_c = tempfile::tempdir().expect("create tempdir");
                match packfile_checkout::run(&repo_path, &commit, tmp_c.path()) {
                    Ok(result) => {
                        print_iteration_line(&result);
                        checkout_results.push(result);
                    }
                    Err(e) => eprintln!("  pack + checkout  FAILED: {e}"),
                }
            }

            if run_d {
                let tmp_d = tempfile::tempdir().expect("create tempdir");
                match filtered::run(&repo_path, &commit, tmp_d.path()) {
                    Ok(fr) => {
                        print_iteration_line(&fr.bench);
                        println!(
                            "    filter: wrote={} skipped={} ({} skipped)  ls-tree={:.2?} cat-file={:.2?}",
                            fr.files_written, fr.files_skipped,
                            format_bytes(fr.bytes_skipped),
                            fr.ls_tree_time, fr.cat_file_time,
                        );
                        filtered_results.push(fr.bench);
                    }
                    Err(e) => eprintln!("  pack + filtered  FAILED: {e}"),
                }
            }

            if run_e {
                let tmp_e = tempfile::tempdir().expect("create tempdir");
                match gix_extract::run_e(&repo_path, &commit, tmp_e.path()) {
                    Ok(gr) => {
                        print_gix_line(&gr);
                        gix_results.push(gr.bench);
                    }
                    Err(e) => eprintln!("  pack+gix         FAILED: {e}"),
                }
            }

            if run_f {
                let tmp_f = tempfile::tempdir().expect("create tempdir");
                match gix_extract::run_f(&repo_path, &commit, tmp_f.path()) {
                    Ok(gr) => {
                        print_gix_line(&gr);
                        nodelta_results.push(gr.bench);
                    }
                    Err(e) => eprintln!("  nodelta+gix      FAILED: {e}"),
                }
            }

            if run_g {
                let tmp_g = tempfile::tempdir().expect("create tempdir");
                match gix_extract::run_g(&repo_path, &commit, tmp_g.path()) {
                    Ok(gr) => {
                        print_gix_line(&gr);
                        rayon_results.push(gr.bench);
                    }
                    Err(e) => eprintln!("  pack+rayon       FAILED: {e}"),
                }
            }

            if run_h {
                let tmp_h = tempfile::tempdir().expect("create tempdir");
                match gix_extract::run_h(&repo_path, &commit, tmp_h.path()) {
                    Ok(gr) => {
                        print_gix_line(&gr);
                        full_gix_results.push(gr.bench);
                    }
                    Err(e) => eprintln!("  full gix+rayon   FAILED: {e}"),
                }
            }
        }

        // Validate correctness between whichever pairs we have
        if let (Some(a), Some(c)) = (archive_results.first(), checkout_results.first()) {
            validate_correctness("archive vs checkout", a, c);
        }
        if let (Some(a), Some(b)) = (archive_results.first(), packfile_results.first()) {
            validate_correctness("archive vs pack+cat-file", a, b);
        }

        // Print summary
        if archive_results.is_empty() && packfile_results.is_empty()
            && checkout_results.is_empty() && filtered_results.is_empty()
            && gix_results.is_empty() && nodelta_results.is_empty() && rayon_results.is_empty()
        {
            continue;
        }
        {
            print_summary(&archive_results, &packfile_results, &checkout_results,
                &filtered_results, &gix_results, &nodelta_results, &rayon_results, &full_gix_results);
        }
    }
}

fn print_iteration_line(r: &BenchResult) {
    println!(
        "  {:<20} cmd={:>8.2?}  extract={:>8.2?}  total={:>8.2?}  bytes={:>10}  files={}",
        r.method, r.git_cmd_time, r.extract_time, r.total_time,
        format_bytes(r.transfer_bytes), r.file_count
    );
}

fn print_gix_line(gr: &gix_extract::GixResult) {
    print_iteration_line(&gr.bench);
    println!(
        "    wrote={} skipped={} ({} skipped)  idx-pack={:.2?} walk+extract={:.2?}",
        gr.files_written, gr.files_skipped,
        format_bytes(gr.bytes_skipped),
        gr.index_pack_time, gr.walk_extract_time,
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

fn validate_correctness(label: &str, a: &BenchResult, b: &BenchResult) {
    println!("\n--- Correctness: {label} ---");

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
        if let Some(b_hash) = b.file_hashes.get(path) {
            if a_hash != b_hash {
                mismatches += 1;
                if mismatches <= 5 {
                    println!("  CONTENT MISMATCH: {path}");
                }
            }
        }
    }

    if mismatches == 0 {
        println!("  Content hashes: OK (all files match)");
    } else {
        println!("  CONTENT MISMATCHES: {mismatches} files differ");
    }
}

fn print_summary(
    archive_results: &[BenchResult],
    packfile_results: &[BenchResult],
    checkout_results: &[BenchResult],
    filtered_results: &[BenchResult],
    gix_results: &[BenchResult],
    nodelta_results: &[BenchResult],
    rayon_results: &[BenchResult],
    full_gix_results: &[BenchResult],
) {
    println!("\n================================================================");
    println!("SUMMARY (averages)");
    println!("================================================================");

    let avg_dur = |results: &[BenchResult], f: fn(&BenchResult) -> Duration| -> Duration {
        if results.is_empty() { return Duration::ZERO; }
        let total: Duration = results.iter().map(f).sum();
        total / results.len() as u32
    };

    let avg_bytes = |results: &[BenchResult]| -> u64 {
        if results.is_empty() { return 0; }
        results.iter().map(|r| r.transfer_bytes).sum::<u64>() / results.len() as u64
    };

    struct Row {
        label: &'static str,
        cmd: Duration,
        ext: Duration,
        tot: Duration,
        bytes: u64,
    }

    let rows = [
        Row {
            label: "A: archive | gzip",
            cmd: avg_dur(archive_results, |r| r.git_cmd_time),
            ext: avg_dur(archive_results, |r| r.extract_time),
            tot: avg_dur(archive_results, |r| r.total_time),
            bytes: avg_bytes(archive_results),
        },
        Row {
            label: "B: pack + cat-file",
            cmd: avg_dur(packfile_results, |r| r.git_cmd_time),
            ext: avg_dur(packfile_results, |r| r.extract_time),
            tot: avg_dur(packfile_results, |r| r.total_time),
            bytes: avg_bytes(packfile_results),
        },
        Row {
            label: "C: pack + checkout",
            cmd: avg_dur(checkout_results, |r| r.git_cmd_time),
            ext: avg_dur(checkout_results, |r| r.extract_time),
            tot: avg_dur(checkout_results, |r| r.total_time),
            bytes: avg_bytes(checkout_results),
        },
        Row {
            label: "D: pack + filtered",
            cmd: avg_dur(filtered_results, |r| r.git_cmd_time),
            ext: avg_dur(filtered_results, |r| r.extract_time),
            tot: avg_dur(filtered_results, |r| r.total_time),
            bytes: avg_bytes(filtered_results),
        },
        Row {
            label: "E: pack + gix",
            cmd: avg_dur(gix_results, |r| r.git_cmd_time),
            ext: avg_dur(gix_results, |r| r.extract_time),
            tot: avg_dur(gix_results, |r| r.total_time),
            bytes: avg_bytes(gix_results),
        },
        Row {
            label: "F: nodelta + gix",
            cmd: avg_dur(nodelta_results, |r| r.git_cmd_time),
            ext: avg_dur(nodelta_results, |r| r.extract_time),
            tot: avg_dur(nodelta_results, |r| r.total_time),
            bytes: avg_bytes(nodelta_results),
        },
        Row {
            label: "G: pack + rayon",
            cmd: avg_dur(rayon_results, |r| r.git_cmd_time),
            ext: avg_dur(rayon_results, |r| r.extract_time),
            tot: avg_dur(rayon_results, |r| r.total_time),
            bytes: avg_bytes(rayon_results),
        },
        Row {
            label: "H: full gix+rayon",
            cmd: avg_dur(full_gix_results, |r| r.git_cmd_time),
            ext: avg_dur(full_gix_results, |r| r.extract_time),
            tot: avg_dur(full_gix_results, |r| r.total_time),
            bytes: avg_bytes(full_gix_results),
        },
    ];

    println!();
    println!("  {:<24} {:>12} {:>12} {:>12} {:>12}", "", "Git Cmd", "Extract", "Total", "Bytes");
    println!("  {:<24} {:>12} {:>12} {:>12} {:>12}",
        "-".repeat(24), "-".repeat(12), "-".repeat(12), "-".repeat(12), "-".repeat(12));

    for row in &rows {
        if row.tot == Duration::ZERO { continue }
        println!(
            "  {:<24} {:>12.2?} {:>12.2?} {:>12.2?} {:>12}",
            row.label, row.cmd, row.ext, row.tot, format_bytes(row.bytes)
        );
    }

    // Compare D vs A if both present
    let a = &rows[0];
    let d = &rows[3];
    if a.tot > Duration::ZERO && d.tot > Duration::ZERO {
        println!();
        let cmd_ratio = d.cmd.as_secs_f64() / a.cmd.as_secs_f64();
        let ext_ratio = d.ext.as_secs_f64() / a.ext.as_secs_f64();
        let tot_ratio = d.tot.as_secs_f64() / a.tot.as_secs_f64();
        let bytes_ratio = d.bytes as f64 / a.bytes as f64;

        println!("  Pack+filtered vs Archive:");
        println!("    Git command time:  {:.2}x ({})", cmd_ratio, if cmd_ratio < 1.0 { "faster" } else { "slower" });
        println!("    Extract time:      {:.2}x ({})", ext_ratio, if ext_ratio < 1.0 { "faster" } else { "slower" });
        println!("    Total time:        {:.2}x ({})", tot_ratio, if tot_ratio < 1.0 { "faster" } else { "slower" });
        println!("    Transfer size:     {:.2}x ({})", bytes_ratio, if bytes_ratio < 1.0 { "smaller" } else { "larger" });

        if tot_ratio < 1.0 {
            println!("\n  Total end-to-end speedup: {:.0}%", (1.0 - tot_ratio) * 100.0);
        }
    }

    // Fall back to C vs A
    let c = &rows[2];

    if a.cmd.as_secs_f64() > 0.0 {
        println!();
        let cmd_ratio = c.cmd.as_secs_f64() / a.cmd.as_secs_f64();
        let ext_ratio = c.ext.as_secs_f64() / a.ext.as_secs_f64();
        let tot_ratio = c.tot.as_secs_f64() / a.tot.as_secs_f64();
        let bytes_ratio = c.bytes as f64 / a.bytes as f64;

        println!("  Pack+checkout vs Archive:");
        println!("    Git command time:  {:.2}x ({})", cmd_ratio, if cmd_ratio < 1.0 { "faster" } else { "slower" });
        println!("    Extract time:      {:.2}x ({})", ext_ratio, if ext_ratio < 1.0 { "faster" } else { "slower" });
        println!("    Total time:        {:.2}x ({})", tot_ratio, if tot_ratio < 1.0 { "faster" } else { "slower" });
        println!("    Transfer size:     {:.2}x ({})", bytes_ratio, if bytes_ratio < 1.0 { "smaller" } else { "larger" });

        if cmd_ratio < 1.0 {
            println!(
                "\n  Gitaly CPU savings:  {:.0}%",
                (1.0 - cmd_ratio) * 100.0
            );
        }
        if ext_ratio < 1.0 {
            println!(
                "  Extract speedup:     {:.0}%",
                (1.0 - ext_ratio) * 100.0
            );
        }
        if tot_ratio < 1.0 {
            println!(
                "  Total speedup:       {:.0}%",
                (1.0 - tot_ratio) * 100.0
            );
        }
    }
}

fn format_bytes(bytes: u64) -> String {
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
