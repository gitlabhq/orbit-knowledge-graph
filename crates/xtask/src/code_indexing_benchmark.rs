use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use tokio::process::Command as AsyncCommand;
use tokio::task::JoinSet;

const WARMUP_RUNS: usize = 1;
const MEASURED_RUNS: usize = 3;

#[derive(Clone, Copy, ValueEnum)]
pub enum Action {
    Run,
    Smoke,
}

pub struct Options {
    pub action: Action,
    pub root: Option<PathBuf>,
    pub config: PathBuf,
}

#[derive(Clone, Deserialize)]
struct Repository {
    name: String,
    source: Source,
    project: String,
    version: String,
    #[serde(default)]
    commit_sha: Option<String>,
    #[serde(default)]
    groups: Vec<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    Gitlab,
    Github,
}

#[derive(Deserialize)]
struct Config {
    repos: Vec<Repository>,
}

#[derive(Serialize)]
struct BenchmarkResults<'a> {
    engine: &'a str,
    platform: String,
    commit: String,
    created_at: String,
    warmup_runs: usize,
    measured_runs: usize,
    repositories: Vec<RepositoryResult>,
}

#[derive(Serialize)]
struct RepositoryResult {
    name: String,
    groups: Vec<String>,
    seconds: Vec<f64>,
    mean_seconds: f64,
    min_seconds: f64,
    max_seconds: f64,
}

pub async fn run(options: Options) -> Result<()> {
    let root = options.root.unwrap_or_else(default_root);
    let config = load_config(&options.config)?;
    validate_repositories(&config.repos)?;
    let workspace = workspace_root()?;

    let repositories_dir = root.join("repos");
    let target_dir = root.join("target");
    let invocation_dir = invocation_dir(&root);
    fs::create_dir_all(&root).with_context(|| format!("failed to create {}", root.display()))?;
    download_repositories(&config.repos, &repositories_dir).await?;
    let executable = build_orbit(&workspace, &target_dir)?;

    match options.action {
        Action::Run => measured_run(
            &workspace,
            &root,
            &invocation_dir,
            &repositories_dir,
            &executable,
            config.repos,
        ),
        Action::Smoke => smoke_run(
            &invocation_dir,
            &repositories_dir,
            &executable,
            config.repos,
        ),
    }
}

fn invocation_dir(root: &Path) -> PathBuf {
    root.join("runs").join(format!(
        "{}-{}",
        Utc::now().format("%Y%m%dT%H%M%S%.6fZ"),
        std::process::id()
    ))
}

fn default_root() -> PathBuf {
    if cfg!(windows) {
        env::temp_dir().join("orbit-code-indexing-benchmark")
    } else {
        PathBuf::from("/tmp/orbit-code-indexing-benchmark")
    }
}

fn workspace_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("xtask must be under <workspace>/crates/xtask")
}

fn load_config(path: &Path) -> Result<Config> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    orbit_utils::yaml::from_str(&contents)
        .with_context(|| format!("failed to parse {}", path.display()))
}

fn validate_repositories(repositories: &[Repository]) -> Result<()> {
    if repositories.is_empty() {
        bail!("benchmark config must contain at least one repository");
    }
    let mut names = std::collections::HashSet::new();
    for repository in repositories {
        if repository.name.contains(['/', '\\']) || repository.name.contains("..") {
            bail!("invalid repository name: {}", repository.name);
        }
        if repository.project.is_empty() || repository.version.is_empty() {
            bail!(
                "repository {} must define project and version",
                repository.name
            );
        }
        if matches!(repository.version.as_str(), "main" | "master")
            && repository.commit_sha.is_none()
        {
            bail!("moving branch {} must have a commit_sha", repository.name);
        }
        if !names.insert(&repository.name) {
            bail!("duplicate repository name: {}", repository.name);
        }
    }
    Ok(())
}

async fn download_repositories(repositories: &[Repository], root: &Path) -> Result<()> {
    fs::create_dir_all(root).with_context(|| format!("failed to create {}", root.display()))?;
    let mut downloads = JoinSet::new();

    for repository in repositories.iter().cloned() {
        let destination = root.join(&repository.name);
        if repository_matches(&repository, &destination) {
            println!("Cached: {}", repository.name);
            continue;
        }
        if destination.exists() {
            fs::remove_dir_all(&destination)
                .with_context(|| format!("failed to remove {}", destination.display()))?;
        }
        downloads.spawn(async move { clone_repository(repository, destination).await });
    }

    while let Some(download) = downloads.join_next().await {
        download.context("repository download task failed")??;
    }
    Ok(())
}

fn repository_matches(repository: &Repository, destination: &Path) -> bool {
    if !destination.exists() {
        return false;
    }
    let Some(head) = git_output(destination, ["rev-parse", "HEAD"]) else {
        return false;
    };
    if let Some(expected) = &repository.commit_sha {
        return head.starts_with(expected) || expected.starts_with(&head);
    }
    if git_output(destination, ["describe", "--tags", "--exact-match", "HEAD"])
        .is_some_and(|tag| tag == repository.version)
    {
        return true;
    }
    false
}

fn git_output<const N: usize>(directory: &Path, arguments: [&str; N]) -> Option<String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(directory)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn clone_repository(repository: Repository, destination: PathBuf) -> Result<()> {
    let pin = repository
        .commit_sha
        .as_deref()
        .unwrap_or(&repository.version);
    println!("Cloning: {} ({pin})", repository.name);
    let url = match repository.source {
        Source::Gitlab => format!("https://gitlab.com/{}.git", repository.project),
        Source::Github => format!("https://github.com/{}.git", repository.project),
    };

    let result = if repository.commit_sha.is_some() {
        clone_commit(&url, pin, &destination).await
    } else {
        let mut command = AsyncCommand::new("git");
        command.args([
            OsStr::new("clone"),
            OsStr::new("--depth"),
            OsStr::new("1"),
            OsStr::new("--branch"),
            OsStr::new(pin),
            OsStr::new("--single-branch"),
            OsStr::new(&url),
            destination.as_os_str(),
        ]);
        checked_output(command).await
    };

    if let Err(error) = result {
        fs::remove_dir_all(&destination).ok();
        return Err(error).with_context(|| format!("failed to clone {}", repository.name));
    }
    println!("Cloned: {} ({pin})", repository.name);
    Ok(())
}

async fn clone_commit(url: &str, pin: &str, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
    checked_git(destination, [OsStr::new("init")]).await?;
    checked_git(
        destination,
        [
            OsStr::new("remote"),
            OsStr::new("add"),
            OsStr::new("origin"),
            OsStr::new(url),
        ],
    )
    .await?;
    checked_git(
        destination,
        [
            OsStr::new("fetch"),
            OsStr::new("--depth"),
            OsStr::new("1"),
            OsStr::new("origin"),
            OsStr::new(pin),
        ],
    )
    .await?;
    checked_git(
        destination,
        [
            OsStr::new("checkout"),
            OsStr::new("--detach"),
            OsStr::new("FETCH_HEAD"),
        ],
    )
    .await
}

async fn checked_git<const N: usize>(directory: &Path, arguments: [&OsStr; N]) -> Result<()> {
    let mut command = AsyncCommand::new("git");
    command.args(arguments).current_dir(directory);
    checked_output(command).await
}

async fn checked_output(mut command: AsyncCommand) -> Result<()> {
    command.kill_on_drop(true);
    let output = command.output().await.context("failed to start git")?;
    if !output.status.success() {
        bail!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn build_orbit(workspace: &Path, target_dir: &Path) -> Result<PathBuf> {
    let status = Command::new("cargo")
        .args([
            "build",
            "-p",
            "orbit-cli",
            "--bin",
            "orbit",
            "--release",
            "--features",
            "duckdb-client/bundled",
        ])
        .current_dir(workspace)
        .env("CARGO_TARGET_DIR", target_dir)
        .status()
        .context("failed to start cargo")?;
    if !status.success() {
        bail!("release build failed");
    }
    let mut executable = target_dir.join("release").join("orbit");
    if cfg!(windows) {
        executable.set_extension("exe");
    }
    Ok(executable)
}

fn smoke_run(
    data_dir: &Path,
    repositories_dir: &Path,
    executable: &Path,
    repositories: Vec<Repository>,
) -> Result<()> {
    for repository in repositories {
        println!("=== Indexing {} with Orbit ===", repository.name);
        index_repository(
            executable,
            &repositories_dir.join(&repository.name),
            data_dir,
            &data_dir.join(format!("{}.duckdb", repository.name)),
        )?;
    }
    Ok(())
}

fn measured_run(
    workspace: &Path,
    root: &Path,
    invocation_dir: &Path,
    repositories_dir: &Path,
    executable: &Path,
    repositories: Vec<Repository>,
) -> Result<()> {
    let mut results = Vec::with_capacity(repositories.len());
    for repository in repositories {
        println!("=== Benchmarking {} with Orbit ===", repository.name);
        for _ in 0..WARMUP_RUNS {
            index_repository(
                executable,
                &repositories_dir.join(&repository.name),
                invocation_dir,
                &invocation_dir.join(format!("{}.duckdb", repository.name)),
            )?;
        }
        let mut seconds = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            let started = Instant::now();
            index_repository(
                executable,
                &repositories_dir.join(&repository.name),
                invocation_dir,
                &invocation_dir.join(format!("{}.duckdb", repository.name)),
            )?;
            seconds.push(started.elapsed().as_secs_f64());
        }
        let mean_seconds = seconds.iter().sum::<f64>() / seconds.len() as f64;
        results.push(RepositoryResult {
            name: repository.name,
            groups: repository.groups,
            mean_seconds,
            min_seconds: seconds.iter().copied().fold(f64::INFINITY, f64::min),
            max_seconds: seconds.iter().copied().fold(0.0, f64::max),
            seconds,
        });
    }

    let results_dir = root.join("results");
    fs::create_dir_all(&results_dir)
        .with_context(|| format!("failed to create {}", results_dir.display()))?;
    let timestamp = Utc::now();
    let output = results_dir.join(format!(
        "{}-orbit-index.json",
        timestamp.format("%Y%m%dT%H%M%SZ")
    ));
    let document = BenchmarkResults {
        engine: "orbit-index",
        platform: format!("{}-{}", env::consts::OS, env::consts::ARCH),
        commit: git_output(workspace, ["rev-parse", "--short=8", "HEAD"])
            .unwrap_or_else(|| "unknown".to_string()),
        created_at: timestamp.to_rfc3339_opts(SecondsFormat::Secs, true),
        warmup_runs: WARMUP_RUNS,
        measured_runs: MEASURED_RUNS,
        repositories: results,
    };
    fs::write(&output, serde_json::to_vec_pretty(&document)?)
        .with_context(|| format!("failed to write {}", output.display()))?;
    println!("Benchmark result: {}", output.display());
    Ok(())
}

fn index_repository(
    executable: &Path,
    repository: &Path,
    data_dir: &Path,
    database: &Path,
) -> Result<()> {
    let status = Command::new(executable)
        .arg("index")
        .arg(repository)
        .args(["--stats", "--db"])
        .arg(database)
        .env("ORBIT_DATA_DIR", data_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("failed to index {}", repository.display()))?;
    if !status.success() {
        bail!("indexing failed for {}", repository.display());
    }
    Ok(())
}
