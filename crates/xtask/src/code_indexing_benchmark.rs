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

pub enum Engine {
    CodeGraph,
    CodeGraphIncremental,
}

pub struct Options {
    pub action: Action,
    pub engine: Engine,
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

trait BenchmarkExecutor {
    fn build(&self, workspace: &Path, target_dir: &Path) -> Result<PathBuf>;
    fn index(&self, executable: &Path, repository: &Path, data_dir: &Path) -> Result<()>;
}

struct ProcessExecutor;

impl BenchmarkExecutor for ProcessExecutor {
    fn build(&self, workspace: &Path, target_dir: &Path) -> Result<PathBuf> {
        build_orbit(workspace, target_dir)
    }

    fn index(&self, executable: &Path, repository: &Path, data_dir: &Path) -> Result<()> {
        index_repository(executable, repository, data_dir)
    }
}

pub async fn run(options: Options) -> Result<()> {
    if matches!(options.engine, Engine::CodeGraphIncremental) {
        println!("code-graph-incremental is not wired yet; skipping.");
        return Ok(());
    }

    let root = options.root.unwrap_or_else(default_root);
    let config = load_config(&options.config)?;
    validate_repositories(&config.repos)?;
    let workspace = workspace_root()?;

    let repositories_dir = root.join("repos");
    let target_dir = root.join("target");
    fs::create_dir_all(&root).with_context(|| format!("failed to create {}", root.display()))?;
    download_repositories(&config.repos, &repositories_dir).await?;
    let executor = ProcessExecutor;
    let executable = executor.build(&workspace, &target_dir)?;

    match options.action {
        Action::Run => measured_run(
            &workspace,
            &root,
            &repositories_dir,
            &executor,
            &executable,
            config.repos,
        ),
        Action::Smoke => smoke_run(
            &executor,
            &root.join("data"),
            &repositories_dir,
            &executable,
            config.repos,
        ),
    }
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
    remote_branch_head(destination, &repository.version)
        .is_some_and(|remote_head| remote_head == head)
}

fn remote_branch_head(repository: &Path, branch: &str) -> Option<String> {
    git_output(
        repository,
        [
            "ls-remote",
            "--exit-code",
            "origin",
            &format!("refs/heads/{branch}"),
        ],
    )
    .and_then(|line| line.split_whitespace().next().map(str::to_string))
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
    executor: &impl BenchmarkExecutor,
    data_dir: &Path,
    repositories_dir: &Path,
    executable: &Path,
    repositories: Vec<Repository>,
) -> Result<()> {
    for repository in repositories {
        println!("=== Indexing {} with code-graph ===", repository.name);
        executor.index(
            executable,
            &repositories_dir.join(&repository.name),
            data_dir,
        )?;
    }
    Ok(())
}

fn measured_run(
    workspace: &Path,
    root: &Path,
    repositories_dir: &Path,
    executor: &impl BenchmarkExecutor,
    executable: &Path,
    repositories: Vec<Repository>,
) -> Result<()> {
    let mut results = Vec::with_capacity(repositories.len());
    for repository in repositories {
        println!("=== Benchmarking {} with code-graph ===", repository.name);
        for _ in 0..WARMUP_RUNS {
            executor.index(
                executable,
                &repositories_dir.join(&repository.name),
                &root.join("data"),
            )?;
        }
        let mut seconds = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            let started = Instant::now();
            executor.index(
                executable,
                &repositories_dir.join(&repository.name),
                &root.join("data"),
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
        "{}-local-cg.json",
        timestamp.format("%Y%m%dT%H%M%SZ")
    ));
    let document = BenchmarkResults {
        engine: "code-graph",
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

fn index_repository(executable: &Path, repository: &Path, data_dir: &Path) -> Result<()> {
    let status = Command::new(executable)
        .arg("index")
        .arg(repository)
        .args(["--threads", "8", "--stats"])
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct RecordingExecutor {
        repositories: Mutex<Vec<PathBuf>>,
    }

    impl BenchmarkExecutor for RecordingExecutor {
        fn build(&self, _workspace: &Path, _target_dir: &Path) -> Result<PathBuf> {
            Ok(PathBuf::from("orbit"))
        }

        fn index(&self, _executable: &Path, repository: &Path, _data_dir: &Path) -> Result<()> {
            self.repositories
                .lock()
                .unwrap()
                .push(repository.to_path_buf());
            Ok(())
        }
    }

    fn repository(name: &str) -> Repository {
        Repository {
            name: name.to_string(),
            source: Source::Github,
            project: "example/project".to_string(),
            version: "main".to_string(),
            commit_sha: None,
            groups: vec!["rust".to_string()],
        }
    }

    #[test]
    fn smoke_indexes_each_configured_repository_once() {
        let executor = RecordingExecutor {
            repositories: Mutex::new(Vec::new()),
        };
        let repositories_dir = Path::new("repos");
        smoke_run(
            &executor,
            Path::new("data"),
            repositories_dir,
            Path::new("orbit"),
            vec![repository("one"), repository("two")],
        )
        .unwrap();
        assert_eq!(
            *executor.repositories.lock().unwrap(),
            vec![repositories_dir.join("one"), repositories_dir.join("two")]
        );
    }

    #[test]
    fn measured_run_writes_four_runs_and_result_json() {
        let temporary = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor {
            repositories: Mutex::new(Vec::new()),
        };
        measured_run(
            Path::new("."),
            temporary.path(),
            Path::new("repos"),
            &executor,
            Path::new("orbit"),
            vec![repository("one")],
        )
        .unwrap();
        assert_eq!(executor.repositories.lock().unwrap().len(), 4);
        let result = fs::read_dir(temporary.path().join("results"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let document: serde_json::Value =
            serde_json::from_slice(&fs::read(result).unwrap()).unwrap();
        assert_eq!(document["warmup_runs"], 1);
        assert_eq!(document["measured_runs"], 3);
        assert_eq!(
            document["repositories"][0]["seconds"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn configured_repository_cache_is_reused() {
        let temporary = tempfile::tempdir().unwrap();
        let repository_path = temporary.path().join("repo");
        run_git(
            temporary.path(),
            ["init", "--initial-branch", "main", "repo"],
        );
        run_git(
            &repository_path,
            ["config", "user.email", "benchmark@example.com"],
        );
        run_git(&repository_path, ["config", "user.name", "Benchmark"]);
        fs::write(repository_path.join("README.md"), "benchmark").unwrap();
        run_git(&repository_path, ["add", "README.md"]);
        run_git(&repository_path, ["commit", "-m", "fixture"]);
        let commit = git_output(&repository_path, ["rev-parse", "HEAD"]).unwrap();
        let configured = Repository {
            name: "repo".to_string(),
            source: Source::Github,
            project: "example/project".to_string(),
            version: "main".to_string(),
            commit_sha: Some(commit),
            groups: Vec::new(),
        };
        assert!(repository_matches(&configured, &repository_path));
    }

    #[test]
    fn branch_cache_requires_the_pinned_remote_head() {
        let temporary = tempfile::tempdir().unwrap();
        let remote_path = temporary.path().join("remote.git");
        run_git(
            temporary.path(),
            ["init", "--bare", "--initial-branch", "main", "remote.git"],
        );

        let source_path = temporary.path().join("source");
        run_git(
            temporary.path(),
            ["clone", remote_path.to_str().unwrap(), "source"],
        );
        run_git(
            &source_path,
            ["config", "user.email", "benchmark@example.com"],
        );
        run_git(&source_path, ["config", "user.name", "Benchmark"]);
        fs::write(source_path.join("README.md"), "first").unwrap();
        run_git(&source_path, ["add", "README.md"]);
        run_git(&source_path, ["commit", "-m", "first"]);
        run_git(&source_path, ["push", "origin", "main"]);

        let repository_path = temporary.path().join("repo");
        run_git(
            temporary.path(),
            [
                "clone",
                "--branch",
                "main",
                remote_path.to_str().unwrap(),
                "repo",
            ],
        );

        let configured = repository("repo");
        assert!(repository_matches(&configured, &repository_path));

        fs::write(source_path.join("README.md"), "second").unwrap();
        run_git(&source_path, ["add", "README.md"]);
        run_git(&source_path, ["commit", "-m", "second"]);
        run_git(&source_path, ["push", "origin", "main"]);
        assert!(!repository_matches(&configured, &repository_path));
    }

    fn run_git<const N: usize>(directory: &Path, arguments: [&str; N]) {
        assert!(
            Command::new("git")
                .args(arguments)
                .current_dir(directory)
                .status()
                .unwrap()
                .success()
        );
    }
}
