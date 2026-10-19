use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use code_graph_incremental::treesitter::SupportLang;

use ontology::Ontology;

use crate::database::{GraphWriter, lock_incremental_state, manifest};
use crate::workspace;

mod input;
mod output;
mod state;

pub(crate) use output::FamilyOutput;
use state::Project;

pub(crate) struct Options {
    pub stats: bool,
    pub verbose: bool,
    pub db: Option<PathBuf>,
    pub budget_seconds: Option<u64>,
}

pub(crate) fn index(path: PathBuf, options: Options) -> Result<Vec<Vec<FamilyOutput>>> {
    run(path, options, Mode::Full)
}

pub(crate) fn reindex(path: PathBuf, options: Options) -> Result<Vec<Vec<FamilyOutput>>> {
    run(path, options, Mode::Changed)
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Full,
    Changed,
}

fn run(path: PathBuf, options: Options, mode: Mode) -> Result<Vec<Vec<FamilyOutput>>> {
    super::install_tracing(options.verbose, false);
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let repos = workspace.resolve_repos(&path)?;
    if repos.is_empty() {
        bail!("no git repository found in {}", path.display());
    }
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let mut outputs = Vec::new();
    for repo in repos {
        let git = match workspace::git_info(&repo) {
            Ok(git) => git,
            Err(error) => {
                manifest::record_git_info_failure(&manifest_path, &repo, &format!("{error:#}"));
                return Err(error);
            }
        };
        let state_dir = workspace.var_dir(git.project_id);
        let _state_lock = lock_incremental_state(&state_dir)?;
        let _ = std::fs::remove_file(state_dir.join(REFRESHING));
        let db_path = workspace::resolve_db_path(options.db.clone(), &git)?;
        let writer = GraphWriter::open(&db_path, &git)?;
        manifest::record_status(
            &manifest_path,
            &git,
            manifest::RepoStatus::Indexing,
            None,
            &db_path,
        )?;
        let project = Project {
            git: &git,
            writer: &writer,
            ontology: &ontology,
            manifest_path: &manifest_path,
            state_dir,
            budget_ms: options
                .budget_seconds
                .map_or(u64::MAX, |seconds| seconds.saturating_mul(1000)),
            stats: options.stats,
        };
        let output = match mode {
            Mode::Full => project.index_all(),
            Mode::Changed => project.index_changed(),
        }
        .and_then(|output| writer.complete(&git.commit_sha).map(|()| output));
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                manifest::record_status(
                    &manifest_path,
                    &git,
                    manifest::RepoStatus::Error,
                    Some(&format!("{error:#}")),
                    &db_path,
                )?;
                return Err(error);
            }
        };
        manifest::record_status(
            &manifest_path,
            &git,
            manifest::RepoStatus::Indexed,
            None,
            &db_path,
        )?;
        outputs.push(output);
    }
    Ok(outputs)
}

pub(crate) enum Freshness {
    NotIncremental,
    Fresh,
    Stale(Vec<String>),
}

const REFRESHING: &str = "refreshing";

pub(crate) fn refresh(
    repo: &std::path::Path,
    db: Option<PathBuf>,
    files: &[&str],
) -> Result<Freshness> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let git = workspace::git_info(repo)?;
    if manifest::load_incremental_state(&manifest_path, git.project_id)?.is_none() {
        return Ok(Freshness::NotIncremental);
    }
    let state_dir = workspace.var_dir(git.project_id);
    let changed = state::changed_since_snapshot(&git.repo_path, &state_dir, files);
    if changed.is_empty() {
        return Ok(Freshness::Fresh);
    }
    if !state_dir.join(REFRESHING).exists() {
        spawn_refresh(&git.repo_path, db.as_deref(), &changed);
    }
    Ok(Freshness::Stale(changed))
}

fn spawn_refresh(repo: &std::path::Path, db: Option<&std::path::Path>, files: &[String]) {
    let Ok(orbit) = std::env::current_exe() else {
        return;
    };
    let mut command = std::process::Command::new(orbit);
    command.arg("refresh").arg(repo);
    if let Some(db) = db {
        command.arg("--db").arg(db);
    }
    command
        .arg("--")
        .args(files)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    let _ = command.spawn();
}

pub(crate) fn refresh_now(repo: PathBuf, db: Option<PathBuf>, files: Vec<String>) -> Result<()> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let git = workspace::git_info(&repo)?;
    let Some(saved) = manifest::load_incremental_state(&manifest_path, git.project_id)? else {
        return Ok(());
    };
    let state_dir = workspace.var_dir(git.project_id);
    let marker = state_dir.join(REFRESHING);
    if std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .is_err()
    {
        return Ok(());
    }
    let files: Vec<&str> = files.iter().map(String::as_str).collect();
    let changed = state::changed_since_snapshot(&git.repo_path, &state_dir, &files);
    let outcome = (|| -> Result<()> {
        if changed.is_empty() {
            return Ok(());
        }
        let _state_lock = lock_incremental_state(&state_dir)?;
        let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
        let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
        let replacements = state::refresh_families(&git, &ontology, &state_dir, owner, changed)?;
        let writer = GraphWriter::open(&workspace::resolve_db_path(db, &git)?, &git)?;
        for replacement in &replacements {
            output::apply(&writer, replacement)?;
        }
        Ok(())
    })();
    std::fs::remove_file(&marker)?;
    outcome
}
