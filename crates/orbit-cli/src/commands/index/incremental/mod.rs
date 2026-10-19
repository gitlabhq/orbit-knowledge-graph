use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};

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
    Refreshed,
}

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
    let _state_lock = lock_incremental_state(&state_dir)?;
    let db_path = workspace::resolve_db_path(db, &git)?;
    let writer = GraphWriter::open(&db_path, &git)?;
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    Project {
        git: &git,
        writer: &writer,
        ontology: &ontology,
        manifest_path: &manifest_path,
        state_dir,
        budget_ms: u64::MAX,
        stats: false,
    }
    .refresh(changed)?;
    Ok(Freshness::Refreshed)
}
