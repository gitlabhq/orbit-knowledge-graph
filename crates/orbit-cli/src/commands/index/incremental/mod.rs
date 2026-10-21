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

#[derive(serde::Serialize)]
pub(crate) struct IndexOutput {
    repository: String,
    path: String,
    time_seconds: f64,
    graph: GraphStats,
    processing: ProcessingStats,
    database_path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    detailed: Vec<FamilyOutput>,
}

#[derive(serde::Serialize)]
struct GraphStats {
    directories: usize,
    files: usize,
    definitions: usize,
    imported_symbols: usize,
    relationships: usize,
}

#[derive(serde::Serialize)]
struct ProcessingStats {
    skipped_files: usize,
    errored_files: usize,
}

fn summary(
    git: &workspace::GitInfo,
    writer: &GraphWriter,
    db_path: &std::path::Path,
    started: std::time::Instant,
    families: Vec<FamilyOutput>,
    stats: bool,
) -> Result<IndexOutput> {
    let rows = |table: &str| writer.count(&format!("SELECT count(*) FROM {table}"));
    Ok(IndexOutput {
        repository: git
            .repo_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default(),
        path: git.repo_path.display().to_string(),
        time_seconds: started.elapsed().as_secs_f64(),
        graph: GraphStats {
            directories: rows("gl_directory")?,
            files: rows("gl_file")?,
            definitions: rows("gl_definition")?,
            imported_symbols: rows("gl_imported_symbol")?,
            relationships: rows("gl_edge")?,
        },
        processing: ProcessingStats {
            skipped_files: families.iter().map(|family| family.skipped_files).sum(),
            errored_files: writer
                .count("SELECT count(*) FROM gl_file WHERE reason LIKE 'fault_%'")?,
        },
        database_path: db_path.display().to_string(),
        detailed: if stats { families } else { Vec::new() },
    })
}

pub(crate) fn index(path: PathBuf, options: Options) -> Result<Vec<IndexOutput>> {
    super::install_tracing(options.verbose, false);
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let repos = workspace.resolve_repos(&path)?;
    if repos.is_empty() {
        bail!(
            "no git repository found in {}. Pass a repository path, or a directory containing one.",
            dunce::canonicalize(&path).unwrap_or(path).display()
        );
    }
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let mut outputs = Vec::new();
    for repo in repos {
        let started = std::time::Instant::now();
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
        let snapshot = options.db.is_none()
            && db_path.exists()
            && manifest::load_incremental_state(&manifest_path, git.project_id)?.is_some();
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
        let output = match snapshot {
            true => project.index_changed().or_else(|_| project.index_all()),
            false => project.index_all(),
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
        outputs.push(summary(
            &git,
            &writer,
            &db_path,
            started,
            output,
            options.stats,
        )?);
    }
    Ok(outputs)
}
