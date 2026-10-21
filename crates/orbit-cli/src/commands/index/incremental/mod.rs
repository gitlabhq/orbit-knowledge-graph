use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context as _, Result};
use code_graph_incremental::pipeline::Observer;

use ontology::Ontology;

use crate::database::{GraphWriter, lock_incremental_state, manifest};
use crate::{tui, workspace};

mod input;
mod output;
mod state;

pub(crate) use output::FamilyOutput;
use state::Project;

#[derive(Default)]
pub(crate) struct Options {
    pub stats: bool,
    pub verbose: bool,
    pub db: Option<PathBuf>,
    pub budget_seconds: Option<u64>,
    pub cancel: Arc<AtomicBool>,
    pub progress: bool,
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

impl IndexOutput {
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn database_path(&self) -> &str {
        &self.database_path
    }

    pub(crate) fn slowest_files(&self) -> Vec<String> {
        self.detailed
            .iter()
            .flat_map(FamilyOutput::slowest_lines)
            .collect()
    }
}

pub(super) struct RepositoryBars {
    group: tui::ProgressGroup,
    phases: OnceLock<PhaseBars>,
}

struct PhaseBars {
    parse: tui::Bar,
    resolve: tui::Bar,
}

impl RepositoryBars {
    fn open(git: &workspace::GitInfo) -> Self {
        let name = git.repo_path.file_name().unwrap_or_default();
        Self {
            group: tui::progress_group(format!(
                "{}  {} @ {}",
                name.to_string_lossy(),
                git.branch,
                git.short_sha()
            )),
            phases: OnceLock::new(),
        }
    }

    pub(super) fn begin(&self, families: &[(&str, Option<usize>)]) {
        let mut changed: Vec<_> = families
            .iter()
            .filter_map(|(family, files)| files.map(|files| (*family, files)))
            .collect();
        changed.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        let total = changed.iter().map(|(_, files)| files).sum();
        let mut note = vec![count_noun(total, "file")];
        let names: Vec<_> = changed.iter().take(3).map(|(family, _)| *family).collect();
        match changed.len().saturating_sub(3) {
            _ if names.is_empty() => {}
            0 => note.push(names.join(", ")),
            hidden => note.push(format!("{} +{hidden} more", names.join(", "))),
        }
        let unchanged: Vec<_> = families
            .iter()
            .filter(|(_, files)| files.is_none())
            .map(|(family, _)| *family)
            .collect();
        if !unchanged.is_empty() {
            note.push(format!("{} unchanged", unchanged.join(", ")));
        }
        self.group.note(note.join(" · "));
        self.phases.get_or_init(|| PhaseBars {
            parse: self.group.bar("Parse  ", total),
            resolve: self.group.bar("Resolve", total),
        });
    }

    pub(super) fn family(self: &Arc<Self>, files: usize) -> FamilyBars {
        FamilyBars {
            bars: self.clone(),
            files,
        }
    }

    fn close(&self, output: &IndexOutput) {
        if let Some(phases) = self.phases.get() {
            phases
                .parse
                .finish(count_noun(output.graph.definitions, "definition"));
            phases.resolve.finish(format!(
                "{} · {:.1} s",
                count_noun(output.graph.relationships, "relationship"),
                output.time_seconds
            ));
        }
        self.group.close();
    }

    fn fail(&self, error: &anyhow::Error) {
        let cancelled = tui::is_cancelled(error);
        if let Some(phases) = self.phases.get() {
            for bar in [&phases.parse, &phases.resolve] {
                match cancelled {
                    true => bar.cancel_at_current_count(),
                    false => bar.fail_at_current_count(),
                }
            }
        }
        match cancelled {
            true => self.group.cancel(),
            false => self.group.fail(format!("{error:#}")),
        }
    }
}

pub(super) struct FamilyBars {
    bars: Arc<RepositoryBars>,
    files: usize,
}

impl Observer for FamilyBars {
    fn finished(&mut self, phase: &str, _: Duration) {
        let Some(phases) = self.bars.phases.get() else {
            return;
        };
        match phase {
            "resolve" => phases.resolve.advance(self.files),
            phase if phase.starts_with("parse") => phases.parse.advance(self.files),
            _ => {}
        }
    }
}

fn count_noun(count: usize, noun: &str) -> String {
    let plural = match count {
        1 => "",
        _ => "s",
    };
    format!("{} {noun}{plural}", tui::format_with_thousands(count))
}

pub(crate) fn index(path: PathBuf, options: Options) -> Result<Vec<IndexOutput>> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let repos = workspace.resolve_repos(&path)?;
    if repos.is_empty() {
        let folder = dunce::canonicalize(&path).unwrap_or(path);
        return Err(super::NoRepositoryFound(folder).into());
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
        let progress = options
            .progress
            .then(|| Arc::new(RepositoryBars::open(&git)));
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
            cancel: &options.cancel,
            progress: progress.as_ref(),
        };
        let cancelled = || options.cancel.load(Ordering::Relaxed);
        let output = match snapshot {
            true => project.index_changed().or_else(|error| match cancelled() {
                true => Err(error),
                false => project.index_all(),
            }),
            false => project.index_all(),
        }
        .and_then(|output| writer.complete(&git.commit_sha).map(|()| output));
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let error = match cancelled() {
                    true => tui::cancelled("Indexing cancelled."),
                    false => error,
                };
                if let Some(bars) = &progress {
                    bars.fail(&error);
                }
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
        let output = summary(&git, &writer, &db_path, started, output, options.stats)?;
        if let Some(bars) = &progress {
            bars.close(&output);
        }
        outputs.push(output);
    }
    Ok(outputs)
}
