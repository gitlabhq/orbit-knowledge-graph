//! `orbit index --ff=inc` and `orbit reindex --ff=inc`: the incremental
//! engine, with its state kept under the workspace `var/` directory so the
//! next run only parses what changed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context as _, Result, bail};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use code_graph_incremental::pipeline::{
    Changes, Context, Display, Emit, Export, FileTiming, Observer, Pipeline, Report, Resolved,
    State,
};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, Envelope, Scalar, inventory, templates};
use ontology::Ontology;
use orbit_utils::fs_walk::{Decision, FileInventoryEntry};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::workspace::{self, GitInfo};

const STATE_FILE: &str = "index.json";

/// What a run leaves behind besides the snapshots: the commit the graph
/// reflects and the working-tree paths that differed from it, so `reindex`
/// knows what to diff and what to look at again; the families it holds; and
/// the one that owns the files no language parses.
#[derive(Serialize, Deserialize)]
struct SavedIndex {
    commit_sha: String,
    dirty: Vec<String>,
    families: Vec<String>,
    owner: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct IncrementalOutput {
    repository: String,
    path: String,
    mode: &'static str,
    time_seconds: f64,
    families: Vec<FamilyOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    changes: Option<ChangeCounts>,
    state_dir: String,
}

#[derive(Serialize)]
struct FamilyOutput {
    family: String,
    files: usize,
    rows: BTreeMap<String, usize>,
    skipped_files: usize,
    phases: Vec<(String, f64)>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    slowest_files: Vec<SlowFile>,
}

#[derive(Serialize)]
struct SlowFile {
    path: String,
    phase: String,
    ms: f64,
}

#[derive(Serialize)]
struct ChangeCounts {
    changed: usize,
    removed: usize,
}

pub(crate) struct Options {
    pub stats: bool,
    pub verbose: bool,
    pub db: Option<PathBuf>,
    /// The run budget of one language family; per-file budgets come from the
    /// crate's limits.
    pub budget_seconds: u64,
}

pub(crate) fn index(path: PathBuf, options: Options) -> Result<()> {
    run(path, options, Mode::Full)
}

pub(crate) fn reindex(path: PathBuf, options: Options) -> Result<()> {
    run(path, options, Mode::Changed)
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Full,
    Changed,
}

fn run(path: PathBuf, options: Options, mode: Mode) -> Result<()> {
    super::install_tracing(options.verbose, false);
    let db_path = workspace::resolve_db_path(options.db.clone())?;
    let workspace = workspace::Workspace::open_default()?;
    let repos = workspace.resolve_repos(&path)?;
    if repos.is_empty() {
        bail!("no git repository found in {}", path.display());
    }
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    workspace::ensure_graph_schema(&db_path, super::LOCAL_DDL)?;
    for repo in repos {
        let git = workspace::git_info(&repo)?;
        let project = Project {
            git: &git,
            db_path: &db_path,
            ontology: &ontology,
            state_dir: workspace.var_dir(git.project_id),
            budget_ms: options.budget_seconds * 1000,
            stats: options.stats,
        };
        let output = match mode {
            Mode::Full => project.index_all()?,
            Mode::Changed => project.index_changed()?,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    }
    Ok(())
}

struct Project<'a> {
    git: &'a GitInfo,
    db_path: &'a Path,
    ontology: &'a Ontology,
    state_dir: PathBuf,
    budget_ms: u64,
    stats: bool,
}

/// The work one family gets in a run.
enum FamilyWork {
    Index(Vec<FileInventoryEntry>),
    Reindex(Changes),
    /// Nothing changed; its rows are re-emitted from the snapshot because the
    /// project's rows are replaced as a whole.
    Keep,
}

impl Project<'_> {
    fn index_all(&self) -> Result<IncrementalOutput> {
        let started = Instant::now();
        let inventory = inventory::walk(&self.git.repo_path)
            .context("failed to walk repository files")?
            .into_inner();
        info!("walk {:.2}s, {} files", started.elapsed().as_secs_f64(), inventory.len());
        let by_family = split_by_family(inventory, None);
        let owner = by_family.values().next().map(|(lang, _)| *lang);
        let work = by_family
            .into_values()
            .map(|(family, entries)| (family, FamilyWork::Index(entries)));
        let families = self.run_families(work, owner, Mode::Full)?;
        Ok(self.output("index", started, families, None))
    }

    fn index_changed(&self) -> Result<IncrementalOutput> {
        let started = Instant::now();
        let saved = self.load_state()?;
        let mut changes = workspace::git_changes(&self.git.repo_path, &saved.commit_sha)?;
        for path in &saved.dirty {
            match self.git.repo_path.join(path).is_file() {
                true => changes.changed.push(path.clone()),
                false => changes.removed.push(path.clone()),
            }
        }
        let changes = changes.settled();
        let counts = ChangeCounts {
            changed: changes.changed.len(),
            removed: changes.removed.len(),
        };
        info!(
            "{} changed, {} removed since {}",
            counts.changed,
            counts.removed,
            &saved.commit_sha[..8.min(saved.commit_sha.len())]
        );
        let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
        let changed = inventory::classify(&self.git.repo_path, changes.changed);
        let mut changed = split_by_family(changed, owner);
        let mut removed = split_paths_by_family(changes.removed, owner);

        let mut work: BTreeMap<&'static str, (SupportLang, FamilyWork)> = BTreeMap::new();
        for name in &saved.families {
            let family = SupportLang::from_family(name).with_context(|| {
                format!("{STATE_FILE} names a language family this build does not know: {name}")
            })?;
            let entries = changed
                .remove(family.family())
                .map_or_else(Vec::new, |(_, entries)| entries);
            let gone = removed.remove(family.family()).unwrap_or_default();
            let job = match entries.is_empty() && gone.is_empty() {
                true => FamilyWork::Keep,
                false => FamilyWork::Reindex(Changes {
                    changed: entries,
                    removed: gone,
                }),
            };
            work.insert(family.family(), (family, job));
        }
        for (family, entries) in changed.into_values() {
            work.insert(family.family(), (family, FamilyWork::Index(entries)));
        }
        let families = self.run_families(work.into_values(), owner, Mode::Changed)?;
        Ok(self.output("reindex", started, families, Some(counts)))
    }

    /// Replaces the project's rows with every family's graph and saves the
    /// snapshots and the commit they reflect.
    fn run_families(
        &self,
        work: impl IntoIterator<Item = (SupportLang, FamilyWork)>,
        owner: Option<SupportLang>,
        mode: Mode,
    ) -> Result<Vec<FamilyOutput>> {
        let client = duckdb_client::DuckDbClient::open(self.db_path)
            .context("failed to open DuckDB for writing")?;
        let mut shared = match mode {
            Mode::Full => {
                super::clear_project(&client, self.git, self.ontology)?;
                SharedRows::default()
            }
            Mode::Changed => {
                self.stamp_commit(&client)?;
                SharedRows::in_database(&client, self.git.project_id)?
            }
        };
        std::fs::create_dir_all(&self.state_dir)?;
        let root = &self.git.repo_path;
        let mut families = Vec::new();
        let mut outputs = Vec::new();
        for (family, job) in work {
            families.push(family.family().to_string());
            let snapshot = self.snapshot_path(family);
            let log = PhaseLog(family.family());
            let mut env;
            let graph = match job {
                FamilyWork::Index(entries) => {
                    env = Env::for_lang(family)?;
                    env.limits.total_ms = self.budget_ms;
                    templates::index(Context::new(&env).observe(log), root, entries)?
                }
                FamilyWork::Reindex(changes) => {
                    let (loaded, state) = State::load(&snapshot, family)
                        .with_context(|| format!("failed to load {}", snapshot.display()))?;
                    env = loaded;
                    env.limits.total_ms = self.budget_ms;
                    let previous = state.trees.iter().map(|t| t.label.as_str());
                    let removed = changes.removed.iter().map(String::as_str);
                    self.forget_files(&client, previous.chain(removed))?;
                    templates::reindex(Context::new(&env).observe(log), state, root, changes)?
                }
                FamilyWork::Keep => continue,
            };
            outputs.push(self.export(family, &env, graph, &client, &mut shared)?);
        }
        let started = Instant::now();
        super::finish_project(&client, self.git, self.ontology)?;
        info!("search index {:.2}s", started.elapsed().as_secs_f64());
        self.save_state(&families, owner)?;
        Ok(outputs)
    }

    /// Kept families' rows stay as they are; they now describe this commit.
    fn stamp_commit(&self, client: &duckdb_client::DuckDbClient) -> Result<()> {
        for table in [
            "gl_directory",
            "gl_file",
            "gl_definition",
            "gl_imported_symbol",
        ] {
            client.execute(
                &format!("UPDATE {table} SET commit_sha = ?, branch = ? WHERE project_id = ?"),
                &[
                    serde_json::json!(self.git.commit_sha),
                    serde_json::json!(self.git.branch),
                    serde_json::json!(self.git.project_id),
                ],
            )?;
        }
        Ok(())
    }

    /// Drops the rows of the given files and every edge touching them: the
    /// family's share of the project before it is exported again.
    fn forget_files<'p>(
        &self,
        client: &duckdb_client::DuckDbClient,
        paths: impl Iterator<Item = &'p str>,
    ) -> Result<()> {
        let paths = RecordBatch::try_from_iter([(
            "path",
            Arc::new(StringArray::from_iter_values(paths)) as ArrayRef,
        )])?;
        client.execute(
            "CREATE OR REPLACE TEMP TABLE stale_paths (path VARCHAR)",
            &[],
        )?;
        client.insert_batch("stale_paths", &paths)?;
        let project = serde_json::json!(self.git.project_id);
        let files = [
            ("gl_file", "path"),
            ("gl_definition", "file_path"),
            ("gl_imported_symbol", "file_path"),
        ];
        let stale = |(table, column): &(&str, &str)| {
            format!(
                "SELECT id FROM {table} WHERE project_id = {} AND {column} IN (SELECT path FROM stale_paths)",
                self.git.project_id
            )
        };
        let stale_ids = files
            .iter()
            .map(stale)
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        client.execute(
            &format!(
                "DELETE FROM gl_edge WHERE source_id IN ({stale_ids}) OR target_id IN ({stale_ids})"
            ),
            &[],
        )?;
        for (table, column) in files {
            client.execute(
                &format!(
                    "DELETE FROM {table} WHERE project_id = ? AND {column} IN (SELECT path FROM stale_paths)"
                ),
                std::slice::from_ref(&project),
            )?;
        }
        Ok(())
    }

    /// Writes the family's rows into DuckDB and the snapshot the next run
    /// starts from.
    fn export(
        &self,
        family: SupportLang,
        env: &Env,
        graph: Pipeline<'_, Resolved>,
        client: &duckdb_client::DuckDbClient,
        shared: &mut SharedRows,
    ) -> Result<FamilyOutput> {
        let envelope = Envelope::new([
            ("project_id", Scalar::Int(self.git.project_id)),
            ("branch", Scalar::Str(&self.git.branch)),
            ("commit_sha", Scalar::Str(&self.git.commit_sha)),
        ]);
        let mut rows: BTreeMap<String, usize> = BTreeMap::new();
        let (context, exported) = graph
            .then(Display)?
            .then(Export {
                ontology: self.ontology,
                envelope,
            })?
            .then(Emit(|table: &str, batch: RecordBatch| -> Result<()> {
                let batch = shared.unseen(table, &batch)?;
                *rows.entry(table.to_string()).or_default() += batch.num_rows();
                Ok(client.insert_batch(table, &batch)?)
            }))?
            .finish();
        let state = exported.state;
        state
            .save(env, &self.snapshot_path(family))
            .context("failed to save snapshot")?;
        Ok(FamilyOutput {
            family: family.family().to_string(),
            files: state.trees.len(),
            rows,
            skipped_files: context.report.skipped.len(),
            phases: phase_seconds(&context.report),
            slowest_files: match self.stats {
                true => slowest_files(&context.report, 20),
                false => Vec::new(),
            },
        })
    }

    fn output(
        &self,
        mode: &'static str,
        started: Instant,
        families: Vec<FamilyOutput>,
        changes: Option<ChangeCounts>,
    ) -> IncrementalOutput {
        IncrementalOutput {
            repository: super::repository_name(self.git),
            path: self.git.repo_path.to_string_lossy().to_string(),
            mode,
            time_seconds: started.elapsed().as_secs_f64(),
            families,
            changes,
            state_dir: self.state_dir.to_string_lossy().to_string(),
        }
    }

    fn snapshot_path(&self, family: SupportLang) -> PathBuf {
        self.state_dir
            .join(format!("graph.{}.bin", family.family()))
    }

    fn save_state(&self, families: &[String], owner: Option<SupportLang>) -> Result<()> {
        let dirty = workspace::git_working_changes(&self.git.repo_path)?;
        let saved = SavedIndex {
            commit_sha: self.git.commit_sha.clone(),
            dirty: dirty.paths().cloned().collect(),
            families: families.to_vec(),
            owner: owner.map(|lang| lang.family().to_string()),
        };
        std::fs::write(
            self.state_dir.join(STATE_FILE),
            serde_json::to_vec_pretty(&saved)?,
        )?;
        Ok(())
    }

    fn load_state(&self) -> Result<SavedIndex> {
        let path = self.state_dir.join(STATE_FILE);
        let bytes = std::fs::read(&path).with_context(|| {
            format!(
                "no incremental state for {}; run `orbit index --ff=inc` first",
                self.git.repo_path.display()
            )
        })?;
        serde_json::from_slice(&bytes).with_context(|| format!("failed to read {}", path.display()))
    }
}

/// One pipeline per language family. Files no language parses belong to the
/// repository rather than a language, so `owner` (the first family when
/// unset) records them.
fn split_by_family(
    inventory: Vec<FileInventoryEntry>,
    owner: Option<SupportLang>,
) -> BTreeMap<&'static str, (SupportLang, Vec<FileInventoryEntry>)> {
    let mut by_family: BTreeMap<&'static str, (SupportLang, Vec<FileInventoryEntry>)> =
        BTreeMap::new();
    let mut unparsed = Vec::new();
    for entry in inventory {
        match parsed_language(&entry) {
            Some(lang) => by_family
                .entry(lang.family())
                .or_insert_with(|| (lang.family_members()[0], Vec::new()))
                .1
                .push(entry),
            None => unparsed.push(entry),
        }
    }
    let owner = owner.or_else(|| by_family.values().next().map(|(lang, _)| *lang));
    if let Some(owner) = owner.filter(|_| !unparsed.is_empty()) {
        by_family
            .entry(owner.family())
            .or_insert_with(|| (owner, Vec::new()))
            .1
            .append(&mut unparsed);
    }
    by_family
}

fn split_paths_by_family(
    paths: Vec<String>,
    owner: Option<SupportLang>,
) -> BTreeMap<&'static str, Vec<String>> {
    let mut by_family: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    for path in paths {
        if let Some(lang) = SupportLang::from_path(&path).or(owner) {
            by_family.entry(lang.family()).or_default().push(path);
        }
    }
    by_family
}

fn parsed_language(entry: &FileInventoryEntry) -> Option<SupportLang> {
    (entry.decision == Decision::Parse)
        .then(|| SupportLang::from_path(&entry.path))
        .flatten()
        .filter(|lang| lang.has_rules())
}

/// Every family exports the directories above its files, so a directory
/// and its edge to a parent come out once per run: the first family wins.
#[derive(Default)]
struct SharedRows {
    directories: FxHashSet<i64>,
    containment: FxHashSet<(i64, i64)>,
}

impl SharedRows {
    /// What a reindex must not add again: the directories and their edges
    /// already in the database.
    fn in_database(client: &duckdb_client::DuckDbClient, project_id: i64) -> Result<Self> {
        let mut shared = Self::default();
        let ids = client.query_arrow(&format!(
            "SELECT id FROM gl_directory WHERE project_id = {project_id}"
        ))?;
        for batch in &ids {
            shared.directories.extend(int_column(batch, "id")?.values());
        }
        let edges = client.query_arrow(&format!(
            "SELECT source_id, target_id FROM gl_edge WHERE target_kind = 'Directory' AND target_id IN (SELECT id FROM gl_directory WHERE project_id = {project_id})"
        ))?;
        for batch in &edges {
            let (sources, targets) = (
                int_column(batch, "source_id")?,
                int_column(batch, "target_id")?,
            );
            shared.containment.extend(
                sources
                    .values()
                    .iter()
                    .zip(targets.values())
                    .map(|(&s, &t)| (s, t)),
            );
        }
        Ok(shared)
    }

    fn unseen(&mut self, table: &str, batch: &RecordBatch) -> Result<RecordBatch> {
        let keep: Vec<bool> = match table {
            "gl_directory" => {
                let ids = int_column(batch, "id")?;
                (0..batch.num_rows())
                    .map(|i| self.directories.insert(ids.value(i)))
                    .collect()
            }
            "gl_edge" => {
                let (sources, targets) = (
                    int_column(batch, "source_id")?,
                    int_column(batch, "target_id")?,
                );
                let kinds = string_column(batch, "target_kind")?;
                (0..batch.num_rows())
                    .map(|i| {
                        kinds.value(i) != "Directory"
                            || self
                                .containment
                                .insert((sources.value(i), targets.value(i)))
                    })
                    .collect()
            }
            _ => return Ok(batch.clone()),
        };
        if keep.iter().all(|&k| k) {
            return Ok(batch.clone());
        }
        Ok(arrow::compute::filter_record_batch(
            batch,
            &BooleanArray::from(keep),
        )?)
    }
}

fn int_column<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .with_context(|| format!("export batch has no Int64 column {name}"))
}

fn string_column<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .with_context(|| format!("export batch has no Utf8 column {name}"))
}

fn slowest_files(report: &Report, count: usize) -> Vec<SlowFile> {
    let mut timings: Vec<&FileTiming> = report.files.iter().collect();
    timings.sort_by_key(|t| std::cmp::Reverse(t.elapsed));
    timings
        .into_iter()
        .take(count)
        .map(|t| SlowFile {
            path: t.path.clone(),
            phase: t.phase.clone(),
            ms: t.elapsed.as_secs_f64() * 1000.0,
        })
        .collect()
}

fn phase_seconds(report: &Report) -> Vec<(String, f64)> {
    report
        .phases
        .iter()
        .map(|p| (p.name.clone(), p.elapsed.as_secs_f64()))
        .collect()
}

/// Phase boundaries and budget skips on the log while a family runs.
struct PhaseLog(&'static str);

impl Observer for PhaseLog {
    fn finished(&mut self, phase: &str, elapsed: std::time::Duration) {
        info!("{:<10} {phase} {:.2}s", self.0, elapsed.as_secs_f64());
    }

    fn skipped(&mut self, killed: &code_graph_incremental::sentinel::Killed) {
        tracing::warn!("{:<10} skipped {killed}", self.0);
    }

    fn failed(&mut self, phase: &str, error: &code_graph_incremental::error::Error) {
        tracing::error!("{:<10} {phase} failed: {error}", self.0);
    }
}
