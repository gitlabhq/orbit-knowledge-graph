use anyhow::Result;
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use code_graph_incremental::export::ExportPlan;
use code_graph_incremental::pipeline::{FileStats, Report};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, Envelope, Scalar, State};
use orbit_arrow::ArrowUtils;
use rustc_hash::FxHashSet;

use crate::database::GraphWriter;
use crate::workspace::GitInfo;

#[derive(serde::Serialize)]
pub(crate) struct FamilyOutput {
    family: String,
    files: usize,
    pub(super) skipped_files: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    slowest_files: Vec<SlowFile>,
}

impl FamilyOutput {
    pub(super) fn slowest_lines(&self) -> impl Iterator<Item = String> + '_ {
        self.slowest_files.iter().map(|file| {
            format!(
                "{:<10} parse {:>6.0} ms  resolve {:>6.0} ms  {}",
                file.language, file.parse_ms, file.resolve_ms, file.path
            )
        })
    }
}

#[derive(serde::Serialize)]
struct SlowFile {
    path: String,
    language: String,
    parse_ms: f64,
    resolve_ms: f64,
}

pub(super) struct Exporter<'a> {
    git: &'a GitInfo,
    writer: &'a GraphWriter,
    ontology: &'a ontology::Ontology,
    directories: FxHashSet<i64>,
    files: FxHashSet<i64>,
    containment: FxHashSet<(i64, i64)>,
    replaced: FxHashSet<String>,
}

impl<'a> Exporter<'a> {
    pub(super) fn new(
        git: &'a GitInfo,
        writer: &'a GraphWriter,
        ontology: &'a ontology::Ontology,
    ) -> Self {
        Self {
            git,
            writer,
            ontology,
            directories: FxHashSet::default(),
            files: FxHashSet::default(),
            containment: FxHashSet::default(),
            replaced: FxHashSet::default(),
        }
    }

    /// Writes the rows of `family`: all of them, or with `only`, the rows of
    /// those files after deleting what the graph held for them.
    pub(super) fn write(
        &mut self,
        family: SupportLang,
        env: &Env,
        state: &State,
        report: &Report,
        stats: bool,
        only: Option<&FxHashSet<u32>>,
    ) -> Result<FamilyOutput> {
        if let Some(only) = only {
            let paths: Vec<String> = only
                .iter()
                .map(|&fi| &state.trees[fi as usize].label)
                .filter(|path| self.replaced.insert((*path).clone()))
                .cloned()
                .collect();
            self.writer.delete_files(&paths)?;
        }
        let plan = ExportPlan::from_config(&env.export_config, self.ontology, &env.lang)?;
        let tables = code_graph_incremental::export::export_trees(
            &state.trees,
            &state.edges,
            &env.lang,
            &plan,
            &self.envelope(),
            only.is_none(),
            &|_| true,
            only,
            &code_graph_incremental::Sentinel::disabled(),
        )?;
        self.insert(tables)?;
        Ok(family_output(family, state.trees.len(), report, stats))
    }

    /// Replaces every directory, and the edges between them, with those of
    /// `paths`.
    pub(super) fn write_directories(&mut self, paths: &[&str]) -> Result<()> {
        self.writer.delete_directories()?;
        self.insert(directory_rows(self.git, self.ontology, paths)?)
    }

    fn envelope(&self) -> Envelope<'a> {
        envelope(self.git)
    }

    fn insert(&mut self, tables: Vec<(String, RecordBatch)>) -> Result<()> {
        for (table, batch) in tables {
            let batch = self.unseen(&table, &batch)?;
            self.writer.insert_batch(&table, &batch)?;
        }
        Ok(())
    }

    fn unseen(&mut self, table: &str, batch: &RecordBatch) -> Result<RecordBatch> {
        let keep: Vec<bool> = match table {
            "gl_directory" => {
                let ids = int_column(batch, "id")?;
                (0..batch.num_rows())
                    .map(|row| self.directories.insert(ids.value(row)))
                    .collect()
            }
            "gl_file" => {
                let ids = int_column(batch, "id")?;
                (0..batch.num_rows())
                    .map(|row| self.files.insert(ids.value(row)))
                    .collect()
            }
            "gl_edge" => {
                let sources = int_column(batch, "source_id")?;
                let targets = int_column(batch, "target_id")?;
                let kinds = string_column(batch, "target_kind")?;
                (0..batch.num_rows())
                    .map(|row| {
                        !matches!(kinds.value(row), "Directory" | "File")
                            || self
                                .containment
                                .insert((sources.value(row), targets.value(row)))
                    })
                    .collect()
            }
            _ => return Ok(batch.clone()),
        };
        if keep.iter().all(|keep| *keep) {
            return Ok(batch.clone());
        }
        Ok(arrow::compute::filter_record_batch(
            batch,
            &BooleanArray::from(keep),
        )?)
    }
}

pub(super) fn family_output(
    family: SupportLang,
    files: usize,
    report: &Report,
    stats: bool,
) -> FamilyOutput {
    FamilyOutput {
        family: family.family().to_string(),
        files,
        skipped_files: report.skipped.len(),
        slowest_files: match stats {
            true => slowest_files(report, 20),
            false => Vec::new(),
        },
    }
}

fn slowest_files(report: &Report, count: usize) -> Vec<SlowFile> {
    let mut files: Vec<(&String, &FileStats)> = report.files.iter().collect();
    files.sort_by_key(|(_, timing)| std::cmp::Reverse(timing.parse + timing.resolve));
    files
        .into_iter()
        .take(count)
        .map(|(path, timing)| SlowFile {
            path: path.clone(),
            language: <&str>::from(timing.language).to_owned(),
            parse_ms: timing.parse.as_secs_f64() * 1000.0,
            resolve_ms: timing.resolve.as_secs_f64() * 1000.0,
        })
        .collect()
}

fn int_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    ArrowUtils::get_column_by_name(batch, name)
        .ok_or_else(|| anyhow::anyhow!("export batch has no Int64 column {name}"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    ArrowUtils::get_column_by_name(batch, name)
        .ok_or_else(|| anyhow::anyhow!("export batch has no Utf8 column {name}"))
}

pub(super) struct Replacement {
    edge_table: String,
    files: Vec<String>,
    rows: Vec<(String, RecordBatch)>,
    edges: Vec<RecordBatch>,
}

pub(super) fn replacement(
    git: &GitInfo,
    ontology: &ontology::Ontology,
    env: &Env,
    state: &State,
    files: &FxHashSet<String>,
) -> Result<Replacement> {
    let envelope = envelope(git);
    let plan = ExportPlan::from_config(&env.export_config, ontology, &env.lang)?;
    let edge_table = ontology
        .local_edge_table_name()
        .ok_or_else(|| anyhow::anyhow!("local_db.edge_table.name must be configured"))?;
    let directories = ontology
        .get_node("Directory")
        .map(|node| node.destination_table.as_str());
    let mut rows = Vec::new();
    let mut edges = Vec::new();
    let only: FxHashSet<u32> = state
        .trees
        .iter()
        .enumerate()
        .filter(|(_, tree)| files.contains(tree.label.as_str()))
        .map(|(index, _)| index as u32)
        .collect();
    for (table, batch) in code_graph_incremental::export::export_trees(
        &state.trees,
        &state.edges,
        &env.lang,
        &plan,
        &envelope,
        false,
        &|_| true,
        Some(&only),
        &code_graph_incremental::Sentinel::disabled(),
    )? {
        if table == edge_table {
            edges.push(batch);
            continue;
        }
        if Some(table.as_str()) == directories {
            continue;
        }
        let Some(column) = ["file_path", "path"]
            .into_iter()
            .find(|name| batch.schema().column_with_name(name).is_some())
        else {
            continue;
        };
        let paths = string_column(&batch, column)?;
        let keep: BooleanArray = (0..batch.num_rows())
            .map(|row| Some(files.contains(paths.value(row))))
            .collect();
        rows.push((table, arrow::compute::filter_record_batch(&batch, &keep)?));
    }
    let mut ids = FxHashSet::default();
    for (_, batch) in &rows {
        ids.extend(int_column(batch, "id")?.values().iter().copied());
    }
    let edges = edges
        .into_iter()
        .map(|batch| {
            let sources = int_column(&batch, "source_id")?;
            let targets = int_column(&batch, "target_id")?;
            let keep: BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    Some(ids.contains(&sources.value(row)) || ids.contains(&targets.value(row)))
                })
                .collect();
            Ok(arrow::compute::filter_record_batch(&batch, &keep)?)
        })
        .collect::<Result<_>>()?;
    Ok(Replacement {
        edge_table: edge_table.to_string(),
        files: files.iter().cloned().collect(),
        rows,
        edges,
    })
}

fn envelope(git: &GitInfo) -> Envelope<'_> {
    Envelope::new([
        ("project_id", Scalar::Int(git.project_id)),
        ("branch", Scalar::Str(&git.branch)),
        ("commit_sha", Scalar::Str(&git.commit_sha)),
    ])
}

/// The directories that hold `paths`, and the edges between them.
pub(super) fn directory_rows(
    git: &GitInfo,
    ontology: &ontology::Ontology,
    paths: &[&str],
) -> Result<Vec<(String, RecordBatch)>> {
    Ok(code_graph_incremental::export::export_directories(
        paths,
        code_graph_incremental::export::ExportConfig::embedded(),
        ontology,
        &envelope(git),
    )?)
}

/// Replaces every directory, and the edges between them, with `rows`.
pub(super) fn apply_directories(
    writer: &GraphWriter,
    rows: &[(String, RecordBatch)],
) -> Result<()> {
    writer.delete_directories()?;
    for (table, batch) in rows {
        writer.insert_batch(table, batch)?;
    }
    Ok(())
}

pub(super) fn apply(writer: &GraphWriter, replacement: &Replacement) -> Result<()> {
    writer.delete_files(&replacement.files)?;
    for (table, batch) in &replacement.rows {
        writer.insert_batch(table, batch)?;
    }
    for batch in &replacement.edges {
        writer.insert_batch(&replacement.edge_table, batch)?;
    }
    Ok(())
}
