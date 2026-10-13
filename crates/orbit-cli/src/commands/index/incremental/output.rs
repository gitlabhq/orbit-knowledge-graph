use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context as _, Result};
use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use code_graph_incremental::pipeline::{FileStats, Observer, Report};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, Envelope, Scalar, templates};
use rustc_hash::FxHashSet;

use crate::database::GraphWriter;
use crate::workspace::GitInfo;

pub(super) type Run = templates::FamilyRun;

#[derive(serde::Serialize)]
pub(super) struct IncrementalOutput {
    pub(super) repository: String,
    pub(super) path: String,
    pub(super) mode: &'static str,
    pub(super) time_seconds: f64,
    pub(super) families: Vec<FamilyOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) changes: Option<ChangeCounts>,
    pub(super) state_dir: String,
}

#[derive(serde::Serialize)]
pub(super) struct FamilyOutput {
    pub(super) family: String,
    pub(super) files: usize,
    pub(super) rows: BTreeMap<String, usize>,
    pub(super) skipped_files: usize,
    pub(super) phases: Vec<(String, f64)>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) slowest_files: Vec<SlowFile>,
}

#[derive(serde::Serialize)]
pub(super) struct SlowFile {
    pub(super) path: String,
    pub(super) language: String,
    pub(super) parse_ms: f64,
    pub(super) resolve_ms: f64,
    pub(super) ms: f64,
}

#[derive(serde::Serialize)]
pub(super) struct ChangeCounts {
    pub(super) changed: usize,
    pub(super) removed: usize,
}

pub(super) struct Exporter<'a> {
    git: &'a GitInfo,
    writer: &'a GraphWriter,
    ontology: &'a ontology::Ontology,
    stats: bool,
    directories: FxHashSet<i64>,
    containment: FxHashSet<(i64, i64)>,
}

impl<'a> Exporter<'a> {
    pub(super) fn new(
        git: &'a GitInfo,
        writer: &'a GraphWriter,
        ontology: &'a ontology::Ontology,
        stats: bool,
    ) -> Self {
        Self {
            git,
            writer,
            ontology,
            stats,
            directories: FxHashSet::default(),
            containment: FxHashSet::default(),
        }
    }

    pub(super) fn family(
        &mut self,
        family: SupportLang,
        env: &Env,
        run: Run,
        snapshot: &Path,
    ) -> Result<FamilyOutput> {
        let envelope = Envelope::new([
            ("project_id", Scalar::Int(self.git.project_id)),
            ("branch", Scalar::Str(&self.git.branch)),
            ("commit_sha", Scalar::Str(&self.git.commit_sha)),
        ]);
        let result = templates::family(
            env,
            run,
            PhaseLog(family.family()),
            &envelope,
            self.ontology,
            true,
        )?;
        let mut rows = BTreeMap::new();
        for (table, batch) in result.batches {
            let batch = self.unseen(&table, &batch)?;
            *rows.entry(table.clone()).or_default() += batch.num_rows();
            self.writer.insert_batch(&table, &batch)?;
        }
        result
            .state
            .save(env, snapshot)
            .context("failed to save snapshot")?;
        Ok(FamilyOutput {
            family: family.family().to_string(),
            files: result.state.trees.len(),
            rows,
            skipped_files: result.report.skipped.len(),
            phases: result
                .report
                .phases
                .iter()
                .map(|phase| (phase.name.clone(), phase.elapsed.as_secs_f64()))
                .collect(),
            slowest_files: if self.stats {
                slowest_files(&result.report, 20)
            } else {
                Vec::new()
            },
        })
    }

    fn unseen(&mut self, table: &str, batch: &RecordBatch) -> Result<RecordBatch> {
        let keep: Vec<bool> = match table {
            "gl_directory" => {
                let ids = int_column(batch, "id")?;
                (0..batch.num_rows())
                    .map(|row| self.directories.insert(ids.value(row)))
                    .collect()
            }
            "gl_edge" => {
                let sources = int_column(batch, "source_id")?;
                let targets = int_column(batch, "target_id")?;
                let kinds = string_column(batch, "target_kind")?;
                (0..batch.num_rows())
                    .map(|row| {
                        kinds.value(row) != "Directory"
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

fn int_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .with_context(|| format!("export batch has no Int64 column {name}"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .with_context(|| format!("export batch has no Utf8 column {name}"))
}

fn slowest_files(report: &Report, count: usize) -> Vec<SlowFile> {
    let mut timings: Vec<(&String, &FileStats)> = report.files.iter().collect();
    timings.sort_by_key(|(_, timing)| std::cmp::Reverse(timing.parse + timing.resolve));
    timings
        .into_iter()
        .take(count)
        .map(|(path, timing)| SlowFile {
            path: path.clone(),
            language: <&str>::from(timing.language).to_owned(),
            parse_ms: timing.parse.as_secs_f64() * 1000.0,
            resolve_ms: timing.resolve.as_secs_f64() * 1000.0,
            ms: (timing.parse + timing.resolve).as_secs_f64() * 1000.0,
        })
        .collect()
}

struct PhaseLog(&'static str);

impl Observer for PhaseLog {
    fn finished(&mut self, phase: &str, elapsed: std::time::Duration) {
        tracing::info!("{:<10} {phase} {:.2}s", self.0, elapsed.as_secs_f64());
    }

    fn skipped(&mut self, killed: &code_graph_incremental::sentinel::Killed) {
        tracing::warn!("{:<10} skipped {killed}", self.0);
    }

    fn failed(&mut self, phase: &str, error: &code_graph_incremental::error::Error) {
        tracing::error!("{:<10} {phase} failed: {error}", self.0);
    }
}
