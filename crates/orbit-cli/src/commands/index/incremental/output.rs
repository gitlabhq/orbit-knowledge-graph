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
pub(super) struct FamilyOutput {
    family: String,
    files: usize,
    skipped_files: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    slowest_files: Vec<SlowFile>,
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
    containment: FxHashSet<(i64, i64)>,
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
            containment: FxHashSet::default(),
        }
    }

    pub(super) fn write(
        &mut self,
        family: SupportLang,
        env: &Env,
        state: &State,
        report: &Report,
        stats: bool,
    ) -> Result<FamilyOutput> {
        let envelope = Envelope::new([
            ("project_id", Scalar::Int(self.git.project_id)),
            ("branch", Scalar::Str(&self.git.branch)),
            ("commit_sha", Scalar::Str(&self.git.commit_sha)),
        ]);
        let plan = ExportPlan::from_config(&env.export_config, self.ontology, &env.lang)?;
        for (table, batch) in code_graph_incremental::export::export(
            &state.trees,
            &state.edges,
            &env.lang,
            &plan,
            &envelope,
            true,
            &|_| true,
            &code_graph_incremental::Sentinel::disabled(),
        )? {
            let batch = self.unseen(&table, &batch)?;
            self.writer.insert_batch(&table, &batch)?;
        }
        Ok(FamilyOutput {
            family: family.family().to_string(),
            files: state.trees.len(),
            skipped_files: report.skipped.len(),
            slowest_files: if stats {
                slowest_files(report, 20)
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
