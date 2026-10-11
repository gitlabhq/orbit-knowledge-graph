use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::pipeline::{Display, Export};
use crate::sentinel::Sentinel;
use crate::treesitter::SupportLang;
use crate::{Context, Env, Envelope, Error, Limits, LoadError, templates};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashSet;

mod outcome;
mod output;
pub use outcome::{FamilyTiming, Outcome, ReadFailure};

pub struct Options {
    pub max_files: usize,
    pub worker_threads: usize,
    pub parse_timeout: Option<Duration>,
    pub rewrite_timeout: Option<Duration>,
    pub link_timeout: Option<Duration>,
    pub resolve_timeout: Option<Duration>,
    pub cross_file_timeout: Option<Duration>,
    pub cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    pub family_finished: Arc<dyn Fn() + Send + Sync>,
}

fn millis(value: Option<Duration>) -> u64 {
    value.map_or(u64::MAX, |duration| {
        duration.as_millis().min(u64::MAX as u128) as u64
    })
}

pub fn candidate_count(repository: &orbit_utils::vfs::Vfs<()>) -> usize {
    repository.files().filter(|file| is_candidate(file)).count()
}

fn is_candidate(file: &orbit_utils::vfs::File<'_, ()>) -> bool {
    file.keeps() && SupportLang::from_path(&file.path).is_some_and(SupportLang::has_rules)
}

pub fn run<E: std::fmt::Display>(
    repository: Arc<orbit_utils::vfs::Vfs<()>>,
    options: Options,
    envelope: &Envelope<'_>,
    ontology: &ontology::Ontology,
    mut emit: impl FnMut(Vec<(String, RecordBatch)>) -> Result<(), E>,
) -> Result<Outcome, Error> {
    let started = Instant::now();
    let selected: Arc<FxHashSet<_>> = Arc::new(
        repository
            .files()
            .filter(|file| is_candidate(file))
            .take(if options.max_files == 0 {
                usize::MAX
            } else {
                options.max_files
            })
            .map(|file| file.path.to_string())
            .collect(),
    );
    let mut families = BTreeMap::new();
    for path in selected.iter() {
        if let Some(language) = SupportLang::from_path(path) {
            families.insert(
                language.family(),
                SupportLang::from_family(language.family()).unwrap_or(language),
            );
        }
    }
    if families.is_empty() {
        families.insert("python", SupportLang::Python);
    }
    let first = families.keys().next().copied().unwrap_or("python");
    let owns = |family: &str, path: &str| {
        SupportLang::from_path(path)
            .filter(|_| selected.contains(path))
            .map_or(first, SupportLang::family)
            == family
    };
    let mut result = Outcome {
        source_bytes: repository.usage().bytes,
        candidates: selected.len(),
        filtered: repository
            .files()
            .filter(|file| matches!(file.decision(), orbit_utils::vfs::Decision::List(_)))
            .count(),
        ..Outcome::default()
    };
    let run = Sentinel::disabled().with_cancellation(options.cancelled.clone());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.worker_threads)
        .stack_size(8 * 1024 * 1024)
        .build()
        .map_err(|error| LoadError::new(error.to_string()))?;
    let mut output = output::Output::default();
    for (family, language) in &families {
        run.check()?;
        let family_started = Instant::now();
        let limits = Limits {
            file_rewrite_ms: millis(options.rewrite_timeout),
            file_link_ms: millis(options.link_timeout),
            file_resolve_ms: millis(options.resolve_timeout),
            total_ms: u64::MAX,
        };
        let env = Env::with_limits(*language, limits)?;
        let mut context = Context::new(&env);
        context.run = run.clone();
        context.parse_timeout_ms = millis(options.parse_timeout);
        context.resolve_timeout_ms = millis(options.cross_file_timeout);
        let pipeline = pool.install(|| {
            templates::index_vfs(
                context,
                crate::pipeline::vfs::Sources {
                    repository: repository.clone(),
                    selected: selected.clone(),
                },
            )
        })?;
        result
            .skipped
            .extend(pipeline.context().report.skipped.iter().cloned());
        let reason_key = env.lang.syms.lookup("reason");
        for tree in &pipeline.value().state.trees {
            if owns(family, &tree.label)
                && tree
                    .root()
                    .tag(reason_key)
                    .is_some_and(|reason| env.lang.syms.resolve(reason) == "fault_file_read")
            {
                result.faults.push(ReadFailure {
                    path: tree.label.clone(),
                    detail: "source or manifest could not be read".into(),
                });
            }
        }
        let graph = pipeline
            .then(Display)
            .and_then(|pipeline| {
                pipeline.then(Export {
                    ontology,
                    envelope: envelope.clone(),
                })
            })?
            .into_value();
        run.check()?;
        let batches = output.select(
            graph.tables,
            ontology,
            |path| owns(family, path),
            &mut result,
        )?;
        emit(batches).map_err(|error| {
            Error::Export(arrow::error::ArrowError::ExternalError(
                error.to_string().into(),
            ))
        })?;
        run.check()?;
        let candidates: Vec<_> = repository
            .files()
            .filter(|file| selected.contains(file.path.as_ref()) && owns(family, &file.path))
            .collect();
        result.families.push(FamilyTiming {
            language: (*family).into(),
            files: candidates.len(),
            bytes: candidates.iter().map(|file| file.size).sum(),
            elapsed: family_started.elapsed(),
        });
        (options.family_finished)();
    }
    result.elapsed = started.elapsed();
    Ok(result)
}
