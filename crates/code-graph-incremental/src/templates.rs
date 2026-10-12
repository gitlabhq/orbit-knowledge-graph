use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::pipeline::{
    Canonicalize, Changes, Context, Display, Each, Insert, ItemPhase, Link, Parse, Pipeline,
    Prepare, ReindexInput, Remap, Report, Resolve, Resolved, Rewrite, Sources, State,
};
use crate::sentinel::Sentinel;
use crate::treesitter::SupportLang;
use crate::{Env, Envelope, Error, Limits, LoadError};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashSet;

pub struct Options {
    pub limits: Limits,
    pub cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    pub family_finished: Arc<dyn Fn() + Send + Sync>,
}

const SMALL_FAMILY_FILES: usize = 650;
const SMALL_FAMILY_THREADS: usize = 4;
const LARGE_FAMILY_THREADS: usize = 16;

pub fn index<'e>(context: Context<'e>, sources: Sources) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, sources)
        .then(Prepare)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}

pub fn reindex<'e>(
    context: Context<'e>,
    state: State,
    changes: Changes,
) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, ReindexInput { state, changes })
        .then(Remap)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}

pub fn repository<E: std::fmt::Display>(
    repository: Arc<orbit_utils::vfs::Vfs<()>>,
    options: Options,
    envelope: &Envelope<'_>,
    ontology: &ontology::Ontology,
    export_config: &crate::export::ExportConfig,
    mut emit: impl FnMut(Vec<(String, RecordBatch)>) -> Result<(), E>,
) -> Result<Report, Error> {
    let limits = options.limits;
    let failed = Arc::new(AtomicBool::new(false));
    let cancelled = options.cancelled.clone();
    let failure = failed.clone();
    let run = Sentinel::new("run", "", limits.total_ms).with_cancellation(Arc::new(move || {
        failure.load(Ordering::Relaxed) || cancelled()
    }));
    run.check()?;
    let mut emit = |batches| -> Result<(), Error> {
        run.check()?;
        emit(batches).map_err(|error| {
            Error::Export(arrow::error::ArrowError::ExternalError(
                error.to_string().into(),
            ))
        })?;
        run.check()?;
        Ok(())
    };
    let selected: FxHashSet<_> = crate::pipeline::candidates(&repository, limits.max_files)
        .map(|file| file.path.to_string())
        .collect();
    let mut families = BTreeMap::new();
    for path in &selected {
        if let Some(language) = SupportLang::from_path(path) {
            families
                .entry(language.family())
                .or_insert_with(|| {
                    (
                        SupportLang::from_family(language.family()).unwrap_or(language),
                        0usize,
                    )
                })
                .1 += 1;
        }
    }
    let mut result = Report::default();
    if families.is_empty() {
        let lang = crate::intern::Lang::new();
        let trees: Vec<_> = repository
            .files()
            .map(|file| {
                let reason = match file.decision() {
                    orbit_utils::vfs::Decision::List(reason) => format!("skip_{reason}"),
                    _ => String::new(),
                };
                crate::tree::Tree::unparsed(&lang, &file.path, file.size, &reason).into()
            })
            .collect();
        emit(crate::export::export(
            &trees,
            &[],
            &lang,
            &crate::export::ExportPlan::from_config(export_config, ontology, &lang)?,
            envelope,
            true,
            &|_| true,
        )?)?;
        return Ok(result);
    }
    let first = *families.first_key_value().expect("nonempty families").0;
    let owns = |family: &str, path: &str| {
        SupportLang::from_path(path)
            .filter(|_| selected.contains(path))
            .map_or(first, SupportLang::family)
            == family
    };
    std::thread::scope(|scope| -> Result<(), Error> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        for (family, (language, file_count)) in families {
            let sender = sender.clone();
            let run = &run;
            let repository = &repository;
            let owns = &owns;
            scope.spawn(move || {
                let outcome = (|| -> Result<_, Error> {
                    run.check()?;
                    let threads = if file_count < SMALL_FAMILY_FILES {
                        SMALL_FAMILY_THREADS
                    } else {
                        LARGE_FAMILY_THREADS
                    };
                    let pool = rayon::ThreadPoolBuilder::new()
                        .num_threads(threads)
                        .stack_size(8 * 1024 * 1024)
                        .build()
                        .map_err(|error| LoadError::new(error.to_string()))?;
                    let mut env = Env::with_limits(language, limits)?;
                    env.export_config = export_config.clone();
                    let mut context = Context::new(&env);
                    context.run = run.clone();
                    let pipeline = pool.install(|| index(context, repository.clone()))?;
                    let (context, graph) = pipeline.then(Display)?.finish();
                    let mut report = context.report;
                    report.unread_files.retain(|path| owns(family, path));
                    run.check()?;
                    let batches = crate::export::export(
                        &graph.state.trees,
                        &graph.state.edges,
                        &env.lang,
                        &crate::export::ExportPlan::from_config(
                            &env.export_config,
                            ontology,
                            &env.lang,
                        )?,
                        envelope,
                        family == first,
                        &|path| owns(family, path),
                    )?;
                    Ok((report, batches))
                })();
                let _ = sender.send(outcome);
            });
        }
        drop(sender);
        for outcome in receiver {
            let outcome = outcome.and_then(|(report, batches)| {
                emit(batches)?;
                Ok(report)
            });
            let report = match outcome {
                Ok(report) => report,
                Err(error) => {
                    failed.store(true, Ordering::Relaxed);
                    return Err(error);
                }
            };
            result.skipped.extend(report.skipped);
            result.unread_files.extend(report.unread_files);
            result.files.extend(report.files);
            result.phases.extend(report.phases);
            (options.family_finished)();
        }
        Ok(())
    })?;
    Ok(result)
}
