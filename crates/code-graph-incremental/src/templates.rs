use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::pipeline::{
    Admission, Canonicalize, Context, Display, Each, Insert, ItemPhase, Link, Parse, Pipeline,
    Prepare, ReindexInput, Remap, Report, Resolve, Resolved, Rewrite, State,
};
use crate::sentinel::Sentinel;
use crate::treesitter::SupportLang;
use crate::{Env, Envelope, Error, Limits, LoadError};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashSet;

pub const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

pub type PhaseHook = Arc<dyn Fn(&str, &str, usize) + Send + Sync>;

pub struct Options {
    pub limits: Limits,
    pub worker_threads: usize,
    pub concurrent_families: usize,
    pub small_family_files: usize,
    pub batch_bytes: usize,
    pub cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    pub family_finished: Arc<dyn Fn() + Send + Sync>,
    pub phase_started: PhaseHook,
    pub admission: Arc<dyn Admission>,
}

struct PhaseStarted {
    family: &'static str,
    files: usize,
    hook: PhaseHook,
}

impl crate::pipeline::Observer for PhaseStarted {
    fn started(&mut self, phase: &str) {
        (self.hook)(self.family, phase, self.files);
    }
}

pub fn index<'e>(context: Context<'e>) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, ())
        .then(Prepare)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}

pub fn reindex<'e>(
    context: Context<'e>,
    state: State,
    removed: Vec<String>,
) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, ReindexInput { state, removed })
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
    emit: impl Fn(Vec<(String, RecordBatch)>) -> Result<(), E> + Sync,
) -> Result<Report, Error> {
    let limits = options.limits;
    let failed = Arc::new(AtomicBool::new(false));
    let cancelled = options.cancelled.clone();
    let failure = failed.clone();
    let run = Sentinel::new("run", "", limits.total_ms).with_cancellation(Arc::new(move || {
        failure.load(Ordering::Relaxed) || cancelled()
    }));
    run.check()?;
    let emit = |batches| -> Result<(), Error> {
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
        on_worker_pool("none", 1, || {
            (options.phase_started)("none", "export", repository.usage().files);
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
            let mut plan = crate::export::ExportPlan::from_config(export_config, ontology, &lang)?;
            plan.batch_bytes = options.batch_bytes;
            emit(crate::export::export(
                &trees,
                &[],
                &lang,
                &plan,
                envelope,
                true,
                &|_| true,
                &run,
            )?)
        })?;
        return Ok(result);
    }
    let first = *families.first_key_value().expect("nonempty families").0;
    let owns = |family: &str, path: &str| {
        SupportLang::from_path(path)
            .filter(|_| selected.contains(path))
            .map_or(first, SupportLang::family)
            == family
    };
    let worker_threads = match options.worker_threads {
        0 => std::thread::available_parallelism().map_or(1, usize::from),
        n => n,
    };
    std::thread::scope(|scope| -> Result<(), Error> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut pending = families
            .into_iter()
            .map(|family| (family, sender.clone()))
            .collect::<Vec<_>>()
            .into_iter();
        drop(sender);
        let mut running = 0;
        loop {
            let idle = options.concurrent_families.max(1) - running;
            for ((family, (language, file_count)), sender) in pending.by_ref().take(idle) {
                running += 1;
                let (run, repository, owns, emit) = (&run, &repository, &owns, &emit);
                let phase_started = options.phase_started.clone();
                let admission = options.admission.clone();
                let family_threads = if file_count <= options.small_family_files {
                    (worker_threads / 4).max(2).min(worker_threads)
                } else {
                    worker_threads
                };
                scope.spawn(move || {
                    let outcome = run.check().map_err(Error::from).and_then(|()| {
                        on_worker_pool(family, family_threads, || {
                            let (report, batches) = {
                                let mut env = Env::with_limits(language, limits)?;
                                env.export_config = export_config.clone();
                                let mut context =
                                    Context::new(&env, repository.clone()).observe(PhaseStarted {
                                        family,
                                        files: file_count,
                                        hook: phase_started.clone(),
                                    });
                                context.run = run.clone();
                                context.admission = admission.clone();
                                let (context, graph) = index(context)?.then(Display)?.finish();
                                let mut report = context.report;
                                report.unread_files.retain(|path| owns(family, path));
                                run.check()?;
                                let mut plan = crate::export::ExportPlan::from_config(
                                    &env.export_config,
                                    ontology,
                                    &env.lang,
                                )?;
                                plan.batch_bytes = options.batch_bytes;
                                phase_started(family, "export", file_count);
                                let batches = crate::export::export(
                                    &graph.state.trees,
                                    &graph.state.edges,
                                    &env.lang,
                                    &plan,
                                    envelope,
                                    family == first,
                                    &|path| owns(family, path),
                                    run,
                                )?;
                                (report, batches)
                            };
                            emit(batches)?;
                            Ok(report)
                        })
                    });
                    let _ = sender.send(outcome);
                });
            }
            let Ok(outcome) = receiver.recv() else {
                break;
            };
            running -= 1;
            let report = match outcome {
                Ok(report) => report,
                Err(error) => {
                    failed.store(true, Ordering::Relaxed);
                    return Err(error);
                }
            };
            result.skipped.extend(report.skipped);
            result.limited.extend(report.limited);
            result.unread_files.extend(report.unread_files);
            result.files.extend(report.files);
            result.phases.extend(report.phases);
            (options.family_finished)();
        }
        Ok(())
    })?;
    Ok(result)
}

fn on_worker_pool<T: Send>(
    family: &'static str,
    threads: usize,
    work: impl FnOnce() -> Result<T, Error> + Send,
) -> Result<T, Error> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .stack_size(WORKER_STACK_BYTES)
            .build()
            .map_err(|error| LoadError::new(error.to_string()))?
            .install(work)
    }))
    .unwrap_or_else(|payload| {
        Err(Error::Panic {
            family,
            message: payload
                .downcast_ref::<&str>()
                .map(|message| message.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_default(),
        })
    })
}
