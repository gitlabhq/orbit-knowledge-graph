use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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
    let limits = Limits {
        file_parse_ms: millis(options.parse_timeout),
        file_rewrite_ms: millis(options.rewrite_timeout),
        file_link_ms: millis(options.link_timeout),
        file_resolve_ms: millis(options.resolve_timeout),
        ..Limits::load()?
    };
    let run =
        Sentinel::new("run", "", limits.total_ms).with_cancellation(options.cancelled.clone());
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
    let selected: FxHashSet<_> = crate::pipeline::candidates(&repository, options.max_files)
        .map(|file| file.path.to_string())
        .collect();
    let mut families = BTreeMap::new();
    for path in selected.iter() {
        if let Some(language) = SupportLang::from_path(path) {
            families.insert(
                language.family(),
                SupportLang::from_family(language.family()).unwrap_or(language),
            );
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
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.worker_threads)
        .stack_size(8 * 1024 * 1024)
        .build()
        .map_err(|error| LoadError::new(error.to_string()))?;
    for (family, language) in &families {
        run.check()?;
        let env = Env::with_limits(*language, limits)?;
        let mut context = Context::new(&env);
        context.export_config = export_config;
        context.run = run.clone();
        context.max_files = options.max_files;
        context.resolve_timeout_ms = millis(options.cross_file_timeout);
        let pipeline = pool.install(|| index(context, repository.clone()))?;
        let (context, graph) = pipeline.then(Display)?.finish();
        result.skipped.extend(context.report.skipped);
        result.unread_files.extend(
            context
                .report
                .unread_files
                .into_iter()
                .filter(|path| owns(family, path)),
        );
        result.files.extend(context.report.files);
        result.phases.extend(context.report.phases);
        run.check()?;
        let batches = crate::export::export(
            &graph.state.trees,
            &graph.state.edges,
            &env.lang,
            &crate::export::ExportPlan::from_config(export_config, ontology, &env.lang)?,
            envelope,
            *family == first,
            &|path| owns(family, path),
        )?;
        emit(batches)?;
        (options.family_finished)();
    }
    Ok(result)
}
