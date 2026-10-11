use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use code_graph::v2::{OnBatch, SinkError};
use code_graph_incremental::pipeline::{
    Canonicalize, DirtyGraph, Display, Export, ItemPhase, Link, Parsed, Resolve, Rewrite,
    SourceFile,
};
use code_graph_incremental::sentinel::Sentinel;
use code_graph_incremental::tree::Tree;
use code_graph_incremental::treesitter::{self, SupportLang};
use code_graph_incremental::{Context, Env, Envelope, Limits, Pipeline, Scalar, State};
use orbit_utils::vfs::Decision;
use rayon::prelude::*;
use rustc_hash::FxHashSet;

use super::super::arrow_converter::{IndexerEnvelope, route_graph};
use super::super::config::CodeTableNames;
use super::{
    Repository,
    outcome::{FamilyTiming, Outcome, ReadFailure},
    output::Output,
};

pub(in crate::modules::code) struct Options {
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

pub(in crate::modules::code) fn run(
    repository: Repository,
    options: Options,
    envelope: IndexerEnvelope,
    ontology: Arc<ontology::Ontology>,
    tables: Arc<CodeTableNames>,
    on_batch: Arc<OnBatch>,
) -> Result<Outcome, SinkError> {
    let started = Instant::now();
    let selected: FxHashSet<_> = repository
        .files()
        .filter(|file| {
            file.keeps() && SupportLang::from_path(&file.path).is_some_and(SupportLang::has_rules)
        })
        .take(if options.max_files == 0 {
            usize::MAX
        } else {
            options.max_files
        })
        .map(|file| file.path.to_string())
        .collect();
    let mut families = BTreeMap::new();
    for path in &selected {
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
        ..Outcome::default()
    };
    let run = Sentinel::disabled().with_cancellation(options.cancelled.clone());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.worker_threads)
        .stack_size(8 * 1024 * 1024)
        .build()
        .map_err(|error| SinkError(error.to_string()))?;
    let mut output = Output::new(&envelope, &ontology);
    for (family, language) in &families {
        run.check().map_err(|error| SinkError(error.to_string()))?;
        let family_started = Instant::now();
        let limits = Limits {
            file_rewrite_ms: millis(options.rewrite_timeout),
            file_link_ms: millis(options.link_timeout),
            file_resolve_ms: millis(options.resolve_timeout),
            total_ms: u64::MAX,
        };
        let env =
            Env::with_limits(*language, limits).map_err(|error| SinkError(error.to_string()))?;
        let mut state = State::new(&env);
        for file in repository.files().filter(|file| file.keeps()) {
            if env
                .resolve
                .config
                .parse_files
                .iter()
                .any(|entry| file.path.rsplit('/').next() == Some(entry.name.as_str()))
            {
                let bytes = repository
                    .read(file.path.as_ref())
                    .map_err(|error| SinkError(error.to_string()))?;
                let content = std::str::from_utf8(&bytes)
                    .map_err(|error| SinkError(error.to_string()))?
                    .to_owned();
                state.configs.push(SourceFile {
                    path: file.path.to_string(),
                    content,
                });
            }
        }
        let candidates: Vec<_> = repository
            .files()
            .filter(|file| selected.contains(file.path.as_ref()) && owns(family, &file.path))
            .collect();
        let processed: Vec<_> = pool.install(|| {
            candidates
                .par_iter()
                .map(|file| {
                    let path = file.path.as_ref();
                    let parse_budget =
                        Sentinel::new("tree-sitter", path, millis(options.parse_timeout))
                            .with_cancellation(options.cancelled.clone());
                    let parsed = repository
                        .read(path)
                        .map_err(|error| error.to_string())
                        .and_then(|bytes| {
                            let source =
                                std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
                            Ok(treesitter::parse_with_sentinel(
                                source,
                                SupportLang::from_path(path).unwrap_or(*language),
                                &env.lang,
                                path,
                                &parse_budget,
                            )
                            .map(Parsed)
                            .and_then(|parsed| {
                                Rewrite
                                    .pipe(Canonicalize)
                                    .pipe(Link)
                                    .run(&env, &run, parsed)
                            }))
                        });
                    (path.to_owned(), file.size, parsed)
                })
                .collect()
        });
        let mut finished = FxHashSet::default();
        for (path, size, parsed) in processed {
            finished.insert(path.clone());
            let reason = match parsed {
                Ok(Ok(file)) => {
                    let index = u32::try_from(state.trees.len())
                        .map_err(|error| SinkError(error.to_string()))?;
                    state.trees.push(file.tree);
                    state.edges.extend(file.edges.into_iter().map(|mut edge| {
                        edge.from_tree = index;
                        edge.to_tree = index;
                        edge
                    }));
                    continue;
                }
                Ok(Err(killed)) => {
                    let reason = match killed.label {
                        "tree-sitter" => "skip_timeout_parse",
                        "rewrite" => "skip_timeout_walk",
                        "link" => "skip_timeout_ssa",
                        _ => "skip_timeout_sentinel",
                    };
                    result.skipped.push(killed);
                    reason.to_owned()
                }
                Err(detail) => {
                    result.faults.push(ReadFailure {
                        path: path.clone(),
                        detail,
                    });
                    "fault_file_read".into()
                }
            };
            state
                .trees
                .push(Tree::unparsed(&env.lang, &path, size, &reason).into());
        }
        for file in repository
            .files()
            .filter(|file| !finished.contains(file.path.as_ref()))
        {
            let reason = match file.decision() {
                Decision::List(reason) => format!("skip_{reason}"),
                _ => String::new(),
            };
            if owns(family, &file.path) && !reason.is_empty() {
                result.filtered += 1;
            }
            state
                .trees
                .push(Tree::unparsed(&env.lang, &file.path, file.size, &reason).into());
        }
        run.check().map_err(|error| SinkError(error.to_string()))?;
        let dirty = (0..state.trees.len()).map(|index| index as u32).collect();
        let mut context = Context::new(&env);
        context.run = Sentinel::new("resolve", "", millis(options.cross_file_timeout))
            .with_cancellation(options.cancelled.clone());
        let pipeline = pool
            .install(|| Pipeline::new(context, DirtyGraph { state, dirty }).then(Resolve))
            .map_err(|error| SinkError(error.to_string()))?;
        for killed in &pipeline.context().report.skipped {
            if !result.skipped.iter().any(|file| file.path == killed.path) {
                result.skipped.push(killed.clone());
            }
        }
        let graph = pipeline
            .then(Display)
            .and_then(|pipeline| {
                pipeline.then(Export {
                    ontology: &ontology,
                    envelope: Envelope::new([
                        ("project_id", Scalar::Int(envelope.project_id)),
                        ("branch", Scalar::Str(&envelope.branch)),
                        ("commit_sha", Scalar::Str(&envelope.commit_sha)),
                    ]),
                })
            })
            .map_err(|error| SinkError(error.to_string()))?
            .into_value();
        run.check().map_err(|error| SinkError(error.to_string()))?;
        let batches = output.convert(graph.tables, |path| owns(family, path))?;
        result.directories += batches.directories.num_rows();
        result.files += batches.files.num_rows();
        result.definitions += batches.definitions.num_rows();
        result.imports += batches.imported_symbols.num_rows();
        result.edges += batches.edges.num_rows();
        for (table, batch) in route_graph(batches, &tables)? {
            run.check().map_err(|error| SinkError(error.to_string()))?;
            on_batch(&table, batch)?;
        }
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
