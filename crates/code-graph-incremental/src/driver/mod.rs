//! Runs the phases of `config/pipeline.yaml` in order over a family's files.
//! A tree phase runs on each file in parallel, a project phase on the
//! project tree, and a tree phase that comes after those runs with the
//! whole graph.

mod repository;
mod state;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rayon::prelude::*;

pub use repository::{Options, WORKER_STACK_BYTES, repository};
pub use state::{SNAPSHOT_VERSION, SourceFile, State};

use crate::engine::Engine;
use crate::engine::{self, Graph, Phase};
use crate::error::Error;
use crate::language::SupportLang;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Compact, Edge, Mutable, Tree};

#[derive(Default)]
pub struct Report {
    pub skipped: Vec<Killed>,
    pub limited: Vec<Killed>,
    pub unread_files: Vec<String>,
    pub files: HashMap<String, FileStats>,
}

pub struct FileStats {
    pub language: SupportLang,
    pub bytes: u64,
    pub parse: Duration,
}

/// The files a repository offers for indexing, in its order, up to `max_files`.
pub fn candidates(
    repository: &orbit_utils::vfs::Vfs<()>,
    max_files: usize,
) -> impl Iterator<Item = &orbit_utils::vfs::File<'static, ()>> {
    repository
        .files()
        .filter(|file| {
            file.keeps() && SupportLang::from_path(&file.path).is_some_and(SupportLang::has_rules)
        })
        .take(if max_files == 0 {
            usize::MAX
        } else {
            max_files
        })
}

/// Indexes the family's files of `repository` into a fresh graph.
pub fn index(
    engine: &Engine,
    repository: &orbit_utils::vfs::Vfs<()>,
    run: &Sentinel,
) -> Result<(State, Report), Error> {
    let mut report = Report::default();
    let (files, configs) = sources(engine, repository, &mut report);
    let mut state = State::new(engine);
    state.configs = configs;
    insert(engine, &mut state, &files, run, &mut report)?;
    unparsed(engine, repository, &mut state, &report);
    finish(engine, &mut state, run, &mut report)?;
    Ok((state, report))
}

/// Re-indexes the changed files of `repository` into `state`, dropping the
/// files in `removed`.
pub fn reindex(
    engine: &Engine,
    repository: &orbit_utils::vfs::Vfs<()>,
    mut state: State,
    removed: &[String],
    run: &Sentinel,
) -> Result<(State, Report), Error> {
    let mut report = Report::default();
    let (files, configs) = sources(engine, repository, &mut report);
    let gone: std::collections::HashSet<&str> = removed
        .iter()
        .map(String::as_str)
        .chain(files.iter().map(|f| f.path.as_str()))
        .chain(configs.iter().map(|f| f.path.as_str()))
        .collect();
    state.retain(|label| !gone.contains(label));
    state.configs.extend(configs);
    insert(engine, &mut state, &files, run, &mut report)?;
    finish(engine, &mut state, run, &mut report)?;
    Ok((state, report))
}

/// The phases before the first that needs more than its own tree, and the rest.
fn split(engine: &Engine) -> (&[Phase], &[Phase]) {
    let phases = engine.pipeline.phases.as_slice();
    let at = phases
        .iter()
        .position(|p| p.scope != "tree")
        .unwrap_or(phases.len());
    phases.split_at(at)
}

fn insert(
    engine: &Engine,
    state: &mut State,
    files: &[SourceFile],
    run: &Sentinel,
    report: &mut Report,
) -> Result<(), Error> {
    let (own, _) = split(engine);
    let done: Vec<_> = files
        .par_iter()
        .map(|file| (file, Instant::now(), one_file(engine, file, own, run)))
        .map(|(file, started, result)| (file, started.elapsed(), result))
        .collect();
    run.check()?;
    for (file, elapsed, result) in done {
        let language =
            SupportLang::from_path(&file.path).expect("a file the engine reads has a language");
        report.files.insert(
            file.path.clone(),
            FileStats {
                language,
                bytes: file.content.len() as u64,
                parse: elapsed,
            },
        );
        match result {
            Ok((tree, edges, cut)) => {
                report.limited.extend(cut);
                state.push(tree, edges);
            }
            Err(killed) => report.skipped.push(killed),
        }
    }
    Ok(())
}

/// Parses one file and runs the phases that read only that file. The tree
/// becomes Compact at the first phase that runs on Compact trees.
fn one_file(
    engine: &Engine,
    file: &SourceFile,
    phases: &[Phase],
    run: &Sentinel,
) -> Result<(Tree<Compact>, Vec<Edge>, Option<Killed>), Killed> {
    let started = std::time::Instant::now();
    let (tree, cut) = crate::frontend::parse(engine, &file.path, &file.content, run)?;
    if *engine::profile::ON {
        engine::profile::phase("parse", started);
    }
    let mut edges = Vec::new();
    let mut at = Stage::Mutable(tree);
    for phase in phases {
        let started = std::time::Instant::now();
        let graph = Graph::new(&edges);
        let found = match phase.builds() {
            true => {
                let Stage::Mutable(tree) = &mut at else {
                    return Err(Killed::limit(phase.label, &file.path, 0, 0));
                };
                engine::run_tree(phase, tree, 0, None, engine, Some(&graph), run)?.0
            }
            false => {
                engine::run_tree(
                    phase,
                    at.compact(engine),
                    0,
                    None,
                    engine,
                    Some(&graph),
                    run,
                )?
                .0
            }
        };
        edges.extend(found);
        if *engine::profile::ON {
            engine::profile::phase(&phase.name, started);
        }
    }
    Ok((at.into_compact(engine), edges, cut))
}

/// A file's tree before and after it turns Compact.
enum Stage {
    Mutable(Tree<Mutable>),
    Compact(Tree<Compact>),
    Moving,
}

impl Stage {
    fn compact(&mut self, engine: &Engine) -> &mut Tree<Compact> {
        if let Stage::Mutable(_) = self {
            let Stage::Mutable(tree) = std::mem::replace(self, Stage::Moving) else {
                unreachable!()
            };
            *self = Stage::Compact(canonical(engine, tree));
        }
        match self {
            Stage::Compact(tree) => tree,
            _ => unreachable!("a stage is Mutable or Compact between phases"),
        }
    }

    fn into_compact(mut self, engine: &Engine) -> Tree<Compact> {
        self.compact(engine);
        match self {
            Stage::Compact(tree) => tree,
            _ => unreachable!("compact leaves a Compact tree"),
        }
    }
}

/// Keeps only canonical nodes with their text frozen into symbols, and drops
/// the source.
fn canonical(engine: &Engine, mut tree: Tree<Mutable>) -> Tree<Compact> {
    let root = tree.root;
    tree.node_mut(root).kind = engine.lang.tree_kinds().source_file;
    tree.prune(&engine.lang);
    for id in tree.preorder() {
        let sym = tree.sym_of(id, &engine.lang);
        tree.node_mut(id).sym = sym;
    }
    tree.source = std::sync::Arc::from("");
    tree.compact_and_remap()
}

/// Runs the phases after the per-file ones: project phases on the project
/// tree, then tree phases over every tree with the whole graph.
fn finish(
    engine: &Engine,
    state: &mut State,
    run: &Sentinel,
    report: &mut Report,
) -> Result<(), Error> {
    let (_, rest) = split(engine);
    for phase in rest {
        run.check()?;
        let started = std::time::Instant::now();
        match phase.scope.as_str() {
            "project" => state.project(engine),
            "graph" => {}
            "family" => {
                state.trees.push(state.project_compact(engine));
                let ran = engine::run_family(phase, &state.trees, &state.edges, engine, run);
                state.trees.pop();
                let (found, stopped) = ran?;
                report.skipped.extend(stopped);
                state.edges.extend(found);
            }
            _ => {
                let graph = Graph::new(&state.edges);
                if phase.builds() {
                    return Err(Error::Load(crate::LoadError::new(format!(
                        "phase {} rewrites after the trees are compact",
                        phase.name
                    ))));
                }
                for (fi, tree) in state.trees.iter_mut().enumerate() {
                    let fi = fi as u32;
                    let (edges, _) =
                        engine::run_tree(phase, tree, fi, None, engine, Some(&graph), run)?;
                    state.edges.extend(edges.into_iter().map(|e| e.in_file(fi)));
                }
            }
        }
        if *engine::profile::ON {
            engine::profile::phase(&phase.name, started);
        }
    }
    Ok(())
}

/// The family's source files and manifests, read; unreadable ones are reported.
fn sources(
    engine: &Engine,
    repository: &orbit_utils::vfs::Vfs<()>,
    report: &mut Report,
) -> (Vec<SourceFile>, Vec<SourceFile>) {
    let read = |path: &str| {
        repository
            .read(path)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
    };
    let mut files = Vec::new();
    for file in candidates(repository, engine.limits.max_files) {
        if !engine.reads(&file.path) {
            continue;
        }
        match read(&file.path) {
            Some(content) => files.push(SourceFile {
                path: file.path.to_string(),
                content,
            }),
            None => report.unread_files.push(file.path.to_string()),
        }
    }
    let cap = engine.limits.max_manifest_bytes;
    let mut bytes = 0u64;
    let mut configs = Vec::new();
    for file in repository.files().filter(|file| file.keeps()) {
        if !engine.project.reads(&file.path) {
            continue;
        }
        bytes = bytes.saturating_add(file.size);
        if cap > 0 && bytes > cap {
            report.limited.push(Killed::limit(
                "manifests",
                &file.path,
                bytes as usize,
                cap as usize,
            ));
            break;
        }
        match read(&file.path) {
            Some(content) => configs.push(SourceFile {
                path: file.path.to_string(),
                content,
            }),
            None => report.unread_files.push(file.path.to_string()),
        }
    }
    (files, configs)
}

/// A `File` row for every repository file that has no tree, with the reason.
fn unparsed(
    engine: &Engine,
    repository: &orbit_utils::vfs::Vfs<()>,
    state: &mut State,
    report: &Report,
) {
    let parsed: std::collections::HashSet<String> =
        state.trees.iter().map(|t| t.label.clone()).collect();
    let skipped: HashMap<&str, &str> = report
        .skipped
        .iter()
        .map(|k| (k.path.as_str(), k.label.as_ref()))
        .collect();
    for file in repository
        .files()
        .filter(|file| !parsed.contains(file.path.as_ref()))
    {
        let path = file.path.as_ref();
        let reason = match file.decision() {
            orbit_utils::vfs::Decision::List(reason) => format!("skip_{reason}"),
            _ => match skipped.get(path) {
                Some(&"parse") => "skip_timeout_parse".into(),
                Some(&"stack") => "skip_stack_limit".into(),
                Some(&"edges") => "skip_too_many_edges".into(),
                Some(label) => engine
                    .pipeline
                    .skip_reason(label)
                    .unwrap_or("skip_timeout_sentinel")
                    .into(),
                None if report.unread_files.iter().any(|p| p == path) => "fault_file_read".into(),
                None => String::new(),
            },
        };
        state
            .trees
            .push(Tree::unparsed(&engine.lang, path, file.size, &reason).into());
    }
}
