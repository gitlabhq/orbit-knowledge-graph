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

use crate::engine::{self, EdgeIndex, Phase, Walk};
use crate::env::Env;
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
    env: &Env,
    repository: &orbit_utils::vfs::Vfs<()>,
    run: &Sentinel,
) -> Result<(State, Report), Error> {
    let mut report = Report::default();
    let (files, configs) = sources(env, repository, &mut report);
    let mut state = State::new(env);
    state.configs = configs;
    insert(env, &mut state, &files, run, &mut report)?;
    unparsed(env, repository, &mut state, &report);
    finish(env, &mut state, run, &mut report)?;
    Ok((state, report))
}

/// Re-indexes the changed files of `repository` into `state`, dropping the
/// files in `removed`.
pub fn reindex(
    env: &Env,
    repository: &orbit_utils::vfs::Vfs<()>,
    mut state: State,
    removed: &[String],
    run: &Sentinel,
) -> Result<(State, Report), Error> {
    let mut report = Report::default();
    let (files, configs) = sources(env, repository, &mut report);
    let gone: std::collections::HashSet<&str> = removed
        .iter()
        .map(String::as_str)
        .chain(files.iter().map(|f| f.path.as_str()))
        .chain(configs.iter().map(|f| f.path.as_str()))
        .collect();
    state.retain(|label| !gone.contains(label));
    state.configs.extend(configs);
    insert(env, &mut state, &files, run, &mut report)?;
    finish(env, &mut state, run, &mut report)?;
    Ok((state, report))
}

/// A bound on reruns of a family phase; each round replaces the last one's edges.
const MAX_FAMILY_ROUNDS: usize = 32;

/// The phases before the first that needs more than its own tree, and the rest.
fn split(env: &Env) -> (&[Phase], &[Phase]) {
    let phases = env.pipeline.phases.as_slice();
    let at = phases
        .iter()
        .position(|p| p.scope != "tree")
        .unwrap_or(phases.len());
    phases.split_at(at)
}

fn insert(
    env: &Env,
    state: &mut State,
    files: &[SourceFile],
    run: &Sentinel,
    report: &mut Report,
) -> Result<(), Error> {
    let (own, _) = split(env);
    let done: Vec<_> = files
        .par_iter()
        .map(|file| (file, Instant::now(), one_file(env, file, own, run)))
        .map(|(file, started, result)| (file, started.elapsed(), result))
        .collect();
    run.check()?;
    for (file, elapsed, result) in done {
        let language = SupportLang::from_path(&file.path).unwrap_or(env.lang_id);
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
    env: &Env,
    file: &SourceFile,
    phases: &[Phase],
    run: &Sentinel,
) -> Result<(Tree<Compact>, Vec<Edge>, Option<Killed>), Killed> {
    let started = std::time::Instant::now();
    let (tree, cut) = crate::frontend::parse(env, &file.path, &file.content, run)?;
    if *engine::profile::ON {
        engine::profile::phase("parse", started);
    }
    let mut edges = Vec::new();
    let mut at = Stage::Mutable(tree);
    for phase in phases {
        let started = std::time::Instant::now();
        if phase.walk == Walk::Fixpoint {
            let Stage::Mutable(tree) = &mut at else {
                return Err(Killed::limit(phase.label, &file.path, 0, 0));
            };
            engine::rewrite_tree(phase, tree, env, run)?;
        } else {
            let tree = at.compact(env);
            match phase.walk {
                Walk::Flow => edges.extend(engine::run_tree(phase, tree, env, run)?),
                _ => engine::tag_tree(phase, tree, 0, env, &EdgeIndex::new(&edges), run)?,
            }
        }
        if *engine::profile::ON {
            engine::profile::phase(&phase.name, started);
        }
    }
    Ok((at.into_compact(env), edges, cut))
}

/// A file's tree before and after it turns Compact.
enum Stage {
    Mutable(Tree<Mutable>),
    Compact(Tree<Compact>),
    Moving,
}

impl Stage {
    fn compact(&mut self, env: &Env) -> &mut Tree<Compact> {
        if let Stage::Mutable(_) = self {
            let Stage::Mutable(tree) = std::mem::replace(self, Stage::Moving) else {
                unreachable!()
            };
            *self = Stage::Compact(canonical(env, tree));
        }
        match self {
            Stage::Compact(tree) => tree,
            _ => unreachable!("a stage is Mutable or Compact between phases"),
        }
    }

    fn into_compact(mut self, env: &Env) -> Tree<Compact> {
        self.compact(env);
        match self {
            Stage::Compact(tree) => tree,
            _ => unreachable!("compact leaves a Compact tree"),
        }
    }
}

/// Keeps only canonical nodes with their text frozen into symbols, and drops
/// the source.
fn canonical(env: &Env, mut tree: Tree<Mutable>) -> Tree<Compact> {
    let root = tree.root;
    tree.node_mut(root).kind = crate::canonical::Canonical::SourceFile.into();
    tree.prune();
    for id in tree.preorder() {
        let sym = tree.sym_of(id, &env.lang);
        tree.node_mut(id).sym = sym;
    }
    tree.source = std::sync::Arc::from("");
    tree.compact_and_remap()
}

/// Runs the phases after the per-file ones: project phases on the project
/// tree, then tree phases over every tree with the whole graph.
fn finish(env: &Env, state: &mut State, run: &Sentinel, report: &mut Report) -> Result<(), Error> {
    let (_, rest) = split(env);
    for phase in rest {
        run.check()?;
        let started = std::time::Instant::now();
        match phase.scope.as_str() {
            "project" => state.project(env),
            "graph" => {}
            "family" => {
                let base = state.edges.len();
                let mut last: std::collections::HashSet<Edge> = Default::default();
                for _ in 0..MAX_FAMILY_ROUNDS {
                    let graph = EdgeIndex::new(&state.edges);
                    let project = state.trees.len() as u32;
                    state.trees.push(state.project_compact(env));
                    let found = engine::run_family(
                        phase,
                        &state.trees,
                        Some(project),
                        &state.edges,
                        &graph,
                        env,
                        run,
                    );
                    state.trees.pop();
                    let known: std::collections::HashSet<Edge> =
                        state.edges[..base].iter().copied().collect();
                    let mut seen = std::collections::HashSet::new();
                    let found = match found {
                        Ok(found) => found,
                        Err(killed) if !killed.cancelled => {
                            report.skipped.push(killed);
                            break;
                        }
                        Err(killed) => return Err(killed.into()),
                    };
                    let found: Vec<Edge> = found
                        .into_iter()
                        .filter(|e| !known.contains(e) && seen.insert(*e))
                        .collect();
                    let stable = seen == last;
                    state.edges.truncate(base);
                    state.edges.extend(found);
                    last = seen;
                    if phase.walk != Walk::Fixpoint || stable {
                        break;
                    }
                }
            }
            _ => {
                let graph = EdgeIndex::new(&state.edges);
                for (fi, tree) in state.trees.iter_mut().enumerate() {
                    match phase.walk {
                        Walk::Each => engine::tag_tree(phase, tree, fi as u32, env, &graph, run)?,
                        Walk::Flow => {
                            let edges = engine::run_tree(phase, tree, env, run)?;
                            state
                                .edges
                                .extend(edges.into_iter().map(|e| e.in_file(fi as u32)));
                        }
                        Walk::Fixpoint => {
                            return Err(Error::Load(crate::LoadError::new(format!(
                                "phase {} rewrites after the trees are compact",
                                phase.name
                            ))));
                        }
                    }
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
    env: &Env,
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
    for file in candidates(repository, env.limits.max_files) {
        if !SupportLang::from_path(&file.path).is_some_and(|l| env.in_family(l)) {
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
    let cap = env.limits.max_manifest_bytes;
    let mut bytes = 0u64;
    let mut configs = Vec::new();
    for file in repository.files().filter(|file| file.keeps()) {
        let name = file.path.rsplit('/').next();
        if !env
            .project
            .parse_files
            .iter()
            .any(|p| Some(p.name.as_str()) == name)
        {
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
        if let Some(content) = read(&file.path) {
            configs.push(SourceFile {
                path: file.path.to_string(),
                content,
            });
        }
    }
    (files, configs)
}

/// A `File` row for every repository file that has no tree, with the reason.
fn unparsed(env: &Env, repository: &orbit_utils::vfs::Vfs<()>, state: &mut State, report: &Report) {
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
                Some(&"tree-sitter") => "skip_timeout_parse".into(),
                Some(&"stack") => "skip_stack_limit".into(),
                Some(&"edges") => "skip_too_many_edges".into(),
                Some(label) => env
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
            .push(Tree::unparsed(&env.lang, path, file.size, &reason).into());
    }
}
