mod repository;
mod state;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rayon::prelude::*;

pub use repository::{Options, WORKER_STACK_BYTES, repository};
pub use state::{SNAPSHOT_VERSION, SourceFile, State};

use smallvec::SmallVec;

use crate::engine::{self, Around, Engine, Outputs, Phase, TreeSets, Trees};
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
    pub outputs: Outputs,
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
    unparsed(engine, repository, &mut state, &report);
    finish(engine, &mut state, run, &mut report)?;
    Ok((state, report))
}

fn insert(
    engine: &Engine,
    state: &mut State,
    files: &[SourceFile],
    run: &Sentinel,
    report: &mut Report,
) -> Result<(), Error> {
    let (own, _) = engine.pipeline.split();
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

fn one_file(
    engine: &Engine,
    file: &SourceFile,
    phases: &[Phase],
    run: &Sentinel,
) -> Result<(Tree<Compact>, Vec<Edge>, Option<Killed>), Killed> {
    let started = Instant::now();
    let (tree, cut) = crate::frontend::parse(engine, &file.path, &file.content, run)?;
    if *engine::profile::ON {
        engine::profile::phase("parse", started);
    }
    let mut edges = Vec::new();
    let mut at = Stage::new(tree, canonical);
    for phase in phases {
        let started = Instant::now();
        let around = Around::alone(engine.lang.input_set());
        let (found, _) = at.run(phase, around, None, engine, run)?;
        edges.extend(found);
        if *engine::profile::ON {
            engine::profile::phase(&phase.name, started);
        }
    }
    Ok((at.into_compact(engine), edges, cut))
}

struct Stage {
    tree: Storage,
    freeze: fn(&Engine, Tree<Mutable>) -> Tree<Compact>,
}

enum Storage {
    Mutable(Tree<Mutable>),
    Compact(Tree<Compact>),
    Moving,
}

impl Stage {
    fn new(tree: Tree<Mutable>, freeze: fn(&Engine, Tree<Mutable>) -> Tree<Compact>) -> Self {
        Self {
            tree: Storage::Mutable(tree),
            freeze,
        }
    }

    /// Runs `phase` on the tree, Mutable while the phases so far build it;
    /// a phase that builds reads no other tree.
    fn run(
        &mut self,
        phase: &Phase,
        around: Around<'_, Compact>,
        rules: Option<usize>,
        engine: &Engine,
        budget: &Sentinel,
    ) -> Result<(Vec<Edge>, engine::Outputs), Killed> {
        if !phase.builds() {
            let tree = self.compact(engine);
            return engine::run_tree(phase, tree, around, rules, engine, budget);
        }
        match &mut self.tree {
            Storage::Mutable(tree) => {
                let around = Around::alone(around.set);
                engine::run_tree(phase, tree, around, rules, engine, budget)
            }
            _ => unreachable!("the pipeline builds a tree only before it turns Compact"),
        }
    }

    fn compacted(&self) -> &Tree<Compact> {
        match &self.tree {
            Storage::Compact(tree) => tree,
            _ => unreachable!("a stage a phase reads is Compact"),
        }
    }

    fn compact(&mut self, engine: &Engine) -> &mut Tree<Compact> {
        if let Storage::Mutable(_) = self.tree {
            let Storage::Mutable(tree) = std::mem::replace(&mut self.tree, Storage::Moving) else {
                unreachable!()
            };
            self.tree = Storage::Compact((self.freeze)(engine, tree));
        }
        match &mut self.tree {
            Storage::Compact(tree) => tree,
            _ => unreachable!("a stage is Mutable or Compact between phases"),
        }
    }

    fn into_compact(mut self, engine: &Engine) -> Tree<Compact> {
        self.compact(engine);
        match self.tree {
            Storage::Compact(tree) => tree,
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

pub fn finish(
    engine: &Engine,
    state: &mut State,
    run: &Sentinel,
    report: &mut Report,
) -> Result<(), Error> {
    let (_, rest) = engine.pipeline.split();
    let input = engine.lang.input_set();
    state.linked_from = state.edges.len();
    let paths: Vec<&str> = state
        .trees
        .iter()
        .map(|tree| tree.label.as_str())
        .chain(state.configs.iter().map(|file| file.path.as_str()))
        .collect();
    let mut whole: Vec<Option<Stage>> = engine
        .sets
        .iter()
        .map(|frontend| {
            let built = frontend
                .as_ref()?
                .build(&paths, &state.configs, &engine.lang);
            Some(Stage::new(built, |_, tree| tree.compact_and_remap()))
        })
        .collect();
    for phase in rest {
        run.check()?;
        let started = Instant::now();
        let seen = TreeSets(phase.walks.0 | phase.reads.0);
        let walked = phase.walks.iter().next().expect("a phase walks a tree set");
        let alone = (!phase.shared() && walked != input).then(|| whole[walked].take());
        for set in seen.iter() {
            if let Some(stage) = whole.get_mut(set).and_then(Option::as_mut) {
                stage.compact(engine);
            }
        }
        if phase.shared() {
            let parts = parts(&state.trees, &whole, input);
            let trees = Trees::Sets(&parts, seen.0);
            let walk: Vec<u32> = phase
                .walks
                .iter()
                .flat_map(|set| trees.range(set))
                .collect();
            let ran = engine::run_shared(phase, trees, &walk, &state.edges, engine, run)?;
            report.skipped.extend(ran.stopped);
            report.outputs.merge(ran.outputs);
            state.edges.extend(ran.edges);
        } else if let Some(mut stage) = alone.flatten() {
            let fi = first_index(engine, state, walked);
            {
                let parts = parts(&state.trees, &whole, input);
                let around = Around {
                    sets: &parts,
                    set: walked,
                    reads: phase.reads.0,
                };
                let rules = Some(engine.family_rules);
                let (edges, outputs) = stage.run(phase, around, rules, engine, run)?;
                report.outputs.merge(outputs);
                state.edges.extend(edges.into_iter().map(|e| e.in_file(fi)));
            }
            whole[walked] = Some(stage);
        } else {
            let parts = parts(&[], &whole, input);
            let around = Around {
                sets: &parts,
                set: input,
                reads: phase.reads.0,
            };
            let done = state
                .trees
                .par_iter_mut()
                .map(|tree| engine::run_tree(phase, tree, around, None, engine, run))
                .collect::<Result<Vec<_>, Killed>>()?;
            for (fi, (edges, outputs)) in done.into_iter().enumerate() {
                report.outputs.merge(outputs);
                state
                    .edges
                    .extend(edges.into_iter().map(|e| e.in_file(fi as u32)));
            }
        }
        if *engine::profile::ON {
            engine::profile::phase(&phase.name, started);
        }
    }
    state.sets = whole
        .into_iter()
        .map(|stage| {
            stage
                .map(|s| vec![s.into_compact(engine)])
                .unwrap_or_default()
        })
        .collect();
    Ok(())
}

/// Each tree set's trees, the files in the set they parse into.
fn parts<'a>(
    files: &'a [Tree<Compact>],
    whole: &'a [Option<Stage>],
    input: usize,
) -> SmallVec<[&'a [Tree<Compact>]; 4]> {
    whole
        .iter()
        .enumerate()
        .map(|(set, stage)| match (set == input, stage) {
            (true, _) => files,
            (false, Some(stage)) => std::slice::from_ref(stage.compacted()),
            (false, None) => &[][..],
        })
        .collect()
}

/// The file index of the first tree of set `set`, every set's trees numbered
/// in order.
fn first_index(engine: &Engine, state: &State, set: usize) -> u32 {
    (0..set)
        .map(|at| match &engine.sets[at] {
            Some(_) => 1,
            None => state.trees.len() as u32,
        })
        .sum()
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
        if !engine.set_reads(&file.path) {
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
