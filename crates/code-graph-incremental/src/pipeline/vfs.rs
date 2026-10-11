use std::borrow::Cow;
use std::sync::Arc;

use orbit_utils::vfs::{Decision, Vfs};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    Context, DirtyGraph, Error, IntoParallel, ItemPhase, LinkedFile, Parsed, Phase, SourceFile,
    State, Workset,
};
use crate::{
    Env,
    sentinel::{Killed, Sentinel},
    tree::Tree,
    treesitter::{self, SupportLang},
};

pub struct Sources {
    pub repository: Arc<Vfs<()>>,
    pub selected: Arc<FxHashSet<String>>,
}

pub struct Inputs {
    repository: Arc<Vfs<()>>,
    paths: Vec<String>,
}

pub struct Inventory {
    repository: Arc<Vfs<()>>,
    candidates: FxHashSet<String>,
    unread_manifests: FxHashSet<String>,
}

impl Phase<Sources> for super::Prepare {
    type Output = Workset<Inputs, Inventory>;

    fn name(&self) -> Cow<'static, str> {
        "prepare".into()
    }

    fn run(self, context: &mut Context, sources: Sources) -> Result<Self::Output, Error> {
        let mut state = State::new(context.env);
        let mut paths = Vec::new();
        let mut unread_manifests = FxHashSet::default();
        for file in sources.repository.files() {
            context.run.check()?;
            let path = file.path.as_ref();
            if matches!(file.decision(), Decision::List(_)) {
                continue;
            }
            if context
                .env
                .resolve
                .config
                .parse_files
                .iter()
                .any(|entry| path.rsplit('/').next() == Some(entry.name.as_str()))
            {
                match read(&sources.repository, path) {
                    Ok(content) => state.configs.push(SourceFile {
                        path: path.into(),
                        content,
                    }),
                    Err(_) => {
                        unread_manifests.insert(path.to_owned());
                    }
                }
            }
            if sources.selected.contains(path)
                && SupportLang::from_path(path).is_some_and(|lang| context.env.in_family(lang))
            {
                paths.push(path.to_owned());
            }
        }
        Ok(Workset {
            state,
            dirty: FxHashSet::default(),
            listed: Inventory {
                repository: sources.repository.clone(),
                candidates: paths.iter().cloned().collect(),
                unread_manifests,
            },
            items: Inputs {
                repository: sources.repository,
                paths,
            },
        })
    }
}

fn read(repository: &Vfs<()>, path: &str) -> std::io::Result<String> {
    let bytes = repository.read(path)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

impl IntoParallel for Inputs {
    type Item = SourceFile;

    fn into_parallel(self) -> impl ParallelIterator<Item = SourceFile> {
        self.paths.into_par_iter().filter_map(move |path| {
            Some(SourceFile {
                content: read(&self.repository, &path).ok()?,
                path,
            })
        })
    }
}

pub struct Parse(pub u64);

impl ItemPhase<SourceFile> for Parse {
    type Output = Parsed;

    fn name(&self) -> Cow<'static, str> {
        "parse".into()
    }

    fn run(&self, env: &Env, run: &Sentinel, file: SourceFile) -> Result<Parsed, Killed> {
        let budget = run.child("tree-sitter", &file.path, self.0);
        treesitter::parse_with_sentinel(
            &file.content,
            SupportLang::from_path(&file.path).unwrap_or(env.lang_id),
            &env.lang,
            &file.path,
            &budget,
        )
        .map(Parsed)
    }
}

impl Phase<Workset<Vec<LinkedFile>, Inventory>> for super::Insert {
    type Output = DirtyGraph;

    fn name(&self) -> Cow<'static, str> {
        "insert".into()
    }

    fn run(
        self,
        context: &mut Context,
        input: Workset<Vec<LinkedFile>, Inventory>,
    ) -> Result<Self::Output, Error> {
        let Workset {
            mut state,
            items,
            mut dirty,
            listed,
        } = input;
        for file in items {
            let index = state.trees.len() as u32;
            dirty.insert(index);
            state.trees.push(file.tree);
            state.edges.extend(file.edges.into_iter().map(|mut edge| {
                edge.from_tree = index;
                edge.to_tree = index;
                edge
            }));
        }
        let parsed: FxHashSet<_> = state.trees.iter().map(|tree| tree.label.clone()).collect();
        let skipped: FxHashMap<_, _> = context
            .report
            .skipped
            .iter()
            .map(|killed| (killed.path.as_str(), killed.label))
            .collect();
        for file in listed
            .repository
            .files()
            .filter(|file| !parsed.contains(file.path.as_ref()))
        {
            context.run.check()?;
            let path = file.path.as_ref();
            let reason = match file.decision() {
                Decision::List(reason) => format!("skip_{reason}"),
                _ if skipped.contains_key(path) => match skipped[path] {
                    "tree-sitter" => "skip_timeout_parse",
                    "rewrite" => "skip_timeout_walk",
                    "link" => "skip_timeout_ssa",
                    _ => "skip_timeout_sentinel",
                }
                .into(),
                _ if listed.candidates.contains(path) || listed.unread_manifests.contains(path) => {
                    "fault_file_read".into()
                }
                _ => String::new(),
            };
            state
                .trees
                .push(Tree::unparsed(&context.env.lang, path, file.size, &reason).into());
        }
        Ok(DirtyGraph { state, dirty })
    }
}
