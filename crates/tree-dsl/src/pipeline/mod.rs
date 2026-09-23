pub mod phases;
pub mod snapshot;
pub mod types;

use rustc_hash::FxHashSet;

use crate::sentinel::{Killed, Limits};
use crate::tree::{Edge, Tree};
use crate::treesitter::SupportLang;

pub use phases::{display, parse, process_file, remap, resolve};
pub use types::{Env, State};

/// The graph plus the files that overran a budget and were left out.
pub struct Indexed {
    pub env: Env,
    pub state: State,
    pub killed: Vec<Killed>,
}

pub fn index(lang_id: SupportLang, files: &[(String, String)]) -> Result<Indexed, Killed> {
    index_with(Env::for_lang(lang_id), files)
}

pub fn index_with(env: Env, files: &[(String, String)]) -> Result<Indexed, Killed> {
    let mut state = State::new(&env);
    let mut killed = parse(&env, &mut state, files.to_vec());
    let all_fis: FxHashSet<usize> = (0..state.trees.len()).collect();
    killed.extend(resolve(&env, &mut state, all_fis, Some(files))?);
    Ok(Indexed { env, state, killed })
}

pub fn reindex(
    env: &Env,
    mut state: State,
    added: &[(String, String)],
    modified: &[(String, String)],
    removed: &[String],
) -> Result<(State, Vec<Killed>), Killed> {
    let old_labels: Vec<String> = state.trees.iter().map(|t| t.label.clone()).collect();
    let dirty: FxHashSet<&str> = removed
        .iter()
        .map(|s| s.as_str())
        .chain(modified.iter().map(|(p, _)| p.as_str()))
        .collect();

    let mut dirty_fis = remap(&mut state, &old_labels, &dirty);
    let new_files: Vec<(String, String)> = modified.iter().chain(added.iter()).cloned().collect();
    let new_base = state.trees.len();
    let mut killed = parse(env, &mut state, new_files);
    dirty_fis.extend(new_base..state.trees.len());
    killed.extend(resolve(env, &mut state, dirty_fis, None)?);
    Ok((state, killed))
}

/// One file with no budgets, for inspection tools.
pub fn parse_single(lang_id: SupportLang, path: &str, source: &str) -> (Env, Tree, Vec<Edge>) {
    let env = Env::with_limits(lang_id, Limits::UNLIMITED);
    let (tree, edges) = process_file(&env, path, source).expect("disabled sentinel never kills");
    let mut state = State::new(&env);
    state.trees.push(tree);
    state.edges = edges;
    let _ = resolve(&env, &mut state, FxHashSet::from_iter([0]), None);
    (env, state.trees.remove(0), state.edges)
}
