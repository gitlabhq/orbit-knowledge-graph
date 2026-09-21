pub mod phases;
pub mod snapshot;
pub mod types;

use rustc_hash::FxHashSet;

use crate::tree::{Edge, Tree};
use crate::treesitter::SupportLang;

pub use phases::{display, parse, process_file, remap, resolve};
pub use types::{Env, State};

pub fn index(lang_id: SupportLang, files: &[(String, String)]) -> (Env, State) {
    let env = Env::for_lang(lang_id);
    let mut state = State::new(&env);
    parse(&env, &mut state, files.to_vec());
    let all_fis: FxHashSet<usize> = (0..state.trees.len()).collect();
    resolve(&env, &mut state, all_fis, Some(files));
    (env, state)
}

pub fn reindex(
    env: &Env,
    mut state: State,
    added: &[(String, String)],
    modified: &[(String, String)],
    removed: &[String],
) -> State {
    let old_labels: Vec<String> = state.trees.iter().map(|t| t.label.clone()).collect();
    let dirty: FxHashSet<&str> = removed
        .iter()
        .map(|s| s.as_str())
        .chain(modified.iter().map(|(p, _)| p.as_str()))
        .collect();

    let mut dirty_fis = remap(&mut state, &old_labels, &dirty);
    let new_files: Vec<(String, String)> = modified.iter().chain(added.iter()).cloned().collect();
    let new_base = state.trees.len();
    parse(env, &mut state, new_files);
    dirty_fis.extend(new_base..state.trees.len());
    resolve(env, &mut state, dirty_fis, None);
    state
}

pub fn parse_single(lang_id: SupportLang, path: &str, source: &str) -> (Env, Tree, Vec<Edge>) {
    let env = Env::for_lang(lang_id);
    let (tree, edges) = process_file(&env, path, source);
    (env, tree, edges)
}
