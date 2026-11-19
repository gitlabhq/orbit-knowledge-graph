//! Frontends turn a source file into a tree for the engine: tree-sitter for
//! grammars, and a hand-written reader for markdown.

pub mod markdown;

use crate::env::Env;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::Tree;
use crate::treesitter::{self, SupportLang};

/// Parses a file with its language's frontend, under the parse budget.
pub fn parse(
    env: &Env,
    path: &str,
    source: &str,
    run: &Sentinel,
) -> Result<(Tree, Option<Killed>), Killed> {
    let language = SupportLang::from_path(path).unwrap_or(env.lang_id);
    let budget = run
        .child("tree-sitter", path, env.limits.file_parse_ms)
        .on_thread_cpu();
    budget.check()?;
    if language == SupportLang::Markdown {
        let tree = markdown::parse(source, &env.lang, path);
        budget.check()?;
        return Ok((tree, None));
    }
    treesitter::parse_with_sentinel(
        source,
        language,
        &env.lang,
        path,
        &budget,
        env.limits.max_tree_depth,
    )
}
