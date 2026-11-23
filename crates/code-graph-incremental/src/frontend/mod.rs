//! Frontends turn a source file into a tree for the engine. An engine mounts
//! one per language it reads; a file goes to the first that accepts it.

pub mod markdown;
pub mod project;
pub mod treesitter;

use crate::engine::Engine;
use crate::intern::Lang;
use crate::language::SupportLang;
use crate::sentinel::{Killed, Limits, Sentinel};
use crate::tree::Tree;

/// The parse budget's label, which reports name a parse timeout by.
const PARSE: &str = "parse";

/// Turns one source file into a tree whose kinds are the pipeline's.
pub trait Frontend: Send + Sync {
    fn name(&self) -> &str;

    /// Whether this frontend reads the file at `path`.
    fn accepts(&self, path: &str) -> bool;

    /// The file's tree, within `limits`, and why parsing stopped early, if
    /// it did.
    fn parse(
        &self,
        source: &str,
        path: &str,
        lang: &Lang,
        limits: &Limits,
        budget: &Sentinel,
    ) -> Result<(Tree, Option<Killed>), Killed>;
}

/// A tree-sitter grammar's frontend.
pub struct TreeSitter {
    pub language: SupportLang,
}

impl Frontend for TreeSitter {
    fn name(&self) -> &str {
        self.language.grammar()
    }

    fn accepts(&self, path: &str) -> bool {
        SupportLang::from_path(path) == Some(self.language)
    }

    fn parse(
        &self,
        source: &str,
        path: &str,
        lang: &Lang,
        limits: &Limits,
        budget: &Sentinel,
    ) -> Result<(Tree, Option<Killed>), Killed> {
        let depth = limits.max_tree_depth;
        treesitter::parse_with_sentinel(source, self.language, lang, path, budget, depth)
    }
}

/// The hand-written markdown reader.
pub struct Markdown;

impl Frontend for Markdown {
    fn name(&self) -> &str {
        "markdown"
    }

    fn accepts(&self, path: &str) -> bool {
        SupportLang::from_path(path) == Some(SupportLang::Markdown)
    }

    fn parse(
        &self,
        source: &str,
        path: &str,
        lang: &Lang,
        _: &Limits,
        budget: &Sentinel,
    ) -> Result<(Tree, Option<Killed>), Killed> {
        let tree = markdown::parse(source, lang, path);
        budget.check()?;
        Ok((tree, None))
    }
}

/// The frontend a language's files parse with.
pub fn for_language(language: SupportLang) -> Box<dyn Frontend> {
    match language {
        SupportLang::Markdown => Box::new(Markdown),
        language => Box::new(TreeSitter { language }),
    }
}

/// Parses a file with the first mounted frontend that accepts it, or the
/// engine's first, under the parse budget.
pub fn parse(
    env: &Engine,
    path: &str,
    source: &str,
    run: &Sentinel,
) -> Result<(Tree, Option<Killed>), Killed> {
    let frontend = &env.frontend_for(path).unwrap_or(&env.frontends[0]).frontend;
    let budget = run
        .child(PARSE, path, env.limits.file_parse_ms)
        .on_thread_cpu();
    budget.check()?;
    frontend.parse(source, path, &env.lang, &env.limits, &budget)
}
