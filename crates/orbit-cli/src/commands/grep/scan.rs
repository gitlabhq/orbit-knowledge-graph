use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};

use grep_regex::RegexMatcher;
use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkMatch};

use super::Options;

use super::defs::Def;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Hit {
    pub(super) file: String,
    pub(super) line: usize,
    pub(super) text: String,
    pub(super) def: Option<Def>,
    /// A `-A`/`-B`/`-C` line around a match rather than a match.
    pub(super) context: bool,
}

/// Collects one file's matches and `-A`/`-B`/`-C` context lines, stopping after `-m` matches.
struct Collect<'a> {
    file: &'a str,
    hits: Vec<Hit>,
    max_count: Option<u64>,
    matched: u64,
}

impl Collect<'_> {
    fn push(&mut self, line: Option<u64>, bytes: &[u8], context: bool) {
        self.hits.push(Hit {
            file: self.file.to_string(),
            line: line.unwrap_or(0) as usize,
            text: String::from_utf8_lossy(bytes)
                .trim_end_matches(['\n', '\r'])
                .to_string(),
            def: None,
            context,
        });
    }
}

impl Sink for Collect<'_> {
    type Error = std::io::Error;

    fn matched(&mut self, _: &Searcher, found: &SinkMatch<'_>) -> std::io::Result<bool> {
        self.push(found.line_number(), found.bytes(), false);
        self.matched += 1;
        Ok(self.max_count.is_none_or(|max| self.matched < max))
    }

    fn context(&mut self, _: &Searcher, around: &SinkContext<'_>) -> std::io::Result<bool> {
        self.push(around.line_number(), around.bytes(), true);
        Ok(true)
    }
}

pub(super) fn scan(
    repo: &Path,
    paths: &[String],
    matcher: &RegexMatcher,
    options: &Options,
) -> Result<Vec<Hit>> {
    let scope = Scope::new(repo, paths)?;
    let scope = &scope;
    let roots = scope.roots(repo);
    let mut walk = ignore::WalkBuilder::new(&roots[0]);
    for root in &roots[1..] {
        walk.add(root);
    }
    walk.hidden(false)
        .require_git(false)
        .filter_entry(|entry| entry.file_name() != ".git");
    if !options.globs.is_empty() {
        let mut globs = ignore::overrides::OverrideBuilder::new(repo);
        for glob in &options.globs {
            globs
                .add(glob)
                .with_context(|| format!("invalid glob {glob:?}"))?;
        }
        walk.overrides(globs.build()?);
    }
    if !options.types.is_empty() || !options.types_not.is_empty() {
        let mut types = ignore::types::TypesBuilder::new();
        types.add_defaults();
        for name in &options.types {
            types.select(name);
        }
        for name in &options.types_not {
            types.negate(name);
        }
        walk.types(types.build()?);
    }
    let found = Mutex::new(Vec::new());
    walk.build_parallel().run(|| {
        let mut searcher = SearcherBuilder::new()
            .binary_detection(BinaryDetection::quit(0))
            .line_number(true)
            .invert_match(options.invert)
            .before_context(options.before)
            .after_context(options.after)
            .build();
        let found = &found;
        Box::new(move |entry| {
            let Ok(entry) = entry else {
                return ignore::WalkState::Continue;
            };
            if !entry.file_type().is_some_and(|t| t.is_file()) {
                return ignore::WalkState::Continue;
            }
            let Ok(relative) = entry.path().strip_prefix(repo) else {
                return ignore::WalkState::Continue;
            };
            let file = relative.to_string_lossy().replace('\\', "/");
            if !scope.contains(&file) {
                return ignore::WalkState::Continue;
            }
            let mut sink = Collect {
                file: &file,
                hits: Vec::new(),
                max_count: options.max_count,
                matched: 0,
            };
            let _ = searcher.search_path(matcher, entry.path(), &mut sink);
            if sink.matched > 0 {
                found.lock().unwrap().extend(sink.hits);
            }
            ignore::WalkState::Continue
        })
    });
    Ok(found.into_inner().unwrap())
}

/// Path scopes compiled once for the walk. Paths that exist are literal, even with glob
/// characters such as `app/[slug]`; the rest are globs over files and directories.
struct Scope {
    globs: Option<globset::GlobSet>,
    prefixes: Vec<String>,
}

impl Scope {
    fn new(repo: &Path, paths: &[String]) -> Result<Self> {
        let root = dunce::canonicalize(repo)?;
        let mut globs = globset::GlobSetBuilder::new();
        let (mut globbed, mut prefixes, mut missing, mut outside) =
            (false, Vec::new(), Vec::new(), Vec::new());
        for path in paths.iter().map(|p| p.trim_end_matches('/')) {
            match dunce::canonicalize(repo.join(path)) {
                Ok(full) if full.starts_with(&root) => prefixes.push(path.to_string()),
                Ok(_) => outside.push(path),
                Err(_) if path.contains(['*', '?', '[']) => {
                    for pattern in [path.to_string(), format!("{path}/**")] {
                        globs.add(
                            globset::Glob::new(&pattern)
                                .with_context(|| format!("invalid path glob {path:?}"))?,
                        );
                    }
                    globbed = true;
                }
                Err(_) => missing.push(path),
            }
        }
        anyhow::ensure!(
            outside.is_empty(),
            "paths outside the repository: {}",
            outside.join(", ")
        );
        for path in &missing {
            eprintln!("orbit: {path}: No such file or directory");
        }
        anyhow::ensure!(
            missing.is_empty() || globbed || !prefixes.is_empty(),
            "no such path in the repository: {}",
            missing.join(", ")
        );
        Ok(Self {
            globs: globbed.then(|| globs.build()).transpose()?,
            prefixes,
        })
    }

    fn roots(&self, repo: &Path) -> Vec<std::path::PathBuf> {
        if self.globs.is_some() || self.prefixes.is_empty() {
            return vec![repo.to_path_buf()];
        }
        let mut sorted = self.prefixes.clone();
        sorted.sort();
        let mut kept: Vec<String> = Vec::new();
        for path in sorted {
            if !kept.iter().any(|outer| within(&path, outer)) {
                kept.push(path);
            }
        }
        kept.iter().map(|path| repo.join(path)).collect()
    }

    fn contains(&self, path: &str) -> bool {
        self.globs.as_ref().is_none_or(|globs| {
            globs.is_match(path) || self.prefixes.iter().any(|scope| within(path, scope))
        })
    }
}

fn within(path: &str, scope: &str) -> bool {
    path.strip_prefix(scope)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::grep::term::{Term, matcher};

    #[test]
    fn scan_reads_the_working_tree_like_ripgrep() {
        let repo = tempfile::tempdir().unwrap();
        for (path, body) in [
            (".gitignore", "dist/\n"),
            ("src/a.rs", "x\nmarkInSync();\n"),
            ("dist/c.js", "markInSync();\n"),
        ] {
            let full = repo.path().join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        }
        let options = Options {
            before: 1,
            ..Options::default()
        };
        let found = matcher(&[Term::parse("mark_in_sync")], &options).unwrap();
        let hits = scan(repo.path(), &[], &found, &options).unwrap();
        let lines: Vec<(&str, usize, bool)> = hits
            .iter()
            .map(|h| (h.file.as_str(), h.line, h.context))
            .collect();
        assert_eq!(lines, [("src/a.rs", 1, true), ("src/a.rs", 2, false)]);
    }
}
