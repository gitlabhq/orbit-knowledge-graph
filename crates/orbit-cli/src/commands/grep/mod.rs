mod defs;
mod kind;
mod local;
mod qualifier;
mod rank;
mod remote;
mod render;
mod scan;
mod term;

pub(crate) use rank::is_test;

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use orbit_search::{RecallFilter, query_alternatives};

use crate::remote::grep::{Outcome, scope_for};
use local::LocalBackend;

/// What a search prints, following rg: matching lines, `-l` file names, `-c` counts, or
/// nothing but the exit status with `-q`; `file:` and `dir:` list where
/// the matches are with their definitions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Output {
    #[default]
    Lines,
    Files,
    Count,
    Quiet,
    FileRows,
    Directories,
}

/// The rg and grep flags a search honors.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Options {
    pub(crate) fixed: bool,
    pub(crate) word: bool,
    pub(crate) invert: bool,
    pub(crate) before: usize,
    pub(crate) after: usize,
    pub(crate) max_count: Option<u64>,
    /// rg `-g` globs: a leading `!` excludes.
    pub(crate) globs: Vec<String>,
    pub(crate) types: Vec<String>,
    pub(crate) types_not: Vec<String>,
    pub(crate) max_columns: Option<usize>,
    pub(crate) output: Output,
}

/// A search that found nothing; exits 1 without a message, as rg does.
#[derive(Debug)]
pub(crate) struct NoMatches;

impl std::fmt::Display for NoMatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no matches")
    }
}

impl std::error::Error for NoMatches {}

pub(crate) async fn run(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    mut paths: Vec<String>,
    mut options: Options,
) -> Result<()> {
    let sections = qualifier::apply(query, &mut options, &mut paths);
    let mut found = false;
    for section in sections {
        if found {
            println!();
        }
        let mut options = options.clone();
        if section.query.as_deref() == Some(".") && section.kinds.is_empty() {
            options.output = Output::Files;
        }
        let filter = RecallFilter {
            kinds: section.kinds,
        };
        match search(
            section.query,
            repo.clone(),
            db.clone(),
            paths.clone(),
            filter,
            options,
        )
        .await
        {
            Ok(()) => found = true,
            Err(error) if error.is::<NoMatches>() => {}
            Err(error) => return Err(error),
        }
    }
    match found {
        true => Ok(()),
        false => Err(NoMatches.into()),
    }
}

async fn search(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    paths: Vec<String>,
    mut filter: RecallFilter,
    options: Options,
) -> Result<()> {
    let root = crate::workspace::git_toplevel(repo.as_deref().unwrap_or(Path::new("."))).ok();
    let exists = |path: &str| {
        Path::new(path).exists() || root.as_ref().is_some_and(|root| root.join(path).exists())
    };
    let indexed = || indexed_checkouts(db.clone());
    let route = remote::route(
        query.as_deref(),
        repo.as_deref(),
        paths,
        &filter.kinds,
        &options,
        remote::Checkouts {
            exists: &exists,
            indexed: &indexed,
        },
    )?;
    for reason in &route.skipped {
        eprintln!("orbit: {reason}");
    }
    let label = query.clone().unwrap_or_default();
    let pending: Vec<_> = route
        .remote
        .into_iter()
        .map(|request| {
            let explicit = request.target.explicit();
            let search = crate::remote::grep::search(request, label.clone());
            (explicit, tokio::spawn(search))
        })
        .collect();
    if !pending.is_empty() {
        filter.kinds.retain(|kind| scope_for(kind).is_none());
        if route.local.is_empty() && !filter.kinds.is_empty() {
            eprintln!(
                "orbit: skipping hosted {}; use `{} query`",
                filter.kinds.join(", "),
                crate::commands::setup::spec::launcher()
            );
        }
    }
    let remote_only = route.local.is_empty();
    let (mut found, mut printed) = (false, false);
    for target in route.local {
        if printed {
            println!();
        }
        found |= local(
            query.clone(),
            target.repo,
            db.clone(),
            target.paths,
            filter.clone(),
            options.clone(),
        )?;
        printed = true;
    }
    for (explicit, task) in pending {
        match task
            .await
            .unwrap_or_else(|error| Outcome::Failed(error.to_string()))
        {
            Outcome::Found(text) => {
                let mut out = std::io::stdout().lock();
                if printed {
                    writeln!(out)?;
                }
                write!(out, "{text}")?;
                out.flush()?;
                (found, printed) = (true, true);
            }
            Outcome::Skipped(reason) if explicit || remote_only => {
                eprintln!("orbit: GitLab search skipped: {reason}");
            }
            Outcome::Failed(reason) => eprintln!("orbit: GitLab search failed: {reason}"),
            Outcome::Empty | Outcome::Skipped(_) => {}
        }
    }
    match found {
        true => Ok(()),
        false => Err(NoMatches.into()),
    }
}

fn indexed_checkouts(db: Option<PathBuf>) -> Vec<(PathBuf, String, String)> {
    if db.is_some() {
        return Vec::new();
    }
    let Ok(manifest) = crate::workspace::Workspace::open_default().map(|w| w.manifest_path())
    else {
        return Vec::new();
    };
    if !manifest.exists() {
        return Vec::new();
    }
    let Ok(batches) = crate::database::manifest::read(
        &manifest,
        "SELECT repo_path FROM _orbit_manifest WHERE CAST(status AS VARCHAR) = 'indexed'",
    ) else {
        return Vec::new();
    };
    duckdb_client::string_column(&batches, "repo_path")
        .into_iter()
        .map(PathBuf::from)
        .filter_map(|checkout| {
            let (host, full_path) = crate::remote::origin::project(&checkout)?;
            Some((checkout, host, full_path))
        })
        .collect()
}

fn local(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    paths: Vec<String>,
    mut filter: RecallFilter,
    mut options: Options,
) -> Result<bool> {
    let languages = kind::languages(&mut filter.kinds);
    options.types.extend(languages.iter().cloned());
    let launcher = crate::commands::setup::spec::launcher();
    let alternatives = match &query {
        Some(query) => query_alternatives(query).map_err(|_| {
            anyhow::anyhow!(
                "no usable search terms in query: {query:?} — for a file's definitions and \
                 connections, run `{launcher} context <path>`"
            )
        })?,
        None => Vec::new(),
    };

    let mut terms: Vec<term::Term> = alternatives.iter().map(|a| term::Term::parse(a)).collect();
    if options.fixed {
        terms = terms.iter().map(term::Term::fixed).collect();
    }
    let mut notes: Vec<String> = Vec::new();
    let scan = match query.is_some() {
        true => {
            let matcher = term::matcher(&terms, &options).or_else(|_| {
                terms = terms.iter().map(term::Term::literal).collect();
                term::matcher(&terms, &options)
            })?;
            let root = crate::workspace::git_toplevel(repo.as_deref().unwrap_or(Path::new(".")))?;
            let scope = crate::workspace::repo_relative_paths(&root, &paths);
            let scope = resolve_missing(&root, scope, &mut notes);
            let mut hits = scan::scan(&root, &scope, &matcher, &options)?;
            if !scope.is_empty()
                && options.output == Output::Lines
                && !options.invert
                && !hits.iter().any(|hit| !hit.context)
            {
                let mut level = scope.clone();
                while let Some(parents) = parents(&level) {
                    let wider = scan::scan(&root, &parents, &matcher, &options)?;
                    if wider.iter().any(|hit| !hit.context) {
                        let shown = match parents.is_empty() {
                            true => "the whole repository".to_string(),
                            false => parents.join(", "),
                        };
                        notes.push(format!(
                            "{NOTE}0 matching lines in {}; showing matches in {shown}",
                            scope.join(", ")
                        ));
                        hits = wider;
                        break;
                    }
                    level = parents;
                }
            }
            let words: Vec<term::Term> = terms
                .iter()
                .filter(|term| !term.is_regex())
                .flat_map(|term| term.raw.split_whitespace().map(term::Term::parse))
                .collect();
            if words.len() > terms.len()
                && !options.fixed
                && !options.invert
                && options.output == Output::Lines
                && !hits.iter().any(|hit| !hit.context)
            {
                let any = scan::scan(&root, &scope, &term::matcher(&words, &options)?, &options)?;
                if any.iter().any(|hit| !hit.context) {
                    notes.push(format!(
                        "{NOTE}0 lines contain {:?}; showing lines with any of its words, files with the most of them first",
                        alternatives.join("|")
                    ));
                    (hits, terms) = (any, words);
                }
            }
            let mut files: Vec<&str> = hits.iter().map(|h| h.file.as_str()).collect();
            files.sort();
            files.dedup();
            use crate::commands::index::incremental::{Freshness, refresh};
            let edited = match refresh(&root, db.clone(), &files) {
                Ok(Freshness::Fresh) => Default::default(),
                Ok(Freshness::Stale(changed)) => changed.into_iter().collect(),
                _ => crate::workspace::edited_since_index(&root, &files),
            };
            Some((hits, edited))
        }
        false => None,
    };
    let backend = LocalBackend::open(repo, db, &paths)?;
    let paths = backend.paths().to_vec();
    let kinds = resolve_kinds(&backend, &filter.kinds)?;
    if let (Some(shape), Output::Lines) = (kinds.output, options.output) {
        options.output = shape;
    }
    let (kinds, shown) = (kinds.definitions, kinds.shown);

    let mut out = std::io::stdout().lock();
    let (Some(query), Some((mut hits, edited))) = (query, scan) else {
        anyhow::bail!("orbit grep needs a pattern, as with rg");
    };
    let alternatives = terms;
    hits.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
    defs::attach_definitions(
        backend.client(),
        backend.git(),
        &mut hits,
        &alternatives,
        &kinds,
        &edited,
    )?;
    let mut header = format!("grep {query:?}");
    for path in &paths {
        header.push_str(&format!(" {path}"));
    }
    if !shown.is_empty() {
        for kind in &shown {
            header.push_str(&format!(" {kind}:"));
        }
    }
    for language in &languages {
        header.push_str(&format!(" -t {language}"));
    }
    header.push_str(&format!(" @ {}", backend.header()));
    let rendered = render::render(
        &header,
        &hits,
        &alternatives,
        &edited,
        &options,
        render::lookup(&backend.git().repo_path, &hits, &alternatives, &options).as_deref(),
    )?;
    match (options.output, rendered.split_once('\n')) {
        (Output::Lines, Some((first, rest))) if !notes.is_empty() => {
            write!(out, "{first}\n{}\n{rest}", notes.join("\n"))?
        }
        _ => {
            for note in &notes {
                eprintln!("orbit: {}", note.trim_start_matches(NOTE));
            }
            write!(out, "{rendered}")?
        }
    }
    out.flush()?;
    Ok(hits.iter().any(|hit| !hit.context))
}

const NOTE: &str = "» ";

/// The parent directories of a scope, deduplicated, or the whole repository (an empty scope)
/// once every path is at the top. None after the whole repository.
fn parents(scope: &[String]) -> Option<Vec<String>> {
    if scope.is_empty() {
        return None;
    }
    let mut up: Vec<String> = scope
        .iter()
        .filter_map(|path| {
            path.trim_end_matches('/')
                .rsplit_once('/')
                .map(|(dir, _)| dir.to_string())
        })
        .collect();
    if up.len() < scope.len() {
        return Some(Vec::new());
    }
    up.sort();
    up.dedup();
    let kept: Vec<String> = up
        .iter()
        .filter(|path| {
            !up.iter()
                .any(|outer| outer != *path && path.starts_with(&format!("{outer}/")))
        })
        .cloned()
        .collect();
    Some(kept)
}
const PATH_CANDIDATES: usize = 3;

/// Scope paths that do not exist are replaced by tracked directories or files with the same last
/// segment, so a guessed prefix such as `pkg/kubectl/describe` still searches
/// `staging/src/k8s.io/kubectl/pkg/describe`. Candidates sharing more of the guessed segments come
/// first, then non-test paths, then shorter ones. Globs and paths with no candidate stay as written.
fn resolve_missing(root: &Path, scope: Vec<String>, notes: &mut Vec<String>) -> Vec<String> {
    let root_text = format!("{}/", root.to_string_lossy().trim_end_matches('/'));
    let scope: Vec<String> = scope
        .into_iter()
        .map(|path| {
            path.strip_prefix(&root_text)
                .map(str::to_string)
                .unwrap_or(path)
        })
        .collect();
    let missing = |path: &String| !path.contains(['*', '?', '[']) && !root.join(path).exists();
    if !scope.iter().any(missing) {
        return scope;
    }
    let tracked: Vec<String> = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z"])
        .stderr(std::process::Stdio::null())
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .split('\0')
                .filter(|file| !file.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let mut resolved = Vec::new();
    for path in scope {
        if !missing(&path) {
            resolved.push(path);
            continue;
        }
        let segments: Vec<&str> = path
            .split('/')
            .filter(|s| !s.is_empty() && *s != ".")
            .collect();
        let Some(last) = segments.last().copied() else {
            resolved.push(path);
            continue;
        };
        let mut found: Vec<String> = tracked
            .iter()
            .filter_map(|file| {
                let parts: Vec<&str> = file.split('/').collect();
                let at = parts.iter().position(|part| *part == last)?;
                Some(parts[..=at].join("/"))
            })
            .collect();
        found.sort();
        found.dedup();
        let shared = |candidate: &str| {
            let parts: Vec<&str> = candidate.split('/').collect();
            segments.iter().filter(|s| parts.contains(s)).count()
        };
        found.sort_by_key(|c| (std::cmp::Reverse(shared(c)), is_test(c), c.len(), c.clone()));
        found.truncate(PATH_CANDIDATES);
        if found.is_empty() {
            resolved.push(path);
            continue;
        }
        notes.push(format!(
            "{NOTE}{path} not found; searched {}",
            found.join(", ")
        ));
        resolved.extend(found);
    }
    resolved
}

fn resolve_kinds(backend: &LocalBackend, requested: &[String]) -> Result<kind::Kinds> {
    if requested.is_empty() {
        return Ok(kind::Kinds::default());
    }
    let git = backend.git();
    let batches = backend.client().query_arrow_json(
        "SELECT DISTINCT definition_type AS kind FROM gl_definition
         WHERE project_id = ?1 AND commit_sha = ?2 ORDER BY 1",
        &[git.project_id.into(), git.commit_sha.clone().into()],
    )?;
    let known = duckdb_client::string_column(&batches, "kind");
    let kinds = kind::resolve(requested, &known)?;
    let launcher = crate::commands::setup::spec::launcher();
    let hosted = kinds.hosted.join(", ");
    let ci = kinds.hosted.iter().any(|kind| {
        matches!(
            kind.to_ascii_lowercase().as_str(),
            "pipeline" | "job" | "stage"
        )
    });
    anyhow::ensure!(
        !ci || kinds.hosted.len() < requested.len(),
        "{hosted} isn't searchable; read CI with `{launcher} context` (the current branch's MR and \
         CI), `{launcher} context branch:name`, `pipeline:ID`, or `job:ID`"
    );
    anyhow::ensure!(
        kinds.hosted.is_empty() || kinds.hosted.len() < requested.len(),
        "{hosted} lives in the hosted graph; `{launcher} grep` searches GitLab for work items, \
         merge requests, commits, notes, milestones, users, projects, and groups only without \
         paths or file filters. Otherwise use `{launcher} query`"
    );
    if !kinds.hosted.is_empty() {
        eprintln!("orbit: skipping hosted {hosted}; use `{launcher} query`");
    }
    if !kinds.unknown.is_empty() {
        eprintln!(
            "orbit: ignoring unknown {}; kinds here: File, Directory, Definition, {}",
            kinds.unknown.join(", "),
            known.join(", ")
        );
    }
    Ok(kinds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(files: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for file in files {
            let full = dir.path().join(file);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, "x\n").unwrap();
        }
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .output()
                .unwrap()
        };
        git(&["init", "-q"]);
        git(&["add", "."]);
        dir
    }

    #[test]
    fn missing_paths_resolve_to_tracked_paths_with_the_same_last_segment() {
        let dir = repo(&[
            "staging/kubectl/pkg/describe/describe.go",
            "staging/kubectl/pkg/cmd/describe/cmd.go",
            "pkg/probe/request.go",
            "staging/apiserver/pkg/audit/request.go",
            "src/real.rs",
        ]);
        let mut notes = Vec::new();
        let scope = resolve_missing(
            dir.path(),
            vec![
                "pkg/kubectl/describe".into(),
                "audit/request.go".into(),
                "src/real.rs".into(),
                "no/such".into(),
            ],
            &mut notes,
        );
        assert_eq!(
            scope,
            [
                "staging/kubectl/pkg/describe",
                "staging/kubectl/pkg/cmd/describe",
                "staging/apiserver/pkg/audit/request.go",
                "pkg/probe/request.go",
                "src/real.rs",
                "no/such",
            ]
        );
        assert_eq!(
            notes,
            [
                "» pkg/kubectl/describe not found; searched staging/kubectl/pkg/describe, staging/kubectl/pkg/cmd/describe",
                "» audit/request.go not found; searched staging/apiserver/pkg/audit/request.go, pkg/probe/request.go",
            ]
        );
    }

    #[test]
    fn parents_climb_one_directory_at_a_time_to_the_whole_repository() {
        let start = vec![
            "a/b/c.rs".to_string(),
            "a/b/d.rs".to_string(),
            "a/e/f".to_string(),
        ];
        let one = parents(&start).unwrap();
        assert_eq!(one, ["a/b", "a/e"]);
        assert_eq!(parents(&one).unwrap(), ["a"]);
        assert_eq!(parents(&["a".to_string()]).unwrap(), Vec::<String>::new());
        assert_eq!(parents(&[]), None);
    }
}
