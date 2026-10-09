mod defs;
mod kind;
mod local;
mod rank;
mod render;
mod scan;
mod term;

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use orbit_search::{RecallFilter, query_alternatives};

use local::LocalBackend;

/// What a search prints, following rg: matching lines, `-l` file names, `-c` counts, or
/// nothing but the exit status with `-q`; `--kind File` and `--kind Directory` list where
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

pub(crate) fn run(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    paths: Vec<String>,
    filter: RecallFilter,
    mut options: Options,
) -> Result<()> {
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
        terms = terms.iter().map(term::Term::literal).collect();
    }
    let scan = match query.is_some() {
        true => {
            let matcher = term::matcher(&terms, &options).or_else(|_| {
                terms = terms.iter().map(term::Term::literal).collect();
                term::matcher(&terms, &options)
            })?;
            let root = crate::workspace::git_toplevel(repo.as_deref().unwrap_or(Path::new(".")))?;
            let scope = crate::workspace::repo_relative_paths(&root, &paths);
            let hits = scan::scan(&root, &scope, &matcher, &options)?;
            let mut files: Vec<&str> = hits.iter().map(|h| h.file.as_str()).collect();
            files.sort();
            files.dedup();
            let edited = crate::workspace::edited_since_index(&root, &files);
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
    let connections = match options.output {
        Output::Lines => defs::connections(backend.client(), &hits)?,
        _ => defs::Connections::new(),
    };
    let mut header = format!("grep {query:?}");
    for path in &paths {
        header.push_str(&format!(" {path}"));
    }
    if !shown.is_empty() {
        header.push_str(&format!(" --kind {}", shown.join(",")));
    }
    header.push_str(&format!(" @ {}", backend.header()));
    write!(
        out,
        "{}",
        render::render(
            &header,
            &hits,
            &alternatives,
            &connections,
            &edited,
            &options,
            render::lookup(
                &backend.git().repo_path,
                &hits,
                &alternatives,
                &connections,
                &options
            )
            .as_deref(),
        )?
    )?;
    out.flush()?;
    match hits.iter().any(|hit| !hit.context) {
        true => Ok(()),
        false => Err(NoMatches.into()),
    }
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
    anyhow::ensure!(
        kinds.hosted.is_empty() || kinds.hosted.len() < requested.len(),
        "{hosted} lives in the hosted graph, which `{launcher} grep` does not search yet; \
         use `{launcher} query`"
    );
    if !kinds.hosted.is_empty() {
        eprintln!("orbit: skipping hosted --kind {hosted}; use `{launcher} query`");
    }
    if !kinds.unknown.is_empty() {
        eprintln!(
            "orbit: ignoring unknown --kind {}; kinds here: File, Directory, Definition, {}",
            kinds.unknown.join(", "),
            known.join(", ")
        );
    }
    Ok(kinds)
}
