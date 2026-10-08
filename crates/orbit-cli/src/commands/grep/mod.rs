mod defs;
mod local;
mod rank;
mod render;
mod scan;
mod term;

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use duckdb_client::search::NodeValue;
use orbit_search::{RecallFilter, query_alternatives};

use crate::commands::context;
use local::LocalBackend;

/// What a search prints, following rg: matching lines, `-l` file names, `-c` counts, or
/// nothing but the exit status with `-q`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Output {
    #[default]
    Lines,
    Files,
    Count,
    Quiet,
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
    options: Options,
) -> Result<()> {
    let launcher = crate::commands::setup::spec::launcher();
    let alternatives = match &query {
        Some(query) => query_alternatives(query).map_err(|_| {
            anyhow::anyhow!(
                "no usable search terms in query: {query:?} — to list every definition in a \
                 file or directory instead, run `{launcher} grep --path <path>`; for a file's \
                 definition map and connections, `{launcher} context <path>`"
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
    check_kinds(&backend, &filter.kinds)?;

    let mut out = std::io::stdout().lock();
    let (Some(query), Some((mut hits, edited))) = (query, scan) else {
        return report_outline(&mut out, &backend, &paths, &filter, launcher);
    };
    let alternatives = terms;
    hits.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
    defs::attach_definitions(
        backend.search().client(),
        backend.git(),
        &mut hits,
        &alternatives,
        &filter.kinds,
        &edited,
    )?;
    let connections = match options.output {
        Output::Lines => defs::connections(backend.search().client(), &hits)?,
        _ => defs::Connections::new(),
    };
    let mut header = format!("grep {query:?}");
    for path in &paths {
        header.push_str(&format!(" --path {path}"));
    }
    if !filter.kinds.is_empty() {
        header.push_str(&format!(" --kind {}", filter.kinds.join(",")));
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
            &options
        )?
    )?;
    out.flush()?;
    match hits.iter().any(|hit| !hit.context) {
        true => Ok(()),
        false => Err(NoMatches.into()),
    }
}

fn report_outline(
    out: &mut impl Write,
    backend: &LocalBackend,
    paths: &[String],
    filter: &RecallFilter,
    launcher: &str,
) -> Result<()> {
    writeln!(out, "outline {} @ {}", paths.join(" "), backend.header())?;
    if !filter.kinds.is_empty() {
        writeln!(out, "kind: {}", filter.kinds.join(" "))?;
    }
    let rows = backend.search().list_corpus(filter)?;
    if rows.is_empty() {
        writeln!(
            out,
            "\nNo indexed definitions under that path. Paths are repo-relative, as printed by `{launcher} grep`."
        )?;
        return Ok(());
    }
    writeln!(out, "\nDefinitions ({}):", rows.len())?;
    for node in &rows {
        report_definition(out, node)?;
    }
    Ok(())
}

fn report_definition(out: &mut impl Write, node: &NodeValue) -> Result<()> {
    let range = context::source_range(node)?;
    writeln!(
        out,
        "  {}  [{}]  {}:{}-{}",
        range.fqn, range.kind, range.file, range.start, range.end
    )?;
    Ok(())
}

fn check_kinds(backend: &LocalBackend, kinds: &[String]) -> Result<()> {
    if kinds.is_empty() {
        return Ok(());
    }
    let git = backend.git();
    let batches = backend.search().client().query_arrow_json(
        "SELECT DISTINCT definition_type AS kind FROM gl_definition
         WHERE project_id = ?1 AND commit_sha = ?2 ORDER BY 1",
        &[git.project_id.into(), git.commit_sha.clone().into()],
    )?;
    let known = duckdb_client::string_column(&batches, "kind");
    let unknown: Vec<&String> = kinds
        .iter()
        .filter(|kind| !known.iter().any(|k| k.eq_ignore_ascii_case(kind)))
        .collect();
    anyhow::ensure!(
        unknown.is_empty(),
        "unknown --kind {}; kinds in this repository: {}",
        unknown
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        known.join(", ")
    );
    Ok(())
}
