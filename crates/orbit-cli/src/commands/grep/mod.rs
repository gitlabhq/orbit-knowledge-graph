mod local;
mod text;

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use duckdb_client::search::NodeValue;
use orbit_search::{RecallFilter, query_alternatives};

use crate::commands::context;
use local::LocalBackend;

pub(crate) fn run(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    _limit: usize,
    paths: Vec<String>,
    filter: RecallFilter,
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

    let mut terms: Vec<text::Term> = alternatives.iter().map(|a| text::Term::parse(a)).collect();
    let scan = match query.is_some() {
        true => {
            let matcher = text::matcher(&terms).or_else(|_| {
                terms = terms.iter().map(text::Term::literal).collect();
                text::matcher(&terms)
            })?;
            let root = crate::workspace::git_toplevel(repo.as_deref().unwrap_or(Path::new(".")))?;
            let scope = crate::workspace::repo_relative_paths(&root, &paths);
            let hits = text::scan(&root, &scope, &matcher)?;
            let mut files: Vec<&str> = hits.iter().map(|h| h.file.as_str()).collect();
            files.sort();
            files.dedup();
            let new = crate::commands::refresh::untracked(&root, &files);
            Some((hits, new))
        }
        false => None,
    };
    let touched = scan
        .as_ref()
        .map(|(_, new)| new.clone())
        .unwrap_or_default();
    let backend = LocalBackend::open(repo, db, &paths, &touched)?;
    let paths = backend.paths().to_vec();
    check_kinds(&backend, &filter.kinds)?;

    let mut out = std::io::stdout().lock();
    let (Some(query), Some((mut hits, _))) = (query, scan) else {
        return report_outline(&mut out, &backend, &paths, &filter, launcher);
    };
    if !paths.is_empty() {
        writeln!(out, "path: {}", paths.join(" "))?;
    }
    if !filter.kinds.is_empty() {
        writeln!(out, "kind: {}", filter.kinds.join(" "))?;
    }
    let alternatives = terms;
    hits.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
    text::attach_definitions(
        backend.search().client(),
        backend.git(),
        &mut hits,
        &alternatives,
        &filter.kinds,
    )?;
    writeln!(out, "grep {:?} @ {}", query, backend.header())?;
    write!(out, "{}", text::render(&hits, &alternatives))?;
    write!(
        out,
        "{}",
        text::top_source(&backend.git().repo_path, &hits, &alternatives)
    )?;
    Ok(())
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
