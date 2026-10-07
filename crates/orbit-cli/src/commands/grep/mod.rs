mod local;
mod text;

use std::io::Write;
use std::path::PathBuf;

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

    let backend = LocalBackend::open(repo, db, &paths)?;
    let paths = backend.paths().to_vec();

    let mut out = std::io::stdout().lock();
    let Some(query) = query else {
        return report_outline(&mut out, &backend, &paths, &filter, launcher);
    };
    if !paths.is_empty() {
        writeln!(out, "path: {}", paths.join(" "))?;
    }
    if !filter.kinds.is_empty() {
        writeln!(out, "kind: {}", filter.kinds.join(" "))?;
    }
    let mut alternatives: Vec<text::Term> =
        alternatives.iter().map(|a| text::Term::parse(a)).collect();
    let search = |terms: &[text::Term]| {
        text::hits(
            backend.search().client(),
            backend.git(),
            terms,
            &paths,
            &filter.kinds,
        )
    };
    let hits = match search(&alternatives) {
        Ok(hits) => hits,
        Err(_) => {
            alternatives = alternatives.iter().map(text::Term::literal).collect();
            search(&alternatives)?
        }
    };
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
