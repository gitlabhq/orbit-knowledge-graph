mod local;

use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::search::NodeValue;
use orbit_search::{RecallFilter, query_alternatives};

use crate::commands::context;
use local::LocalBackend;

const CONTEXT_HINT_LIMIT: usize = 3;

pub(crate) fn run(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    limit: usize,
    paths: Vec<String>,
    filter: RecallFilter,
) -> Result<()> {
    let launcher = crate::commands::setup::spec::launcher();
    if let Some(query) = &query
        && query_alternatives(query).is_err()
    {
        anyhow::bail!(
            "no usable search terms in query: {query:?} — to list every definition in a \
             file or directory instead, run `{launcher} grep --path <path>`; for a file's \
             definition map and connections, `{launcher} context <path>`"
        );
    }

    let backend = LocalBackend::open(repo, db, &paths)?;

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

    writeln!(out, "grep {:?} @ {}", query, backend.header())?;
    let (outcome, nodes) = backend.grep(&query, limit, &filter)?;
    report_exact_query_note(&mut out, &outcome)?;

    if nodes.is_empty() {
        if paths.is_empty() && filter.is_empty() {
            writeln!(
                out,
                "No definitions match. Try a symbol name, or `{launcher} context <path>`."
            )?;
        } else {
            writeln!(
                out,
                "No definitions match in scope. Drop --path/--kind, or try a symbol name."
            )?;
        }
        return Ok(());
    }

    let defs: Vec<_> = nodes
        .iter()
        .zip(&outcome.matches)
        .filter(|(_, hit)| hit.exact_name || hit.name_match)
        .take(CONTEXT_HINT_LIMIT)
        .map(|(node, _)| node.clone())
        .collect();
    report_context_hint(&mut out, &defs, launcher)?;
    report_results(&mut out, &outcome, &nodes)?;
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

fn report_results(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
    nodes: &[NodeValue],
) -> Result<()> {
    for (node, hit) in nodes.iter().zip(&outcome.matches) {
        let range = context::source_range(node)?;
        let label = if hit.exact_name {
            "exact-name"
        } else if hit.name_match {
            "name/path"
        } else {
            "body-only"
        };
        writeln!(
            out,
            "  {}:{}  {}  [{}]  {label}",
            node.entity_type, node.id, range.fqn, range.kind
        )?;
    }
    let hidden = outcome.total.saturating_sub(outcome.matches.len());
    if hidden >= BROAD_HIDDEN_HITS {
        writeln!(
            out,
            "  … {hidden} more; broad query. Add --path/--kind or a specific name."
        )?;
    } else if hidden > 0 {
        writeln!(out, "  … {hidden} more; add --path/--kind or --limit.")?;
    }
    Ok(())
}

fn report_context_hint(out: &mut impl Write, nodes: &[NodeValue], launcher: &str) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }
    write!(out, "next: {launcher} context")?;
    for node in nodes {
        write!(out, " {}:{}", node.entity_type, node.id)?;
    }
    writeln!(out)?;
    Ok(())
}

fn report_definition(out: &mut impl Write, node: &NodeValue) -> Result<()> {
    let range = context::source_range(node)?;
    writeln!(
        out,
        "  {}:{}  {}  [{}]  {}:{}-{}",
        node.entity_type, node.id, range.fqn, range.kind, range.file, range.start, range.end
    )?;
    Ok(())
}

const BROAD_HIDDEN_HITS: usize = 100;

fn report_exact_query_note(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
) -> std::io::Result<()> {
    let exact: HashSet<_> = outcome
        .exact_alternatives
        .iter()
        .map(|alternative| alternative.to_lowercase())
        .collect();
    let alternatives: Vec<_> = outcome
        .alternatives
        .iter()
        .map(String::as_str)
        .filter(|alternative| !alternative.chars().any(char::is_whitespace))
        .collect();
    let matched: Vec<_> = alternatives
        .iter()
        .copied()
        .filter(|alternative| exact.contains(&alternative.to_lowercase()))
        .collect();
    let missing: Vec<_> = alternatives
        .iter()
        .copied()
        .filter(|alternative| !exact.contains(&alternative.to_lowercase()))
        .collect();
    if !matched.is_empty() {
        writeln!(out, "exact: {}", matched.join(" | "))?;
    }
    if !missing.is_empty() {
        writeln!(out, "exact-miss: {}", missing.join(" | "))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome() -> orbit_search::GrepOutcome {
        orbit_search::GrepOutcome {
            alternatives: Vec::new(),
            exact_alternatives: Vec::new(),
            matches: Vec::new(),
            total: 0,
        }
    }

    #[test]
    fn results_print_definition_identity_without_location() {
        let mut result = outcome();
        result.matches.push(orbit_search::GrepMatch {
            id: 481,
            score: 1.0,
            exact_name: true,
            name_match: true,
        });
        result.total = 1;
        let node = NodeValue {
            entity_type: "Definition".to_string(),
            id: 481,
            properties: serde_json::from_value(serde_json::json!({
                "fqn": "Repo::commit_hook",
                "definition_type": "Method",
                "file_path": "crates/repo/src/lib.rs",
                "start_line": 42,
                "end_line": 57
            }))
            .unwrap(),
        };
        let mut buf = Vec::new();
        report_results(&mut buf, &result, &[node]).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "  Definition:481  Repo::commit_hook  [Method]  exact-name\n"
        );
    }

    #[test]
    fn truncated_results_report_how_many_were_hidden() {
        let mut o = outcome();
        o.total = 42;
        let mut buf = Vec::new();
        report_results(&mut buf, &o, &[]).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("42 more; add"), "{text}");

        o.total = 0;
        let mut buf = Vec::new();
        report_results(&mut buf, &o, &[]).unwrap();
        assert!(!String::from_utf8(buf).unwrap().contains(" more"));
    }

    #[test]
    fn exact_query_note_distinguishes_or_alternatives() {
        let mut result = outcome();
        result.alternatives = vec![
            "present".into(),
            "missing".into(),
            "natural language".into(),
        ];
        result.exact_alternatives = vec!["present".to_string()];
        let mut buf = Vec::new();
        report_exact_query_note(&mut buf, &result).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "exact: present\nexact-miss: missing\n"
        );
    }

    #[test]
    fn context_hint_is_copyable_and_batched() {
        let nodes = [481, 482].map(|id| NodeValue {
            entity_type: "Definition".to_string(),
            id,
            properties: serde_json::Map::new(),
        });
        let mut buf = Vec::new();
        report_context_hint(&mut buf, &nodes, "orbit").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "next: orbit context Definition:481 Definition:482\n"
        );
    }
}
