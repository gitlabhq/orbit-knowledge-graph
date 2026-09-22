mod local;

use std::collections::HashSet;
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
        let body = hit
            .body_offset
            .filter(|_| !hit.exact_name && !hit.name_match)
            .map(|offset| {
                let text: String = hit.body_text.chars().take(BODY_PREVIEW_CHARS).collect();
                (range.start + offset - 1, text)
            });
        let mentions = match body {
            Some(_) => format!(" \u{d7}{}", hit.mentions),
            None => String::new(),
        };
        writeln!(
            out,
            "  {}:{}  {}  [{}]  {}:{}-{}  {label}{mentions}",
            node.entity_type, node.id, range.fqn, range.kind, range.file, range.start, range.end
        )?;
        if let Some((line, text)) = body {
            writeln!(out, "      {line}| {text}")?;
        }
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
const BODY_PREVIEW_CHARS: usize = 100;

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
    fn results_print_definition_identity_and_range() {
        let mut result = outcome();
        result.matches.push(orbit_search::GrepMatch {
            id: 481,
            score: 1.0,
            exact_name: true,
            name_match: true,
            body_offset: None,
            body_text: String::new(),
            mentions: 0,
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
            "  Definition:481  Repo::commit_hook  [Method]  crates/repo/src/lib.rs:42-57  exact-name\n"
        );
    }

    #[test]
    fn body_only_results_print_the_first_matching_line() {
        let mut result = outcome();
        result.matches.push(orbit_search::GrepMatch {
            id: 7,
            score: 1.0,
            exact_name: false,
            name_match: false,
            body_offset: Some(3),
            body_text: "x".repeat(140),
            mentions: 2,
        });
        result.total = 1;
        let node = NodeValue {
            entity_type: "Definition".to_string(),
            id: 7,
            properties: serde_json::from_value(serde_json::json!({
                "fqn": "Repo::run",
                "definition_type": "Method",
                "file_path": "src/lib.rs",
                "start_line": 10,
                "end_line": 20
            }))
            .unwrap(),
        };
        let mut buf = Vec::new();
        report_results(&mut buf, &result, &[node]).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            format!(
                "  Definition:7  Repo::run  [Method]  src/lib.rs:10-20  body-only ×2\n      12| {}\n",
                "x".repeat(100)
            )
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
}
