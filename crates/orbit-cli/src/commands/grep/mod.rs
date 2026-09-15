mod local;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::search::NodeValue;
use orbit_search::{RecallFilter, SearchVocab, content_words};

use crate::commands::context;
use local::LocalBackend;

fn build_vocab<S: orbit_search::grep::GrepSource>(source: &S) -> Result<SearchVocab, S::Error> {
    use strum::IntoEnumIterator;
    let parts: Vec<(String, String)> = code_graph::v2::types::EdgeKind::iter()
        .flat_map(|kind| {
            let name = kind.as_ref().to_string();
            SearchVocab::kind_name_parts(kind.as_ref())
                .map(|part| (part.to_string(), name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    let words: Vec<String> = parts.iter().map(|(part, _)| part.clone()).collect();
    let stems = source.stem(&words)?;
    Ok(SearchVocab::new(
        stems
            .into_iter()
            .zip(parts.into_iter().map(|(_, kind)| kind)),
    ))
}

const MIN_HITS_PER_QUERY: usize = 3;
const BODY_LIMIT: usize = 3;

pub(crate) fn run(
    queries: Vec<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    limit: usize,
    paths: Vec<String>,
    filter: RecallFilter,
) -> Result<()> {
    let launcher = crate::commands::setup::spec::launcher();
    if let Some(query) = queries.iter().find(|q| content_words(q).is_empty()) {
        anyhow::bail!(
            "no usable search terms in query: {query:?} — to list every definition in a \
             file or directory instead, run `{launcher} grep --path <path>`; to print a whole \
             file, `{launcher} context <path>`"
        );
    }

    let backend = LocalBackend::open(repo, db, &paths)?;

    let mut out = std::io::stdout().lock();
    if queries.is_empty() {
        return report_outline(&mut out, &backend, &paths, &filter, launcher);
    }
    if !paths.is_empty() {
        writeln!(out, "path: {}", paths.join(" "))?;
    }
    if !filter.kinds.is_empty() {
        writeln!(out, "kind: {}", filter.kinds.join(" "))?;
    }

    let vocab = build_vocab(backend.search())?;
    let per_query_limit = (limit / queries.len()).max(MIN_HITS_PER_QUERY.min(limit));
    for (i, query) in queries.iter().enumerate() {
        if i > 0 {
            writeln!(out)?;
        }
        writeln!(out, "grep {:?} @ {}", query, backend.header())?;
        let (outcome, nodes) = backend.grep(query, per_query_limit, &vocab, &filter)?;
        let typed: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
        if outcome.terms != typed {
            writeln!(out, "terms: {}", outcome.terms.join(" "))?;
        }

        if nodes.is_empty() {
            if paths.is_empty() && filter.is_empty() {
                writeln!(out, "\nNo definitions match those terms.")?;
            } else {
                writeln!(
                    out,
                    "\nNo definitions match those terms within that scope; drop --path/--kind to widen."
                )?;
            }
            writeln!(
                out,
                "Rephrase and retry once — use synonyms or identifier fragments \
                 from the code (e.g. \"throttle\" → \"rate limit\"). If the retry \
                 also misses, fall back to text grep."
            )?;
            continue;
        }

        report_results(&mut out, &outcome, &nodes)?;
        let defs: Vec<NodeValue> = nodes.iter().take(BODY_LIMIT).cloned().collect();
        writeln!(out)?;
        write!(
            out,
            "{}",
            context::render_bodies(backend.search().client(), backend.git(), &defs)?
        )?;
    }
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
    report_confidence(out, outcome)?;
    writeln!(out, "\nDefinitions:")?;
    for node in nodes {
        report_definition(out, node)?;
    }
    let hidden = outcome.total.saturating_sub(outcome.matches.len());
    if hidden >= BROAD_HIDDEN_HITS {
        writeln!(
            out,
            "  … {hidden} more — the query is broad; scope with --path <dir>/--kind <Kind> or use a more specific identifier"
        )?;
    } else if hidden > 0 {
        writeln!(
            out,
            "  … {hidden} more (narrow with --path/--kind, or raise --limit)"
        )?;
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

const COMPOUND_TERM_HINT: usize = 5;
const BROAD_HIDDEN_HITS: usize = 100;

fn report_confidence(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
) -> std::io::Result<()> {
    if outcome.terms.len() >= COMPOUND_TERM_HINT {
        writeln!(
            out,
            "note: {} search terms — long queries dilute matching. grep matches \
             symbol-name words, so use one to three identifier-like words per \
             query and batch several queries in one call instead.",
            outcome.terms.len()
        )?;
    }
    if outcome.weak {
        writeln!(
            out,
            "note: weak matches — no term anchors a symbol name, so the results \
             below may be coincidental. Use an identifier fragment the code would \
             use, or scope with --path/--kind."
        )?;
    }
    if !outcome.unmatched_terms.is_empty() {
        writeln!(
            out,
            "note: no matches for: {} — results reflect only the matched terms \
             and may be incomplete. If they look off, retry once with a synonym \
             or identifier fragment for each unmatched term (e.g. \"throttle\" \
             → \"rate limit\").",
            outcome.unmatched_terms.join(", ")
        )?;
    }
    if !outcome.unmatched_terms.is_empty() && !outcome.term_anchors.is_empty() {
        let anchors: Vec<String> = outcome
            .term_anchors
            .iter()
            .map(|(term, fqn)| format!("{term} → {fqn}"))
            .collect();
        writeln!(
            out,
            "note: matched terms anchored on: {}",
            anchors.join(", ")
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(unmatched: Vec<&str>, weak: bool) -> orbit_search::GrepOutcome {
        orbit_search::GrepOutcome {
            terms: Vec::new(),
            matches: Vec::new(),
            total: 0,
            weak,
            unmatched_terms: unmatched.into_iter().map(String::from).collect(),
            term_anchors: Vec::new(),
        }
    }

    #[test]
    fn partial_anchor_note_lists_unmatched_terms_with_a_retry_instruction() {
        let mut buf = Vec::new();
        report_confidence(&mut buf, &outcome(vec!["throttle", "dlq"], false)).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("no matches for: throttle, dlq"), "{text}");
        assert!(text.contains("retry once"), "{text}");
        assert!(!text.contains("weak matches"), "{text}");
    }

    #[test]
    fn weak_and_unmatched_notes_stack() {
        let mut buf = Vec::new();
        report_confidence(&mut buf, &outcome(vec!["throttle"], true)).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("weak matches"), "{text}");
        assert!(text.contains("no matches for: throttle"), "{text}");
    }

    #[test]
    fn results_print_definition_identity_and_full_location() {
        let mut result = outcome(Vec::new(), false);
        result.matches.push(orbit_search::GrepMatch {
            id: 481,
            score: 1.0,
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
            "\nDefinitions:\n  Definition:481  Repo::commit_hook  [Method]  crates/repo/src/lib.rs:42-57\n"
        );
    }

    #[test]
    fn truncated_results_report_how_many_were_hidden() {
        let mut o = outcome(Vec::new(), false);
        o.total = 42;
        let mut buf = Vec::new();
        report_results(&mut buf, &o, &[]).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("42 more (narrow"), "{text}");

        o.total = 0;
        let mut buf = Vec::new();
        report_results(&mut buf, &o, &[]).unwrap();
        assert!(!String::from_utf8(buf).unwrap().contains(" more"));
    }

    #[test]
    fn confident_full_anchor_prints_no_notes() {
        let mut buf = Vec::new();
        report_confidence(&mut buf, &outcome(Vec::new(), false)).unwrap();
        assert!(buf.is_empty());
    }
}
