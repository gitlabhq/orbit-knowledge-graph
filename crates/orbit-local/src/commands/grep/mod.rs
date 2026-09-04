mod local;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use orbit_search::{SearchVocab, content_words};

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

pub(crate) fn run(
    query: String,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    limit: usize,
    paths: Vec<String>,
) -> Result<()> {
    if content_words(&query).is_empty() {
        anyhow::bail!("no usable search terms in query: {query:?}");
    }

    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let backend = LocalBackend::open(&repo_path, db, &paths)?;

    let mut out = std::io::stdout().lock();
    writeln!(out, "grep {:?} — {}", query, backend.header())?;
    if !paths.is_empty() {
        writeln!(out, "path: {}", paths.join(" "))?;
    }

    let vocab = build_vocab(backend.search())?;
    let outcome = backend.grep(&query, limit, &vocab)?;
    writeln!(out, "terms: {}", outcome.terms.join(" "))?;

    if outcome.matches.is_empty() {
        if paths.is_empty() {
            writeln!(out, "\nNo definitions match those terms.")?;
        } else {
            writeln!(
                out,
                "\nNo definitions under {} match those terms.",
                paths.join(", ")
            )?;
        }
        writeln!(
            out,
            "Rephrase and retry once — use synonyms or identifier fragments \
             from the code (e.g. \"throttle\" → \"rate limit\"). If the retry \
             also misses, fall back to text grep."
        )?;
        return Ok(());
    }

    report_results(&mut out, &outcome)?;
    let launcher = crate::commands::setup::spec::launcher();
    writeln!(
        out,
        "\n{launcher} show \"<fqn>\" prints a body; {launcher} describe \"<fqn>\" lists its callers and every other connection."
    )?;
    Ok(())
}

fn report_results(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
) -> std::io::Result<()> {
    report_confidence(out, outcome)?;
    writeln!(out, "\nNodes:")?;
    for m in &outcome.matches {
        writeln!(out, "  {}  [{}]  {}", m.row.fqn, m.row.kind, m.row.loc)?;
    }
    let hidden = outcome.total.saturating_sub(outcome.matches.len());
    if hidden > 0 {
        writeln!(
            out,
            "  … {hidden} more not shown — raise --limit or narrow with --path."
        )?;
    }
    if !outcome.weak && !outcome.edges.is_empty() {
        writeln!(out, "\nEdges:")?;
        for e in &outcome.edges {
            writeln!(out, "  {}  -{}->  {}", e.source, e.kind, e.target)?;
        }
    }
    Ok(())
}

const COMPOUND_TERM_HINT: usize = 7;

fn report_confidence(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
) -> std::io::Result<()> {
    if outcome.terms.len() >= COMPOUND_TERM_HINT {
        writeln!(
            out,
            "note: {} search terms — long or compound queries dilute matching. \
             Search one thing at a time (\"where is X defined\", then \"how is Y \
             applied\") for sharper results.",
            outcome.terms.len()
        )?;
    }
    if outcome.weak {
        writeln!(
            out,
            "note: weak matches — too few question terms anchor a symbol name, \
             so the results below may be coincidental and edge details are \
             omitted. Rephrase with a code identifier, or use `{} sql` \
             for an exact-name lookup.",
            crate::commands::setup::spec::launcher()
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
            edges: Vec::new(),
            weak,
            unmatched_terms: unmatched.into_iter().map(String::from).collect(),
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
    fn weak_results_omit_edges_as_the_note_promises() {
        let mut o = outcome(Vec::new(), true);
        o.edges.push(orbit_search::Edge {
            kind: "CALLS".into(),
            source: "A::a".into(),
            source_loc: String::new(),
            target: "B::b".into(),
            target_loc: String::new(),
        });
        let mut buf = Vec::new();
        report_results(&mut buf, &o).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("edge details are omitted"), "{text}");
        assert!(!text.contains("Edges:"), "{text}");

        o.weak = false;
        let mut buf = Vec::new();
        report_results(&mut buf, &o).unwrap();
        assert!(String::from_utf8(buf).unwrap().contains("Edges:"));
    }

    #[test]
    fn truncated_results_report_how_many_were_hidden() {
        let mut o = outcome(Vec::new(), false);
        o.total = 42;
        let mut buf = Vec::new();
        report_results(&mut buf, &o).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("42 more not shown"), "{text}");

        o.total = 0;
        let mut buf = Vec::new();
        report_results(&mut buf, &o).unwrap();
        assert!(!String::from_utf8(buf).unwrap().contains("more not shown"));
    }

    #[test]
    fn confident_full_anchor_prints_no_notes() {
        let mut buf = Vec::new();
        report_confidence(&mut buf, &outcome(Vec::new(), false)).unwrap();
        assert!(buf.is_empty());
    }
}
