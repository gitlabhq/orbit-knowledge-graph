mod local;
mod rerank;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use orbit_search::{KindRates, SearchVocab, content_words};
use std::collections::HashMap;

use local::LocalBackend;

fn build_vocab<S: orbit_search::ask::AskSource>(source: &S) -> Result<SearchVocab, S::Error> {
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

fn kind_rates() -> &'static HashMap<String, KindRates> {
    static RATES: std::sync::OnceLock<HashMap<String, KindRates>> = std::sync::OnceLock::new();
    RATES.get_or_init(|| {
        use strum::IntoEnumIterator;
        let Ok(ontology) = ontology::Ontology::load_embedded() else {
            return HashMap::new();
        };
        code_graph::v2::types::EdgeKind::iter()
            .filter_map(|kind| {
                let name = kind.as_ref().to_uppercase();
                let weight = ontology.edge_search_weight(&name)?;
                Some((name, KindRates::new(weight)))
            })
            .collect()
    })
}

pub(crate) fn run(
    question: String,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    limit: usize,
) -> Result<()> {
    if content_words(&question).is_empty() {
        anyhow::bail!("no usable search terms in question: {question:?}");
    }

    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let backend = LocalBackend::open(&repo_path, db)?;

    let mut out = std::io::stdout().lock();
    writeln!(out, "ask {:?} — {}", question, backend.header())?;

    let vocab = build_vocab(backend.search())?;
    let reranker = match rerank::CrossEncoder::load(backend.root()) {
        Ok(r) => Some(r),
        Err(err) => {
            eprintln!("note: reranker unavailable, using lexical order only: {err:#}");
            None
        }
    };
    let outcome = backend.ask(
        &question,
        limit,
        &vocab,
        kind_rates(),
        reranker.as_ref().map(|r| r as &dyn orbit_search::Reranker),
    )?;
    writeln!(out, "terms: {}", outcome.terms.join(" "))?;

    if outcome.matches.is_empty() {
        writeln!(out, "\nNo definitions match those terms.")?;
        writeln!(
            out,
            "Rephrase and retry once — use synonyms or identifier fragments \
             from the code (e.g. \"throttle\" → \"rate limit\"). If the retry \
             also misses, fall back to grep."
        )?;
        return Ok(());
    }

    report_confidence(&mut out, &outcome)?;
    writeln!(out, "\nNodes:")?;
    for m in outcome.matches.iter().chain(&outcome.surfaced) {
        writeln!(out, "  {}  [{}]  {}", m.row.fqn, m.row.kind, m.row.loc)?;
    }
    if !outcome.edges.is_empty() {
        writeln!(out, "\nEdges:")?;
        for e in &outcome.edges {
            writeln!(out, "  {}  -{}->  {}", e.source, e.kind, e.target)?;
        }
    }
    let launcher = crate::commands::setup::spec::launcher();
    writeln!(
        out,
        "\n{launcher} show \"<fqn>\" prints a body; {launcher} describe \"<fqn>\" prints every connection."
    )?;
    Ok(())
}

const COMPOUND_TERM_HINT: usize = 7;

fn report_confidence(
    out: &mut impl Write,
    outcome: &orbit_search::AskOutcome,
) -> std::io::Result<()> {
    if outcome.terms.len() >= COMPOUND_TERM_HINT {
        writeln!(
            out,
            "note: {} search terms — long or compound questions dilute matching. \
             Ask one question at a time (\"where is X defined\", then \"how is Y \
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

    fn outcome(unmatched: Vec<&str>, weak: bool) -> orbit_search::AskOutcome {
        orbit_search::AskOutcome {
            terms: Vec::new(),
            matches: Vec::new(),
            surfaced: Vec::new(),
            focus: None,
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
    fn confident_full_anchor_prints_no_notes() {
        let mut buf = Vec::new();
        report_confidence(&mut buf, &outcome(Vec::new(), false)).unwrap();
        assert!(buf.is_empty());
    }
}
