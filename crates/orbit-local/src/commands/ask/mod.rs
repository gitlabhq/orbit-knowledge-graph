mod local;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use orbit_search::{KindRates, SearchVocab, content_words};
use std::collections::HashMap;

use local::LocalBackend;

const SNIPPET_LINES: usize = 4;
const SNIPPET_LINE_CHARS: usize = 160;

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
    let outcome = backend.ask(&question, limit, &vocab, kind_rates())?;
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
    write_matches(&mut out, "\nMatches:", &outcome.matches, backend.root())?;
    if !outcome.surfaced.is_empty() {
        write_matches(
            &mut out,
            "\nConnected to your terms (structurally surfaced):",
            &outcome.surfaced,
            backend.root(),
        )?;
    }

    if outcome.weak {
        return Ok(());
    }
    if outcome.edges.is_empty() {
        writeln!(out, "\nNo connections found around the top matches.")?;
        return Ok(());
    }

    writeln!(
        out,
        "\nConnections (around {} anchors):",
        outcome.seed_count
    )?;
    let hidden: HashMap<&str, usize> = outcome
        .hidden_by_kind
        .iter()
        .map(|(kind, n)| (kind.as_str(), *n))
        .collect();
    let mut current = "";
    let mut grouped: Vec<(String, Vec<String>)> = Vec::new();
    let flush_group = |out: &mut dyn Write, grouped: &mut Vec<(String, Vec<String>)>| {
        for (source, targets) in grouped.drain(..) {
            writeln!(out, "    {source} --> {}", targets.join(", "))?;
        }
        std::io::Result::Ok(())
    };
    for e in &outcome.edges {
        if e.kind != current {
            flush_group(&mut out, &mut grouped)?;
            report_hidden(&mut out, current, &hidden)?;
            current = &e.kind;
            writeln!(out, "  {current}:")?;
        }
        let source = format!("{}{}", e.source, fmt_loc(&e.source_loc));
        let target = format!("{}{}", e.target, fmt_loc(&e.target_loc));
        match grouped.iter_mut().find(|(s, _)| *s == source) {
            Some((_, targets)) => targets.push(target),
            None => grouped.push((source, vec![target])),
        }
    }
    flush_group(&mut out, &mut grouped)?;
    report_hidden(&mut out, current, &hidden)?;
    for (kind, n) in &outcome.hidden_by_kind {
        if !outcome.edges.iter().any(|e| &e.kind == kind) {
            writeln!(out, "  {kind}: … {n} below the relevance cut")?;
        }
    }
    Ok(())
}

fn fmt_loc(loc: &str) -> String {
    if loc.is_empty() {
        String::new()
    } else {
        format!(" ({loc})")
    }
}

fn write_matches(
    out: &mut impl Write,
    header: &str,
    matches: &[orbit_search::AskMatch],
    root: &std::path::Path,
) -> std::io::Result<()> {
    writeln!(out, "{header}")?;
    for m in matches {
        writeln!(
            out,
            "  {}  [{}]  {}  (links {})",
            m.row.fqn, m.row.kind, m.row.loc, m.row.degree
        )?;
        if !m.callers.is_empty() {
            let shown: Vec<String> = m
                .callers
                .iter()
                .take(orbit_search::ask::CALLERS_SHOWN)
                .map(|c| format!("{} ({})", c.label, c.loc))
                .collect();
            let extra = m
                .callers_total
                .max(m.callers.len())
                .saturating_sub(shown.len());
            let suffix = if extra > 0 {
                format!(" … +{extra} more")
            } else {
                String::new()
            };
            writeln!(out, "    called by: {}{suffix}", shown.join(", "))?;
        }
        for line in snippet(root, &m.row.loc, m.row.end_line) {
            writeln!(out, "{line}")?;
        }
    }
    Ok(())
}

fn snippet(root: &std::path::Path, loc: &str, end_line: i64) -> Vec<String> {
    let Some((path, start)) = loc.rsplit_once(':') else {
        return Vec::new();
    };
    let Ok(start) = start.parse::<usize>() else {
        return Vec::new();
    };
    let Ok(content) = std::fs::read_to_string(root.join(path)) else {
        return Vec::new();
    };
    let last = usize::try_from(end_line)
        .ok()
        .filter(|&e| e >= start)
        .unwrap_or(start)
        .min(start + SNIPPET_LINES - 1);
    content
        .lines()
        .enumerate()
        .skip(start.saturating_sub(1))
        .take_while(|(i, _)| *i < last)
        .map(|(i, text)| {
            let mut text = text.trim_end();
            if text.len() > SNIPPET_LINE_CHARS {
                let mut cut = SNIPPET_LINE_CHARS;
                while !text.is_char_boundary(cut) {
                    cut -= 1;
                }
                text = &text[..cut];
            }
            format!("    {} | {}", i + 1, text)
        })
        .collect()
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
             omitted. Rephrase with a code identifier, or use `orbit local sql` \
             for an exact-name lookup."
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

fn report_hidden(
    out: &mut impl Write,
    kind: &str,
    hidden: &HashMap<&str, usize>,
) -> std::io::Result<()> {
    if let Some(n) = hidden.get(kind).filter(|_| !kind.is_empty()) {
        writeln!(out, "    … {n} more")?;
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
            seed_count: 0,
            focus: None,
            edges: Vec::new(),
            hidden_by_kind: Vec::new(),
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
