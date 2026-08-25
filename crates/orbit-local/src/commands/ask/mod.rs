mod local;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use orbit_search::{KindRates, SearchVocab, content_words};
use std::collections::HashMap;

use local::LocalBackend;

const SNIPPET_LINES: usize = 4;
const SNIPPET_LINE_CHARS: usize = 160;

fn vocab() -> &'static SearchVocab {
    static VOCAB: std::sync::OnceLock<SearchVocab> = std::sync::OnceLock::new();
    VOCAB.get_or_init(|| {
        use strum::IntoEnumIterator;
        SearchVocab::new(
            code_graph::v2::types::EdgeKind::iter().map(|kind| kind.as_ref().to_string()),
        )
    })
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

    let outcome = backend.ask(&question, limit, vocab(), kind_rates())?;
    writeln!(out, "terms: {}", outcome.terms.join(" "))?;

    if outcome.matches.is_empty() {
        writeln!(out, "\nNo definitions match those terms.")?;
        return Ok(());
    }

    report_confidence(&mut out, &outcome)?;
    write_matches(&mut out, "\nMatches:", &outcome.matches, 1, backend.root())?;
    if !outcome.surfaced.is_empty() {
        write_matches(
            &mut out,
            "\nConnected to your terms (structurally surfaced):",
            &outcome.surfaced,
            3,
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
    precision: usize,
    root: &std::path::Path,
) -> std::io::Result<()> {
    writeln!(out, "{header}")?;
    for m in matches {
        writeln!(
            out,
            "  {}  [{}]  {}  (score {:.precision$}, links {})",
            m.row.fqn, m.row.kind, m.row.loc, m.score, m.row.degree
        )?;
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

fn report_confidence(
    out: &mut impl Write,
    outcome: &orbit_search::AskOutcome,
) -> std::io::Result<()> {
    if !outcome.weak && outcome.unmatched_terms.is_empty() {
        return Ok(());
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
            "note: no matches for: {}",
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

    #[test]
    fn vocab_maps_question_verbs_to_relational_intent_and_focus_kinds() {
        for word in [
            "calls", "calling", "imports", "extends", "defines", "contains",
        ] {
            assert!(vocab().is_relational(word), "{word} should be relational");
        }
        for word in ["dlq", "widget", "backpressure", "hooks"] {
            assert!(
                !vocab().is_relational(word),
                "{word} should not be relational"
            );
        }
        let kind = |q: &str| vocab().focus_edge_kind(&content_words(q));
        assert_eq!(kind("who calls execute_hooks"), Some("CALLS".to_string()));
        assert_eq!(
            kind("what imports the ontology"),
            Some("IMPORTS".to_string())
        );
        assert_eq!(
            kind("what extends HandlerError"),
            Some("EXTENDS".to_string())
        );
        assert_eq!(kind("where do we send messages to the dlq"), None);
    }
}
