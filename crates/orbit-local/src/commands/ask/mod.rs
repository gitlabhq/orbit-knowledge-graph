mod local;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use orbit_search::{FOCUS_EDGES_PER_KIND, MAX_EDGES_PER_KIND, SearchVocab, content_words};

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

pub(crate) fn run(
    question: String,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    limit: usize,
) -> Result<()> {
    // Duplicates ask()'s guard so a termless question fails before open(),
    // which may auto-index the whole repository.
    if content_words(&question).is_empty() {
        anyhow::bail!("no usable search terms in question: {question:?}");
    }

    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let backend = LocalBackend::open(&repo_path, db)?;

    let mut out = std::io::stdout().lock();
    writeln!(out, "ask {:?} — {}", question, backend.header())?;

    let outcome = backend.ask(&question, limit, vocab())?;
    writeln!(out, "terms: {}", outcome.terms.join(" "))?;

    if outcome.matches.is_empty() {
        writeln!(out, "\nNo definitions match those terms.")?;
        return Ok(());
    }

    writeln!(out, "\nMatches:")?;
    for m in &outcome.matches {
        writeln!(
            out,
            "  {}  [{}]  {}  (score {:.1}, links {})",
            m.row.fqn, m.row.kind, m.row.loc, m.score, m.row.degree
        )?;
        for line in snippet(backend.root(), &m.row.loc, &m.row.end_line) {
            writeln!(out, "{line}")?;
        }
    }

    if outcome.edges.is_empty() {
        writeln!(out, "\nNo connections found around the top matches.")?;
        return Ok(());
    }

    writeln!(
        out,
        "\nConnections (1 hop around top {}):",
        outcome.seed_count
    )?;
    let focus = outcome.focus;
    let kind_cap = |kind: &str| {
        if focus.as_deref() == Some(kind) {
            FOCUS_EDGES_PER_KIND
        } else {
            MAX_EDGES_PER_KIND
        }
    };
    let mut current = "";
    let mut in_kind = 0usize;
    for e in &outcome.edges {
        if e.kind != current {
            report_hidden(&mut out, current, in_kind, kind_cap(current))?;
            current = &e.kind;
            in_kind = 0;
            writeln!(out, "  {current}:")?;
        }
        if in_kind < kind_cap(current) {
            writeln!(
                out,
                "    {}{} --> {}{}",
                e.source,
                fmt_loc(&e.source_loc),
                e.target,
                fmt_loc(&e.target_loc)
            )?;
        }
        in_kind += 1;
    }
    report_hidden(&mut out, current, in_kind, kind_cap(current))?;
    Ok(())
}

fn fmt_loc(loc: &str) -> String {
    if loc.is_empty() {
        String::new()
    } else {
        format!(" ({loc})")
    }
}

fn snippet(root: &std::path::Path, loc: &str, end_line: &str) -> Vec<String> {
    let Some((path, start)) = loc.rsplit_once(':') else {
        return Vec::new();
    };
    let Ok(start) = start.parse::<usize>() else {
        return Vec::new();
    };
    let Ok(content) = std::fs::read_to_string(root.join(path)) else {
        return Vec::new();
    };
    let last = end_line
        .parse::<usize>()
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

fn report_hidden(
    out: &mut impl Write,
    kind: &str,
    total: usize,
    cap: usize,
) -> std::io::Result<()> {
    if !kind.is_empty() && total > cap {
        writeln!(out, "    … {} more", total - cap)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relational_intent_derives_from_edge_kinds_and_synonyms() {
        for word in [
            "calls",
            "called",
            "imports",
            "importing",
            "extends",
            "defined",
            "contains",
            "renders",
            "rendering",
            "mentioned",
            "uses",
            "callers",
        ] {
            assert!(vocab().is_relational(word), "{word} should be relational");
        }
        for word in ["dlq", "widget", "backpressure", "user", "hooks"] {
            assert!(
                !vocab().is_relational(word),
                "{word} should not be relational"
            );
        }
    }

    #[test]
    fn focus_edge_kind_maps_question_verbs_to_relationships() {
        let kind = |q: &str| vocab().focus_edge_kind(&content_words(q));
        assert_eq!(kind("who calls execute_hooks"), Some("CALLS".to_string()));
        assert_eq!(kind("who uses sql_template"), Some("CALLS".to_string()));
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
