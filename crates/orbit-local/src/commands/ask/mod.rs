mod local;

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use orbit_search::{
    CANDIDATE_FACTOR, CorpusRow, EXPAND_SEEDS, FOCUS_EDGES_PER_KIND, MAX_EDGES_PER_KIND,
    SEED_GAP_RATIO, SearchVocab, content_words, dedupe_by_parent, rank,
};

use local::LocalBackend;

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
    let terms = content_words(&question);
    if terms.is_empty() {
        anyhow::bail!("no usable search terms in question: {question:?}");
    }

    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let backend = LocalBackend::open(&repo_path, db)?;

    let mut out = std::io::stdout().lock();
    writeln!(out, "ask {:?} — {}", question, backend.header())?;
    writeln!(out, "terms: {}", terms.join(" "))?;

    let (corpus, weights) = backend.search(&terms)?;
    if corpus.is_empty() {
        writeln!(out, "\nNo definitions match those terms.")?;
        return Ok(());
    }
    let results = dedupe_by_parent(
        rank(
            &terms,
            &corpus,
            limit * CANDIDATE_FACTOR,
            weights.as_deref(),
            vocab(),
        ),
        &corpus,
        limit,
    );
    if results.is_empty() {
        writeln!(out, "\nNo definitions match those terms.")?;
        return Ok(());
    }

    writeln!(out, "\nMatches:")?;
    for r in &results {
        let row = &corpus[r.index];
        writeln!(
            out,
            "  {}  [{}]  {}  (score {:.1}, links {})",
            row.fqn, row.kind, row.loc, r.score, row.degree
        )?;
    }

    let cutoff = results
        .first()
        .map_or(0.0, |top| top.score * SEED_GAP_RATIO);
    let seeds: Vec<&CorpusRow> = results
        .iter()
        .take(EXPAND_SEEDS)
        .take_while(|r| r.score >= cutoff)
        .map(|r| &corpus[r.index])
        .collect();
    let focus = vocab().focus_edge_kind(&terms);
    let edges = backend.expand(&seeds, focus.as_deref())?;
    if edges.is_empty() {
        writeln!(out, "\nNo connections found around the top matches.")?;
        return Ok(());
    }

    writeln!(out, "\nConnections (1 hop around top {}):", seeds.len())?;
    let kind_cap = |kind: &str| {
        if focus.as_deref() == Some(kind) {
            FOCUS_EDGES_PER_KIND
        } else {
            MAX_EDGES_PER_KIND
        }
    };
    let mut current = "";
    let mut in_kind = 0usize;
    for e in &edges {
        if e.kind != current {
            report_hidden(&mut out, current, in_kind, kind_cap(current))?;
            current = &e.kind;
            in_kind = 0;
            writeln!(out, "  {current}:")?;
        }
        if in_kind < kind_cap(current) {
            writeln!(out, "    {} --> {}", e.source, e.target)?;
        }
        in_kind += 1;
    }
    report_hidden(&mut out, current, in_kind, kind_cap(current))?;
    Ok(())
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
