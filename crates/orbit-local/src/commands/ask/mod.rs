mod local;

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;

use crate::search::{split_words, stem};
use local::LocalBackend;

const EXPAND_SEEDS: usize = 5;
const MAX_PER_PARENT: usize = 2;
const MAX_PER_FILE: usize = 3;
const SEED_GAP_RATIO: f64 = 0.2;
const CANDIDATE_FACTOR: usize = 5;
const MAX_EDGES_PER_KIND: usize = 5;
const FOCUS_EDGES_PER_KIND: usize = 15;

const EDGE_KIND_SYNONYMS: &[(&str, &str)] = &[
    ("use", "CALLS"),
    ("invoke", "CALLS"),
    ("caller", "CALLS"),
    ("callee", "CALLS"),
    ("depend", "IMPORTS"),
    ("implement", "EXTENDS"),
    ("inherit", "EXTENDS"),
];

const EXACT_BONUS: f64 = 1000.0;
const PREFIX_BONUS: f64 = 100.0;
const SUBSTRING_BONUS: f64 = 1.0;
const SOURCE_BONUS: f64 = 0.5;

#[rustfmt::skip]
const RELATIONAL_SYNONYMS: &[&str] = &[
    "caller", "callee", "depend", "export", "implement", "invoke", "mention",
    "reference", "render", "use", "used", "uses", "using",
];

fn focus_edge_kind(terms: &[String]) -> Option<String> {
    static BY_STEM: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    let by_stem = BY_STEM.get_or_init(|| {
        use strum::IntoEnumIterator;
        let mut map: HashMap<String, String> = code_graph::v2::types::EdgeKind::iter()
            .map(|kind| {
                let name: &str = kind.as_ref();
                (stem(&name.to_lowercase()), name.to_uppercase())
            })
            .collect();
        for (word, kind) in EDGE_KIND_SYNONYMS {
            map.insert(stem(word), (*kind).to_string());
        }
        map
    });
    terms.iter().find_map(|t| by_stem.get(&stem(t)).cloned())
}

fn is_relational(term: &str) -> bool {
    static STEMS: std::sync::OnceLock<std::collections::HashSet<String>> =
        std::sync::OnceLock::new();
    STEMS
        .get_or_init(|| {
            use strum::IntoEnumIterator;
            let mut stems: std::collections::HashSet<String> =
                code_graph::v2::types::EdgeKind::iter()
                    .flat_map(|kind| split_words(kind.as_ref()))
                    .map(|word| stem(&word))
                    .collect();
            stems.extend(RELATIONAL_SYNONYMS.iter().map(|word| stem(word)));
            stems
        })
        .contains(&stem(term))
}

const QUERY_STOPWORDS: &[&str] = &[
    "a", "about", "all", "an", "and", "any", "are", "be", "been", "being", "but", "can", "could",
    "did", "do", "does", "for", "from", "had", "has", "have", "here", "how", "in", "into", "is",
    "get", "it", "its", "may", "might", "must", "not", "of", "off", "on", "onto", "or", "our",
    "set", "shall", "should", "some", "that", "the", "their", "them", "there", "these", "they",
    "this", "those", "to", "was", "we", "were", "what", "when", "where", "which", "while", "who",
    "whom", "whose", "why", "will", "with", "without", "work", "working", "works", "would", "you",
    "your",
];

struct CorpusRow {
    id: String,
    fqn: String,
    kind: String,
    loc: String,
    degree: String,
}

struct Edge {
    kind: String,
    source: String,
    target: String,
}

struct Hit {
    index: usize,
    score: f64,
    tiered: bool,
    guaranteed: bool,
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
    let focus = focus_edge_kind(&terms);
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

enum Tier {
    Exact,
    Prefix,
    Inner,
    None,
}

fn tier_of(term: &str, tokens: &[String]) -> Tier {
    if tokens.iter().any(|t| t == term) {
        Tier::Exact
    } else if tokens.iter().any(|t| t.starts_with(term)) {
        Tier::Prefix
    } else if tokens.iter().any(|t| t.contains(term)) {
        Tier::Inner
    } else {
        Tier::None
    }
}

struct RowTokens {
    name: Vec<String>,
    name_stems: Vec<String>,
    path: Vec<String>,
}

impl RowTokens {
    fn name_tier(&self, term: &str, term_stem: &str) -> Tier {
        match tier_of(term, &self.name) {
            Tier::None if self.name_stems.iter().any(|s| s == term_stem) => Tier::Prefix,
            tier => tier,
        }
    }

    fn matches(&self, term: &str, term_stem: &str) -> bool {
        !matches!(self.name_tier(term, term_stem), Tier::None)
            || matches!(tier_of(term, &self.path), Tier::Exact | Tier::Prefix)
    }
}

fn rank(terms: &[String], corpus: &[CorpusRow], cap: usize, weights: Option<&[f64]>) -> Vec<Hit> {
    let joined = terms.join(" ");
    let term_stems: Vec<String> = terms.iter().map(|t| stem(t)).collect();
    let rows: Vec<RowTokens> = corpus
        .iter()
        .map(|r| {
            let name = split_words(&r.fqn);
            let name_stems = name.iter().map(|t| stem(t)).collect();
            RowTokens {
                name,
                name_stems,
                path: split_words(&r.loc),
            }
        })
        .collect();
    let weights: Vec<f64> = match weights {
        Some(w) if w.len() == terms.len() => terms
            .iter()
            .zip(w)
            .map(|(term, weight)| {
                if is_relational(term) {
                    weight.min(1.0)
                } else {
                    *weight
                }
            })
            .collect(),
        _ => vec![1.0; terms.len()],
    };

    let mut per_term_best: Vec<Option<(f64, usize)>> = vec![None; terms.len()];
    let mut hits: Vec<Hit> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let name_joined = row.name.join(" ");
        let mut score = 0.0;
        if !joined.is_empty() {
            if name_joined == joined {
                score += EXACT_BONUS * 10.0;
            } else if name_joined.starts_with(&joined) {
                score += PREFIX_BONUS * 10.0;
            }
        }
        let mut tiered = 0.0;
        let mut matched = 0usize;
        for ((term, term_stem), weight) in terms.iter().zip(&term_stems).zip(&weights) {
            match row.name_tier(term, term_stem) {
                Tier::Exact => {
                    tiered += EXACT_BONUS * weight;
                    matched += 1;
                }
                Tier::Prefix => {
                    tiered += PREFIX_BONUS * weight;
                    matched += 1;
                }
                Tier::Inner => {
                    score += SUBSTRING_BONUS * weight;
                    matched += 1;
                }
                Tier::None => {
                    if matches!(tier_of(term, &row.path), Tier::Exact | Tier::Prefix) {
                        score += SOURCE_BONUS * weight;
                    }
                }
            }
        }
        if tiered > 0.0 {
            let coverage = matched as f64 / terms.len() as f64;
            score += tiered * coverage * coverage;
        }
        if score > 0.0 {
            hits.push(Hit {
                index,
                score,
                tiered: tiered > 0.0,
                guaranteed: false,
            });
            for ((slot, term), term_stem) in per_term_best.iter_mut().zip(terms).zip(&term_stems) {
                if row.matches(term, term_stem) && slot.is_none_or(|(best, _)| score > best) {
                    *slot = Some((score, index));
                }
            }
        }
    }
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                if a.tiered && b.tiered {
                    corpus[a.index].fqn.len().cmp(&corpus[b.index].fqn.len())
                } else {
                    std::cmp::Ordering::Equal
                }
            })
    });
    hits.truncate(cap);

    let mut guaranteed = false;
    for (slot, term) in per_term_best.iter().zip(terms) {
        let Some((score, index)) = slot else { continue };
        if is_relational(term) {
            continue;
        }
        if let Some(hit) = hits.iter_mut().find(|h| h.index == *index) {
            hit.guaranteed = true;
            continue;
        }
        hits.push(Hit {
            index: *index,
            score: *score,
            tiered: false,
            guaranteed: true,
        });
        guaranteed = true;
    }
    if guaranteed {
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    hits
}

fn dedupe_by_parent(results: Vec<Hit>, corpus: &[CorpusRow], limit: usize) -> Vec<Hit> {
    let mut per_parent: HashMap<String, usize> = HashMap::new();
    let mut per_file: HashMap<String, usize> = HashMap::new();
    let mut kept: Vec<Hit> = Vec::with_capacity(limit);
    let mut overflow_guaranteed: Vec<Hit> = Vec::new();
    for r in results {
        if kept.len() >= limit && !r.guaranteed {
            continue;
        }
        let row = &corpus[r.index];
        let file = row
            .loc
            .rsplit_once(':')
            .map_or(row.loc.clone(), |(f, _)| f.to_string());
        if !file.is_empty() {
            let seen = per_file.entry(file).or_insert(0);
            if *seen >= MAX_PER_FILE {
                continue;
            }
            *seen += 1;
        }
        let count = per_parent.entry(parent_key(&row.fqn)).or_insert(0);
        if *count >= MAX_PER_PARENT {
            continue;
        }
        *count += 1;
        if kept.len() < limit {
            kept.push(r);
        } else {
            overflow_guaranteed.push(r);
        }
    }
    for g in overflow_guaranteed {
        let Some(pos) = kept.iter().rposition(|h| !h.guaranteed) else {
            break;
        };
        kept.remove(pos);
        kept.push(g);
    }
    kept
}

fn parent_key(fqn: &str) -> String {
    match fqn.rfind("::") {
        Some(i) => fqn[..i].to_string(),
        None => match fqn.rfind('.') {
            Some(i) => fqn[..i].to_string(),
            None => fqn.to_string(),
        },
    }
}

fn content_words(input: &str) -> Vec<String> {
    let words = split_words(input);
    let content: Vec<String> = words
        .iter()
        .filter(|w| !QUERY_STOPWORDS.contains(&w.as_str()))
        .cloned()
        .collect();
    if content.is_empty() { words } else { content }
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
    fn content_words_drops_question_words_but_keeps_the_subject() {
        assert_eq!(
            content_words("which issues mention the ontology"),
            vec!["issues", "mention", "ontology"]
        );
    }

    #[test]
    fn content_words_falls_back_when_every_word_is_filler() {
        assert_eq!(content_words("what is this"), vec!["what", "is", "this"]);
    }

    fn row(fqn: &str) -> CorpusRow {
        CorpusRow {
            id: fqn.to_string(),
            fqn: fqn.to_string(),
            kind: "Definition".to_string(),
            loc: String::new(),
            degree: "0".to_string(),
        }
    }

    #[test]
    fn rank_prefers_rows_matching_more_query_terms() {
        let corpus = vec![row("issues found during testing"), row("ontology issues")];
        let terms = vec!["issues".to_string(), "ontology".to_string()];
        let hits = rank(&terms, &corpus, 10, None);
        assert_eq!(corpus[hits[0].index].fqn, "ontology issues");
    }

    #[test]
    fn rank_scores_every_substring_match_above_zero() {
        let corpus = vec![row("feat(ontology): add plan ontology")];
        let hits = rank(&["ontology".to_string()], &corpus, 10, None);
        assert_eq!(hits.len(), 1);
        assert!(hits[0].score > 0.0, "score was {}", hits[0].score);
    }

    #[test]
    fn idf_lets_a_rare_term_outrank_common_filler_when_the_corpus_is_complete() {
        let mut corpus: Vec<CorpusRow> = (0..40)
            .map(|i| row(&format!("pkg::send_thing_{i}")))
            .collect();
        corpus.push(row("indexer::nats::message::NatsMessage::to_dlq"));
        let terms = vec!["send".to_string(), "dlq".to_string()];

        let weighted = rank(&terms, &corpus, 5, Some(&[0.69, 3.07]));
        assert!(
            corpus[weighted[0].index].fqn.ends_with("to_dlq"),
            "weighted top was {}",
            corpus[weighted[0].index].fqn
        );

        let flat = rank(&terms, &corpus, 5, None);
        let top = flat[0].score;
        assert!(
            flat.iter().all(|h| (h.score - top).abs() < f64::EPSILON),
            "without weights every one-term match should tie"
        );
    }

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
            assert!(is_relational(word), "{word} should be relational");
        }
        for word in ["dlq", "widget", "backpressure", "user", "hooks"] {
            assert!(!is_relational(word), "{word} should not be relational");
        }
    }

    #[test]
    fn rank_prefers_the_short_exact_symbol_over_a_longer_tie() {
        let corpus = vec![
            row("Ci::ExecuteBuildHooksWorker::execute_hooks_for_created_build"),
            row("Group::execute_hooks"),
        ];
        let terms = vec!["execute".to_string(), "hooks".to_string()];
        let hits = rank(&terms, &corpus, 10, None);
        assert_eq!(corpus[hits[0].index].fqn, "Group::execute_hooks");
    }

    #[test]
    fn focus_edge_kind_maps_question_verbs_to_relationships() {
        let kind = |q: &str| focus_edge_kind(&content_words(q));
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

    #[test]
    fn guaranteed_rare_term_row_survives_the_display_cap() {
        let mut corpus: Vec<CorpusRow> = (0..20)
            .map(|i| row(&format!("pkg{i}::parse_file_entry")))
            .collect();
        corpus.push(row("code_graph::langs::js::frameworks::vue"));
        let terms = vec!["parse".to_string(), "vue".to_string(), "file".to_string()];
        let hits = dedupe_by_parent(rank(&terms, &corpus, 15, None), &corpus, 3);
        assert_eq!(hits.len(), 3);
        assert!(
            hits.iter().any(|h| corpus[h.index].fqn.ends_with("::vue")),
            "the only row matching 'vue' must survive the cap"
        );
    }

    #[test]
    fn stemmed_query_matches_inflected_symbols() {
        let corpus = vec![
            row("ontology::validation::validate"),
            row("indexer::unrelated::thing"),
        ];
        let hits = rank(&["validated".to_string()], &corpus, 10, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(corpus[hits[0].index].fqn, "ontology::validation::validate");
    }

    #[test]
    fn parent_key_strips_the_last_segment_for_rust_and_go_fqns() {
        assert_eq!(parent_key("a::B::field"), "a::B");
        assert_eq!(parent_key("pkg.Func"), "pkg");
        assert_eq!(parent_key("bare"), "bare");
    }
}
