use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::anchor::{term_base_sets, unmatched_terms};
use crate::expand::{GraphSource, expand_neighborhood};
use crate::ppr::KindRates;
use crate::rank::rank_and_trim;
use crate::text::content_words;
use crate::types::{CorpusRow, Edge, TermSeeds};
use crate::vocab::SearchVocab;

pub const SURFACED_LIMIT: usize = 3;

pub struct AskOutcome {
    pub terms: Vec<String>,
    pub matches: Vec<AskMatch>,
    /// Neighbours the graph walk pulled in; only populated for relational questions.
    pub surfaced: Vec<AskMatch>,
    pub focus: Option<String>,
    /// Edges whose both endpoints are among `matches` and `surfaced`.
    pub edges: Vec<Edge>,
    pub weak: bool,
    pub unmatched_terms: Vec<String>,
}

pub struct AskMatch {
    pub row: CorpusRow,
    pub score: f64,
}

pub struct TermRecall {
    pub hits: Vec<(i64, f64)>,
    pub matched: u64,
    pub corpus: u64,
}

impl TermRecall {
    pub fn idf(&self) -> f64 {
        (1.0 + self.corpus as f64 / (1.0 + self.matched as f64)).ln()
    }
}

pub trait AskSource: GraphSource {
    fn stem(&self, words: &[String]) -> Result<Vec<String>, Self::Error>;
    fn recall(&self, terms: &[String]) -> Result<Vec<TermRecall>, Self::Error>;
    fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<CorpusRow>, Self::Error>;
    fn edges_among(&self, ids: &[i64]) -> Result<Vec<Edge>, Self::Error>;
}

#[derive(Debug)]
pub enum AskError<E> {
    NoUsableTerms(String),
    Source(E),
}

impl<E: fmt::Display> fmt::Display for AskError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUsableTerms(q) => write!(f, "no usable search terms in question: {q:?}"),
            Self::Source(e) => e.fmt(f),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for AskError<E> {}

impl<E> From<E> for AskError<E> {
    fn from(e: E) -> Self {
        Self::Source(e)
    }
}

pub fn ask<S: AskSource>(
    source: &S,
    question: &str,
    limit: usize,
    vocab: &SearchVocab,
    kind_rates: &HashMap<String, KindRates>,
) -> Result<AskOutcome, AskError<S::Error>> {
    let terms = content_words(question);
    if terms.is_empty() {
        return Err(AskError::NoUsableTerms(question.to_string()));
    }
    let stems = source.stem(&terms)?;
    let searchable: Vec<String> = terms
        .iter()
        .zip(&stems)
        .filter(|(_, stem)| !vocab.is_relational(stem))
        .map(|(term, _)| term.clone())
        .collect();
    let search_terms = if searchable.is_empty() {
        terms.clone()
    } else {
        searchable
    };
    let recalls = source.recall(&search_terms)?;
    let unmatched = unmatched_terms(&search_terms, &recalls);

    let mut ids: Vec<i64> = Vec::new();
    let mut seen: HashSet<i64> = HashSet::new();
    for &(id, _) in recalls.iter().flat_map(|r| r.hits.iter()) {
        if seen.insert(id) {
            ids.push(id);
        }
    }
    let corpus = source.rows_by_ids(&ids)?;
    let index: HashMap<i64, usize> = corpus
        .iter()
        .enumerate()
        .map(|(i, row)| (row.id, i))
        .collect();
    let mut sims = vec![vec![0.0; search_terms.len()]; corpus.len()];
    for (t, recall) in recalls.iter().enumerate() {
        for &(id, sim) in &recall.hits {
            if let Some(&i) = index.get(&id) {
                sims[i][t] = sim;
            }
        }
    }
    let idfs: Vec<f64> = recalls
        .iter()
        .map(|r| if r.hits.is_empty() { 0.0 } else { r.idf() })
        .collect();

    let hits = rank_and_trim(&corpus, &sims, &idfs, limit);
    let focus = vocab.focus_edge_kind(&stems);
    let weak = hits.first().is_none_or(|h| !h.confident());
    let matches: Vec<AskMatch> = hits
        .into_iter()
        .map(|h| AskMatch {
            row: corpus[h.index].clone(),
            score: h.score,
        })
        .collect();

    let surfaced = if focus.is_some() {
        let mut term_seeds = term_base_sets(&recalls);
        if term_seeds.is_empty() && !matches.is_empty() {
            term_seeds = vec![TermSeeds {
                seeds: matches.iter().map(|m| (m.row.id, m.score)).collect(),
                weight: 1.0,
            }];
        }
        if term_seeds.is_empty() {
            Vec::new()
        } else {
            let expanded = expand_neighborhood(source, &term_seeds, kind_rates, focus.as_deref())?;
            let shown: HashSet<i64> = matches.iter().map(|m| m.row.id).collect();
            let candidates: Vec<(i64, f64)> = expanded
                .surfaced
                .into_iter()
                .filter(|(id, _)| !shown.contains(id))
                .take(SURFACED_LIMIT)
                .collect();
            let rows =
                source.rows_by_ids(&candidates.iter().map(|&(id, _)| id).collect::<Vec<_>>())?;
            candidates
                .into_iter()
                .filter_map(|(id, score)| {
                    rows.iter().find(|r| r.id == id).map(|r| AskMatch {
                        row: r.clone(),
                        score,
                    })
                })
                .collect()
        }
    } else {
        Vec::new()
    };
    let node_ids: Vec<i64> = matches.iter().chain(&surfaced).map(|m| m.row.id).collect();
    let edges = source.edges_among(&node_ids)?;
    Ok(AskOutcome {
        terms,
        matches,
        surfaced,
        focus,
        edges,
        weak,
        unmatched_terms: unmatched,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expand::NodeLabel;
    use crate::testutil::{row, test_vocab};
    use crate::types::{Graph, GraphEdge};

    const HOOK_ID: i64 = 7;

    struct FakeRecallSource;

    impl GraphSource for FakeRecallSource {
        type Error = std::convert::Infallible;

        fn graph(&self, _seeds: &[i64]) -> Result<Graph, Self::Error> {
            Ok(Graph {
                kinds: vec!["CALLS".to_string()],
                edges: vec![GraphEdge {
                    kind: 0,
                    source: HOOK_ID,
                    target: 8,
                }],
            })
        }

        fn labels(&self, ids: &[i64]) -> Result<HashMap<i64, NodeLabel>, Self::Error> {
            Ok(ids
                .iter()
                .map(|&id| {
                    (
                        id,
                        NodeLabel {
                            label: format!("node{id}"),
                            loc: String::new(),
                        },
                    )
                })
                .collect())
        }
    }

    impl AskSource for FakeRecallSource {
        fn stem(&self, words: &[String]) -> Result<Vec<String>, Self::Error> {
            Ok(words
                .iter()
                .map(|w| crate::testutil::test_stem(w))
                .collect())
        }

        fn recall(&self, terms: &[String]) -> Result<Vec<TermRecall>, Self::Error> {
            Ok(terms
                .iter()
                .map(|t| {
                    let hits = if t == "commit" || t == "hook" {
                        vec![(HOOK_ID, 1.0)]
                    } else {
                        Vec::new()
                    };
                    TermRecall {
                        matched: hits.len() as u64,
                        corpus: 1000,
                        hits,
                    }
                })
                .collect())
        }

        fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<CorpusRow>, Self::Error> {
            Ok(ids.iter().map(|&id| row(id, "Repo::commit_hook")).collect())
        }

        fn edges_among(&self, ids: &[i64]) -> Result<Vec<Edge>, Self::Error> {
            Ok(ids
                .iter()
                .filter(|&&id| id == HOOK_ID)
                .map(|_| Edge {
                    kind: "CALLS".to_string(),
                    source: "Repo::after_commit".to_string(),
                    source_loc: String::new(),
                    target: "Repo::commit_hook".to_string(),
                    target_loc: String::new(),
                })
                .collect())
        }
    }

    #[test]
    fn ask_ranks_recalled_rows_and_expands_around_them() {
        let outcome = ask(
            &FakeRecallSource,
            "who calls commit hook",
            5,
            &test_vocab(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(outcome.matches.len(), 1);
        assert_eq!(outcome.matches[0].row.id, HOOK_ID);
        assert!(!outcome.weak, "both terms fully anchor one row");
        assert_eq!(outcome.focus.as_deref(), Some("CALLS"));
        assert!(outcome.unmatched_terms.is_empty());
        assert!(!outcome.edges.is_empty());
    }

    #[test]
    fn unrecalled_terms_are_reported_without_deflating_confidence() {
        let outcome = ask(
            &FakeRecallSource,
            "commit zzzz yyyy",
            5,
            &test_vocab(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            outcome.unmatched_terms,
            vec!["zzzz".to_string(), "yyyy".to_string()]
        );
        assert!(
            !outcome.weak,
            "terms no row can match must not count against coverage"
        );
    }

    #[test]
    fn relational_only_questions_fall_back_to_all_terms() {
        let err = ask(&FakeRecallSource, "", 5, &test_vocab(), &HashMap::new())
            .err()
            .expect("empty question must fail");
        assert!(matches!(err, AskError::NoUsableTerms(_)));

        let outcome = ask(
            &FakeRecallSource,
            "calls",
            5,
            &test_vocab(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(outcome.focus.as_deref(), Some("CALLS"));
        assert!(outcome.matches.is_empty());
    }
}
