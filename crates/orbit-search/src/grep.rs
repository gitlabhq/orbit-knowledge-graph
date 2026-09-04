use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::rank::rank_and_trim;
use crate::text::content_words;
use crate::types::{CorpusRow, Edge};
use crate::vocab::SearchVocab;

pub struct GrepOutcome {
    pub terms: Vec<String>,
    pub matches: Vec<GrepMatch>,
    pub total: usize,
    /// Edges whose both endpoints are among `matches`.
    pub edges: Vec<Edge>,
    pub weak: bool,
    pub unmatched_terms: Vec<String>,
}

pub struct GrepMatch {
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecallFilter {
    pub kinds: Vec<String>,
}

impl RecallFilter {
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }
}

pub trait GrepSource {
    type Error;

    fn stem(&self, words: &[String]) -> Result<Vec<String>, Self::Error>;
    fn recall(
        &self,
        terms: &[String],
        filter: &RecallFilter,
    ) -> Result<Vec<TermRecall>, Self::Error>;
    fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<CorpusRow>, Self::Error>;
    fn edges_among(&self, ids: &[i64]) -> Result<Vec<Edge>, Self::Error>;
}

#[derive(Debug)]
pub enum GrepError<E> {
    NoUsableTerms(String),
    Source(E),
}

impl<E: fmt::Display> fmt::Display for GrepError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUsableTerms(q) => write!(f, "no usable search terms in query: {q:?}"),
            Self::Source(e) => e.fmt(f),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for GrepError<E> {}

impl<E> From<E> for GrepError<E> {
    fn from(e: E) -> Self {
        Self::Source(e)
    }
}

pub fn unmatched_terms(terms: &[String], recalls: &[TermRecall]) -> Vec<String> {
    terms
        .iter()
        .zip(recalls)
        .filter(|(_, recall)| recall.hits.is_empty())
        .map(|(term, _)| term.clone())
        .collect()
}

pub fn grep<S: GrepSource>(
    source: &S,
    query: &str,
    limit: usize,
    vocab: &SearchVocab,
    filter: &RecallFilter,
) -> Result<GrepOutcome, GrepError<S::Error>> {
    let terms = content_words(query);
    if terms.is_empty() {
        return Err(GrepError::NoUsableTerms(query.to_string()));
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
    let recalls = source.recall(&search_terms, filter)?;
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
    let weak = hits.first().is_none_or(|h| !h.confident());
    let matches: Vec<GrepMatch> = hits
        .into_iter()
        .map(|h| GrepMatch {
            row: corpus[h.index].clone(),
            score: h.score,
        })
        .collect();
    let node_ids: Vec<i64> = matches.iter().map(|m| m.row.id).collect();
    let edges = source.edges_among(&node_ids)?;
    Ok(GrepOutcome {
        terms,
        matches,
        total: corpus.len(),
        edges,
        weak,
        unmatched_terms: unmatched,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{row, test_vocab};

    const HOOK_ID: i64 = 7;
    const CALLER_ID: i64 = 8;

    struct FakeRecallSource;

    impl GrepSource for FakeRecallSource {
        type Error = std::convert::Infallible;

        fn stem(&self, words: &[String]) -> Result<Vec<String>, Self::Error> {
            Ok(words
                .iter()
                .map(|w| crate::testutil::test_stem(w))
                .collect())
        }

        fn recall(
            &self,
            terms: &[String],
            _filter: &RecallFilter,
        ) -> Result<Vec<TermRecall>, Self::Error> {
            Ok(terms
                .iter()
                .map(|t| {
                    let hits = match t.as_str() {
                        "commit" => vec![(HOOK_ID, 1.0), (CALLER_ID, 0.9)],
                        "hook" => vec![(HOOK_ID, 1.0)],
                        _ => Vec::new(),
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
            Ok(ids
                .iter()
                .map(|&id| {
                    if id == HOOK_ID {
                        row(id, "Repo::commit_hook")
                    } else {
                        row(id, "Repo::after_commit")
                    }
                })
                .collect())
        }

        fn edges_among(&self, ids: &[i64]) -> Result<Vec<Edge>, Self::Error> {
            if ids.contains(&HOOK_ID) && ids.contains(&CALLER_ID) {
                return Ok(vec![Edge {
                    kind: "CALLS".to_string(),
                    source: "Repo::after_commit".to_string(),
                    source_loc: String::new(),
                    target: "Repo::commit_hook".to_string(),
                    target_loc: String::new(),
                }]);
            }
            Ok(Vec::new())
        }
    }

    #[test]
    fn grep_ranks_recalled_rows_and_lists_edges_among_them() {
        let outcome = grep(
            &FakeRecallSource,
            "who calls commit hook",
            5,
            &test_vocab(),
            &RecallFilter::default(),
        )
        .unwrap();
        assert_eq!(outcome.matches.len(), 2);
        assert_eq!(outcome.total, 2);
        assert_eq!(outcome.matches[0].row.id, HOOK_ID);
        assert!(!outcome.weak, "both terms fully anchor one row");
        assert!(outcome.unmatched_terms.is_empty());
        assert_eq!(outcome.edges.len(), 1);
    }

    #[test]
    fn limit_trims_matches_but_total_reports_every_recalled_row() {
        let outcome = grep(
            &FakeRecallSource,
            "commit",
            1,
            &test_vocab(),
            &RecallFilter::default(),
        )
        .unwrap();
        assert_eq!(outcome.matches.len(), 1);
        assert_eq!(outcome.total, 2);
        assert!(outcome.edges.is_empty());
    }

    #[test]
    fn unrecalled_terms_are_reported_without_deflating_confidence() {
        let outcome = grep(
            &FakeRecallSource,
            "commit zzzz yyyy",
            5,
            &test_vocab(),
            &RecallFilter::default(),
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
        let err = grep(
            &FakeRecallSource,
            "",
            5,
            &test_vocab(),
            &RecallFilter::default(),
        )
        .err()
        .expect("empty query must fail");
        assert!(matches!(err, GrepError::NoUsableTerms(_)));

        let outcome = grep(
            &FakeRecallSource,
            "calls",
            5,
            &test_vocab(),
            &RecallFilter::default(),
        )
        .unwrap();
        assert!(outcome.matches.is_empty());
    }
}
