use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::rank::rank_alternatives_and_trim;
use crate::text::{camel_words, content_words};
use crate::types::SearchCandidate;
use crate::vocab::SearchVocab;

pub struct GrepOutcome {
    pub terms: Vec<String>,
    pub matches: Vec<GrepMatch>,
    pub total: usize,
}

pub struct GrepMatch {
    pub id: i64,
    pub score: f64,
    pub exact_name: bool,
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
    fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<SearchCandidate>, Self::Error>;
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

type QueryRecall = (Vec<String>, Vec<TermRecall>);

fn recall_query<S: GrepSource>(
    source: &S,
    query: &str,
    vocab: &SearchVocab,
    filter: &RecallFilter,
) -> Result<QueryRecall, GrepError<S::Error>> {
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
    let mut recalls = source.recall(&search_terms, filter)?;
    let split: Vec<(usize, String)> = query
        .split_whitespace()
        .filter_map(|raw| {
            let term = raw
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase();
            let i = search_terms.iter().position(|t| *t == term)?;
            recalls[i]
                .hits
                .is_empty()
                .then(|| camel_words(raw))
                .flatten()
                .map(|w| (i, w))
        })
        .collect();
    if !split.is_empty() {
        let words: Vec<String> = split.iter().map(|(_, w)| w.clone()).collect();
        for ((i, _), recall) in split.iter().zip(source.recall(&words, filter)?) {
            recalls[*i] = recall;
        }
    }
    Ok((terms, recalls))
}

pub fn grep<S: GrepSource>(
    source: &S,
    query: &str,
    limit: usize,
    vocab: &SearchVocab,
    filter: &RecallFilter,
) -> Result<GrepOutcome, GrepError<S::Error>> {
    let mut alternatives = Vec::new();
    let mut terms = Vec::new();
    let mut recalls = Vec::new();
    let mut queries = HashSet::new();
    for alternative in query.split('|').map(str::trim) {
        if !queries.insert(alternative) {
            continue;
        }
        let (words, recalled) = recall_query(source, alternative, vocab, filter)?;
        if !terms.is_empty() {
            terms.push("|".to_string());
        }
        terms.extend(words);
        alternatives.push(recalls.len()..recalls.len() + recalled.len());
        recalls.extend(recalled);
    }
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
    let mut sims = vec![vec![0.0; recalls.len()]; corpus.len()];
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

    let hits = rank_alternatives_and_trim(&corpus, &sims, &idfs, &alternatives, limit);
    let matches: Vec<GrepMatch> = hits
        .into_iter()
        .map(|h| GrepMatch {
            id: corpus[h.index].id,
            score: h.score,
            exact_name: h.exact_name,
        })
        .collect();
    Ok(GrepOutcome {
        terms,
        matches,
        total: corpus.len(),
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

        fn rows_by_ids(&self, ids: &[i64]) -> Result<Vec<SearchCandidate>, Self::Error> {
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
    }

    #[test]
    fn grep_ranks_recalled_rows() {
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
        assert_eq!(outcome.matches[0].id, HOOK_ID);
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
