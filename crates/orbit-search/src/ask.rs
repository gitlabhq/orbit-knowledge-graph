use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::anchor::{term_base_sets, unmatched_terms};
use crate::expand::{NeighborhoodSource, expand_neighborhood};
use crate::ppr::KindRates;
use crate::rank::rank_and_trim;
use crate::text::{candidate_splits, content_words, query_tokens};
use crate::types::{CorpusRow, Edge};
use crate::vocab::SearchVocab;

pub const SURFACED_LIMIT: usize = 3;

pub struct AskOutcome {
    pub terms: Vec<String>,
    pub splits: Vec<(String, String)>,
    pub matches: Vec<AskMatch>,
    pub surfaced: Vec<AskMatch>,
    pub seed_count: usize,
    pub focus: Option<String>,
    pub edges: Vec<Edge>,
    pub hidden_by_kind: Vec<(String, usize)>,
    pub weak: bool,
    pub unmatched_terms: Vec<String>,
}

pub struct AskMatch {
    pub row: CorpusRow,
    pub score: f64,
}

pub type Corpus = (Vec<CorpusRow>, Option<Vec<f64>>);

pub trait AskSource: NeighborhoodSource {
    fn corpus(&self, terms: &[String]) -> Result<Corpus, Self::Error>;
    fn token_df(&self, tokens: &[String]) -> Result<Vec<i64>, Self::Error>;
    fn rows_by_ids(&self, ids: &[&str]) -> Result<Vec<CorpusRow>, Self::Error>;
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
    let splits = accepted_splits(source, &terms)?;
    let mut anchor_terms = terms.clone();
    for (_, a, b) in &splits {
        anchor_terms.push(a.clone());
        anchor_terms.push(b.clone());
    }
    let (corpus, anchor_weights) = source.corpus(&anchor_terms)?;
    let weights = anchor_weights
        .as_deref()
        .and_then(|w| w.get(..terms.len()))
        .map(|w| w.to_vec());
    let hits = rank_and_trim(&terms, &corpus, limit, weights.as_deref(), vocab);
    let focus = vocab.focus_edge_kind(&terms);
    let weak = hits.first().is_none_or(|h| !h.confident());
    let unmatched = unmatched_terms(&terms, &corpus, vocab);
    let matches: Vec<AskMatch> = hits
        .into_iter()
        .map(|h| AskMatch {
            row: corpus[h.index].clone(),
            score: h.score,
        })
        .collect();
    let mut term_seeds: Vec<Vec<(String, f64)>> =
        term_base_sets(&anchor_terms, &corpus, anchor_weights.as_deref(), vocab)
            .into_iter()
            .map(|set| {
                set.into_iter()
                    .map(|(i, w)| (corpus[i].id.clone(), w))
                    .collect()
            })
            .collect();
    if term_seeds.is_empty() && !matches.is_empty() {
        term_seeds = vec![
            matches
                .iter()
                .map(|m| (m.row.id.clone(), m.score))
                .collect(),
        ];
    }
    let seed_count = term_seeds
        .iter()
        .flatten()
        .map(|(id, _)| id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let (edges, hidden_by_kind, surfaced) = if term_seeds.is_empty() {
        (Vec::new(), Vec::new(), Vec::new())
    } else {
        let expanded = expand_neighborhood(source, &term_seeds, kind_rates, focus.as_deref())?;
        let shown: HashSet<&str> = matches.iter().map(|m| m.row.id.as_str()).collect();
        let candidates: Vec<(String, f64)> = expanded
            .surfaced
            .into_iter()
            .filter(|(id, _)| !shown.contains(id.as_str()))
            .take(SURFACED_LIMIT)
            .collect();
        let rows = source.rows_by_ids(
            &candidates
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
        )?;
        let surfaced = candidates
            .into_iter()
            .filter_map(|(id, score)| {
                rows.iter().find(|r| r.id == id).map(|r| AskMatch {
                    row: r.clone(),
                    score,
                })
            })
            .collect();
        (expanded.edges, expanded.hidden_by_kind, surfaced)
    };
    Ok(AskOutcome {
        terms,
        splits: splits
            .into_iter()
            .map(|(term, a, b)| (term, format!("{a} {b}")))
            .collect(),
        seed_count,
        matches,
        surfaced,
        focus,
        edges,
        hidden_by_kind,
        weak,
        unmatched_terms: unmatched,
    })
}

fn accepted_splits<S: AskSource>(
    source: &S,
    terms: &[String],
) -> Result<Vec<(String, String, String)>, S::Error> {
    let candidates: Vec<(String, String, String)> = terms
        .iter()
        .flat_map(|term| {
            candidate_splits(term)
                .into_iter()
                .map(|(a, b)| (term.clone(), a, b))
        })
        .collect();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut parts: Vec<String> = candidates
        .iter()
        .flat_map(|(_, a, b)| [a.clone(), b.clone()])
        .collect();
    parts.sort();
    parts.dedup();
    let tokens: Vec<String> = parts
        .iter()
        .map(|p| {
            query_tokens(std::slice::from_ref(p))
                .into_iter()
                .next()
                .unwrap_or_else(|| p.clone())
        })
        .collect();
    let dfs = source.token_df(&tokens)?;
    let known: HashSet<&str> = parts
        .iter()
        .zip(&dfs)
        .filter(|&(_, &df)| df > 0)
        .map(|(p, _)| p.as_str())
        .collect();
    let mut accepted: Vec<(String, String, String)> = Vec::new();
    for (term, a, b) in candidates {
        if accepted.iter().any(|(t, _, _)| *t == term) {
            continue;
        }
        if known.contains(a.as_str()) && known.contains(b.as_str()) {
            accepted.push((term, a, b));
        }
    }
    Ok(accepted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expand::NodeLabel;
    use crate::ppr::NeighborhoodEdge;
    use crate::testutil::{row, test_vocab};

    struct ShortWeightSource;

    impl NeighborhoodSource for ShortWeightSource {
        type Error = std::convert::Infallible;

        fn hop(&self, _ids: &[&str], _cap: usize) -> Result<Vec<NeighborhoodEdge>, Self::Error> {
            Ok(Vec::new())
        }

        fn degrees(&self, _ids: &[&str]) -> Result<HashMap<String, u64>, Self::Error> {
            Ok(HashMap::new())
        }

        fn labels(&self, _ids: &[&str]) -> Result<HashMap<String, NodeLabel>, Self::Error> {
            Ok(HashMap::new())
        }
    }

    impl AskSource for ShortWeightSource {
        fn corpus(&self, _terms: &[String]) -> Result<Corpus, Self::Error> {
            Ok((vec![row("Repo::commit_hook")], Some(Vec::new())))
        }

        fn token_df(&self, tokens: &[String]) -> Result<Vec<i64>, Self::Error> {
            Ok(vec![0; tokens.len()])
        }

        fn rows_by_ids(&self, _ids: &[&str]) -> Result<Vec<CorpusRow>, Self::Error> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn short_anchor_weights_fall_back_to_defaults() {
        let outcome = ask(
            &ShortWeightSource,
            "commit hook",
            5,
            &test_vocab(),
            &HashMap::new(),
        )
        .unwrap();
        assert!(!outcome.matches.is_empty());
    }
}
