use std::collections::HashMap;

use crate::anchor::{RowTokens, Tier, tier_of};
use crate::text::stem;
use crate::types::CorpusRow;
use crate::vocab::SearchVocab;

pub const BM25_K1: f64 = 1.2;
pub const BM25_B: f64 = 0.75;

const CANDIDATE_FACTOR: usize = 5;

const MAX_PER_PARENT: usize = 2;
const MAX_PER_FILE: usize = 3;

const EXACT_BONUS: f64 = 1000.0;
const PREFIX_BONUS: f64 = 100.0;
const SUBSTRING_BONUS: f64 = 1.0;
const SOURCE_BONUS: f64 = 0.5;

pub struct Hit {
    pub index: usize,
    pub score: f64,
    tiered: bool,
    coverage: f64,
}

pub const CONFIDENT_COVERAGE: f64 = 0.5;

impl Hit {
    pub fn tiered(&self) -> bool {
        self.tiered
    }

    pub fn confident(&self) -> bool {
        self.tiered && self.coverage >= CONFIDENT_COVERAGE
    }
}

pub fn rank_and_trim(
    terms: &[String],
    corpus: &[CorpusRow],
    limit: usize,
    weights: Option<&[f64]>,
    vocab: &SearchVocab,
) -> Vec<Hit> {
    dedupe_by_parent(
        rank(terms, corpus, limit * CANDIDATE_FACTOR, weights, vocab),
        corpus,
        limit,
    )
}

fn rank(
    terms: &[String],
    corpus: &[CorpusRow],
    cap: usize,
    weights: Option<&[f64]>,
    vocab: &SearchVocab,
) -> Vec<Hit> {
    let joined = terms.join(" ");
    let term_stems: Vec<String> = terms.iter().map(|t| stem(t)).collect();
    let rows: Vec<RowTokens> = corpus.iter().map(RowTokens::of).collect();
    let weights: Vec<f64> = match weights {
        Some(w) if w.len() == terms.len() => terms
            .iter()
            .zip(w)
            .map(|(term, weight)| {
                if vocab.is_relational(term) {
                    weight.min(1.0)
                } else {
                    *weight
                }
            })
            .collect(),
        _ => vec![1.0; terms.len()],
    };

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
        let mut anchored = 0usize;
        for ((term, term_stem), weight) in terms.iter().zip(&term_stems).zip(&weights) {
            match row.name_tier(term, term_stem) {
                Tier::Exact => {
                    tiered += EXACT_BONUS * weight;
                    matched += 1;
                    anchored += 1;
                }
                Tier::Prefix => {
                    tiered += PREFIX_BONUS * weight;
                    matched += 1;
                    anchored += 1;
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
                coverage: anchored as f64 / terms.len().max(1) as f64,
            });
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
    hits
}

fn dedupe_by_parent(results: Vec<Hit>, corpus: &[CorpusRow], limit: usize) -> Vec<Hit> {
    let mut per_parent: HashMap<String, usize> = HashMap::new();
    let mut per_file: HashMap<String, usize> = HashMap::new();
    let mut kept: Vec<Hit> = Vec::with_capacity(limit);
    for r in results {
        if kept.len() >= limit {
            break;
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
        kept.push(r);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{row, test_vocab};

    #[test]
    fn rank_dedupe_and_parent_keys_respect_limits() {
        let corpus = vec![row("Repo::commit_hook"), row("Project::setup")];
        let hits = rank(&["commit".to_string()], &corpus, 10, None, &test_vocab());
        assert_eq!(hits.len(), 1);
        let limited = dedupe_by_parent(hits, &corpus, 0);
        assert!(limited.is_empty());

        assert_eq!(parent_key("a::B::field"), "a::B");
        assert_eq!(parent_key("pkg.Func"), "pkg");
        assert_eq!(parent_key("bare"), "bare");
    }
}
