use std::collections::HashSet;

use crate::text::{split_words, stem};
use crate::types::CorpusRow;
use crate::vocab::SearchVocab;

pub const BASE_SET_PER_TERM: usize = 10;
pub const MAX_SEEDS: usize = 50;

pub fn term_base_sets(
    terms: &[String],
    corpus: &[CorpusRow],
    weights: Option<&[f64]>,
    vocab: &SearchVocab,
) -> Vec<Vec<(usize, f64)>> {
    let rows: Vec<RowTokens> = corpus.iter().map(RowTokens::of).collect();
    let mut sets: Vec<Vec<(usize, f64)>> = Vec::new();
    let mut seeded: HashSet<usize> = HashSet::new();
    for (t, term) in terms.iter().enumerate() {
        if vocab.is_relational(term) {
            continue;
        }
        let idf = weights.and_then(|w| w.get(t)).copied().unwrap_or(1.0);
        let term_stem = stem(term);
        let mut matched: Vec<(usize, f64)> = rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                let tier_factor = match row.name_tier(term, &term_stem) {
                    Tier::Exact => 1.0,
                    Tier::Prefix => 0.7,
                    Tier::Inner => 0.2,
                    Tier::None => match tier_of(term, &row.path) {
                        Tier::Exact | Tier::Prefix => 0.3,
                        _ => return None,
                    },
                };
                Some((i, idf * tier_factor))
            })
            .collect();
        matched.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        matched.truncate(BASE_SET_PER_TERM);
        if !matched.is_empty() {
            seeded.extend(matched.iter().map(|&(i, _)| i));
            sets.push(matched);
            if seeded.len() >= MAX_SEEDS {
                break;
            }
        }
    }
    sets
}

pub fn unmatched_terms(terms: &[String], corpus: &[CorpusRow], vocab: &SearchVocab) -> Vec<String> {
    terms
        .iter()
        .filter(|term| !vocab.is_relational(term))
        .filter(|term| {
            let term_stem = stem(term);
            !corpus
                .iter()
                .any(|row| RowTokens::of(row).matches(term, &term_stem))
        })
        .cloned()
        .collect()
}

pub(crate) enum Tier {
    Exact,
    Prefix,
    Inner,
    None,
}

pub(crate) fn tier_of(term: &str, tokens: &[String]) -> Tier {
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

pub(crate) struct RowTokens {
    pub(crate) name: Vec<String>,
    name_stems: Vec<String>,
    pub(crate) path: Vec<String>,
}

impl RowTokens {
    pub(crate) fn of(row: &CorpusRow) -> Self {
        let name = split_words(&row.fqn);
        let name_stems = name.iter().map(|t| stem(t)).collect();
        Self {
            name,
            name_stems,
            path: split_words(&row.loc),
        }
    }

    pub(crate) fn name_tier(&self, term: &str, term_stem: &str) -> Tier {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{row, test_vocab};

    #[test]
    fn base_sets_skip_relational_terms_and_cap_per_term() {
        let corpus = vec![row("Repo::commit_hook"), row("Project::setup")];
        let terms = vec![
            "commit".to_string(),
            "setup".to_string(),
            "uses".to_string(),
        ];
        assert_eq!(
            term_base_sets(&terms, &corpus, None, &test_vocab()).len(),
            2
        );
        let big: Vec<CorpusRow> = (0..40).map(|i| row(&format!("m{i}::commit"))).collect();
        let capped = term_base_sets(&["commit".to_string()], &big, None, &test_vocab());
        assert_eq!(capped[0].len(), BASE_SET_PER_TERM);
    }

    #[test]
    fn unmatched_terms_ignore_relational_and_report_misses() {
        let corpus = vec![row("Repo::commit_hook"), row("Project::setup")];
        let terms = vec![
            "commit".to_string(),
            "setup".to_string(),
            "uses".to_string(),
        ];
        assert_eq!(
            unmatched_terms(&terms, &corpus, &test_vocab()),
            Vec::<String>::new()
        );
        assert_eq!(
            unmatched_terms(
                &["zzzz".to_string(), "uses".to_string()],
                &corpus,
                &test_vocab()
            ),
            vec!["zzzz".to_string()]
        );
    }
}
