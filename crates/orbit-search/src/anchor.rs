use crate::grep::TermRecall;
use crate::types::TermSeeds;

pub const BASE_SET_PER_TERM: usize = 10;
pub const MIN_SEEDS_PER_TERM: usize = 3;
pub const MAX_SEEDS: usize = 50;

pub const SEED_DF_CEILING: f64 = 0.25;

fn seeds_consensus(recall: &TermRecall) -> bool {
    !recall.hits.is_empty() && (recall.matched as f64) < recall.corpus as f64 * SEED_DF_CEILING
}

pub fn term_base_sets(recalls: &[TermRecall]) -> Vec<TermSeeds> {
    let searchable = recalls.iter().filter(|r| seeds_consensus(r)).count().max(1);
    let per_term = (MAX_SEEDS / searchable).clamp(MIN_SEEDS_PER_TERM, BASE_SET_PER_TERM);
    recalls
        .iter()
        .filter(|r| seeds_consensus(r))
        .map(|recall| {
            let mut set = recall.hits.clone();
            set.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            set.truncate(per_term);
            TermSeeds {
                seeds: set,
                weight: recall.idf(),
            }
        })
        .collect()
}

pub fn unmatched_terms(terms: &[String], recalls: &[TermRecall]) -> Vec<String> {
    terms
        .iter()
        .zip(recalls)
        .filter(|(_, recall)| recall.hits.is_empty())
        .map(|(term, _)| term.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recall(ids: &[(i64, f64)]) -> TermRecall {
        TermRecall {
            hits: ids.to_vec(),
            matched: ids.len() as u64,
            corpus: 1000,
        }
    }

    #[test]
    fn base_sets_sort_by_sim_and_cap_per_term() {
        let big = TermRecall {
            hits: (0..40).map(|i| (i, 1.0 / (i + 1) as f64)).collect(),
            matched: 40,
            corpus: 1000,
        };
        let sets = term_base_sets(&[big, recall(&[(100, 0.9)])]);
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].seeds.len(), BASE_SET_PER_TERM);
        assert_eq!(sets[0].seeds[0].0, 0);
    }

    #[test]
    fn generic_terms_do_not_seed_the_consensus() {
        let generic = TermRecall {
            hits: vec![(1, 1.0)],
            matched: 400,
            corpus: 1000,
        };
        let rare = recall(&[(2, 0.8)]);
        let sets = term_base_sets(&[generic, rare]);
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].seeds[0].0, 2);
    }

    #[test]
    fn shrinks_per_term_budget_instead_of_dropping_terms() {
        let recalls: Vec<TermRecall> = (0..8)
            .map(|t| TermRecall {
                hits: (0..12).map(|i| (t * 100 + i, 0.5)).collect(),
                matched: 12,
                corpus: 1000,
            })
            .collect();
        let sets = term_base_sets(&recalls);
        assert_eq!(sets.len(), 8);
        assert!(sets.iter().all(|s| s.seeds.len() == MAX_SEEDS / 8));
    }

    #[test]
    fn unmatched_terms_report_empty_recalls() {
        let terms = vec!["commit".to_string(), "zzzz".to_string()];
        let recalls = vec![recall(&[(1, 1.0)]), recall(&[])];
        assert_eq!(unmatched_terms(&terms, &recalls), vec!["zzzz".to_string()]);
    }

    #[test]
    fn idf_orders_rare_above_flood_terms() {
        let rare = TermRecall {
            hits: Vec::new(),
            matched: 3,
            corpus: 50_000,
        };
        let flood = TermRecall {
            hits: Vec::new(),
            matched: 25_000,
            corpus: 50_000,
        };
        assert!(rare.idf() > 5.0 * flood.idf());
    }
}
