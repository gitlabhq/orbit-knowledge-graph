use crate::types::CorpusRow;

pub const ANCHOR_SIM: f64 = 0.999;
pub const EXACT_NAME_SIM: f64 = 1.0;
pub const EXACT_NAME_BOOST: f64 = 2.0;
pub const CONFIDENT_COVERAGE: f64 = 0.5;
pub const LENGTH_NORM_B: f64 = 0.75;
pub const DEGREE_WEIGHT: f64 = 0.5;
pub const DEGREE_CAP: u64 = 200;

pub struct Hit {
    pub index: usize,
    pub score: f64,
    anchored: bool,
    coverage: f64,
}

impl Hit {
    pub fn anchored(&self) -> bool {
        self.anchored
    }

    pub fn confident(&self) -> bool {
        self.anchored && self.coverage >= CONFIDENT_COVERAGE
    }
}

pub fn rank_and_trim(
    corpus: &[CorpusRow],
    sims: &[Vec<f64>],
    idfs: &[f64],
    cap: usize,
) -> Vec<Hit> {
    let measured: Vec<f64> = corpus
        .iter()
        .filter(|r| r.grams > 0)
        .map(|r| r.grams as f64)
        .collect();
    let avgdl = if measured.is_empty() {
        1.0
    } else {
        measured.iter().sum::<f64>() / measured.len() as f64
    };
    let idf_total: f64 = idfs.iter().sum::<f64>().max(f64::MIN_POSITIVE);
    let mut hits: Vec<Hit> = Vec::new();
    for (index, row_sims) in sims.iter().enumerate() {
        let total: f64 = row_sims.iter().zip(idfs).map(|(sim, idf)| sim * idf).sum();
        if total <= 0.0 {
            continue;
        }
        let len = corpus[index].grams.max(1) as f64;
        let length_norm = 1.0 - LENGTH_NORM_B + LENGTH_NORM_B * len / avgdl;
        let matched_idf: f64 = row_sims
            .iter()
            .zip(idfs)
            .filter(|&(&s, _)| s > 0.0)
            .map(|(_, idf)| idf)
            .sum();
        let anchored_idf: f64 = row_sims
            .iter()
            .zip(idfs)
            .filter(|&(&s, _)| s >= ANCHOR_SIM)
            .map(|(_, idf)| idf)
            .sum();
        let exact_idf: f64 = row_sims
            .iter()
            .zip(idfs)
            .filter(|&(&s, _)| s >= EXACT_NAME_SIM)
            .map(|(_, idf)| idf)
            .sum();
        let coverage = matched_idf / idf_total;
        let exactness = 1.0 + EXACT_NAME_BOOST * exact_idf / idf_total;
        let degree = corpus[index].degree.min(DEGREE_CAP) as f64;
        let connectedness = 1.0 + DEGREE_WEIGHT * (1.0 + degree).ln();
        hits.push(Hit {
            index,
            score: total * coverage * coverage * exactness * connectedness / length_norm,
            anchored: anchored_idf > 0.0,
            coverage,
        });
    }
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| corpus[a.index].fqn.len().cmp(&corpus[b.index].fqn.len()))
            .then_with(|| corpus[a.index].id.cmp(&corpus[b.index].id))
    });
    hits.truncate(cap);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::row;

    #[test]
    fn flood_terms_do_not_dilute_confidence() {
        let corpus = vec![row(1, "Repo::commit_hook")];
        let sims = vec![vec![1.0, 1.0, 0.0, 0.0, 0.0, 0.0]];
        let high_idf_anchors = rank_and_trim(&corpus, &sims, &[5.0, 5.0, 0.2, 0.2, 0.2, 0.2], 10);
        assert!(
            high_idf_anchors[0].confident(),
            "anchoring the informative mass must clear the bar despite four flood terms"
        );
        let low_idf_anchors = rank_and_trim(&corpus, &sims, &[0.2, 0.2, 5.0, 5.0, 5.0, 5.0], 10);
        assert!(
            !low_idf_anchors[0].confident(),
            "anchoring only flood terms must stay weak"
        );
    }

    #[test]
    fn full_sim_outranks_fuzzy_and_coverage_squares_partial_matches() {
        let corpus = vec![
            row(1, "Repo::commit"),
            row(2, "Repo::komit"),
            row(3, "Repo::other"),
        ];
        let sims = vec![
            vec![1.0, 1.0, 1.0],
            vec![0.8, 0.8, 0.8],
            vec![1.0, 0.0, 0.0],
        ];
        let hits = rank_and_trim(&corpus, &sims, &[1.0, 1.0, 1.0], 10);
        let order: Vec<&str> = hits.iter().map(|h| corpus[h.index].fqn.as_str()).collect();
        assert_eq!(order, vec!["Repo::commit", "Repo::komit", "Repo::other"]);
        assert!(hits[0].confident());
        assert!(!hits[1].anchored());
        assert!(
            !hits[2].confident(),
            "one anchored term of three must stay below the confidence floor"
        );
        assert!(hits[0].score > 4.0 * hits[2].score);
    }

    #[test]
    fn zero_sim_rows_are_dropped_and_ties_prefer_shorter_fqns() {
        let corpus = vec![
            row(4, "Repo::commit_hook"),
            row(5, "Repo::commit"),
            row(6, "X::y"),
        ];
        let sims = vec![vec![1.0], vec![1.0], vec![0.0]];
        let hits = rank_and_trim(&corpus, &sims, &[1.0], 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(corpus[hits[0].index].fqn, "Repo::commit");
    }

    #[test]
    fn ranking_keeps_siblings_up_to_the_result_limit() {
        for count in [4, 12] {
            let corpus: Vec<_> = (0..count)
                .map(|i| row(i, &format!("Type::member{i}")))
                .collect();
            let sims = vec![vec![1.0]; corpus.len()];
            for limit in [0, 10] {
                assert_eq!(
                    rank_and_trim(&corpus, &sims, &[1.0], limit).len(),
                    corpus.len().min(limit)
                );
            }
        }
    }

    #[test]
    fn connected_definitions_outrank_leaf_values_on_equal_name_matches() {
        let mut leaf = row(1, "resources.limits");
        leaf.degree = 2;
        let mut hub = row(2, "Compiler::check_depth_limit");
        hub.degree = 20;
        let corpus = vec![leaf, hub];
        let sims = vec![vec![1.0], vec![1.0]];
        let hits = rank_and_trim(&corpus, &sims, &[1.0], 10);
        assert_eq!(corpus[hits[0].index].fqn, "Compiler::check_depth_limit");
        assert!(
            hits[0].score < 2.0 * hits[1].score,
            "degree is a tiebreaker, not a dominant signal"
        );
    }

    #[test]
    fn exact_name_outranks_stem_hit_despite_degree() {
        let mut exact = row(1, "compiler::compile");
        exact.degree = 2;
        let mut stem = row(2, "code_graph::STRUCTURAL_LABELS");
        stem.degree = 1_000_000;
        let corpus = vec![stem, exact];
        let sims = vec![vec![ANCHOR_SIM], vec![EXACT_NAME_SIM]];
        let hits = rank_and_trim(&corpus, &sims, &[1.0], 10);
        assert_eq!(corpus[hits[0].index].fqn, "compiler::compile");
        assert!(hits[0].anchored() && hits[1].anchored());
    }

    #[test]
    fn degree_does_not_override_coverage() {
        let mut hub = row(1, "Repo::other");
        hub.degree = 200;
        let corpus = vec![hub, row(2, "Repo::commit_hook")];
        let sims = vec![vec![1.0, 0.0], vec![1.0, 1.0]];
        let hits = rank_and_trim(&corpus, &sims, &[1.0, 1.0], 10);
        assert_eq!(corpus[hits[0].index].fqn, "Repo::commit_hook");
    }

    #[test]
    fn idf_weights_rare_terms_above_flood_terms() {
        let corpus = vec![
            row(9, "Ci::AutoCancel"),
            row(10, "Project::Repository::List"),
        ];
        let sims = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let hits = rank_and_trim(&corpus, &sims, &[9.0, 1.1], 10);
        assert_eq!(corpus[hits[0].index].fqn, "Ci::AutoCancel");
        assert!(hits[0].score > 5.0 * hits[1].score);
    }
}
